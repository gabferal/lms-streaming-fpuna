"""
Core Apache Beam transforms for the LMS streaming pipeline.

This module contains NO Kafka IO — it is intentionally decoupled from the
transport layer so that every transform can be unit-tested with
beam.Create() / TestPipeline / TestStream without a running Kafka cluster.

Pipeline shape:
  (key_bytes, value_bytes)
      → ValidateAndParseFn  (main: dict, dlq: dict)
      → AssignEventTimeFn   (TimestampedValue)
      → WindowInto(FixedWindows(300s))
      → KeyByStudentFn
      → DeduplicateFn       (stateful, per student per window)
      → FilterEngagementFn
      → CombinePerKey(CountAndScoreCombineFn)
      → FormatMetricFn      (window-aware)
      → IdempotentFileSinkFn
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterator

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms import trigger as tr
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer
from apache_beam.utils.timestamp import Duration

from src.config import (
    ALLOWED_LATENESS_SECONDS,
    EARLY_TRIGGER_SECONDS,
    ENGAGEMENT_EVENT_TYPES,
    OUTPUT_DIR,
    WINDOW_SECONDS,
)
from src.events import parse_utc, validate_event

logger = logging.getLogger(__name__)

# ── Beam 2.74 compatibility ─────────────────────────────────────────────────
# Duration.seconds property was removed; restore it via monkey-patch.
if not hasattr(Duration, "seconds"):
    Duration.seconds = property(lambda self: self.micros / 1_000_000)


# ─── Helpers ────────────────────────────────────────────────────────────────

def _ts_to_iso(beam_ts: Any) -> str:
    """Convert a Beam timestamp (float seconds since epoch) to ISO-8601 UTC."""
    return datetime.fromtimestamp(float(beam_ts), tz=timezone.utc).isoformat()


# ─── Validation ────────────────────────────────────────────────────────────

class ValidateAndParseFn(beam.DoFn):
    """Parse raw Kafka (key_bytes, value_bytes) tuples.

    Outputs:
        main  → validated event dict
        "dlq" → {"raw": str, "reason": str}
    """

    DLQ = "dlq"

    def process(self, element: tuple, *args, **kwargs) -> Iterator:
        _, raw = element  # discard Kafka key; use message value
        event, error = validate_event(raw)
        if error:
            yield beam.pvalue.TaggedOutput(
                self.DLQ,
                {"raw": raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw),
                 "reason": error},
            )
        else:
            yield event.to_dict()


# ─── Timestamp assignment ───────────────────────────────────────────────────

class AssignEventTimeFn(beam.DoFn):
    """Stamp each validated event dict with its domain event_time.

    This is the critical step that switches processing from ingestion
    (arrival) time to event time, enabling correct window assignment.
    """

    def process(self, element: dict, *args, **kwargs) -> Iterator:
        from apache_beam.transforms.window import TimestampedValue
        et = parse_utc(element["event_time"])
        yield TimestampedValue(element, et.timestamp())


# ─── Keying ─────────────────────────────────────────────────────────────────

class KeyByStudentFn(beam.DoFn):
    """Key each event dict by student_id for per-student operations."""

    def process(self, element: dict, *args, **kwargs) -> Iterator:
        yield element["student_id"], element


# ─── Stateful deduplication ─────────────────────────────────────────────────

class DeduplicateFn(beam.DoFn):
    """Drop events whose event_id has already been seen for the same student
    within the current window.

    State: SetStateSpec tracking seen event_ids (strings).
    Timer:  Watermark timer at window.end to clear the state after the window
            closes, releasing memory and preventing cross-window leakage.
    """

    SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
    EXPIRY   = TimerSpec("expiry", TimeDomain.WATERMARK)

    def process(
        self,
        element: tuple,
        seen_ids=beam.DoFn.StateParam(SEEN_IDS),
        window=beam.DoFn.WindowParam,
        expiry=beam.DoFn.TimerParam(EXPIRY),
    ) -> Iterator:
        student_id, event = element
        event_id: str = event["event_id"]

        existing = set(seen_ids.read())
        if event_id not in existing:
            seen_ids.add(event_id)
            expiry.set(window.end)   # schedule cleanup at window close
            yield element            # forward unique event
        else:
            logger.debug("Duplicate dropped: event_id=%s student=%s", event_id, student_id)

    @on_timer(EXPIRY)
    def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)) -> None:
        """Clear the seen-IDs set after the window closes."""
        seen_ids.clear()


# ─── Aggregation combiner ────────────────────────────────────────────────────

class CountAndScoreCombineFn(beam.CombineFn):
    """Accumulate (event_count, total_score) for a student within a window.

    The combine function runs over the *values* of a (student_id, event_dict)
    KV pair after CombinePerKey is applied.
    """

    def create_accumulator(self) -> tuple[int, int]:
        return (0, 0)

    def add_input(self, accumulator: tuple[int, int], element: dict) -> tuple[int, int]:
        count, total = accumulator
        score = int(element.get("payload", {}).get("score", 0) or 0)
        return count + 1, total + score

    def merge_accumulators(self, accumulators) -> tuple[int, int]:
        return (sum(a[0] for a in accumulators), sum(a[1] for a in accumulators))

    def extract_output(self, accumulator: tuple[int, int]) -> tuple[int, int]:
        return accumulator


# ─── Output formatting ────────────────────────────────────────────────────────

class FormatMetricFn(beam.DoFn):
    """Convert (student_id, (count, score)) + window into a metrics dict."""

    def process(
        self,
        element: tuple,
        window=beam.DoFn.WindowParam,
    ) -> Iterator:
        student_id, (count, total_score) = element
        avg_score = round(total_score / count, 2) if count > 0 else 0.0
        yield {
            "student_id":   student_id,
            "window_start": _ts_to_iso(window.start),
            "window_end":   _ts_to_iso(window.end),
            "event_count":  count,
            "total_score":  total_score,
            "avg_score":    avg_score,
        }


# ─── Idempotent file sink ────────────────────────────────────────────────────

class IdempotentFileSinkFn(beam.DoFn):
    """Write metrics to a newline-delimited JSON file using UPSERT semantics.

    Idempotency key: ``student_id|window_start``

    On each write the in-memory dict is flushed to disk, so retried outputs
    for the same key simply overwrite the previous record — the final state
    of the file is always the same regardless of how many times an element
    is delivered (at-least-once → effectively exactly-once for the sink).
    """

    def __init__(self, output_dir: str = OUTPUT_DIR) -> None:
        self._output_dir = output_dir
        self._store: dict[str, dict] = {}
        self._path: str = ""

    def setup(self) -> None:
        os.makedirs(self._output_dir, exist_ok=True)
        self._path = os.path.join(self._output_dir, "metrics.jsonl")
        # Resume from existing file to survive restarts
        if os.path.exists(self._path):
            with open(self._path) as fh:
                for line in fh:
                    try:
                        row = json.loads(line)
                        key = f"{row['student_id']}|{row['window_start']}"
                        self._store[key] = row
                    except Exception:
                        pass

    def process(self, element: dict, *args, **kwargs) -> Iterator:
        key = f"{element['student_id']}|{element['window_start']}"
        is_new = key not in self._store
        self._store[key] = element
        # Atomic rewrite — UPSERT semantics
        tmp = self._path + ".tmp"
        with open(tmp, "w") as fh:
            for row in self._store.values():
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        os.replace(tmp, self._path)
        action = "INSERT" if is_new else "UPSERT"
        logger.info("[sink] %s key=%s count=%d score=%d",
                    action, key, element["event_count"], element["total_score"])
        yield element


# ─── Window policy factory ────────────────────────────────────────────────────

def make_window_policy(
    window_seconds: int = WINDOW_SECONDS,
    allowed_lateness_seconds: int = ALLOWED_LATENESS_SECONDS,
    early_trigger_seconds: int = EARLY_TRIGGER_SECONDS,
) -> beam.WindowInto:
    """Return a WindowInto transform with the project's trigger policy.

    Trigger:
        Early  – (production only) emit a speculative result every
                 ``early_trigger_seconds`` of processing time, so dashboards
                 see data before the window closes.  When
                 ``early_trigger_seconds <= 0`` the early trigger is omitted
                 (required for TestPipeline / PrismRunner, which have no
                 real-time clock).
        Late   – emit one refinement for each late element that arrives within
                 the allowed lateness window.
    Accumulation: ACCUMULATING so that each pane carries the full window total.
    """
    if early_trigger_seconds > 0:
        trigger = tr.AfterWatermark(
            early=tr.AfterProcessingTime(early_trigger_seconds),
            late=tr.AfterCount(1),
        )
    else:
        # No processing-time early trigger — safe to use in tests.
        trigger = tr.AfterWatermark(late=tr.AfterCount(1))

    return beam.WindowInto(
        beam.window.FixedWindows(window_seconds),
        trigger=trigger,
        accumulation_mode=tr.AccumulationMode.ACCUMULATING,
        allowed_lateness=Duration(seconds=allowed_lateness_seconds),
    )


# ─── Core processing PTransform ──────────────────────────────────────────────

class LmsProcessingPipeline(beam.PTransform):
    """End-to-end processing logic, transport-agnostic.

    Input:  PCollection of (key_bytes, value_bytes) tuples, one per Kafka msg.
    Output: PCollection of metric dicts ready for the sink.

    The DLQ side output can be retrieved via the ``.dlq`` attribute after
    ``expand()`` is called.  Example::

        result = pcoll | "Process" >> LmsProcessingPipeline()
        dlq    = result.dlq   # PCollection[dict]
        metrics = result      # PCollection[dict]
    """

    def __init__(
        self,
        window_seconds: int = WINDOW_SECONDS,
        allowed_lateness_seconds: int = ALLOWED_LATENESS_SECONDS,
        output_dir: str = OUTPUT_DIR,
        early_trigger_seconds: int = EARLY_TRIGGER_SECONDS,
    ) -> None:
        super().__init__()
        self._window_seconds = window_seconds
        self._allowed_lateness = allowed_lateness_seconds
        self._output_dir = output_dir
        self._early_trigger_seconds = early_trigger_seconds

    def expand(self, pcoll):
        # ── 1. Validate ────────────────────────────────────────────────────
        parsed = pcoll | "Validate" >> beam.ParDo(
            ValidateAndParseFn()
        ).with_outputs(ValidateAndParseFn.DLQ, main="valid")

        valid = parsed.valid
        self.dlq = parsed.dlq  # expose for caller

        # ── 2. Assign event-time ───────────────────────────────────────────
        timestamped = valid | "AssignTimestamp" >> beam.ParDo(AssignEventTimeFn())

        # ── 3. Window ──────────────────────────────────────────────────────
        windowed = timestamped | "Window" >> make_window_policy(
            window_seconds=self._window_seconds,
            allowed_lateness_seconds=self._allowed_lateness,
            early_trigger_seconds=self._early_trigger_seconds,
        )

        # ── 4. Key by student ──────────────────────────────────────────────
        keyed = windowed | "KeyByStudent" >> beam.ParDo(KeyByStudentFn())

        # ── 5. Dedup ───────────────────────────────────────────────────────
        deduped = keyed | "Dedup" >> beam.ParDo(DeduplicateFn())

        # ── 6. Filter engagement events ────────────────────────────────────
        engagement = deduped | "FilterEngagement" >> beam.Filter(
            lambda kv: kv[1]["event_type"] in ENGAGEMENT_EVENT_TYPES
        )

        # ── 7. Aggregate ───────────────────────────────────────────────────
        aggregated = engagement | "Aggregate" >> beam.CombinePerKey(
            CountAndScoreCombineFn()
        )

        # ── 8. Format metrics ──────────────────────────────────────────────
        metrics = aggregated | "FormatMetrics" >> beam.ParDo(FormatMetricFn())

        # ── 9. File sink (idempotent) ──────────────────────────────────────
        _ = metrics | "WriteToFile" >> beam.ParDo(
            IdempotentFileSinkFn(self._output_dir)
        )

        return metrics
