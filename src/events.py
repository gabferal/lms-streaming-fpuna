"""
Event schema, parsing and validation for the LMS streaming pipeline.

Contract (schema_version 1.0):
  event_id      – UUID-like string; globally unique per occurrence
  student_id    – learner identifier, used as Kafka key and grouping key
  course_id     – course identifier
  event_type    – one of QUIZ_SUBMIT | VIDEO_COMPLETE | PAGE_VIEW | LOGIN | LOGOUT
  event_time    – ISO-8601 UTC timestamp of when the event occurred on the client
  arrival_time  – ISO-8601 UTC timestamp of when the event reached the ingestion layer
  payload       – domain-specific dictionary (score, resource_id, duration_seconds…)
  schema_version – (optional) "1.0"; used for forward compatibility
"""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

from src.config import REQUIRED_FIELDS, SCHEMA_VERSION, VALID_EVENT_TYPES


# ── Timestamp helpers ───────────────────────────────────────────────────────

def parse_utc(raw: str) -> datetime:
    """Parse an ISO-8601 timestamp string and return a UTC-aware datetime.

    Raises ValueError for missing, non-string, or unparseable values.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"Timestamp must be a non-empty string, got: {raw!r}")
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Cannot parse ISO-8601 timestamp {raw!r}: {exc}") from exc
    if dt.tzinfo is None:
        raise ValueError(f"Timestamp must be timezone-aware: {raw!r}")
    return dt.astimezone(timezone.utc)


def to_iso(dt: datetime) -> str:
    """Format a datetime as 'YYYY-MM-DDTHH:MM:SSZ'."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Domain model ────────────────────────────────────────────────────────────

@dataclass
class LmsEvent:
    """Validated, strongly-typed LMS domain event."""

    event_id: str
    student_id: str
    course_id: str
    event_type: str
    event_time: str           # ISO-8601 string (kept as-is for serialisation)
    arrival_time: str
    payload: dict[str, Any] = field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION

    # ── convenience ────────────────────────────────────────────────────────

    def event_time_dt(self) -> datetime:
        return parse_utc(self.event_time)

    def arrival_time_dt(self) -> datetime:
        return parse_utc(self.arrival_time)

    def delay_seconds(self) -> float:
        """Seconds between event occurrence and ingestion arrival."""
        return (self.arrival_time_dt() - self.event_time_dt()).total_seconds()

    def score(self) -> int:
        """Quiz score from payload, 0 if absent."""
        return int(self.payload.get("score", 0) or 0)

    # ── serialisation ───────────────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LmsEvent":
        return cls(
            event_id=data["event_id"],
            student_id=data["student_id"],
            course_id=data["course_id"],
            event_type=data["event_type"],
            event_time=data["event_time"],
            arrival_time=data["arrival_time"],
            payload=data.get("payload", {}),
            schema_version=data.get("schema_version", SCHEMA_VERSION),
        )


# ── Validation ──────────────────────────────────────────────────────────────

def validate_event(raw: bytes | str) -> Tuple[Optional[LmsEvent], Optional[str]]:
    """Parse and validate a raw Kafka message value.

    Returns:
        (LmsEvent, None)       on success.
        (None,  error_reason)  on failure; the reason is routed to the DLQ.
    """
    # 1. JSON decode
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data: dict = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return None, f"parse_error:{exc}"

    if not isinstance(data, dict):
        return None, "not_a_json_object"

    # 2. Required fields
    missing = REQUIRED_FIELDS - data.keys()
    if missing:
        return None, f"missing_fields:{sorted(missing)}"

    # 3. Event type
    if data["event_type"] not in VALID_EVENT_TYPES:
        return None, f"invalid_event_type:{data['event_type']}"

    # 4. Timestamp parseability
    for field_name in ("event_time", "arrival_time"):
        try:
            parse_utc(data[field_name])
        except ValueError as exc:
            return None, f"invalid_{field_name}:{exc}"

    # 5. Construct domain object
    try:
        event = LmsEvent.from_dict(data)
    except (KeyError, TypeError, ValueError) as exc:
        return None, f"schema_error:{exc}"

    return event, None


# ── Factory for tests and producer ──────────────────────────────────────────

def make_event(
    student_id: str,
    course_id: str,
    event_type: str,
    event_time: datetime,
    payload: Optional[dict] = None,
    arrival_delay_seconds: float = 1.0,
    event_id: Optional[str] = None,
) -> LmsEvent:
    """Construct a new LmsEvent with auto-generated event_id if not supplied."""
    arrival = event_time + timedelta(seconds=arrival_delay_seconds)
    return LmsEvent(
        schema_version=SCHEMA_VERSION,
        event_id=event_id or f"evt-{uuid.uuid4().hex[:12]}",
        student_id=student_id,
        course_id=course_id,
        event_type=event_type,
        event_time=to_iso(event_time),
        arrival_time=to_iso(arrival),
        payload=payload or {},
    )
