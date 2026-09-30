"""
Replay producer: reads events.jsonl and publishes to the lms-events Kafka topic.

Scenarios injected automatically:
  - Duplicates  : the first event is sent twice to simulate a network retry.
  - Out-of-order: the data file already contains an event whose event_time falls
                  in an earlier window but is placed late in the file.
  - Late event  : after all normal events a synthetic late event is injected
                  with a very old event_time to test the allowed-lateness logic.

Usage:
    uv run python -m src.producer                  # replay data/events.jsonl
    uv run python -m src.producer --file path.jsonl --speed 2.0 --no-inject
"""
from __future__ import annotations

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer, KafkaError

from src.config import (
    DATA_FILE,
    INPUT_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    REPLAY_SPEED,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def delivery_report(err, msg):
    if err:
        print(f"[producer] ✗ delivery failed: {err}")
    else:
        print(f"[producer] ✓ {msg.topic()}[{msg.partition()}]@{msg.offset()} "
              f"key={msg.key().decode() if msg.key() else None}")


def wait_for_kafka(bootstrap: str, retries: int = 30, delay: float = 2.0) -> None:
    """Poll until Kafka is reachable (useful in Docker Compose start-up)."""
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for attempt in range(1, retries + 1):
        try:
            md = admin.list_topics(timeout=3)
            print(f"[producer] Kafka ready ({len(md.topics)} topics)")
            return
        except Exception as exc:
            print(f"[producer] Waiting for Kafka ({attempt}/{retries}): {exc}")
            time.sleep(delay)
    raise RuntimeError(f"Kafka not available after {retries} retries")


def produce_events(
    bootstrap: str,
    topic: str,
    events: list[dict],
    *,
    replay_speed: float = 1.0,
    inject_scenarios: bool = True,
) -> None:
    """Publish events to Kafka, optionally injecting adversarial scenarios."""
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
    })

    sorted_events = sorted(events, key=lambda e: e["event_time"])
    ref_event_time: datetime | None = None
    ref_wall: float | None = None

    print(f"[producer] Sending {len(sorted_events)} events → {topic}")

    for i, event in enumerate(sorted_events):
        # Replay pacing: advance wall-clock proportionally to event-time gaps
        from src.events import parse_utc
        et = parse_utc(event["event_time"])
        if ref_event_time is None:
            ref_event_time = et
            ref_wall = time.time()
        else:
            elapsed_event = (et - ref_event_time).total_seconds()
            elapsed_wall  = time.time() - ref_wall
            sleep_sec     = (elapsed_event / replay_speed) - elapsed_wall
            if sleep_sec > 0.01:
                time.sleep(sleep_sec)

        # ── SCENARIO: duplicate (send the first event twice) ────────────────
        if inject_scenarios and i == 0:
            dup = {**event, "arrival_time": _now_iso()}
            producer.produce(
                topic,
                key=(event.get("student_id") or "unknown").encode(),
                value=json.dumps(dup).encode(),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            print(f"[producer] ⚠  DUPLICATE injected event_id={event['event_id']}")

        # ── normal send with current arrival_time ───────────────────────────
        payload = {**event, "arrival_time": _now_iso()}
        producer.produce(
            topic,
            key=(event.get("student_id") or "unknown").encode(),
            value=json.dumps(payload).encode(),
            on_delivery=delivery_report,
        )
        producer.poll(0)

    # ── SCENARIO: late event (event_time far in the past) ───────────────────
    if inject_scenarios and sorted_events:
        time.sleep(2)
        late_event = {
            **sorted_events[0],
            "event_id":    f"evt-late-{uuid.uuid4().hex[:8]}",
            # event_time stays the same (window 1), but arrival is now → big delay
            "arrival_time": _now_iso(),
            "payload":      {**sorted_events[0].get("payload", {}), "injected": "late"},
        }
        producer.produce(
            topic,
            key=(event.get("student_id") or "unknown").encode(),
            value=json.dumps(late_event).encode(),
            on_delivery=delivery_report,
        )
        producer.poll(0)
        print(f"[producer] ⚠  LATE event injected event_id={late_event['event_id']}")

    producer.flush()
    print(f"[producer] Done — {len(sorted_events)} events sent (+ scenarios if enabled)")


def main() -> None:
    parser = argparse.ArgumentParser(description="LMS event replay producer")
    parser.add_argument("--file",     default=DATA_FILE,    help="Path to events.jsonl")
    parser.add_argument("--speed",    type=float, default=REPLAY_SPEED, help="Replay speed multiplier")
    parser.add_argument("--no-inject", action="store_true",  help="Disable adversarial scenario injection")
    args = parser.parse_args()

    wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    path = Path(args.file)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    events = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    print(f"[producer] Loaded {len(events)} events from {path}")

    produce_events(
        KAFKA_BOOTSTRAP_SERVERS,
        INPUT_TOPIC,
        events,
        replay_speed=args.speed,
        inject_scenarios=not args.no_inject,
    )


if __name__ == "__main__":
    main()
