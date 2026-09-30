"""
Unit tests for src/events.py — pure Python, no Beam, no Kafka.

Covers:
  - parse_utc: valid timestamps, edge cases, error paths
  - validate_event: schema validation pipeline
  - make_event: factory helper
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from src.events import LmsEvent, make_event, parse_utc, validate_event


# ── parse_utc ────────────────────────────────────────────────────────────────

class TestParseUtc:
    def test_z_suffix(self):
        dt = parse_utc("2026-09-30T13:00:05Z")
        assert dt.tzinfo is not None
        assert dt.tzinfo == timezone.utc
        assert dt.year == 2026 and dt.month == 9 and dt.day == 30
        assert dt.hour == 13 and dt.minute == 0 and dt.second == 5

    def test_offset_plus00(self):
        dt = parse_utc("2026-09-30T13:00:05+00:00")
        assert dt == parse_utc("2026-09-30T13:00:05Z")

    def test_non_utc_converted(self):
        # UTC-03:00 (Paraguay time) → 16:00 UTC
        dt = parse_utc("2026-09-30T13:00:00-03:00")
        assert dt.hour == 16
        assert dt.tzinfo == timezone.utc

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            parse_utc("")

    def test_none_raises(self):
        with pytest.raises((ValueError, AttributeError)):
            parse_utc(None)  # type: ignore

    def test_not_a_string_raises(self):
        with pytest.raises((ValueError, AttributeError)):
            parse_utc(1234567890)  # type: ignore

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            parse_utc("30/09/2026 13:00:05")

    def test_missing_timezone_raises(self):
        with pytest.raises(ValueError):
            parse_utc("2026-09-30T13:00:05")  # no tz info


# ── validate_event ───────────────────────────────────────────────────────────

_GOOD = {
    "schema_version": "1.0",
    "event_id":    "evt-001",
    "student_id":  "s-alice",
    "course_id":   "course-ml",
    "event_type":  "QUIZ_SUBMIT",
    "event_time":  "2026-09-30T13:00:05Z",
    "arrival_time":"2026-09-30T13:00:06Z",
    "payload":     {"score": 85},
}


def _raw(d: dict) -> bytes:
    return json.dumps(d).encode()


class TestValidateEvent:
    def test_valid_event_accepted(self):
        event, err = validate_event(_raw(_GOOD))
        assert err is None
        assert isinstance(event, LmsEvent)
        assert event.student_id == "s-alice"
        assert event.event_id   == "evt-001"

    def test_valid_event_score_accessible(self):
        event, _ = validate_event(_raw(_GOOD))
        assert event.score() == 85

    def test_missing_student_id_rejected(self):
        bad = {k: v for k, v in _GOOD.items() if k != "student_id"}
        _, err = validate_event(_raw(bad))
        assert err is not None
        assert "missing_fields" in err
        assert "student_id" in err

    def test_missing_event_id_rejected(self):
        bad = {k: v for k, v in _GOOD.items() if k != "event_id"}
        _, err = validate_event(_raw(bad))
        assert err is not None

    def test_invalid_event_type_rejected(self):
        bad = {**_GOOD, "event_type": "WATCH_VIDEO"}  # not in VALID_EVENT_TYPES
        _, err = validate_event(_raw(bad))
        assert err is not None
        assert "invalid_event_type" in err

    def test_all_valid_event_types_accepted(self):
        for et in ("QUIZ_SUBMIT", "VIDEO_COMPLETE", "PAGE_VIEW", "LOGIN", "LOGOUT"):
            e, err = validate_event(_raw({**_GOOD, "event_type": et}))
            assert err is None, f"Unexpected rejection for event_type={et}: {err}"

    def test_invalid_event_time_rejected(self):
        bad = {**_GOOD, "event_time": "not-a-timestamp"}
        _, err = validate_event(_raw(bad))
        assert err is not None
        assert "invalid_event_time" in err

    def test_invalid_arrival_time_rejected(self):
        bad = {**_GOOD, "arrival_time": "2026/09/30"}
        _, err = validate_event(_raw(bad))
        assert err is not None
        assert "invalid_arrival_time" in err

    def test_non_json_bytes_rejected(self):
        _, err = validate_event(b"this is not json {{{")
        assert err is not None
        assert "parse_error" in err

    def test_json_array_rejected(self):
        _, err = validate_event(b"[1, 2, 3]")
        assert err is not None
        assert "not_a_json_object" in err

    def test_str_input_accepted(self):
        event, err = validate_event(json.dumps(_GOOD))
        assert err is None
        assert isinstance(event, LmsEvent)

    def test_optional_payload_missing(self):
        no_payload = {k: v for k, v in _GOOD.items() if k != "payload"}
        event, err = validate_event(_raw(no_payload))
        assert err is None
        assert event.payload == {}

    def test_delay_seconds_calculated_correctly(self):
        event, _ = validate_event(_raw(_GOOD))
        # event_time=13:00:05, arrival_time=13:00:06 → 1 second
        assert event.delay_seconds() == pytest.approx(1.0)


# ── make_event factory ────────────────────────────────────────────────────────

class TestMakeEvent:
    def test_make_event_fields(self):
        dt = datetime(2026, 9, 30, 13, 0, 0, tzinfo=timezone.utc)
        ev = make_event("s-bob", "course-db", "VIDEO_COMPLETE", dt, payload={"score": 0})
        assert ev.student_id == "s-bob"
        assert ev.course_id  == "course-db"
        assert ev.event_type == "VIDEO_COMPLETE"
        assert ev.score()    == 0

    def test_make_event_delay(self):
        dt = datetime(2026, 9, 30, 13, 0, 0, tzinfo=timezone.utc)
        ev = make_event("s-alice", "c1", "PAGE_VIEW", dt, arrival_delay_seconds=30.0)
        assert ev.delay_seconds() == pytest.approx(30.0)

    def test_make_event_explicit_id(self):
        dt = datetime(2026, 9, 30, 13, 0, 0, tzinfo=timezone.utc)
        ev = make_event("s-x", "c1", "LOGIN", dt, event_id="fixed-id")
        assert ev.event_id == "fixed-id"

    def test_make_event_auto_id_unique(self):
        dt = datetime(2026, 9, 30, 13, 0, 0, tzinfo=timezone.utc)
        ev1 = make_event("s-x", "c1", "LOGIN", dt)
        ev2 = make_event("s-x", "c1", "LOGIN", dt)
        assert ev1.event_id != ev2.event_id

    def test_make_event_serialisable(self):
        dt = datetime(2026, 9, 30, 13, 0, 0, tzinfo=timezone.utc)
        ev = make_event("s-alice", "c1", "QUIZ_SUBMIT", dt, payload={"score": 90})
        raw = ev.to_json()
        restored, err = validate_event(raw)
        assert err is None
        assert restored.event_id == ev.event_id
        assert restored.score()  == 90
