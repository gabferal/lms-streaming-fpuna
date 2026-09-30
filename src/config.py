"""Configuration constants — all overridable via environment variables."""
from __future__ import annotations

import os

# ── Kafka ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC: str = os.getenv("INPUT_TOPIC", "lms-events")
OUTPUT_TOPIC: str = os.getenv("OUTPUT_TOPIC", "lms-metrics")
DLQ_TOPIC: str = os.getenv("DLQ_TOPIC", "lms-dlq")
CONSUMER_GROUP_ID: str = os.getenv("CONSUMER_GROUP_ID", "beam-lms-pipeline-v1")

# ── Pipeline ───────────────────────────────────────────────────────────────
WINDOW_SECONDS: int = int(os.getenv("WINDOW_SECONDS", "300"))         # 5 minutes
ALLOWED_LATENESS_SECONDS: int = int(os.getenv("ALLOWED_LATENESS_SECONDS", "120"))
EARLY_TRIGGER_SECONDS: int = int(os.getenv("EARLY_TRIGGER_SECONDS", "30"))

# ── Producer ───────────────────────────────────────────────────────────────
REPLAY_SPEED: float = float(os.getenv("REPLAY_SPEED", "1.0"))
DATA_FILE: str = os.getenv("DATA_FILE", "data/events.jsonl")

# ── Sink ───────────────────────────────────────────────────────────────────
OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "output")

# ── Event schema ───────────────────────────────────────────────────────────
SCHEMA_VERSION: str = "1.0"
VALID_EVENT_TYPES: frozenset[str] = frozenset({
    "QUIZ_SUBMIT", "VIDEO_COMPLETE", "PAGE_VIEW", "LOGIN", "LOGOUT",
})
ENGAGEMENT_EVENT_TYPES: frozenset[str] = frozenset({
    "QUIZ_SUBMIT", "VIDEO_COMPLETE", "PAGE_VIEW",
})
REQUIRED_FIELDS: frozenset[str] = frozenset({
    "event_id", "student_id", "course_id",
    "event_type", "event_time", "arrival_time",
})
