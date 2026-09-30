"""
Full streaming pipeline: Kafka → Beam → Kafka + file sink.

Runner: Apache Beam DirectRunner (streaming mode).
Kafka IO: cross-language ReadFromKafka / WriteToKafka via the Java expansion
          service, auto-started by the Python SDK when Java is available.

Usage (Docker):
    uv run python -m src.pipeline

Usage (local dev with Kafka running):
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run python -m src.pipeline
"""
from __future__ import annotations

import json
import logging
import os

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from src.config import (
    CONSUMER_GROUP_ID,
    DLQ_TOPIC,
    INPUT_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    OUTPUT_DIR,
    OUTPUT_TOPIC,
    WINDOW_SECONDS,
    ALLOWED_LATENESS_SECONDS,
)
from src.transforms import LmsProcessingPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def build_pipeline(pipeline: beam.Pipeline) -> None:
    """Wire the full Kafka-connected pipeline into *pipeline*."""

    kafka_consumer_config = {
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           CONSUMER_GROUP_ID,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": "true",
    }
    kafka_producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }

    # ── 1. Read from Kafka (cross-language KafkaIO) ────────────────────────
    # ReadFromKafka returns (key_bytes, value_bytes) tuples.
    # The Python SDK auto-starts the Java expansion service when Java is
    # available on the PATH (see Dockerfile: default-jre-headless).
    raw = pipeline | "ReadFromKafka" >> ReadFromKafka(
        consumer_config=kafka_consumer_config,
        topics=[INPUT_TOPIC],
        # expansion_service auto-discovered; set explicitly if needed:
        # expansion_service=("localhost", 8097),
    )

    # ── 2. Core processing (validate → window → dedup → aggregate) ─────────
    processing = LmsProcessingPipeline(
        window_seconds=WINDOW_SECONDS,
        allowed_lateness_seconds=ALLOWED_LATENESS_SECONDS,
        output_dir=OUTPUT_DIR,
    )
    metrics = raw | "Process" >> processing
    dlq     = processing.dlq   # side output from validation

    # ── 3. Write metrics to Kafka output topic ─────────────────────────────
    _ = metrics | "SerializeMetrics" >> beam.Map(
        lambda m: (m["student_id"].encode("utf-8"), json.dumps(m).encode("utf-8"))
    ) | "WriteMetrics" >> WriteToKafka(
        producer_config=kafka_producer_config,
        topic=OUTPUT_TOPIC,
    )

    # ── 4. Write DLQ events to dead-letter topic ───────────────────────────
    _ = dlq | "SerializeDLQ" >> beam.Map(
        lambda e: (None, json.dumps(e).encode("utf-8"))
    ) | "WriteDLQ" >> WriteToKafka(
        producer_config=kafka_producer_config,
        topic=DLQ_TOPIC,
    )

    logger.info(
        "Pipeline wired: %s → Beam → %s / DLQ:%s",
        INPUT_TOPIC, OUTPUT_TOPIC, DLQ_TOPIC,
    )


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    logger.info("Starting LMS streaming pipeline (DirectRunner, streaming=True)")
    logger.info("Kafka: %s  topic-in=%s  topic-out=%s", KAFKA_BOOTSTRAP_SERVERS, INPUT_TOPIC, OUTPUT_TOPIC)

    with beam.Pipeline(options=options) as p:
        build_pipeline(p)


if __name__ == "__main__":
    main()
