"""
LMS Streaming pipeline: Kafka → Beam → Kafka + file sink.

Architecture note
-----------------
Apache Beam's cross-language ReadFromKafka (Java KafkaIO) is designed for
production runners (Flink, Dataflow).  In the Python DirectRunner streaming
mode the Java→Python bridge stalls: elements are read by the Java SDF but
never advance through the downstream Python transforms.

This version uses *Python confluent_kafka* for both read and write, while
keeping the entire Beam processing graph (windowing, stateful dedup,
combiners, idempotent sink) unchanged.  The DirectRunner runs in *batch
mode* over each micro-poll batch:

  1. read_kafka_batch() pulls all available messages with confluent_kafka
  2. beam.Create(messages) feeds them into a bounded PCollection
  3. DirectRunner processes the batch, advancing watermark to +inf at end
  4. All FixedWindows close and AfterWatermark trigger fires -> metrics
  5. IdempotentFileSinkFn writes / upserts output/metrics.jsonl
  6. KafkaSinkFn (Python) produces results to lms-metrics / lms-dlq
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Iterator

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

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


class KafkaSinkFn(beam.DoFn):
    """Produce each element to a Kafka topic using Python confluent_kafka."""

    def __init__(self, bootstrap_servers: str, topic: str, key_field: str | None = None) -> None:
        self._bootstrap = bootstrap_servers
        self._topic = topic
        self._key_field = key_field
        self._producer = None

    def setup(self) -> None:
        from confluent_kafka import Producer
        self._producer = Producer({"bootstrap.servers": self._bootstrap})

    def process(self, element: dict, *args, **kwargs) -> Iterator:
        key = (element.get(self._key_field, "") or "").encode("utf-8") if self._key_field else None
        value = json.dumps(element, ensure_ascii=False).encode("utf-8")
        self._producer.produce(self._topic, key=key, value=value)
        self._producer.poll(0)
        yield element

    def teardown(self) -> None:
        if self._producer:
            self._producer.flush(timeout=10)


def read_kafka_batch(
    bootstrap: str,
    topic: str,
    group_id: str,
    *,
    idle_polls: int = 5,
    poll_timeout: float = 2.0,
) -> list[tuple[bytes, bytes]]:
    """Read all currently available messages. Returns (key_bytes, value_bytes) list."""
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id":          group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    messages: list[tuple[bytes, bytes]] = []
    empty = 0

    while empty < idle_polls:
        msg = consumer.poll(timeout=poll_timeout)
        if msg is None:
            empty += 1
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty += 1
            else:
                logger.error("Kafka consumer error: %s", msg.error())
                break
        else:
            empty = 0
            messages.append((msg.key() or b"", msg.value() or b""))

    consumer.close()
    return messages


def run_pipeline(messages: list[tuple[bytes, bytes]]) -> None:
    """Run one Beam batch pass over messages and write results."""
    logger.info("Running Beam pipeline over %d messages ...", len(messages))

    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        raw = p | "CreateMessages" >> beam.Create(messages)

        processing = LmsProcessingPipeline(
            window_seconds=WINDOW_SECONDS,
            allowed_lateness_seconds=ALLOWED_LATENESS_SECONDS,
            output_dir=OUTPUT_DIR,
            early_trigger_seconds=0,
        )
        metrics = raw | "Process" >> processing
        dlq     = processing.dlq

        _ = metrics | "WriteMetrics" >> beam.ParDo(
            KafkaSinkFn(KAFKA_BOOTSTRAP_SERVERS, OUTPUT_TOPIC, key_field="student_id")
        )
        _ = dlq | "WriteDLQ" >> beam.ParDo(
            KafkaSinkFn(KAFKA_BOOTSTRAP_SERVERS, DLQ_TOPIC)
        )

    logger.info("Pipeline pass complete. output -> %s/metrics.jsonl  kafka -> %s",
                OUTPUT_DIR, OUTPUT_TOPIC)


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("LMS pipeline starting — Kafka: %s  in=%s  out=%s",
                KAFKA_BOOTSTRAP_SERVERS, INPUT_TOPIC, OUTPUT_TOPIC)
    logger.info("Polling for messages every 10 s. Run the producer to send events.")

    last_count = -1

    while True:
        messages = read_kafka_batch(
            KAFKA_BOOTSTRAP_SERVERS,
            INPUT_TOPIC,
            CONSUMER_GROUP_ID,
            idle_polls=3,
            poll_timeout=2.0,
        )
        count = len(messages)

        if count == 0:
            logger.info("No messages in topic yet — waiting 10 s ...")
        elif count != last_count:
            logger.info("Found %d messages (prev=%d). Starting pipeline pass.",
                        count, last_count)
            run_pipeline(messages)
            last_count = count
            logger.info("Done. Sleeping 30 s before next poll.")
        else:
            logger.info("Message count unchanged (%d). Sleeping 30 s.", count)

        time.sleep(10)


if __name__ == "__main__":
    main()
