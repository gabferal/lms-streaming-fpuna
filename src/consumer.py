"""
Output consumer: reads lms-metrics and lms-dlq topics and pretty-prints results.

Intended as a demo/observation tool, not part of the data path.

Usage:
    uv run python -m src.consumer
    uv run python -m src.consumer --topic lms-dlq --timeout 30
"""
from __future__ import annotations

import argparse
import json
import signal
import sys

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.config import (
    DLQ_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    OUTPUT_TOPIC,
)

_RUNNING = True


def _sigint(sig, frame):
    global _RUNNING
    print("\n[consumer] Interrupted — shutting down.")
    _RUNNING = False


signal.signal(signal.SIGINT, _sigint)


def pretty_metric(data: dict) -> str:
    return (
        f"  🎓 {data.get('student_id','?'):12s} "
        f"window [{data.get('window_start','?')[11:16]} – {data.get('window_end','?')[11:16]}]  "
        f"events={data.get('event_count',0):3d}  "
        f"score={data.get('total_score',0):4d}  "
        f"avg={data.get('avg_score',0):5.1f}"
    )


def consume(bootstrap: str, topic: str, group_id: str, timeout: float) -> None:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id":          group_id,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    print(f"[consumer] Subscribed to {topic} (Ctrl-C to stop)\n")

    empty_polls = 0
    max_empty   = int(timeout)

    while _RUNNING:
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            if empty_polls >= max_empty:
                print(f"[consumer] No messages for {timeout}s — stopping.")
                break
            continue
        empty_polls = 0

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(msg.error())

        try:
            data = json.loads(msg.value().decode("utf-8"))
            if topic == OUTPUT_TOPIC:
                print(pretty_metric(data))
            else:
                print(f"  ⚠  DLQ  reason={data.get('reason','?')}  raw={data.get('raw','')[:80]}")
        except Exception as exc:
            print(f"  [!] Could not parse message: {exc}  raw={msg.value()}")

    consumer.close()
    print("[consumer] Closed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="LMS metrics consumer")
    parser.add_argument("--topic",   default=OUTPUT_TOPIC, help="Topic to consume")
    parser.add_argument("--timeout", type=float, default=60.0, help="Idle timeout in seconds")
    parser.add_argument("--group",   default="lms-console-consumer", help="Consumer group ID")
    args = parser.parse_args()

    consume(KAFKA_BOOTSTRAP_SERVERS, args.topic, args.group, args.timeout)


if __name__ == "__main__":
    main()
