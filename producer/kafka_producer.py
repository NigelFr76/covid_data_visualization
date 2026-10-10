"""
Publish COVID records to Kafka topic covid-raw-records.

  python -m producer.kafka_producer --mode historical --limit 200 --speed 0.01
  python -m producer.kafka_producer --mode synthetic --limit 20 --speed 0.2
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from kafka import KafkaProducer

from config.settings import get_settings
from ingestion.cleaning import iter_csv_dicts
from ingestion.download_data import FILE_PATH, FIXTURE_PATH
from producer.messages import build_record, serialize_record

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STOP_FLAG = os.path.join(ROOT, "data", "checkpoints", "producer.stop")


def get_producer(broker: str | None = None) -> KafkaProducer:
    settings = get_settings()
    return KafkaProducer(
        bootstrap_servers=[broker or settings.kafka_broker],
        value_serializer=lambda v: serialize_record(v) if isinstance(v, dict) else v,
        linger_ms=20,
        retries=3,
    )


def clear_stop_flag() -> None:
    if os.path.exists(STOP_FLAG):
        os.remove(STOP_FLAG)


def request_stop() -> None:
    os.makedirs(os.path.dirname(STOP_FLAG), exist_ok=True)
    with open(STOP_FLAG, "w", encoding="utf-8") as handle:
        handle.write("stop")


def _should_stop() -> bool:
    return os.path.exists(STOP_FLAG)


def replay_historical(producer, speed: float = 0.05, limit: int | None = None, path: str | None = None) -> int:
    settings = get_settings()
    csv_path = path or (FILE_PATH if os.path.exists(FILE_PATH) else FIXTURE_PATH)
    logger.info("Historical replay from %s topic=%s", csv_path, settings.kafka_topic)
    count = 0
    clear_stop_flag()
    for row in iter_csv_dicts(csv_path):
        if _should_stop() or (limit is not None and count >= limit):
            break
        try:
            record = build_record(row, source_type="historical_replay")
        except ValueError:
            continue
        producer.send(settings.kafka_topic, record)
        count += 1
        if count % 100 == 0:
            logger.info("Sent %s historical_replay records", count)
            producer.flush()
        if speed > 0:
            time.sleep(speed)
    producer.flush()
    logger.info("Finished historical replay: %s messages", count)
    return count


def synthetic_mode(
    producer,
    interval: float = 0.2,
    limit: int | None = 50,
    seed: int = 42,
) -> int:
    settings = get_settings()
    rng = random.Random(seed)
    logger.info("Synthetic demonstration mode (seed=%s). Records are labelled source_type=synthetic.", seed)
    count = 0
    base = datetime(2021, 6, 1)
    clear_stop_flag()
    while True:
        if _should_stop() or (limit is not None and count >= limit):
            break
        increment = rng.randint(10, 80)
        record = {
            "iso_code": "SYN",
            "continent": "Demo",
            "location": "Synthetic Region",
            "date": (base + timedelta(days=count)).strftime("%Y-%m-%d"),
            "total_cases": float(1000 + increment * count),
            "new_cases": float(increment),
            "total_deaths": float(10 + count),
            "new_deaths": 1.0,
            "source_type": "synthetic",
            "ingestion_timestamp": datetime.utcnow().isoformat(),
        }
        producer.send(settings.kafka_topic, record)
        count += 1
        logger.info("Sent synthetic record %s", count)
        if interval > 0:
            time.sleep(interval)
    producer.flush()
    logger.info("Finished synthetic demonstration: %s messages", count)
    return count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["historical", "synthetic"], default="historical")
    parser.add_argument("--speed", type=float, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--input", default=None)
    args = parser.parse_args()
    settings = get_settings()
    producer = get_producer()
    try:
        if args.mode == "historical":
            replay_historical(
                producer,
                speed=args.speed if args.speed is not None else settings.replay_speed,
                limit=args.limit if args.limit is not None else settings.replay_limit,
                path=args.input,
            )
        else:
            synthetic_mode(
                producer,
                interval=args.speed if args.speed is not None else 0.2,
                limit=args.limit if args.limit is not None else 20,
                seed=args.seed,
            )
    finally:
        producer.close()


if __name__ == "__main__":
    main()
