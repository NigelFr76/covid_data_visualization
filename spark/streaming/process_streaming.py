"""
Spark Structured Streaming consumer for topic covid-raw-records.

At-least-once delivery: checkpoints track Kafka offsets; PostgreSQL upserts
are idempotent. This is not exactly-once.

  python -m spark.streaming.process_streaming
"""

from __future__ import annotations

import logging
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from pyspark.sql import SparkSession

from analytics.metrics import attach_rolling_and_cfr
from config.settings import get_settings
from ingestion.db import (
    connect,
    insert_quarantine,
    log_pipeline,
    upsert_aggregates,
    upsert_cleaned,
    upsert_raw,
    upsert_status,
)
from spark.streaming.transforms import parse_kafka, split_valid_invalid

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CHECKPOINT = os.environ.get(
    "CHECKPOINT_LOCATION", os.path.join(ROOT, "data", "checkpoints", "streaming")
)


def get_spark_session() -> SparkSession:
    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    return (
        SparkSession.builder.appName("CovidStreamingProcessing")
        .config("spark.jars.packages", packages)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def process_microbatch(df, epoch_id: int) -> None:
    if df.rdd.isEmpty():
        return
    valid, invalid = split_valid_invalid(parse_kafka(df))
    invalid_rows = [
        {"raw_data": {"json": row["json"] if "json" in row.asDict() else row.asDict()}, "error_reason": "schema or field validation failed"}
        for row in invalid.collect()
    ]
    valid_rows = [row.asDict(recursive=True) for row in valid.collect()]
    for row in valid_rows:
        row.pop("json", None)
        row.pop("timestamp", None)
        row["notes"] = "streaming micro-batch"

    aggregates = attach_rolling_and_cfr(valid_rows) if valid_rows else []

    conn = connect()
    try:
        if valid_rows:
            upsert_raw(conn, valid_rows)
            upsert_cleaned(conn, valid_rows)
            upsert_aggregates(conn, aggregates)
        if invalid_rows:
            insert_quarantine(conn, invalid_rows)
        latest = max((r["date"] for r in valid_rows), default=None)
        log_pipeline(conn, "spark_streaming", len(valid_rows), latest, "success")
        upsert_status(
            conn,
            "spark_streaming",
            "running",
            records_in=len(valid_rows) + len(invalid_rows),
            records_out=len(valid_rows),
            records_quarantined=len(invalid_rows),
            extra={"epoch_id": epoch_id},
        )
        conn.commit()
        logger.info(
            "epoch=%s valid=%s quarantined=%s",
            epoch_id,
            len(valid_rows),
            len(invalid_rows),
        )
    except Exception as exc:
        conn.rollback()
        logger.exception("epoch %s failed: %s", epoch_id, exc)
        try:
            upsert_status(conn, "spark_streaming", "error", last_error=str(exc), extra={"epoch_id": epoch_id})
            conn.commit()
        except Exception:
            conn.rollback()
        raise
    finally:
        conn.close()


def start_streaming(spark: SparkSession):
    settings = get_settings()
    os.makedirs(CHECKPOINT, exist_ok=True)
    logger.info("Reading Kafka %s topic %s", settings.kafka_broker, settings.kafka_topic)
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_broker)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    query = (
        raw.writeStream.foreachBatch(process_microbatch)
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("update")
        .start()
    )
    logger.info("Streaming query started. Checkpoint=%s", CHECKPOINT)
    query.awaitTermination()


if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    start_streaming(spark)
