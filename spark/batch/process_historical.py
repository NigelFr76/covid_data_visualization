"""
PySpark batch job: clean OWID CSV, write Parquet, upsert PostgreSQL.

  python -m spark.batch.process_historical
  python -m spark.batch.process_historical --fixture
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as spark_count, max as spark_max, row_number
from pyspark.sql.window import Window

from analytics.metrics import percentage_change
from config.settings import get_settings
from ingestion.db import (
    connect,
    log_pipeline,
    upsert_aggregates,
    upsert_cleaned,
    upsert_raw,
    upsert_status,
    upsert_trends,
)
from ingestion.download_data import FILE_PATH, FIXTURE_PATH
from spark.batch.transforms import clean_and_enrich, load_owid_csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PARQUET_DIR = os.path.join(ROOT, "data", "parquet")
CHUNK = 3000


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("CovidBatchProcessing")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _chunks(df, size: int = CHUNK):
    buf = []
    for row in df.toLocalIterator():
        buf.append(row.asDict(recursive=True))
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def _annotate(row: dict) -> dict:
    row["notes"] = (
        "Cumulative restatement; reported daily values preserved, excluded from rolling averages."
        if row.get("revision_flag")
        else None
    )
    return row


def _signal_from_latest(row: dict, threshold: float, min_obs: int) -> dict | None:
    recent = row.get("recent_7d_avg")
    previous = row.get("previous_7d_avg")
    if recent is None or previous is None:
        return None
    pct = percentage_change(recent, previous)
    increasing = pct is not None and pct > threshold
    if pct is None:
        why = (
            f"Insufficient previous-period baseline (previous 7-day average is {previous:.2f}); "
            "percentage change is undefined."
        )
    elif increasing:
        why = (
            f"Latest 7-day average of daily reported cases ({recent:.2f}) is {pct:.1f}% higher "
            f"than the preceding 7-day average ({previous:.2f}), above the {threshold:.0f}% "
            "configuration threshold. This is a statistical increase signal, not a medical diagnosis."
        )
    else:
        why = (
            f"Latest 7-day average ({recent:.2f}) vs preceding ({previous:.2f}) changed by "
            f"{pct:.1f}%, which is not above {threshold:.0f}%."
        )
    return {
        "iso_code": row["iso_code"],
        "location": row.get("location"),
        "date": row.get("date"),
        "recent_7d_avg": recent,
        "previous_7d_avg": previous,
        "percentage_change": pct,
        "is_increasing": increasing,
        "observation_count": None,
        "threshold_pct": threshold,
        "explanation": why,
        "source_type": row.get("source_type", "historical"),
    }


def process_batch(spark: SparkSession, input_path: str, source_type: str = "historical"):
    logger.info("Reading %s", input_path)
    df = clean_and_enrich(load_owid_csv(spark, input_path), source_type=source_type)
    df = df.cache()

    cleaned_path = os.path.join(PARQUET_DIR, "cleaned")
    agg_path = os.path.join(PARQUET_DIR, "aggregates")
    os.makedirs(PARQUET_DIR, exist_ok=True)
    df.write.mode("overwrite").parquet(cleaned_path)

    stats = df.agg(spark_count("*").alias("n"), spark_max("date").alias("latest")).collect()[0]
    latest = stats["latest"]
    logger.info("Spark transforms complete: rows=%s latest=%s", stats["n"], latest)

    df.select(
        "iso_code",
        "continent",
        "location",
        "date",
        "daily_cases",
        "daily_deaths",
        "rolling_7d_cases",
        "rolling_7d_deaths",
        "case_fatality_ratio",
        "source_type",
    ).write.mode("overwrite").parquet(agg_path)

    settings = get_settings()
    latest_w = Window.partitionBy("iso_code", "source_type").orderBy(col("date").desc())
    latest_rows = [
        r.asDict(recursive=True)
        for r in df.withColumn("_rn", row_number().over(latest_w)).filter(col("_rn") == 1).collect()
    ]
    signals = []
    for row in latest_rows:
        sig = _signal_from_latest(row, settings.trend_threshold_pct, settings.trend_min_obs)
        if sig is not None:
            signals.append(sig)

    conn = connect()
    written = 0
    try:
        for chunk in _chunks(df):
            chunk = [_annotate(r) for r in chunk]
            upsert_raw(conn, chunk)
            upsert_cleaned(conn, chunk)
            upsert_aggregates(conn, chunk)
            written += len(chunk)
            conn.commit()
            logger.info("Upserted %s / %s rows", written, stats["n"])
        upsert_trends(conn, signals)
        log_pipeline(conn, "spark_batch", int(stats["n"]), latest, "success")
        upsert_status(conn, "spark_batch", "success", records_in=int(stats["n"]), records_out=written)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        df.unpersist()

    logger.info("Batch processing complete. Parquet: %s", PARQUET_DIR)
    return {"rows": int(stats["n"]), "latest": str(latest) if latest else None, "signals": len(signals)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", action="store_true")
    parser.add_argument("--input", default=None)
    args = parser.parse_args()
    path = args.input or (FIXTURE_PATH if args.fixture else FILE_PATH)
    if not os.path.exists(path):
        path = FIXTURE_PATH
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        process_batch(spark, path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
