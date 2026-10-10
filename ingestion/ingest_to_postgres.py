"""
Historical ingestion without Spark (laptop-friendly path).

  python -m ingestion.ingest_to_postgres
  python -m ingestion.ingest_to_postgres --fixture
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from analytics.metrics import attach_rolling_and_cfr, regional_increase_signals
from config.settings import get_settings
from ingestion.cleaning import clean_records, iter_csv_dicts
from ingestion.db import (
    connect,
    insert_quarantine,
    log_pipeline,
    upsert_aggregates,
    upsert_cleaned,
    upsert_raw,
    upsert_status,
    upsert_trends,
)
from ingestion.download_data import FILE_PATH, FIXTURE_PATH, resolve_input_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def run(path: str) -> dict:
    settings = get_settings()
    log.info("Ingesting %s into %s", path, settings.db_url)
    cleaned, quarantined = clean_records(iter_csv_dicts(path))
    aggregates = attach_rolling_and_cfr(cleaned)
    signals = regional_increase_signals(
        aggregates,
        threshold_pct=settings.trend_threshold_pct,
        min_obs=settings.trend_min_obs,
    )
    latest = max((r["date"] for r in cleaned), default=None)

    conn = connect()
    try:
        upsert_raw(conn, cleaned)
        upsert_cleaned(conn, cleaned)
        upsert_aggregates(conn, aggregates)
        upsert_trends(conn, signals)
        if quarantined:
            insert_quarantine(conn, quarantined)
        log_pipeline(conn, "ingest_to_postgres", len(cleaned), latest, "success")
        upsert_status(
            conn,
            "ingest_to_postgres",
            "success",
            records_in=len(cleaned) + len(quarantined),
            records_out=len(cleaned),
            records_quarantined=len(quarantined),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log_pipeline(conn, "ingest_to_postgres", len(cleaned), latest, f"error: {exc}")
        conn.commit()
        raise
    finally:
        conn.close()

    log.info(
        "Ingestion complete: cleaned=%s quarantined=%s signals=%s latest=%s",
        len(cleaned),
        len(quarantined),
        len(signals),
        latest,
    )
    return {
        "cleaned": len(cleaned),
        "quarantined": len(quarantined),
        "signals": len(signals),
        "latest": str(latest) if latest else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", action="store_true", help="Use the offline CSV fixture")
    parser.add_argument("--input", default=None, help="Path to a CSV file")
    args = parser.parse_args()
    if args.input:
        path = args.input
    elif args.fixture:
        path = FIXTURE_PATH
    else:
        path = resolve_input_path(prefer_download=False)
        if not os.path.exists(path):
            path = FILE_PATH if os.path.exists(FILE_PATH) else FIXTURE_PATH
    run(path)


if __name__ == "__main__":
    main()
