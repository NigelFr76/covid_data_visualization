"""Integration tests against PostgreSQL using the offline fixture."""

import os

import pytest

from analytics.metrics import regional_increase_signals
from ingestion.cleaning import clean_records, iter_csv_dicts
from ingestion.db import connect, upsert_aggregates, upsert_cleaned, upsert_raw, upsert_trends
from ingestion.download_data import FIXTURE_PATH

pytestmark = pytest.mark.integration


def _db_or_skip():
    try:
        conn = connect()
        conn.close()
    except Exception as exc:
        pytest.skip(f"PostgreSQL not available: {exc}")


def test_fixture_ingest_upsert_and_aggregates():
    _db_or_skip()
    cleaned, quarantined = clean_records(iter_csv_dicts(FIXTURE_PATH))
    assert any(row["revision_flag"] for row in cleaned)
    assert len(quarantined) >= 2

    from analytics.metrics import attach_rolling_and_cfr

    aggregates = attach_rolling_and_cfr(cleaned)
    usa = [r for r in aggregates if r["iso_code"] == "USA"]
    usa.sort(key=lambda r: r["date"])
    assert usa[-1]["rolling_7d_cases"] is not None

    signals = regional_increase_signals(aggregates, threshold_pct=10, min_obs=7)
    usa_signals = [s for s in signals if s["iso_code"] == "USA"]
    assert usa_signals
    assert usa_signals[0]["is_increasing"] is True

    conn = connect()
    try:
        upsert_raw(conn, cleaned)
        upsert_cleaned(conn, cleaned)
        upsert_aggregates(conn, aggregates)
        upsert_trends(conn, signals)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT SUM(new_cases) FROM regional_aggregates
                WHERE iso_code='USA' AND source_type='historical'
                  AND date BETWEEN '2020-03-01' AND '2020-03-14'
                """
            )
            total = cur.fetchone()[0]
            cur.execute(
                """
                SELECT COUNT(*) FROM regional_aggregates
                WHERE iso_code='USA' AND date='2020-03-14' AND source_type='historical'
                """
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT COUNT(*) FROM cleaned_covid_data WHERE iso_code='USA' AND source_type='historical'"
            )
            assert cur.fetchone()[0] >= 14
        assert total is not None
    finally:
        conn.close()


def test_upsert_is_idempotent():
    _db_or_skip()
    cleaned, _ = clean_records(iter_csv_dicts(FIXTURE_PATH))
    conn = connect()
    try:
        upsert_raw(conn, cleaned)
        upsert_raw(conn, cleaned)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM raw_covid_data
                WHERE iso_code='USA' AND date='2020-03-01' AND source_type='historical'
                """
            )
            assert cur.fetchone()[0] == 1
    finally:
        conn.close()
