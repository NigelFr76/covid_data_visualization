"""Parameterized, transactional upserts shared by ingestion and Spark sinks."""

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Optional

import psycopg2
from psycopg2.extras import Json, execute_values

from config.settings import get_settings


def connect(db_url: Optional[str] = None):
    return psycopg2.connect(db_url or get_settings().db_url)


def upsert_raw(conn, rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO raw_covid_data (
        iso_code, continent, location, date, total_cases, new_cases,
        total_deaths, new_deaths, original_new_cases, original_new_deaths,
        daily_cases_derived, daily_deaths_derived, revision_flag,
        source_type, ingestion_timestamp, notes
    ) VALUES %s
    ON CONFLICT (iso_code, date, source_type) DO UPDATE SET
        continent = EXCLUDED.continent,
        location = EXCLUDED.location,
        total_cases = EXCLUDED.total_cases,
        new_cases = EXCLUDED.new_cases,
        total_deaths = EXCLUDED.total_deaths,
        new_deaths = EXCLUDED.new_deaths,
        original_new_cases = EXCLUDED.original_new_cases,
        original_new_deaths = EXCLUDED.original_new_deaths,
        daily_cases_derived = EXCLUDED.daily_cases_derived,
        daily_deaths_derived = EXCLUDED.daily_deaths_derived,
        revision_flag = EXCLUDED.revision_flag,
        ingestion_timestamp = EXCLUDED.ingestion_timestamp,
        notes = EXCLUDED.notes
    """
    now = datetime.utcnow()
    vals = [
        (
            r["iso_code"],
            r.get("continent"),
            r.get("location"),
            r["date"],
            r.get("total_cases"),
            r.get("original_new_cases", r.get("new_cases")),
            r.get("total_deaths"),
            r.get("original_new_deaths", r.get("new_deaths")),
            r.get("original_new_cases", r.get("new_cases")),
            r.get("original_new_deaths", r.get("new_deaths")),
            r.get("daily_cases_derived"),
            r.get("daily_deaths_derived"),
            r.get("revision_flag", False),
            r.get("source_type", "historical"),
            now,
            r.get("notes"),
        )
        for r in rows
    ]
    if not vals:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, sql, vals, page_size=2000)
    return len(vals)


def upsert_cleaned(conn, rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO cleaned_covid_data (
        iso_code, continent, location, date, total_cases, total_deaths,
        reported_new_cases, reported_new_deaths, daily_cases, daily_deaths,
        revision_flag, source_type, processed_at
    ) VALUES %s
    ON CONFLICT (iso_code, date, source_type) DO UPDATE SET
        continent = EXCLUDED.continent,
        location = EXCLUDED.location,
        total_cases = EXCLUDED.total_cases,
        total_deaths = EXCLUDED.total_deaths,
        reported_new_cases = EXCLUDED.reported_new_cases,
        reported_new_deaths = EXCLUDED.reported_new_deaths,
        daily_cases = EXCLUDED.daily_cases,
        daily_deaths = EXCLUDED.daily_deaths,
        revision_flag = EXCLUDED.revision_flag,
        processed_at = EXCLUDED.processed_at
    """
    now = datetime.utcnow()
    vals = [
        (
            r["iso_code"],
            r.get("continent"),
            r.get("location"),
            r["date"],
            r.get("total_cases"),
            r.get("total_deaths"),
            r.get("original_new_cases", r.get("reported_new_cases")),
            r.get("original_new_deaths", r.get("reported_new_deaths")),
            r.get("daily_cases"),
            r.get("daily_deaths"),
            r.get("revision_flag", False),
            r.get("source_type", "historical"),
            now,
        )
        for r in rows
    ]
    if not vals:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, sql, vals, page_size=2000)
    return len(vals)


def upsert_aggregates(conn, rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO regional_aggregates (
        iso_code, continent, location, date, total_cases, total_deaths,
        new_cases, new_deaths, rolling_7d_cases, rolling_7d_deaths,
        case_fatality_ratio, source_type
    ) VALUES %s
    ON CONFLICT (iso_code, date, source_type) DO UPDATE SET
        continent = EXCLUDED.continent,
        location = EXCLUDED.location,
        total_cases = EXCLUDED.total_cases,
        total_deaths = EXCLUDED.total_deaths,
        new_cases = EXCLUDED.new_cases,
        new_deaths = EXCLUDED.new_deaths,
        rolling_7d_cases = EXCLUDED.rolling_7d_cases,
        rolling_7d_deaths = EXCLUDED.rolling_7d_deaths,
        case_fatality_ratio = EXCLUDED.case_fatality_ratio
    """
    vals = [
        (
            r["iso_code"],
            r.get("continent"),
            r.get("location"),
            r["date"],
            r.get("total_cases"),
            r.get("total_deaths"),
            r.get("daily_cases", r.get("new_cases")),
            r.get("daily_deaths", r.get("new_deaths")),
            r.get("rolling_7d_cases"),
            r.get("rolling_7d_deaths"),
            r.get("case_fatality_ratio"),
            r.get("source_type", "historical"),
        )
        for r in rows
    ]
    if not vals:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, sql, vals, page_size=2000)
    return len(vals)


def upsert_trends(conn, rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO regional_trend_signals (
        iso_code, location, date, recent_7d_avg, previous_7d_avg,
        percentage_change, is_increasing, observation_count, threshold_pct,
        explanation, source_type
    ) VALUES %s
    ON CONFLICT (iso_code, date, source_type) DO UPDATE SET
        location = EXCLUDED.location,
        recent_7d_avg = EXCLUDED.recent_7d_avg,
        previous_7d_avg = EXCLUDED.previous_7d_avg,
        percentage_change = EXCLUDED.percentage_change,
        is_increasing = EXCLUDED.is_increasing,
        observation_count = EXCLUDED.observation_count,
        threshold_pct = EXCLUDED.threshold_pct,
        explanation = EXCLUDED.explanation
    """
    vals = [
        (
            r["iso_code"],
            r.get("location"),
            r["date"],
            r.get("recent_7d_avg"),
            r.get("previous_7d_avg"),
            r.get("percentage_change"),
            r.get("is_increasing"),
            r.get("observation_count"),
            r.get("threshold_pct"),
            r.get("explanation"),
            r.get("source_type", "historical"),
        )
        for r in rows
    ]
    if not vals:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, sql, vals)
    return len(vals)


def insert_quarantine(conn, rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO quarantined_records (raw_data, error_reason, source_type)
    VALUES %s
    """
    vals = [
        (
            Json(r.get("raw_data") or r),
            r.get("error_reason"),
            (r.get("raw_data") or r).get("source_type") if isinstance(r.get("raw_data"), dict) else r.get("source_type"),
        )
        for r in rows
    ]
    if not vals:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, sql, vals)
    return len(vals)


def log_pipeline(conn, name: str, count: int, latest: Optional[date], status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_metrics (process_name, records_processed, latest_date, status)
            VALUES (%s, %s, %s, %s)
            """,
            (name, count, latest, status),
        )


def upsert_status(
    conn,
    process_name: str,
    status: str,
    records_in: int = 0,
    records_out: int = 0,
    records_quarantined: int = 0,
    last_error: Optional[str] = None,
    extra: Optional[dict] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_status (
                process_name, status, records_in, records_out,
                records_quarantined, last_error, extra, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (process_name) DO UPDATE SET
                status = EXCLUDED.status,
                records_in = pipeline_status.records_in + EXCLUDED.records_in,
                records_out = pipeline_status.records_out + EXCLUDED.records_out,
                records_quarantined = pipeline_status.records_quarantined + EXCLUDED.records_quarantined,
                last_error = EXCLUDED.last_error,
                extra = EXCLUDED.extra,
                updated_at = NOW()
            """,
            (process_name, status, records_in, records_out, records_quarantined, last_error, Json(extra) if extra else None),
        )
