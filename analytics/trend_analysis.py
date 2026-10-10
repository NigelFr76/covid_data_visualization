"""Regional increase signals from PostgreSQL aggregates."""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from analytics.metrics import regional_increase_signals
from config.settings import get_settings
from ingestion.db import connect, upsert_status, upsert_trends
from psycopg2.extras import RealDictCursor


def load_aggregates(conn, source_type: str = "historical") -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT iso_code, location, date, new_cases AS daily_cases, source_type
            FROM regional_aggregates
            WHERE source_type = %s
            ORDER BY iso_code, date
            """,
            (source_type,),
        )
        return [dict(r) for r in cur.fetchall()]


def run(source_type: str = "historical") -> list[dict]:
    settings = get_settings()
    conn = connect()
    try:
        rows = load_aggregates(conn, source_type)
        signals = regional_increase_signals(
            rows,
            threshold_pct=settings.trend_threshold_pct,
            min_obs=settings.trend_min_obs,
        )
        upsert_trends(conn, signals)
        upsert_status(conn, "regional_signals", "success", records_out=len(signals))
        conn.commit()
        return signals
    finally:
        conn.close()


if __name__ == "__main__":
    signals = run()
    print(f"Computed {len(signals)} regional increase signals.")
    increasing = [s for s in signals if s["is_increasing"]]
    print(f"Flagged as increasing: {len(increasing)}")
    for row in increasing[:10]:
        print(f"  {row['location']}: {row['percentage_change']}%  {row['explanation'][:80]}...")
