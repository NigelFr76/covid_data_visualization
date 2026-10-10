"""Print row counts used to verify batch and streaming writes."""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from ingestion.db import connect


QUERIES = [
    ("raw_covid_data", "SELECT source_type, COUNT(*) FROM raw_covid_data GROUP BY 1 ORDER BY 1"),
    ("cleaned_covid_data", "SELECT source_type, COUNT(*) FROM cleaned_covid_data GROUP BY 1 ORDER BY 1"),
    ("regional_aggregates", "SELECT source_type, COUNT(*), MAX(date) FROM regional_aggregates GROUP BY 1 ORDER BY 1"),
    ("regional_trend_signals", "SELECT source_type, COUNT(*), SUM(CASE WHEN is_increasing THEN 1 ELSE 0 END) AS increasing FROM regional_trend_signals GROUP BY 1"),
    ("pipeline_status", "SELECT process_name, status, records_out, records_quarantined FROM pipeline_status"),
    ("quarantined_records", "SELECT COUNT(*) FROM quarantined_records"),
]


def main() -> None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            for title, sql in QUERIES:
                print(f"\n== {title} ==")
                cur.execute(sql)
                for row in cur.fetchall():
                    print(row)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
