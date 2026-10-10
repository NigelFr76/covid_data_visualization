"""Remove replay, synthetic, and E2E demo rows. Keeps historical OWID loads."""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from ingestion.db import connect


def main() -> None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            for table in (
                "raw_covid_data",
                "cleaned_covid_data",
                "regional_aggregates",
                "regional_trend_signals",
            ):
                cur.execute(
                    f"DELETE FROM {table} WHERE source_type IN ('historical_replay', 'synthetic') OR iso_code IN ('E2E', 'SYN')"
                )
            cur.execute("DELETE FROM quarantined_records")
            cur.execute("DELETE FROM pipeline_metrics WHERE process_name LIKE %s", ("spark_streaming%",))
        conn.commit()
        print("Demo replay/synthetic rows removed. Historical rows kept.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
