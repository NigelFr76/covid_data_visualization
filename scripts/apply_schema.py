"""Apply sql/init.sql then sql/migrate.sql to the running database."""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from ingestion.db import connect


def _run_file(cur, path: str) -> None:
    with open(path, encoding="utf-8") as handle:
        sql = handle.read()
    cur.execute(sql)


def main() -> None:
    conn = connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _run_file(cur, os.path.join(ROOT, "sql", "init.sql"))
            _run_file(cur, os.path.join(ROOT, "sql", "migrate.sql"))
        print("Schema applied.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
