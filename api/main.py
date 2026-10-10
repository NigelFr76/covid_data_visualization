from __future__ import annotations

import logging
import os
import sys
import threading
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import get_settings
from ingestion.db import connect
from producer.kafka_producer import get_producer, replay_historical, request_stop, synthetic_mode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="COVID-19 Data Platform API", version="1.0.0")
_replay_lock = threading.Lock()
_replay_thread: Optional[threading.Thread] = None
_replay_state = {"running": False, "mode": None, "error": None}


class ReplayRequest(BaseModel):
    mode: str = Field(default="historical", pattern="^(historical|synthetic)$")
    limit: int = Field(default=200, ge=1, le=20000)
    speed: float = Field(default=0.02, ge=0, le=5)


def _db():
    try:
        return connect()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc


@app.get("/health")
def health_check():
    try:
        conn = connect()
        conn.close()
        db = "up"
    except Exception:
        db = "down"
    return {"status": "ok" if db == "up" else "degraded", "database": db, "replay": _replay_state}


@app.get("/api/summary")
def get_summary(source_type: str = Query(default="historical")):
    if source_type not in {"historical", "historical_replay", "synthetic"}:
        raise HTTPException(status_code=400, detail="invalid source_type")
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    MAX(total_cases) FILTER (WHERE location = 'World') AS world_total_cases,
                    MAX(total_deaths) FILTER (WHERE location = 'World') AS world_total_deaths,
                    SUM(new_cases) AS sum_daily_cases,
                    SUM(new_deaths) AS sum_daily_deaths,
                    MAX(date) AS latest_date,
                    COUNT(*) AS row_count
                FROM regional_aggregates
                WHERE source_type = %s
                """,
                (source_type,),
            )
            row = cur.fetchone()
        return {
            "source_type": source_type,
            "world_total_cases": row[0],
            "world_total_deaths": row[1],
            "sum_daily_cases": row[2],
            "sum_daily_deaths": row[3],
            "latest_date": str(row[4]) if row[4] else None,
            "row_count": row[5],
        }
    finally:
        conn.close()


@app.get("/api/trends")
def get_trends(
    location: Optional[str] = None,
    source_type: str = Query(default="historical"),
    limit: int = Query(default=30, ge=1, le=500),
):
    conn = _db()
    try:
        sql = """
            SELECT location, date, new_cases, new_deaths, rolling_7d_cases,
                   rolling_7d_deaths, case_fatality_ratio, source_type
            FROM regional_aggregates
            WHERE source_type = %s
        """
        params: list = [source_type]
        if location:
            sql += " AND location = %s"
            params.append(location)
        sql += " ORDER BY date DESC LIMIT %s"
        params.append(limit)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


@app.get("/api/regions")
def get_regions(source_type: str = Query(default="historical")):
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT iso_code, location, date, recent_7d_avg, previous_7d_avg,
                       percentage_change, is_increasing, observation_count,
                       threshold_pct, explanation, source_type
                FROM regional_trend_signals
                WHERE source_type = %s
                ORDER BY percentage_change DESC NULLS LAST
                """,
                (source_type,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


@app.get("/api/pipeline/status")
def pipeline_status():
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT process_name, status, records_in, records_out,
                       records_quarantined, last_error, updated_at
                FROM pipeline_status
                ORDER BY updated_at DESC
                """
            )
            cols = [d[0] for d in cur.description]
            status_rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.execute(
                """
                SELECT process_name, records_processed, latest_date, status, process_timestamp
                FROM pipeline_metrics
                ORDER BY process_timestamp DESC
                LIMIT 20
                """
            )
            mcols = [d[0] for d in cur.description]
            metrics = [dict(zip(mcols, r)) for r in cur.fetchall()]
            cur.execute("SELECT COUNT(*) FROM quarantined_records")
            quarantined = cur.fetchone()[0]
        return {
            "status": status_rows,
            "recent_metrics": metrics,
            "quarantined_records": quarantined,
            "replay": _replay_state,
        }
    finally:
        conn.close()


def _run_replay(mode: str, limit: int, speed: float) -> None:
    _replay_state.update({"running": True, "mode": mode, "error": None})
    producer = None
    try:
        producer = get_producer()
        if mode == "historical":
            replay_historical(producer, speed=speed, limit=limit)
        else:
            synthetic_mode(producer, interval=speed, limit=limit)
    except Exception as exc:
        logger.exception("Replay failed")
        _replay_state["error"] = str(exc)
    finally:
        if producer is not None:
            producer.close()
        _replay_state["running"] = False


@app.post("/api/pipeline/replay/start")
def replay_start(body: ReplayRequest):
    global _replay_thread
    with _replay_lock:
        if _replay_state["running"]:
            raise HTTPException(status_code=409, detail="replay already running")
        _replay_thread = threading.Thread(
            target=_run_replay,
            args=(body.mode, body.limit, body.speed),
            daemon=True,
        )
        _replay_thread.start()
    return {"started": True, "mode": body.mode, "limit": body.limit, "speed": body.speed}


@app.post("/api/pipeline/replay/stop")
def replay_stop():
    request_stop()
    return {"stop_requested": True, "running": _replay_state["running"]}
