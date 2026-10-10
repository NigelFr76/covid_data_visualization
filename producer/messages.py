from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Mapping, Optional

from ingestion.cleaning import validate_record

VALID_SOURCES = ("historical", "historical_replay", "synthetic")


def build_record(
    row: Mapping[str, Any],
    source_type: str,
    ingestion_timestamp: Optional[str] = None,
) -> dict:
    rec, err = validate_record({**dict(row), "source_type": "historical"})
    if rec is None:
        raise ValueError(err)
    if source_type not in VALID_SOURCES:
        raise ValueError(f"unknown source_type: {source_type}")
    return {
        "iso_code": rec["iso_code"],
        "continent": rec.get("continent"),
        "location": rec["location"],
        "date": rec["date"].isoformat(),
        "total_cases": rec.get("total_cases"),
        "new_cases": rec.get("new_cases"),
        "total_deaths": rec.get("total_deaths"),
        "new_deaths": rec.get("new_deaths"),
        "source_type": source_type,
        "ingestion_timestamp": ingestion_timestamp or datetime.utcnow().isoformat(),
    }


def serialize_record(record: Mapping[str, Any]) -> bytes:
    def default(obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        raise TypeError(type(obj))

    return json.dumps(dict(record), default=default).encode("utf-8")


def deserialize_record(payload: bytes) -> dict:
    return json.loads(payload.decode("utf-8"))
