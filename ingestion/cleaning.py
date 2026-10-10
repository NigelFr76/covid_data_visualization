"""
Record-level cleaning used by ingestion, Spark (via equivalent DataFrame logic),
the Kafka producer, and unit tests.

Duplicate rule
--------------
For a given (iso_code, date, source_type), the last well-formed row in the
source wins. Our World in Data publishes at most one country-date observation;
duplicates are treated as restatements.

Revision rule
-------------
If cumulative cases or deaths decrease versus the previous observation for the
same iso_code, the row is flagged ``revision_flag=True``. The originally
reported new_cases/new_deaths are preserved. Daily counts used in rolling
averages are set to NULL for that date so a restatement is not converted into
a fake negative (or zero) daily incidence.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Iterator, Mapping, Optional

VALID_SOURCE_TYPES = frozenset(
    {"historical", "historical_replay", "synthetic"}
)


def parse_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        if value != value:  # NaN
            return default
        return float(value)
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none", "null"}:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    return None


def validate_record(raw: Mapping[str, Any]) -> tuple[Optional[dict], Optional[str]]:
    """Return (normalized_dict, None) or (None, error_reason)."""
    iso = str(raw.get("iso_code") or "").strip()
    if not iso:
        return None, "missing iso_code"
    dt = parse_date(raw.get("date"))
    if dt is None:
        return None, "invalid or missing date"
    source = str(raw.get("source_type") or "historical").strip() or "historical"
    if source not in VALID_SOURCE_TYPES:
        return None, f"unknown source_type: {source}"

    location = str(raw.get("location") or iso).strip()
    continent = str(raw.get("continent") or "").strip() or None

    return {
        "iso_code": iso,
        "continent": continent,
        "location": location,
        "date": dt,
        "total_cases": parse_float(raw.get("total_cases")),
        "new_cases": parse_float(raw.get("new_cases")),
        "total_deaths": parse_float(raw.get("total_deaths")),
        "new_deaths": parse_float(raw.get("new_deaths")),
        "source_type": source,
    }, None


def _derive_daily(
    current_total: Optional[float],
    previous_total: Optional[float],
    reported: Optional[float],
) -> tuple[Optional[float], Optional[float], bool]:
    """
    Returns (daily_for_analytics, derived_from_cumulative, is_revision).

    Revisions (cumulative decrease) do not become daily incidence.
    """
    derived = None
    is_revision = False
    if current_total is not None and previous_total is not None:
        derived = current_total - previous_total
        if derived < 0:
            is_revision = True
            derived = None

    if is_revision:
        return None, derived, True
    if reported is not None:
        if reported < 0:
            # Negative reported daily with non-decreasing cumulative is still a restatement.
            return None, derived, True
        return reported, derived, False
    return derived, derived, False


def deduplicate(records: Iterable[dict]) -> list[dict]:
    """Last row for (iso_code, date, source_type) wins."""
    seen: dict[tuple, dict] = {}
    for row in records:
        key = (row["iso_code"], row["date"], row["source_type"])
        seen[key] = row
    return list(seen.values())


def clean_records(raw_rows: Iterable[Mapping[str, Any]]) -> tuple[list[dict], list[dict]]:
    """
    Validate, deduplicate, and attach derived daily counts.

    Returns (cleaned_rows, quarantined_rows).
    """
    valid: list[dict] = []
    quarantined: list[dict] = []
    for raw in raw_rows:
        rec, err = validate_record(raw)
        if rec is None:
            quarantined.append({"raw_data": dict(raw), "error_reason": err})
            continue
        rec["original_new_cases"] = rec["new_cases"]
        rec["original_new_deaths"] = rec["new_deaths"]
        valid.append(rec)

    valid = deduplicate(valid)
    valid.sort(key=lambda r: (r["iso_code"], r["source_type"], r["date"]))

    prev_cases: dict[tuple, Optional[float]] = {}
    prev_deaths: dict[tuple, Optional[float]] = {}
    cleaned: list[dict] = []

    for rec in valid:
        key = (rec["iso_code"], rec["source_type"])
        daily_c, derived_c, rev_c = _derive_daily(
            rec["total_cases"], prev_cases.get(key), rec["original_new_cases"]
        )
        daily_d, derived_d, rev_d = _derive_daily(
            rec["total_deaths"], prev_deaths.get(key), rec["original_new_deaths"]
        )
        if rec["total_cases"] is not None:
            prev_cases[key] = rec["total_cases"]
        if rec["total_deaths"] is not None:
            prev_deaths[key] = rec["total_deaths"]

        rec["daily_cases_derived"] = derived_c
        rec["daily_deaths_derived"] = derived_d
        rec["daily_cases"] = daily_c
        rec["daily_deaths"] = daily_d
        rec["revision_flag"] = bool(rev_c or rev_d)
        rec["notes"] = (
            "Cumulative restatement; reported daily values preserved, excluded from rolling averages."
            if rec["revision_flag"]
            else None
        )
        cleaned.append(rec)

    return cleaned, quarantined


def iter_csv_dicts(path: str) -> Iterator[dict]:
    import csv

    with open(path, newline="", encoding="utf-8") as handle:
        yield from csv.DictReader(handle)
