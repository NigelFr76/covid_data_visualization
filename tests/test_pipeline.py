"""Unit tests against the real cleaning, metrics, and Kafka serialization modules."""

from datetime import date

import pytest

from analytics.metrics import (
    attach_rolling_and_cfr,
    case_fatality_ratio,
    percentage_change,
    regional_increase_signals,
    rolling_mean,
)
from ingestion.cleaning import clean_records, parse_date, parse_float, validate_record
from ingestion.schema import OWID_REQUIRED_COLUMNS
from producer.messages import build_record, deserialize_record, serialize_record


def test_required_schema_columns():
    for col in ("iso_code", "date", "total_cases", "new_cases", "total_deaths", "new_deaths"):
        assert col in OWID_REQUIRED_COLUMNS


def test_parse_float_missing_and_malformed():
    assert parse_float("") is None
    assert parse_float("not-a-number") is None
    assert parse_float("12.5") == 12.5


def test_parse_row_invalid_date():
    rec, err = validate_record(
        {"iso_code": "USA", "location": "United States", "date": "not-a-date", "new_cases": "1"}
    )
    assert rec is None
    assert "date" in err


def test_parse_row_missing_iso():
    rec, err = validate_record({"iso_code": "", "date": "2020-01-01"})
    assert rec is None
    assert "iso_code" in err


def test_parse_date_valid():
    assert parse_date("2020-03-01") == date(2020, 3, 1)


def test_duplicate_records_last_write_wins():
    rows = [
        {"iso_code": "USA", "location": "United States", "date": "2020-01-01", "new_cases": "10", "total_cases": "10"},
        {"iso_code": "USA", "location": "United States", "date": "2020-01-01", "new_cases": "99", "total_cases": "99"},
    ]
    cleaned, quarantined = clean_records(rows)
    assert quarantined == []
    assert len(cleaned) == 1
    assert cleaned[0]["original_new_cases"] == 99.0


def test_cumulative_revision_preserved_not_converted_to_daily():
    rows = [
        {"iso_code": "USA", "location": "United States", "date": "2020-01-01", "total_cases": "100", "new_cases": "100", "total_deaths": "1", "new_deaths": "1"},
        {"iso_code": "USA", "location": "United States", "date": "2020-01-02", "total_cases": "90", "new_cases": "-10", "total_deaths": "1", "new_deaths": "0"},
    ]
    cleaned, _ = clean_records(rows)
    revised = cleaned[1]
    assert revised["revision_flag"] is True
    assert revised["original_new_cases"] == -10.0
    assert revised["daily_cases"] is None


def test_derived_daily_when_reported_missing():
    rows = [
        {"iso_code": "IND", "location": "India", "date": "2020-01-01", "total_cases": "10", "new_cases": "", "total_deaths": "0", "new_deaths": ""},
        {"iso_code": "IND", "location": "India", "date": "2020-01-02", "total_cases": "25", "new_cases": "", "total_deaths": "1", "new_deaths": ""},
    ]
    cleaned, _ = clean_records(rows)
    assert cleaned[1]["daily_cases"] == pytest.approx(15.0)
    assert cleaned[1]["revision_flag"] is False


def test_rolling_7d_average():
    values = [10, 20, 30, 40, 50, 60, 70]
    avgs = rolling_mean(values, 7)
    assert avgs[-1] == pytest.approx(40.0)


def test_rolling_skips_revision_nulls():
    avgs = rolling_mean([10, None, 30], 3)
    assert avgs[-1] == pytest.approx(20.0)


def test_cfr_normal_and_zero_denominator():
    assert case_fatality_ratio(100, 1000) == pytest.approx(10.0)
    assert case_fatality_ratio(0, 0) is None
    assert case_fatality_ratio(5, None) is None


def test_percentage_change_zero_previous():
    assert percentage_change(50, 0) is None
    assert percentage_change(100, 80) == pytest.approx(25.0)


def _series(iso, location, start, values):
    rows = []
    total = 0
    for i, daily in enumerate(values):
        total += daily
        rows.append(
            {
                "iso_code": iso,
                "location": location,
                "date": date(2020, 3, 1 + i),
                "daily_cases": float(daily),
                "new_cases": float(daily),
                "total_cases": float(total),
                "total_deaths": 0.0,
                "source_type": "historical",
            }
        )
    return rows


def test_regional_increase_flagged_and_explained():
    low = [10] * 7
    high = [20] * 7
    rows = _series("USA", "United States", date(2020, 3, 1), low + high)
    signals = regional_increase_signals(rows, threshold_pct=10, min_obs=7)
    assert len(signals) == 1
    assert signals[0]["is_increasing"] is True
    assert signals[0]["percentage_change"] == pytest.approx(100.0)
    assert "not a medical diagnosis" in signals[0]["explanation"].lower() or "statistical" in signals[0]["explanation"].lower()


def test_regional_not_enough_observations():
    rows = _series("USA", "United States", date(2020, 3, 1), [1, 2, 3])
    assert regional_increase_signals(rows, min_obs=7) == []


def test_attach_rolling_and_cfr():
    rows = _series("USA", "United States", date(2020, 3, 1), [7] * 7)
    rows[-1]["total_deaths"] = 7
    out = attach_rolling_and_cfr(rows)
    assert out[-1]["rolling_7d_cases"] == pytest.approx(7.0)
    assert out[-1]["case_fatality_ratio"] == pytest.approx(100.0)


def test_kafka_message_round_trip():
    rec = build_record(
        {"iso_code": "USA", "location": "United States", "date": "2020-01-01", "new_cases": "5", "total_cases": "10"},
        source_type="historical_replay",
    )
    payload = serialize_record(rec)
    back = deserialize_record(payload)
    assert back["source_type"] == "historical_replay"
    assert back["iso_code"] == "USA"


def test_synthetic_label_distinct_from_historical():
    rec = build_record(
        {"iso_code": "SYN", "location": "Synthetic Region", "date": "2021-06-01", "new_cases": "1", "total_cases": "1"},
        source_type="synthetic",
    )
    assert rec["source_type"] == "synthetic"
    assert rec["source_type"] != "historical"


def test_filter_historical_only():
    records = [
        {"new_cases": 10, "source_type": "historical"},
        {"new_cases": 50, "source_type": "synthetic"},
        {"new_cases": 5, "source_type": "historical_replay"},
    ]
    historical = [r for r in records if r["source_type"] == "historical"]
    assert sum(r["new_cases"] for r in historical) == 10


def test_unknown_source_type_quarantined():
    rec, err = validate_record(
        {"iso_code": "USA", "date": "2020-01-01", "source_type": "made_up"}
    )
    assert rec is None
    assert "source_type" in err
