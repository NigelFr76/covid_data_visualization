import os

import pytest

from ingestion.download_data import FIXTURE_PATH

pytestmark = pytest.mark.spark


def test_spark_batch_transforms_on_fixture():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from spark.batch.transforms import clean_and_enrich, load_owid_csv

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("covid-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = clean_and_enrich(load_owid_csv(spark, FIXTURE_PATH))
        rows = [r.asDict() for r in df.collect()]
        assert rows
        usa = [r for r in rows if r["iso_code"] == "USA"]
        assert any(r["revision_flag"] for r in usa)
        assert all(r["source_type"] == "historical" for r in rows)
        # Duplicate 2020-03-14 collapsed to one row.
        assert sum(1 for r in usa if str(r["date"]) == "2020-03-14") == 1
    finally:
        spark.stop()


def test_streaming_split_valid_invalid():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from spark.streaming.transforms import split_valid_invalid

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("covid-stream-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        parsed = spark.createDataFrame(
            [
                {
                    "iso_code": "USA",
                    "continent": "North America",
                    "location": "United States",
                    "date": "2020-03-01",
                    "total_cases": 10.0,
                    "new_cases": 10.0,
                    "total_deaths": 0.0,
                    "new_deaths": 0.0,
                    "source_type": "historical_replay",
                    "ingestion_timestamp": "2026-01-01T00:00:00",
                    "json": "{}",
                    "timestamp": None,
                },
                {
                    "iso_code": None,
                    "continent": None,
                    "location": None,
                    "date": "nope",
                    "total_cases": None,
                    "new_cases": None,
                    "total_deaths": None,
                    "new_deaths": None,
                    "source_type": "nope",
                    "ingestion_timestamp": None,
                    "json": "{}",
                    "timestamp": None,
                },
            ]
        )
        valid, invalid = split_valid_invalid(parsed)
        assert valid.count() == 1
        assert invalid.count() == 1
        assert valid.first()["source_type"] == "historical_replay"
    finally:
        spark.stop()
