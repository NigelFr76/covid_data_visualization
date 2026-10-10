"""Explicit OWID / pipeline field definitions shared by ingestion, Spark, and tests."""

OWID_REQUIRED_COLUMNS = [
    "iso_code",
    "continent",
    "location",
    "date",
    "total_cases",
    "new_cases",
    "total_deaths",
    "new_deaths",
]

record_schema_fields = OWID_REQUIRED_COLUMNS + [
    "source_type",
    "original_new_cases",
    "original_new_deaths",
    "daily_cases_derived",
    "daily_deaths_derived",
    "revision_flag",
]
