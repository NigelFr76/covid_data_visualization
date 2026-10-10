from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_date, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from spark.schemas import kafka_json_spark_schema


def parse_kafka(raw: DataFrame) -> DataFrame:
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json", "timestamp")
        .select(from_json(col("json"), kafka_json_spark_schema()).alias("data"), col("json"), col("timestamp"))
        .select("data.*", "json", "timestamp")
    )
    return parsed


def split_valid_invalid(parsed: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid_mask = (
        col("iso_code").isNotNull()
        & (col("iso_code") != "")
        & col("date").isNotNull()
        & to_date(col("date"), "yyyy-MM-dd").isNotNull()
        & col("source_type").isin("historical", "historical_replay", "synthetic")
    )
    invalid = parsed.filter(~valid_mask)
    valid = parsed.filter(valid_mask).dropDuplicates(["iso_code", "date", "source_type"])
    valid = (
        valid.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        .withColumn(
            "revision_flag",
            when(col("new_cases") < 0, True).when(col("new_deaths") < 0, True).otherwise(False),
        )
        .withColumn(
            "daily_cases",
            when(col("revision_flag"), None).otherwise(col("new_cases")),
        )
        .withColumn(
            "daily_deaths",
            when(col("revision_flag"), None).otherwise(col("new_deaths")),
        )
        .withColumn("original_new_cases", col("new_cases"))
        .withColumn("original_new_deaths", col("new_deaths"))
        .withColumn("rolling_7d_cases", col("daily_cases"))
        .withColumn("rolling_7d_deaths", col("daily_deaths"))
        .withColumn(
            "case_fatality_ratio",
            when(col("total_cases") > 0, (col("total_deaths") / col("total_cases")) * 100.0),
        )
    )
    return valid, invalid
