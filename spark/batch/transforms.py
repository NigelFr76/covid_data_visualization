"""Spark DataFrame transformations for historical COVID records."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, lag, lit, to_date, when
from pyspark.sql.window import Window

from ingestion.schema import OWID_REQUIRED_COLUMNS


def load_owid_csv(spark, path: str) -> DataFrame:
    raw = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(path)
    for name in OWID_REQUIRED_COLUMNS:
        if name not in raw.columns:
            raw = raw.withColumn(name, lit(None).cast("string"))
    return raw.select(
        col("iso_code").cast("string"),
        col("continent").cast("string"),
        col("location").cast("string"),
        col("date").cast("string"),
        col("total_cases").cast("double"),
        col("new_cases").cast("double"),
        col("total_deaths").cast("double"),
        col("new_deaths").cast("double"),
    )


def clean_and_enrich(df: DataFrame, source_type: str = "historical") -> DataFrame:
    df = df.filter(col("iso_code").isNotNull() & col("date").isNotNull())
    df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    df = df.filter(col("date").isNotNull())
    df = df.withColumn("source_type", lit(source_type))
    df = df.dropDuplicates(["iso_code", "date", "source_type"])

    order = Window.partitionBy("iso_code", "source_type").orderBy("date")
    df = df.withColumn("prev_total_cases", lag("total_cases").over(order))
    df = df.withColumn("prev_total_deaths", lag("total_deaths").over(order))
    df = df.withColumn("daily_cases_derived", col("total_cases") - col("prev_total_cases"))
    df = df.withColumn("daily_deaths_derived", col("total_deaths") - col("prev_total_deaths"))
    df = df.withColumn(
        "revision_flag",
        when(col("daily_cases_derived") < 0, True)
        .when(col("daily_deaths_derived") < 0, True)
        .when(col("new_cases") < 0, True)
        .when(col("new_deaths") < 0, True)
        .otherwise(False),
    )
    df = df.withColumn(
        "daily_cases",
        when(col("revision_flag"), lit(None).cast("double"))
        .when(col("new_cases").isNotNull() & (col("new_cases") >= 0), col("new_cases"))
        .otherwise(col("daily_cases_derived")),
    )
    df = df.withColumn(
        "daily_deaths",
        when(col("revision_flag"), lit(None).cast("double"))
        .when(col("new_deaths").isNotNull() & (col("new_deaths") >= 0), col("new_deaths"))
        .otherwise(col("daily_deaths_derived")),
    )
    df = df.withColumn("original_new_cases", col("new_cases"))
    df = df.withColumn("original_new_deaths", col("new_deaths"))

    roll = Window.partitionBy("iso_code", "source_type").orderBy("date").rowsBetween(-6, 0)
    prev7 = Window.partitionBy("iso_code", "source_type").orderBy("date").rowsBetween(-13, -7)
    df = df.withColumn("rolling_7d_cases", avg("daily_cases").over(roll))
    df = df.withColumn("rolling_7d_deaths", avg("daily_deaths").over(roll))
    df = df.withColumn(
        "case_fatality_ratio",
        when(
            col("total_cases") > 0,
            (col("total_deaths") / col("total_cases")) * 100.0,
        ).otherwise(lit(None).cast("double")),
    )
    df = df.withColumn("recent_7d_avg", col("rolling_7d_cases"))
    df = df.withColumn("previous_7d_avg", avg("daily_cases").over(prev7))
    df = df.withColumn(
        "percentage_change",
        when(
            col("previous_7d_avg") > 0,
            ((col("recent_7d_avg") - col("previous_7d_avg")) / col("previous_7d_avg")) * 100.0,
        ).otherwise(lit(None).cast("double")),
    )
    return df
