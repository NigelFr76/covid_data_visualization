from pyspark.sql.types import DoubleType, StringType, StructField, StructType


def owid_csv_spark_schema() -> StructType:
    return StructType(
        [
            StructField("iso_code", StringType(), True),
            StructField("continent", StringType(), True),
            StructField("location", StringType(), True),
            StructField("date", StringType(), True),
            StructField("total_cases", DoubleType(), True),
            StructField("new_cases", DoubleType(), True),
            StructField("new_cases_smoothed", DoubleType(), True),
            StructField("total_deaths", DoubleType(), True),
            StructField("new_deaths", DoubleType(), True),
            StructField("new_deaths_smoothed", DoubleType(), True),
        ]
    )


def kafka_json_spark_schema() -> StructType:
    return StructType(
        [
            StructField("iso_code", StringType(), True),
            StructField("continent", StringType(), True),
            StructField("location", StringType(), True),
            StructField("date", StringType(), True),
            StructField("total_cases", DoubleType(), True),
            StructField("new_cases", DoubleType(), True),
            StructField("total_deaths", DoubleType(), True),
            StructField("new_deaths", DoubleType(), True),
            StructField("source_type", StringType(), True),
            StructField("ingestion_timestamp", StringType(), True),
        ]
    )
