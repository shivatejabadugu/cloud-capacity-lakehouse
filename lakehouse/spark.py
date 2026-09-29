"""SparkSession factory with Delta Lake (and optionally the Kafka connector)."""

import os

from pyspark.sql import SparkSession

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.1"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def get_spark(app_name: str, with_kafka: bool = False) -> SparkSession:
    packages = [DELTA_PACKAGE] + ([KAFKA_PACKAGE] if with_kafka else [])
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        # Small local data: the default of 200 shuffle partitions just creates tiny files
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        # Write fewer, larger files on MERGE/INSERT
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
