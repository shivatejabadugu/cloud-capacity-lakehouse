"""Kafka -> bronze Delta. Raw payloads land untouched as strings, so a malformed
message can never break ingestion and anything can be replayed from bronze.

Exactly-once into Delta: Kafka offsets are tracked in the checkpoint and the
Delta sink commits each micro-batch atomically, keyed by batch id.

  python -m lakehouse.jobs.bronze_ingest              # run once over new data (Airflow)
  python -m lakehouse.jobs.bronze_ingest --continuous # long-running, 30s micro-batches
"""

import argparse

from pyspark.sql import functions as F

from lakehouse.config import KAFKA_BOOTSTRAP, TOPICS, get_paths
from lakehouse.spark import get_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--continuous", action="store_true")
    args = parser.parse_args()

    paths = get_paths()
    spark = get_spark("bronze_ingest", with_kafka=True)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", ",".join(TOPICS))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200_000)  # bound batch size after an outage
        .option("failOnDataLoss", "false")  # topic retention may expire unread offsets
        .load()
    )
    bronze = raw.select(
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.current_timestamp().alias("_ingested_at"),
    ).withColumn("ingest_date", F.to_date("_ingested_at"))

    writer = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingest_date")
        .option("checkpointLocation", paths.checkpoint("bronze_telemetry"))
        .queryName("bronze_telemetry")
    )
    writer = writer.trigger(processingTime="30 seconds") if args.continuous else writer.trigger(availableNow=True)
    query = writer.start(paths.bronze_raw)
    query.awaitTermination()
    progress = query.lastProgress
    if progress:
        print(f"bronze_ingest: last batch rows={progress['numInputRows']}")


if __name__ == "__main__":
    main()
