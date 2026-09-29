"""bronze -> silver.node_metrics (+ silver.quarantine).

Incremental: streams from the bronze Delta table (checkpoint tracks the Delta
versions already processed). Each micro-batch is parsed, validated, deduplicated
within the batch, then MERGEd on event_id with insert-only semantics, which
makes it idempotent across batches, retries, and full replays of bronze.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.config import TOPIC_METRICS, get_paths
from lakehouse.delta_utils import ensure_table, in_list_sql
from lakehouse.spark import get_spark
from lakehouse.transforms import silver as T

paths = get_paths()


def write_quarantine(bad: DataFrame, source: str, batch_id: int) -> None:
    # txnAppId/txnVersion make this append idempotent: if the micro-batch is
    # retried after a failure, Delta skips a batch_id it has already committed.
    (
        T.to_quarantine(bad, source)
        .write.format("delta")
        .mode("append")
        .option("txnAppId", f"quarantine_{source}")
        .option("txnVersion", batch_id)
        .partitionBy("quarantine_date")
        .save(paths.silver_quarantine)
    )


def process_batch(batch: DataFrame, batch_id: int) -> None:
    if batch.isEmpty():
        return
    spark = batch.sparkSession
    valid, bad = T.validate_metrics(T.parse_metrics(batch))
    write_quarantine(bad, "metrics", batch_id)

    curated = T.to_silver_metrics(T.dedupe_latest(valid)).withColumn("_silver_loaded_at", F.current_timestamp()).cache()
    dates = [r.event_date for r in curated.select("event_date").distinct().collect()]
    if dates:
        target = ensure_table(spark, curated, paths.silver_metrics, ["event_date"])
        (
            target.alias("t")
            .merge(
                curated.alias("s"),
                # static partition predicate -> file pruning on the target
                f"{in_list_sql('t.event_date', dates)} AND t.event_id = s.event_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    curated.unpersist()


def main() -> None:
    spark = get_spark("silver_metrics")
    stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 200)
        .load(paths.bronze_raw)
        .where(F.col("topic") == TOPIC_METRICS)
    )
    query = (
        stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", paths.checkpoint("silver_metrics"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    if DeltaTable.isDeltaTable(spark, paths.silver_metrics):
        print(f"silver_metrics: {spark.read.format('delta').load(paths.silver_metrics).count()} rows")


if __name__ == "__main__":
    main()
