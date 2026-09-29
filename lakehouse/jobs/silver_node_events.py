"""bronze -> silver.node_events: the deduplicated, validated history of node
configuration snapshots. Same idempotent insert-only MERGE pattern as metrics."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.config import TOPIC_NODE_CHANGES, get_paths
from lakehouse.delta_utils import ensure_table
from lakehouse.jobs.silver_metrics import write_quarantine
from lakehouse.spark import get_spark
from lakehouse.transforms import silver as T

paths = get_paths()


def process_batch(batch: DataFrame, batch_id: int) -> None:
    if batch.isEmpty():
        return
    spark = batch.sparkSession
    valid, bad = T.validate_node_events(T.parse_node_events(batch))
    write_quarantine(bad, "node_events", batch_id)
    curated = T.to_silver_node_events(T.dedupe_latest(valid)).withColumn("_silver_loaded_at", F.current_timestamp())
    target = ensure_table(spark, curated, paths.silver_node_events)
    (target.alias("t").merge(curated.alias("s"), "t.event_id = s.event_id").whenNotMatchedInsertAll().execute())


def main() -> None:
    spark = get_spark("silver_node_events")
    stream = spark.readStream.format("delta").load(paths.bronze_raw).where(F.col("topic") == TOPIC_NODE_CHANGES)
    (
        stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", paths.checkpoint("silver_node_events"))
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
