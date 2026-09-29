"""silver.node_metrics + gold.dim_node -> gold.fact_node_utilization_hourly.

Late data is the reason this isn't a simple append: an event for 09:00 can
arrive at 11:30. So each run finds every hour touched by rows loaded since the
last watermark, recomputes those hours completely from silver, and MERGEs them
over the existing rows. Hours with orphan facts (node config not yet in dim_node
at the time) are recomputed too, so they heal once the dimension catches up.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from lakehouse.config import get_paths
from lakehouse.delta_utils import ensure_table, get_watermark, in_list_sql, read_pinned, set_watermark
from lakehouse.spark import get_spark
from lakehouse.transforms.gold import attach_node_dim, hourly_node_utilization

JOB = "gold_fact_node_hourly"


def main() -> None:
    paths = get_paths()
    spark = get_spark(JOB)
    metrics, _ = read_pinned(spark, paths.silver_metrics)
    wm = get_watermark(spark, paths, JOB)
    new = metrics if wm is None else metrics.where(F.col("_silver_loaded_at") > F.lit(wm))
    new_hwm = new.agg(F.max("_silver_loaded_at")).first()[0]

    hours = {r.event_hour for r in new.select("event_hour").distinct().collect()}
    if DeltaTable.isDeltaTable(spark, paths.gold_fact_node_hourly):
        orphans = (
            spark.read.format("delta")
            .load(paths.gold_fact_node_hourly)
            .where(F.col("node_sk").isNull())
            .select("hour_start")
            .distinct()
            .collect()
        )
        hours |= {r.hour_start for r in orphans}
    if not hours:
        print(f"{JOB}: nothing to do")
        return

    dates = sorted({h.date() for h in hours})
    to_recompute = metrics.where(in_list_sql("event_date", dates)).where(F.col("event_hour").isin(list(hours)))
    dim = spark.read.format("delta").load(paths.gold_dim_node)
    fact = attach_node_dim(hourly_node_utilization(to_recompute), dim).withColumn(
        "_gold_loaded_at", F.current_timestamp()
    )

    target = ensure_table(spark, fact, paths.gold_fact_node_hourly, ["usage_date"])
    (
        target.alias("t")
        .merge(
            fact.alias("s"),
            f"{in_list_sql('t.usage_date', dates)} AND t.node_id = s.node_id AND t.hour_start = s.hour_start",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    if new_hwm is not None:
        set_watermark(spark, paths, JOB, new_hwm)
    print(f"{JOB}: recomputed {len(hours)} hours across {len(dates)} days")


if __name__ == "__main__":
    main()
