"""gold.fact_node_utilization_hourly -> gold.agg_cluster_capacity_daily.

Recomputes only the days touched since the last run."""

from pyspark.sql import functions as F

from lakehouse.config import get_paths
from lakehouse.delta_utils import ensure_table, get_watermark, in_list_sql, read_pinned, set_watermark
from lakehouse.spark import get_spark
from lakehouse.transforms.gold import cluster_capacity_daily

JOB = "gold_cluster_capacity_daily"


def main() -> None:
    paths = get_paths()
    spark = get_spark(JOB)
    fact, _ = read_pinned(spark, paths.gold_fact_node_hourly)
    wm = get_watermark(spark, paths, JOB)
    new = fact if wm is None else fact.where(F.col("_gold_loaded_at") > F.lit(wm))
    new_hwm = new.agg(F.max("_gold_loaded_at")).first()[0]
    if new_hwm is None:
        print(f"{JOB}: nothing new")
        return

    dates = sorted(r.usage_date for r in new.select("usage_date").distinct().collect())
    daily = cluster_capacity_daily(fact.where(in_list_sql("usage_date", dates))).withColumn(
        "_gold_loaded_at", F.current_timestamp()
    )
    target = ensure_table(spark, daily, paths.gold_cluster_daily, ["usage_date"])
    (
        target.alias("t")
        .merge(
            daily.alias("s"),
            f"{in_list_sql('t.usage_date', dates)} AND t.cluster_id = s.cluster_id AND t.usage_date = s.usage_date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        # a cluster that lost all its nodes on a recomputed day shouldn't linger
        .whenNotMatchedBySourceDelete(condition=in_list_sql("t.usage_date", dates))
        .execute()
    )
    set_watermark(spark, paths, JOB, new_hwm)
    print(f"{JOB}: recomputed {len(dates)} days")


if __name__ == "__main__":
    main()
