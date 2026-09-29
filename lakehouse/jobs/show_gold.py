"""Prints the gold layer so you can see results without a BI tool.

python -m lakehouse.jobs.show_gold
"""

from pyspark.sql import functions as F

from lakehouse.config import get_paths
from lakehouse.spark import get_spark


def main() -> None:
    paths = get_paths()
    spark = get_spark("show_gold")

    def read(path):
        return spark.read.format("delta").load(path)

    print("\n== Clusters by capacity risk (latest day) ==")
    daily = read(paths.gold_cluster_daily)
    latest = daily.agg(F.max("usage_date")).first()[0]
    daily.where(F.col("usage_date") == latest).orderBy(F.desc("p95_util_pct")).select(
        "cluster_id",
        "region",
        "peak_node_count",
        "provisioned_vcores",
        "p95_util_pct",
        "headroom_pct",
        "capacity_status",
    ).show(20, truncate=False)

    print("== Nodes with config history (SCD2) ==")
    dim = read(paths.gold_dim_node)
    changed = dim.groupBy("node_id").count().where("count > 1").select("node_id")
    dim.join(changed, "node_id").orderBy("node_id", "effective_from").select(
        "node_id", "cluster_id", "sku", "vcores", "status", "effective_from", "effective_to", "is_current"
    ).show(15, truncate=False)

    print("== Quarantine reasons ==")
    read(paths.silver_quarantine).groupBy("source", "_dq_reason").count().orderBy(F.desc("count")).show(truncate=False)

    print("== Latest DQ run ==")
    dq = read(paths.ops_dq_results)
    last = dq.agg(F.max("run_at")).first()[0]
    dq.where(F.col("run_at") == last).select("table", "check", "severity", "passed", "observed").show(truncate=False)


if __name__ == "__main__":
    main()
