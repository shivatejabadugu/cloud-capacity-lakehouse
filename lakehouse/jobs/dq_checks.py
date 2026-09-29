"""Runs data quality checks across silver and gold, appends results to
ops.dq_results, and exits non-zero if any critical check fails (which fails the
Airflow task and blocks anything downstream)."""

import sys
import uuid

from pyspark.sql import functions as F

from lakehouse.config import get_paths
from lakehouse.dq import checks as C
from lakehouse.spark import get_spark


def main() -> None:
    paths = get_paths()
    spark = get_spark("dq_checks")

    def read(path):
        return spark.read.format("delta").load(path)

    metrics = read(paths.silver_metrics)
    fact = read(paths.gold_fact_node_hourly)
    dim = read(paths.gold_dim_node)
    daily = read(paths.gold_cluster_daily)
    quarantine = read(paths.silver_quarantine)

    last_day = F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 1 DAY")
    results = [
        C.unique(metrics, "silver.node_metrics", ["event_id"]),
        C.in_range(metrics, "silver.node_metrics", "cpu_pct", 0, 100),
        C.freshness(metrics, "silver.node_metrics", "_ingested_at", max_lag_minutes=60),
        C.ratio(
            quarantine.where(last_day).count(),
            metrics.where(last_day).count(),
            "silver.quarantine",
            "quarantine_rate_24h",
            max_rate=0.02,
        ),
        C.unique(fact, "gold.fact_node_utilization_hourly", ["node_id", "hour_start"]),
        C.in_range(fact, "gold.fact_node_utilization_hourly", "p95_cpu_pct", 0, 100),
        C.null_rate(fact, "gold.fact_node_utilization_hourly", "node_sk", max_rate=0.01),
        C.unique(daily, "gold.agg_cluster_capacity_daily", ["cluster_id", "usage_date"]),
        *C.scd2_integrity(dim),
    ]

    run_id = str(uuid.uuid4())
    (
        spark.createDataFrame([r.as_row() for r in results])
        .withColumn("run_id", F.lit(run_id))
        .withColumn("run_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .save(paths.ops_dq_results)
    )

    for r in results:
        status = "PASS" if r.passed else ("FAIL" if r.severity == C.CRITICAL else "WARN")
        print(f"[{status}] {r.table} {r.check}: observed={r.observed} threshold={r.threshold}")

    if any(not r.passed and r.severity == C.CRITICAL for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
