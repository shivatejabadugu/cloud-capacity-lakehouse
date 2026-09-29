"""Gold-layer transformations (pure functions, no I/O).

Model:
  dim_node                        SCD Type 2, one row per version of a node's config
  fact_node_utilization_hourly    grain: node_id x hour_start
  agg_cluster_capacity_daily      grain: cluster_id x usage_date
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.config import AT_RISK_P95_CPU, UNDERUTILIZED_P95_CPU

SCD2_TRACKED_COLS = ["cluster_id", "region", "sku", "vcores", "memory_gb", "status"]
OPEN_END = "9999-12-31 00:00:00"


# --------------------------------------------------------------------------- dim_node
def build_scd2(node_events: DataFrame) -> DataFrame:
    """Build the complete SCD2 history for every node present in `node_events`.

    Input is the node's *full* event history (not just new events). Rebuilding
    per affected key makes late and out-of-order events trivially correct: a
    late event just lands in its right place in the sequence on the next run.

    Consecutive events that don't change any tracked attribute are collapsed,
    so re-sent snapshots don't create empty versions.
    """
    order = Window.partitionBy("node_id").orderBy("event_ts", "event_id")
    attr_hash = F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in SCD2_TRACKED_COLS]),
        256,
    )
    versions = (
        node_events.withColumn("attr_hash", attr_hash)
        .withColumn("_prev_hash", F.lag("attr_hash").over(order))
        .where(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("attr_hash")))
        .withColumn("_next_from", F.lead("event_ts").over(order))
    )
    return versions.select(
        F.xxhash64("node_id", "event_ts").alias("node_sk"),
        "node_id",
        *SCD2_TRACKED_COLS,
        "attr_hash",
        F.col("event_ts").alias("effective_from"),
        F.coalesce("_next_from", F.to_timestamp(F.lit(OPEN_END))).alias("effective_to"),
        F.col("_next_from").isNull().alias("is_current"),
    )


# --------------------------------------------------------------------------- hourly fact
def hourly_node_utilization(metrics: DataFrame) -> DataFrame:
    """Aggregate silver metrics to node x hour. If a node moved clusters mid-hour,
    the hour is attributed to where it was at its last sample, which keeps the
    grain at exactly one row per node-hour."""
    return metrics.groupBy("node_id", F.col("event_hour").alias("hour_start")).agg(
        F.max_by("cluster_id", "event_ts").alias("cluster_id"),
        F.max_by("region", "event_ts").alias("region"),
        F.max("event_ts").alias("last_sample_ts"),
        F.count("*").alias("sample_count"),
        F.round(F.avg("cpu_pct"), 2).alias("avg_cpu_pct"),
        F.round(F.percentile_approx("cpu_pct", 0.95), 2).alias("p95_cpu_pct"),
        F.max("cpu_pct").alias("max_cpu_pct"),
        F.round(F.avg("mem_pct"), 2).alias("avg_mem_pct"),
        F.round(F.percentile_approx("mem_pct", 0.95), 2).alias("p95_mem_pct"),
        F.max("disk_pct").alias("max_disk_pct"),
        F.round(F.avg("active_dbs"), 1).alias("avg_active_dbs"),
    )


def attach_node_dim(hourly: DataFrame, dim_node: DataFrame) -> DataFrame:
    """Point-in-time (as-of) join: pick the dim version that was in effect at the
    hour's last sample. Left join, so facts whose node config hasn't arrived yet
    are kept with a null node_sk (late-arriving dimension) instead of dropped;
    the DQ job tracks that rate. dim_node is small, so it's broadcast."""
    d = dim_node.select(
        "node_sk",
        F.col("node_id").alias("_d_node_id"),
        "sku",
        "vcores",
        "memory_gb",
        "effective_from",
        "effective_to",
    )
    joined = hourly.join(
        F.broadcast(d),
        (hourly.node_id == d._d_node_id)
        & (hourly.last_sample_ts >= d.effective_from)
        & (hourly.last_sample_ts < d.effective_to),
        "left",
    )
    return joined.select(
        "node_id",
        "hour_start",
        F.to_date("hour_start").alias("usage_date"),
        "node_sk",
        "cluster_id",
        "region",
        "sku",
        "vcores",
        "memory_gb",
        "sample_count",
        "avg_cpu_pct",
        "p95_cpu_pct",
        "max_cpu_pct",
        "avg_mem_pct",
        "p95_mem_pct",
        "max_disk_pct",
        "avg_active_dbs",
        F.round(F.col("vcores") * F.col("p95_cpu_pct") / 100, 2).alias("used_vcores_p95"),
        "last_sample_ts",
    )


# --------------------------------------------------------------------------- daily cluster agg
def cluster_capacity_daily(fact_hourly: DataFrame) -> DataFrame:
    """Roll node-hours up to cluster-days and classify capacity health.

    Summing per-node p95s overstates the true cluster p95 (node peaks don't all
    line up), which errs on the safe side for capacity planning."""
    per_hour = (
        fact_hourly.where(F.col("node_sk").isNotNull())
        .groupBy("cluster_id", "region", "hour_start")
        .agg(
            F.countDistinct("node_id").alias("node_count"),
            F.sum("vcores").alias("provisioned_vcores"),
            F.sum("used_vcores_p95").alias("used_vcores_p95"),
        )
        .withColumn("util_pct", F.col("used_vcores_p95") / F.col("provisioned_vcores") * 100)
    )
    daily = per_hour.groupBy("cluster_id", "region", F.to_date("hour_start").alias("usage_date")).agg(
        F.count("*").alias("hours_observed"),
        F.max("node_count").alias("peak_node_count"),
        F.max("provisioned_vcores").alias("provisioned_vcores"),
        F.round(F.avg("util_pct"), 2).alias("avg_util_pct"),
        F.round(F.percentile_approx("util_pct", 0.95), 2).alias("p95_util_pct"),
        F.round(F.max("util_pct"), 2).alias("peak_util_pct"),
    )
    return daily.withColumn("headroom_pct", F.round(100 - F.col("p95_util_pct"), 2)).withColumn(
        "capacity_status",
        F.when(F.col("p95_util_pct") >= AT_RISK_P95_CPU, "at_risk")
        .when(F.col("p95_util_pct") < UNDERUTILIZED_P95_CPU, "underutilized")
        .otherwise("healthy"),
    )
