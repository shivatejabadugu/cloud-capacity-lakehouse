"""Silver-layer transformations. Pure DataFrame-in, DataFrame-out functions with
no I/O, so they're unit-tested with plain Spark (no Kafka, no Delta)."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.config import TOPIC_METRICS, TOPIC_NODE_CHANGES
from lakehouse.schemas import BRONZE_LINEAGE_COLS, METRIC_SCHEMA, NODE_EVENT_SCHEMA

VALID_NODE_STATUSES = ["active", "decommissioned"]


def _parse(bronze: DataFrame, topic: str, schema) -> DataFrame:
    return (
        bronze.where(F.col("topic") == topic)
        .withColumn("j", F.from_json("payload", schema))
        .select(*BRONZE_LINEAGE_COLS, "payload", "j.*")
        # ISO-8601 with offset; a malformed string becomes null and gets quarantined
        .withColumn("event_ts", F.to_timestamp("event_ts"))
    )


def parse_metrics(bronze: DataFrame) -> DataFrame:
    return _parse(bronze, TOPIC_METRICS, METRIC_SCHEMA)


def parse_node_events(bronze: DataFrame) -> DataFrame:
    return _parse(bronze, TOPIC_NODE_CHANGES, NODE_EVENT_SCHEMA)


def _pct_invalid(c: str):
    return F.col(c).isNull() | ~F.col(c).between(0, 100)


def _split(df: DataFrame, reason) -> tuple[DataFrame, DataFrame]:
    tagged = df.withColumn("_dq_reason", reason)
    valid = tagged.where(F.col("_dq_reason").isNull()).drop("_dq_reason")
    quarantined = tagged.where(F.col("_dq_reason").isNotNull())
    return valid, quarantined


def validate_metrics(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (valid, quarantined). First failing rule wins, so each bad row
    carries exactly one reason."""
    reason = (
        F.when(F.col("event_id").isNull(), F.lit("unparseable_or_missing_event_id"))
        .when(F.col("event_ts").isNull(), F.lit("invalid_event_ts"))
        .when(F.col("node_id").isNull() | F.col("cluster_id").isNull(), F.lit("missing_node_or_cluster"))
        .when(_pct_invalid("cpu_pct"), F.lit("cpu_pct_out_of_range"))
        .when(_pct_invalid("mem_pct"), F.lit("mem_pct_out_of_range"))
        .when(_pct_invalid("disk_pct"), F.lit("disk_pct_out_of_range"))
        # event claims to be from >10 min in the future: clock skew on the host
        .when(
            F.col("event_ts") > F.col("kafka_ts") + F.expr("INTERVAL 10 MINUTES"),
            F.lit("event_ts_in_future"),
        )
    )
    return _split(df, reason)


def validate_node_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    reason = (
        F.when(F.col("event_id").isNull(), F.lit("unparseable_or_missing_event_id"))
        .when(F.col("event_ts").isNull(), F.lit("invalid_event_ts"))
        .when(F.col("node_id").isNull(), F.lit("missing_node_id"))
        .when(
            F.col("status").isNull() | ~F.col("status").isin(VALID_NODE_STATUSES),
            F.lit("invalid_status"),
        )
        .when(F.col("vcores").isNull() | (F.col("vcores") <= 0), F.lit("invalid_vcores"))
    )
    return _split(df, reason)


def dedupe_latest(df: DataFrame, key: str = "event_id") -> DataFrame:
    """Keep one row per key: the most recently delivered (highest kafka_ts,
    then highest partition/offset as a deterministic tie-break)."""
    w = Window.partitionBy(key).orderBy(F.col("kafka_ts").desc(), F.col("partition").desc(), F.col("offset").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def to_silver_metrics(valid: DataFrame) -> DataFrame:
    return valid.select(
        "event_id",
        "event_ts",
        F.to_date("event_ts").alias("event_date"),
        F.date_trunc("hour", "event_ts").alias("event_hour"),
        "region",
        "cluster_id",
        "node_id",
        "cpu_pct",
        "mem_pct",
        "disk_pct",
        "active_dbs",
        "gpu_pct",
        F.coalesce("schema_version", F.lit(1)).alias("schema_version"),
        "kafka_ts",
        "_ingested_at",
    )


def to_silver_node_events(valid: DataFrame) -> DataFrame:
    return valid.select(
        "event_id",
        "event_ts",
        "change_type",
        "node_id",
        "cluster_id",
        "region",
        "sku",
        "vcores",
        "memory_gb",
        "status",
        "kafka_ts",
        "_ingested_at",
    )


def to_quarantine(quarantined: DataFrame, source: str) -> DataFrame:
    return quarantined.select(
        F.lit(source).alias("source"),
        "_dq_reason",
        "payload",
        *BRONZE_LINEAGE_COLS,
        F.current_timestamp().alias("_quarantined_at"),
        F.current_date().alias("quarantine_date"),
    )
