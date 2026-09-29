"""Explicit schemas for the JSON payloads. Never infer schemas on streaming data:
inference is slow and a single odd record silently changes column types."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

METRIC_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("event_ts", StringType()),
        StructField("region", StringType()),
        StructField("cluster_id", StringType()),
        StructField("node_id", StringType()),
        StructField("cpu_pct", DoubleType()),
        StructField("mem_pct", DoubleType()),
        StructField("disk_pct", DoubleType()),
        StructField("active_dbs", IntegerType()),
        # added in schema_version 2 by newer telemetry agents; null for v1 events
        StructField("gpu_pct", DoubleType()),
    ]
)

NODE_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("change_type", StringType()),
        StructField("node_id", StringType()),
        StructField("cluster_id", StringType()),
        StructField("region", StringType()),
        StructField("sku", StringType()),
        StructField("vcores", IntegerType()),
        StructField("memory_gb", IntegerType()),
        StructField("status", StringType()),
    ]
)

# Columns carried from bronze into every downstream row, for lineage/debugging
BRONZE_LINEAGE_COLS = ["topic", "partition", "offset", "kafka_ts", "_ingested_at"]
