import json
import os
import time
from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_SCHEMA = StructType(
    [
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("kafka_ts", TimestampType()),
        StructField("_ingested_at", TimestampType()),
        StructField("payload", StringType()),
    ]
)

# Python-side datetimes are interpreted in the local zone; pin it so tests
# behave the same on a laptop and in CI.
os.environ["TZ"] = "UTC"
time.tzset()

T0 = datetime(2026, 9, 1, 10, 0, 0)


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    if os.environ.get("WITH_DELTA"):
        from lakehouse.spark import DELTA_PACKAGE

        builder = (
            builder.config("spark.jars.packages", DELTA_PACKAGE)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    s = builder.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def make_bronze(spark):
    """Build a bronze-shaped DataFrame from (topic, payload) pairs.
    Payload may be a dict (serialised to JSON) or a raw string."""

    def _make(rows):
        out = []
        for i, (topic, payload) in enumerate(rows):
            body = payload if isinstance(payload, str) else json.dumps(payload)
            ts = T0 + timedelta(seconds=i)
            out.append((topic, 0, i, ts, ts, body))
        return spark.createDataFrame(out, BRONZE_SCHEMA)

    return _make


def metric(event_id="e1", node="eastus-c01-n001", cluster="eastus-c01", ts="2026-09-01T10:00:00+00:00", cpu=50.0, **kw):
    base = {
        "event_id": event_id,
        "schema_version": 1,
        "event_ts": ts,
        "region": "eastus",
        "cluster_id": cluster,
        "node_id": node,
        "cpu_pct": cpu,
        "mem_pct": 60.0,
        "disk_pct": 40.0,
        "active_dbs": 10,
    }
    base.update(kw)
    return base


def node_event(event_id, ts, node="eastus-c01-n001", cluster="eastus-c01", sku="Gen5_8", vcores=8, status="active"):
    return {
        "event_id": event_id,
        "event_ts": ts,
        "change_type": "resized",
        "node_id": node,
        "cluster_id": cluster,
        "region": "eastus",
        "sku": sku,
        "vcores": vcores,
        "memory_gb": vcores * 5,
        "status": status,
    }
