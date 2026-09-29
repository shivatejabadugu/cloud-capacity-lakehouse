"""Small Delta Lake helpers shared by the jobs."""

from __future__ import annotations

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from lakehouse.config import Paths


def ensure_table(spark: SparkSession, like: DataFrame, path: str, partition_by: list[str] | None = None) -> DeltaTable:
    """Create an empty Delta table with `like`'s schema if one doesn't exist yet."""
    if not DeltaTable.isDeltaTable(spark, path):
        writer = like.limit(0).write.format("delta")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
    return DeltaTable.forPath(spark, path)


def read_pinned(spark: SparkSession, path: str) -> tuple[DataFrame, int]:
    """Read a table at an explicit version. Every action on the returned frame sees
    the same snapshot, so a watermark computed from it can't skip rows that a
    concurrent writer committed between two reads."""
    version = DeltaTable.forPath(spark, path).history(1).select("version").first()[0]
    df = spark.read.format("delta").option("versionAsOf", version).load(path)
    return df, version


def in_list_sql(column: str, values) -> str:
    """Render `column IN (...)` with literals. Used to give MERGE a static predicate
    on the target's partition column so Delta prunes files instead of scanning
    the whole table."""
    rendered = ", ".join(f"'{v}'" for v in values)
    return f"{column} IN ({rendered})"


# --------------------------------------------------------------------------- watermarks
_WM_SCHEMA = StructType(
    [
        StructField("job_name", StringType(), False),
        StructField("high_watermark", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


def get_watermark(spark: SparkSession, paths: Paths, job_name: str) -> datetime | None:
    if not DeltaTable.isDeltaTable(spark, paths.ops_watermarks):
        return None
    row = (
        spark.read.format("delta")
        .load(paths.ops_watermarks)
        .where(F.col("job_name") == job_name)
        .select("high_watermark")
        .first()
    )
    return row[0] if row else None


def set_watermark(spark: SparkSession, paths: Paths, job_name: str, value: datetime) -> None:
    src = spark.createDataFrame([(job_name, value, None)], _WM_SCHEMA).withColumn("updated_at", F.current_timestamp())
    table = ensure_table(spark, src, paths.ops_watermarks)
    (
        table.alias("t")
        .merge(src.alias("s"), "t.job_name = s.job_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
