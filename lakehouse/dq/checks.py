"""Data quality checks. Each returns a CheckResult; the dq job persists them and
fails the Airflow task if any critical check fails. Written as plain PySpark so
they're easy to read in an interview and run anywhere (no framework lock-in)."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

CRITICAL = "critical"
WARN = "warn"


@dataclass
class CheckResult:
    table: str
    check: str
    severity: str
    passed: bool
    observed: float
    threshold: float

    def as_row(self) -> dict:
        return asdict(self)


def unique(df: DataFrame, table: str, cols: list[str], severity: str = CRITICAL) -> CheckResult:
    dupes = df.groupBy(*cols).count().where("count > 1").count()
    return CheckResult(table, f"unique({','.join(cols)})", severity, dupes == 0, dupes, 0)


def in_range(df: DataFrame, table: str, col: str, lo: float, hi: float, severity: str = CRITICAL) -> CheckResult:
    bad = df.where(F.col(col).isNotNull() & ~F.col(col).between(lo, hi)).count()
    return CheckResult(table, f"range({col},{lo},{hi})", severity, bad == 0, bad, 0)


def null_rate(df: DataFrame, table: str, col: str, max_rate: float, severity: str = WARN) -> CheckResult:
    row = df.agg(F.count("*").alias("n"), F.sum(F.col(col).isNull().cast("int")).alias("nulls")).first()
    rate = (row.nulls or 0) / row.n if row.n else 0.0
    return CheckResult(table, f"null_rate({col})", severity, rate <= max_rate, round(rate, 4), max_rate)


def freshness(
    df: DataFrame,
    table: str,
    ts_col: str,
    max_lag_minutes: float,
    now: Column | None = None,
    severity: str = WARN,
) -> CheckResult:
    now = now if now is not None else F.current_timestamp()
    lag = df.agg(((F.unix_timestamp(now) - F.unix_timestamp(F.max(ts_col))) / 60).alias("lag")).first().lag
    lag = float("inf") if lag is None else round(lag, 1)
    return CheckResult(table, f"freshness({ts_col})", severity, lag <= max_lag_minutes, lag, max_lag_minutes)


def ratio(
    bad_count: int, good_count: int, table: str, check: str, max_rate: float, severity: str = WARN
) -> CheckResult:
    total = bad_count + good_count
    rate = bad_count / total if total else 0.0
    return CheckResult(table, check, severity, rate <= max_rate, round(rate, 4), max_rate)


def scd2_integrity(dim: DataFrame, table: str = "gold.dim_node") -> list[CheckResult]:
    """Every node has exactly one current version, and versions tile time with no
    gaps or overlaps (each effective_to equals the next version's effective_from)."""
    current_counts = dim.groupBy("node_id").agg(F.sum(F.col("is_current").cast("int")).alias("c"))
    bad_current = current_counts.where("c <> 1").count()

    w = Window.partitionBy("node_id").orderBy("effective_from")
    broken = (
        dim.withColumn("_next_from", F.lead("effective_from").over(w))
        .where(F.col("_next_from").isNotNull() & (F.col("_next_from") != F.col("effective_to")))
        .count()
    )
    return [
        CheckResult(table, "scd2_one_current_per_key", CRITICAL, bad_current == 0, bad_current, 0),
        CheckResult(table, "scd2_contiguous_versions", CRITICAL, broken == 0, broken, 0),
    ]
