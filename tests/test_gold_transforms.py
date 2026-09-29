from datetime import datetime

import pytest
from pyspark.sql import functions as F

from lakehouse.dq import checks as C
from lakehouse.transforms import gold as G

NODE = "eastus-c01-n001"


@pytest.fixture
def node_events(spark):
    def _make(rows):
        # rows: (event_id, ts, cluster, sku, vcores, status)
        return spark.createDataFrame(
            [
                (eid, datetime.fromisoformat(ts), NODE, cl, "eastus", sku, vc, vc * 5, st)
                for eid, ts, cl, sku, vc, st in rows
            ],
            "event_id string, event_ts timestamp, node_id string, cluster_id string, region string, "
            "sku string, vcores int, memory_gb int, status string",
        )

    return _make


def test_scd2_versions_changes_and_collapses_repeats(node_events):
    dim = G.build_scd2(
        node_events(
            [
                ("a", "2026-09-01 08:00:00", "eastus-c01", "Gen5_8", 8, "active"),
                ("b", "2026-09-01 09:00:00", "eastus-c01", "Gen5_8", 8, "active"),  # re-sent snapshot
                ("c", "2026-09-01 12:00:00", "eastus-c01", "Gen5_16", 16, "active"),  # resize
                ("d", "2026-09-01 15:00:00", "eastus-c02", "Gen5_16", 16, "active"),  # move
            ]
        )
    ).orderBy("effective_from")
    rows = dim.collect()
    assert [r.sku for r in rows] == ["Gen5_8", "Gen5_16", "Gen5_16"]
    assert [r.is_current for r in rows] == [False, False, True]
    assert rows[0].effective_to == rows[1].effective_from == datetime(2026, 9, 1, 12)
    assert rows[-1].effective_to.year == 9999
    assert all(r.passed for r in C.scd2_integrity(dim))


def test_scd2_is_order_independent(node_events):
    """Late / out-of-order events must produce the same history."""
    rows = [
        ("a", "2026-09-01 08:00:00", "eastus-c01", "Gen5_8", 8, "active"),
        ("c", "2026-09-01 12:00:00", "eastus-c01", "Gen5_16", 16, "active"),
        ("e", "2026-09-01 18:00:00", "eastus-c01", "Gen5_16", 16, "decommissioned"),
    ]
    in_order = G.build_scd2(node_events(rows)).drop("node_sk")
    shuffled = G.build_scd2(node_events(list(reversed(rows)))).drop("node_sk")
    assert sorted(in_order.collect()) == sorted(shuffled.collect())


@pytest.fixture
def silver_metrics(spark):
    def _make(rows):
        # rows: (event_id, ts, cluster, cpu)
        df = spark.createDataFrame(
            [(e, datetime.fromisoformat(ts), NODE, cl, "eastus", float(cpu)) for e, ts, cl, cpu in rows],
            "event_id string, event_ts timestamp, node_id string, cluster_id string, region string, cpu_pct double",
        )
        return (
            df.withColumn("event_hour", F.date_trunc("hour", "event_ts"))
            .withColumn("mem_pct", F.lit(50.0))
            .withColumn("disk_pct", F.lit(40.0))
            .withColumn("active_dbs", F.lit(10))
        )

    return _make


def test_hourly_grain_is_one_row_per_node_hour_even_after_a_move(silver_metrics):
    hourly = G.hourly_node_utilization(
        silver_metrics(
            [
                ("1", "2026-09-01 10:05:00", "eastus-c01", 20),
                ("2", "2026-09-01 10:35:00", "eastus-c02", 40),  # moved mid-hour
                ("3", "2026-09-01 11:10:00", "eastus-c02", 60),
            ]
        )
    ).orderBy("hour_start")
    rows = hourly.collect()
    assert len(rows) == 2
    assert rows[0].cluster_id == "eastus-c02"  # attributed to where it ended the hour
    assert rows[0].sample_count == 2 and rows[0].avg_cpu_pct == 30.0
    assert C.unique(hourly, "fact", ["node_id", "hour_start"]).passed


def test_point_in_time_join_uses_the_version_in_effect(node_events, silver_metrics):
    dim = G.build_scd2(
        node_events(
            [
                ("a", "2026-09-01 08:00:00", "eastus-c01", "Gen5_8", 8, "active"),
                ("c", "2026-09-01 12:00:00", "eastus-c01", "Gen5_16", 16, "active"),
            ]
        )
    )
    hourly = G.hourly_node_utilization(
        silver_metrics(
            [
                ("1", "2026-09-01 10:30:00", "eastus-c01", 50),
                ("2", "2026-09-01 13:30:00", "eastus-c01", 50),
                ("3", "2026-09-01 07:30:00", "eastus-c01", 50),  # before node config exists
            ]
        )
    )
    fact = {r.hour_start.hour: r for r in G.attach_node_dim(hourly, dim).collect()}
    assert fact[10].vcores == 8 and fact[10].used_vcores_p95 == 4.0
    assert fact[13].vcores == 16 and fact[13].used_vcores_p95 == 8.0
    assert fact[7].node_sk is None, "late-arriving dimension should be kept as an orphan, not dropped"


def test_cluster_capacity_status(spark):
    fact = spark.createDataFrame(
        [
            # hot cluster: 2 nodes x 8 vcores, ~7 used each
            (1, "n1", "hot", "eastus", datetime(2026, 9, 1, h), 8, 7.0)
            for h in range(24)
        ]
        + [(2, "n2", "hot", "eastus", datetime(2026, 9, 1, h), 8, 6.8) for h in range(24)]
        + [(3, "n3", "idle", "eastus", datetime(2026, 9, 1, h), 16, 1.0) for h in range(24)]
        + [(4, "n4", "ok", "eastus", datetime(2026, 9, 1, h), 16, 8.0) for h in range(24)],
        "node_sk long, node_id string, cluster_id string, region string, hour_start timestamp, "
        "vcores int, used_vcores_p95 double",
    )
    daily = {r.cluster_id: r for r in G.cluster_capacity_daily(fact).collect()}
    assert daily["hot"].capacity_status == "at_risk"
    assert daily["idle"].capacity_status == "underutilized"
    assert daily["ok"].capacity_status == "healthy"
    assert daily["hot"].provisioned_vcores == 16 and daily["hot"].hours_observed == 24


def test_dq_checks_catch_problems(spark):
    df = spark.createDataFrame([("a", 50.0), ("a", 150.0), ("b", None)], "id string, v double")
    assert not C.unique(df, "t", ["id"]).passed
    assert not C.in_range(df, "t", "v", 0, 100).passed
    nr = C.null_rate(df, "t", "v", max_rate=0.2)
    assert not nr.passed and nr.observed == pytest.approx(0.3333, abs=1e-3)
    assert C.ratio(1, 99, "t", "q", 0.02).passed
    assert not C.ratio(5, 95, "t", "q", 0.02).passed
