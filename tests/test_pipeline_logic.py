"""Runs the full bronze -> silver -> gold transform chain on 6 simulated hours of
fleet data, in memory (no Kafka, no Delta). Catches wiring mistakes between
layers and checks the output against what the simulator was told to do."""

from datetime import UTC, datetime, timedelta

from pyspark.sql import functions as F

from lakehouse.dq import checks as C
from lakehouse.transforms import gold as G
from lakehouse.transforms import silver as T
from producer.simulator import FleetSimulator

# afternoon UTC, when the simulator's diurnal load curve peaks
START = datetime(2026, 9, 1, 12, tzinfo=UTC)


def test_full_chain_on_simulated_fleet(spark):
    sim = FleetSimulator(seed=11, change_rate=0.05)
    msgs = sim.initial_inventory(START - timedelta(hours=1))
    for i in range(1, 6 * 60 + 1):  # one tick per minute for 6 hours
        msgs += sim.tick(START + timedelta(minutes=i))

    rows = []
    for offset, (topic, _key, value) in enumerate(msgs):
        # backfilled history is delivered after the fact, so broker time is "now"
        delivered = START + timedelta(hours=6, minutes=1)
        rows.append((topic, 0, offset, delivered, delivered, value))
    bronze = spark.createDataFrame(
        rows, "topic string, partition int, offset long, kafka_ts timestamp, _ingested_at timestamp, payload string"
    ).cache()

    m_valid, m_bad = T.validate_metrics(T.parse_metrics(bronze))
    metrics = T.to_silver_metrics(T.dedupe_latest(m_valid)).cache()
    n_valid, _ = T.validate_node_events(T.parse_node_events(bronze))
    node_events = T.to_silver_node_events(T.dedupe_latest(n_valid))

    dim = G.build_scd2(node_events).cache()
    fact = G.attach_node_dim(G.hourly_node_utilization(metrics), dim).cache()
    daily = G.cluster_capacity_daily(fact)

    # silver
    assert C.unique(metrics, "silver", ["event_id"]).passed
    reasons = {r._dq_reason for r in m_bad.select("_dq_reason").distinct().collect()}
    assert "event_ts_in_future" not in reasons
    bad_rate = m_bad.count() / (m_bad.count() + metrics.count())
    assert 0 < bad_rate < 0.02
    # the simulator changed some nodes, so some have multiple versions
    assert dim.groupBy("node_id").count().where("count > 1").count() > 0
    assert all(r.passed for r in C.scd2_integrity(dim))
    # gold
    assert C.unique(fact, "fact", ["node_id", "hour_start"]).passed
    assert C.null_rate(fact, "fact", "node_sk", max_rate=0.01).passed
    statuses = {r.capacity_status for r in daily.select("capacity_status").distinct().collect()}
    assert "at_risk" in statuses, "the simulator's hot clusters should be flagged"
    hot = {r.cluster_id for r in daily.where(F.col("capacity_status") == "at_risk").collect()}
    assert hot <= sim.hot_clusters
