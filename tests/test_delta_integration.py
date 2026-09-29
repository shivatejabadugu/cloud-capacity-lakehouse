"""End-to-end checks against real Delta tables. Run with:
    WITH_DELTA=1 pytest -m integration
(first run downloads the Delta jars from Maven Central)."""

import importlib
import os

import pytest
from conftest import metric, node_event

from lakehouse.config import TOPIC_METRICS, TOPIC_NODE_CHANGES
from lakehouse.dq import checks as C

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not os.environ.get("WITH_DELTA"), reason="set WITH_DELTA=1"),
]


@pytest.fixture
def jobs(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    from lakehouse.jobs import silver_metrics, silver_node_events

    importlib.reload(silver_metrics)  # module-level paths pick up the temp root
    importlib.reload(silver_node_events)
    return silver_metrics, silver_node_events


def test_silver_merge_is_idempotent_under_replay_and_retry(spark, make_bronze, jobs):
    silver_metrics, _ = jobs
    batch = make_bronze(
        [
            (TOPIC_METRICS, metric("e1")),
            (TOPIC_METRICS, metric("e1")),  # duplicate delivery
            (TOPIC_METRICS, metric("e2")),
            (TOPIC_METRICS, '{"broken'),
        ]
    )
    silver_metrics.process_batch(batch, 0)
    silver_metrics.process_batch(batch, 1)  # full replay of the same data
    silver_metrics.process_batch(batch, 0)  # retry of an already-committed batch

    p = silver_metrics.paths
    assert spark.read.format("delta").load(p.silver_metrics).count() == 2
    # quarantine: batch 0 and batch 1 each wrote one row; the retry of batch 0 was skipped
    assert spark.read.format("delta").load(p.silver_quarantine).count() == 2


def test_dim_node_incremental_scd2(spark, make_bronze, jobs):
    _, silver_node_events = jobs
    from lakehouse.config import get_paths
    from lakehouse.jobs import gold_dim_node

    paths = get_paths()
    silver_node_events.process_batch(
        make_bronze([(TOPIC_NODE_CHANGES, node_event("a", "2026-09-01T08:00:00+00:00"))]), 0
    )
    gold_dim_node.main()
    assert spark.read.format("delta").load(paths.gold_dim_node).count() == 1

    silver_node_events.process_batch(
        make_bronze([(TOPIC_NODE_CHANGES, node_event("b", "2026-09-01T12:00:00+00:00", sku="Gen5_16", vcores=16))]),
        1,
    )
    gold_dim_node.main()
    dim = spark.read.format("delta").load(paths.gold_dim_node)
    assert dim.count() == 2
    assert dim.where("is_current").first().vcores == 16
    assert all(r.passed for r in C.scd2_integrity(dim))

    gold_dim_node.main()  # nothing new: no change
    assert spark.read.format("delta").load(paths.gold_dim_node).count() == 2
