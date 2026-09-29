from conftest import metric, node_event

from lakehouse.config import TOPIC_METRICS, TOPIC_NODE_CHANGES
from lakehouse.transforms import silver as T


def reasons(df):
    return sorted(r._dq_reason for r in df.select("_dq_reason").collect())


def test_valid_metric_passes_and_is_shaped_for_silver(make_bronze):
    bronze = make_bronze([(TOPIC_METRICS, metric())])
    valid, bad = T.validate_metrics(T.parse_metrics(bronze))
    assert bad.count() == 0
    row = T.to_silver_metrics(valid).first()
    assert row.event_hour.hour == 10 and str(row.event_date) == "2026-09-01"
    assert row.schema_version == 1 and row.gpu_pct is None


def test_schema_v2_field_is_kept(make_bronze):
    bronze = make_bronze([(TOPIC_METRICS, metric(schema_version=2, gpu_pct=12.5))])
    valid, _ = T.validate_metrics(T.parse_metrics(bronze))
    assert T.to_silver_metrics(valid).first().gpu_pct == 12.5


def test_bad_records_are_quarantined_with_one_reason_each(make_bronze):
    bronze = make_bronze(
        [
            (TOPIC_METRICS, '{"event_id": "trunc'),
            (TOPIC_METRICS, metric("e2", cpu=187.3)),
            (TOPIC_METRICS, metric("e3", node=None)),
            (TOPIC_METRICS, metric("e4", ts="not-a-timestamp")),
            (TOPIC_METRICS, metric("e5", ts="2026-09-02T10:00:00+00:00")),  # a day ahead of kafka_ts
            (TOPIC_METRICS, metric("e6")),
        ]
    )
    valid, bad = T.validate_metrics(T.parse_metrics(bronze))
    assert [r.event_id for r in valid.collect()] == ["e6"]
    assert reasons(bad) == [
        "cpu_pct_out_of_range",
        "event_ts_in_future",
        "invalid_event_ts",
        "missing_node_or_cluster",
        "unparseable_or_missing_event_id",
    ]
    q = T.to_quarantine(bad, "metrics")
    assert q.where("payload IS NULL").count() == 0, "raw payload must be kept for replay"


def test_parse_only_reads_its_own_topic(make_bronze):
    bronze = make_bronze(
        [(TOPIC_METRICS, metric()), (TOPIC_NODE_CHANGES, node_event("n1", "2026-09-01T09:00:00+00:00"))]
    )
    assert T.parse_metrics(bronze).count() == 1
    assert T.parse_node_events(bronze).count() == 1


def test_dedupe_keeps_one_row_per_event_id(make_bronze):
    bronze = make_bronze([(TOPIC_METRICS, metric("e1")), (TOPIC_METRICS, metric("e1")), (TOPIC_METRICS, metric("e2"))])
    valid, _ = T.validate_metrics(T.parse_metrics(bronze))
    deduped = T.dedupe_latest(valid)
    assert deduped.count() == 2
    # the later delivery (higher offset) wins
    assert deduped.where("event_id = 'e1'").first().offset == 1


def test_node_event_validation(make_bronze):
    bronze = make_bronze(
        [
            (TOPIC_NODE_CHANGES, node_event("n1", "2026-09-01T09:00:00+00:00")),
            (TOPIC_NODE_CHANGES, node_event("n2", "2026-09-01T09:00:00+00:00", status="exploded")),
            (TOPIC_NODE_CHANGES, node_event("n3", "2026-09-01T09:00:00+00:00", status=None)),
            (TOPIC_NODE_CHANGES, node_event("n4", "2026-09-01T09:00:00+00:00", vcores=0)),
        ]
    )
    valid, bad = T.validate_node_events(T.parse_node_events(bronze))
    assert valid.count() == 1
    assert reasons(bad) == ["invalid_status", "invalid_status", "invalid_vcores"]
