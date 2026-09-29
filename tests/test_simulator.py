import json
from datetime import datetime, timedelta, timezone

from producer.simulator import TOPIC_METRICS, TOPIC_NODE_CHANGES, FleetSimulator

START = datetime(2026, 9, 1, tzinfo=timezone.utc)


def run(sim, ticks=120):
    msgs = sim.initial_inventory(START)
    for i in range(1, ticks + 1):
        msgs += sim.tick(START + timedelta(minutes=i))
    return msgs


def test_deterministic_for_same_seed():
    assert run(FleetSimulator(seed=7), 20) == run(FleetSimulator(seed=7), 20)


def test_emits_inventory_for_every_node():
    sim = FleetSimulator()
    inv = sim.initial_inventory(START)
    assert len(inv) == len(sim.nodes)
    assert {t for t, _, _ in inv} == {TOPIC_NODE_CHANGES}


def test_dirty_data_is_actually_dirty():
    msgs = [m for m in run(FleetSimulator(seed=1)) if m[0] == TOPIC_METRICS]
    values = [v for _, _, v in msgs]
    assert len(values) != len(set(values)), "expected duplicate deliveries"

    unparseable = 0
    parsed = []
    for v in values:
        try:
            parsed.append(json.loads(v))
        except json.JSONDecodeError:
            unparseable += 1
    out_of_range = [p for p in parsed if not 0 <= p["cpu_pct"] <= 100]
    v2 = [p for p in parsed if p["schema_version"] == 2]
    assert unparseable + len(out_of_range) > 0
    assert v2 and all("gpu_pct" in p for p in v2)


def test_node_keys_partition_by_node():
    for topic, key, value in run(FleetSimulator(), 5):
        if topic == TOPIC_NODE_CHANGES:
            assert json.loads(value)["node_id"] == key
