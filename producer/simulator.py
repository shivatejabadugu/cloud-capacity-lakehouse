"""Simulates a fleet of database host nodes across Azure-style regions.

Emits two event streams:
  * telemetry.metrics       one utilisation sample per active node per tick
  * inventory.node_changes  full-state snapshot whenever a node is provisioned,
                            resized, moved to another cluster, or decommissioned

The data is deliberately dirty so the downstream pipeline has real work to do:
duplicate deliveries, late-arriving events, malformed JSON, out-of-range values,
missing keys, and a schema change (v2 agents add gpu_pct).

Pure Python and seeded, so it's deterministic and unit-testable without Kafka.
"""

from __future__ import annotations

import json
import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

TOPIC_METRICS = "telemetry.metrics"
TOPIC_NODE_CHANGES = "inventory.node_changes"

REGIONS = {"eastus": 4, "westeurope": 3, "southeastasia": 2}  # clusters per region
NODES_PER_CLUSTER = 8
SKUS = {"Gen5_8": (8, 40), "Gen5_16": (16, 80), "Gen5_32": (32, 160)}  # vcores, memory_gb
SKU_ORDER = list(SKUS)

Message = tuple[str, str, str]  # (topic, key, value)


@dataclass
class Node:
    node_id: str
    cluster_id: str
    region: str
    sku: str
    base_load: float
    agent_v2: bool
    status: str = "active"

    def snapshot(self, change_type: str, ts: datetime, rng: random.Random) -> dict:
        vcores, memory_gb = SKUS[self.sku]
        return {
            "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "event_ts": ts.isoformat(),
            "change_type": change_type,
            "node_id": self.node_id,
            "cluster_id": self.cluster_id,
            "region": self.region,
            "sku": self.sku,
            "vcores": vcores,
            "memory_gb": memory_gb,
            "status": self.status,
        }


class FleetSimulator:
    def __init__(
        self,
        seed: int = 42,
        dup_rate: float = 0.02,
        late_rate: float = 0.01,
        bad_rate: float = 0.005,
        change_rate: float = 0.02,
    ):
        self.rng = random.Random(seed)
        self.dup_rate = dup_rate
        self.late_rate = late_rate
        self.bad_rate = bad_rate
        self.change_rate = change_rate
        self.nodes: list[Node] = []
        # A couple of clusters trend hot so the gold layer has "at_risk" findings
        self.hot_clusters: set[str] = set()
        for region, n_clusters in REGIONS.items():
            for c in range(1, n_clusters + 1):
                cluster_id = f"{region}-c{c:02d}"
                for n in range(1, NODES_PER_CLUSTER + 1):
                    self.nodes.append(
                        Node(
                            node_id=f"{cluster_id}-n{n:03d}",
                            cluster_id=cluster_id,
                            region=region,
                            sku=self.rng.choice(SKU_ORDER[:2]),
                            base_load=self.rng.uniform(10, 55),
                            agent_v2=self.rng.random() < 0.15,
                        )
                    )
        clusters = sorted({n.cluster_id for n in self.nodes})
        self.hot_clusters = set(self.rng.sample(clusters, 2))

    # ------------------------------------------------------------------ inventory
    def initial_inventory(self, ts: datetime) -> list[Message]:
        return [self._node_msg(n, "provisioned", ts) for n in self.nodes]

    def _node_msg(self, node: Node, change_type: str, ts: datetime) -> Message:
        body = node.snapshot(change_type, ts, self.rng)
        return (TOPIC_NODE_CHANGES, node.node_id, json.dumps(body))

    def _maybe_change_a_node(self, ts: datetime) -> list[Message]:
        if self.rng.random() >= self.change_rate:
            return []
        active = [n for n in self.nodes if n.status == "active"]
        if not active:
            return []
        node = self.rng.choice(active)
        roll = self.rng.random()
        if roll < 0.6 and node.sku != SKU_ORDER[-1]:
            node.sku = SKU_ORDER[SKU_ORDER.index(node.sku) + 1]
            change = "resized"
        elif roll < 0.9:
            siblings = sorted({n.cluster_id for n in self.nodes if n.region == node.region} - {node.cluster_id})
            if not siblings:
                return []
            node.cluster_id = self.rng.choice(siblings)
            change = "moved"
        else:
            node.status = "decommissioned"
            change = "decommissioned"
        return [self._node_msg(node, change, ts)]

    # ------------------------------------------------------------------ metrics
    def _cpu(self, node: Node, ts: datetime) -> float:
        # diurnal curve peaking mid-afternoon UTC, plus noise
        hour = ts.hour + ts.minute / 60
        diurnal = 18 * math.sin((hour - 9) / 24 * 2 * math.pi)
        # bigger SKUs spread the same workload over more cores
        size_factor = {"Gen5_8": 1.0, "Gen5_16": 0.75, "Gen5_32": 0.55}[node.sku]
        # hot clusters carry extra tenant load regardless of node size
        hot = 40 if node.cluster_id in self.hot_clusters else 0
        cpu = node.base_load * size_factor + diurnal + hot + self.rng.gauss(0, 5)
        return round(min(max(cpu, 0.5), 99.5), 2)

    def _metric_event(self, node: Node, ts: datetime) -> dict:
        cpu = self._cpu(node, ts)
        event = {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "schema_version": 2 if node.agent_v2 else 1,
            "event_ts": ts.isoformat(),
            "region": node.region,
            "cluster_id": node.cluster_id,
            "node_id": node.node_id,
            "cpu_pct": cpu,
            "mem_pct": round(min(99.0, 35 + cpu * 0.5 + self.rng.gauss(0, 4)), 2),
            "disk_pct": round(min(99.0, 40 + self.rng.uniform(0, 30)), 2),
            "active_dbs": self.rng.randint(5, 60),
        }
        if node.agent_v2:
            event["gpu_pct"] = round(self.rng.uniform(0, 40), 2)
        return event

    def _corrupt(self, event: dict) -> str:
        kind = self.rng.choice(["truncated", "cpu_out_of_range", "null_node", "bad_ts"])
        if kind == "truncated":
            return json.dumps(event)[: self.rng.randint(10, 40)]
        if kind == "cpu_out_of_range":
            event["cpu_pct"] = self.rng.choice([-5.0, 187.3])
        elif kind == "null_node":
            event["node_id"] = None
        else:
            event["event_ts"] = "not-a-timestamp"
        return json.dumps(event)

    def tick(self, ts: datetime) -> list[Message]:
        """All messages produced at simulated time `ts`, in send order."""
        out: list[Message] = self._maybe_change_a_node(ts)
        for node in self.nodes:
            if node.status != "active":
                continue
            event_ts = ts
            if self.rng.random() < self.late_rate:
                event_ts = ts - timedelta(minutes=self.rng.randint(5, 45))
            event = self._metric_event(node, event_ts)
            if self.rng.random() < self.bad_rate:
                out.append((TOPIC_METRICS, node.node_id, self._corrupt(event)))
                continue
            msg = (TOPIC_METRICS, node.node_id, json.dumps(event))
            out.append(msg)
            if self.rng.random() < self.dup_rate:
                out.append(msg)  # at-least-once redelivery: same event_id twice
        return out


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)
