"""Central configuration. Everything environment-specific comes from env vars so the
same code runs locally (file paths) or on Databricks/ADLS (abfss:// paths)."""

import os
from dataclasses import dataclass

TOPIC_METRICS = "telemetry.metrics"
TOPIC_NODE_CHANGES = "inventory.node_changes"
TOPICS = (TOPIC_METRICS, TOPIC_NODE_CHANGES)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")

# Capacity thresholds used by the gold layer (p95 CPU, percent)
AT_RISK_P95_CPU = float(os.environ.get("AT_RISK_P95_CPU", "80"))
UNDERUTILIZED_P95_CPU = float(os.environ.get("UNDERUTILIZED_P95_CPU", "20"))


@dataclass(frozen=True)
class Paths:
    root: str

    # bronze: raw, append-only, exactly what Kafka delivered
    @property
    def bronze_raw(self) -> str:
        return f"{self.root}/bronze/telemetry_raw"

    # silver: parsed, validated, deduplicated
    @property
    def silver_metrics(self) -> str:
        return f"{self.root}/silver/node_metrics"

    @property
    def silver_node_events(self) -> str:
        return f"{self.root}/silver/node_events"

    @property
    def silver_quarantine(self) -> str:
        return f"{self.root}/silver/quarantine"

    # gold: dimensional model for analytics
    @property
    def gold_dim_node(self) -> str:
        return f"{self.root}/gold/dim_node"

    @property
    def gold_fact_node_hourly(self) -> str:
        return f"{self.root}/gold/fact_node_utilization_hourly"

    @property
    def gold_cluster_daily(self) -> str:
        return f"{self.root}/gold/agg_cluster_capacity_daily"

    # ops: pipeline metadata
    @property
    def ops_watermarks(self) -> str:
        return f"{self.root}/ops/watermarks"

    @property
    def ops_dq_results(self) -> str:
        return f"{self.root}/ops/dq_results"

    def checkpoint(self, name: str) -> str:
        return f"{self.root}/_checkpoints/{name}"


def get_paths() -> Paths:
    return Paths(os.environ.get("LAKEHOUSE_ROOT", "./data/lakehouse"))
