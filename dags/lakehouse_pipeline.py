"""Incremental lakehouse pipeline, every 15 minutes.

bronze_ingest -> silver (metrics, node_events in parallel) -> gold dim ->
gold fact -> gold daily agg -> data quality gate.

Streaming jobs run with trigger(availableNow=True): they process everything new
since their checkpoint and stop. That gives streaming's exactly-once bookkeeping
with batch's cost profile (no always-on cluster).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"


def job(task_id: str, module: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT} && python -m lakehouse.jobs.{module}",
    )


with DAG(
    dag_id="lakehouse_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=15),
    catchup=False,
    # one run at a time: checkpoints and watermarks assume a single writer
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse"],
) as dag:
    bronze = job("bronze_ingest", "bronze_ingest")
    silver_metrics = job("silver_metrics", "silver_metrics")
    silver_nodes = job("silver_node_events", "silver_node_events")
    dim_node = job("gold_dim_node", "gold_dim_node")
    fact = job("gold_fact_node_hourly", "gold_fact_node_hourly")
    daily = job("gold_cluster_capacity_daily", "gold_cluster_capacity_daily")
    dq = BashOperator(
        task_id="dq_checks",
        bash_command=f"cd {PROJECT} && python -m lakehouse.jobs.dq_checks",
        retries=0,  # a failed quality gate should page, not retry
    )

    bronze >> [silver_metrics, silver_nodes]
    silver_nodes >> dim_node
    [silver_metrics, dim_node] >> fact >> daily >> dq
