"""Nightly OPTIMIZE / Z-ORDER / VACUUM."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="lakehouse_maintenance",
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse"],
) as dag:
    BashOperator(
        task_id="optimize_and_vacuum",
        bash_command="cd /opt/project && python -m lakehouse.jobs.maintenance",
    )
