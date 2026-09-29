.PHONY: up down logs password run-once show test lint clean

up:            ## build and start kafka, producer, airflow
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f producer airflow

password:      ## airflow standalone admin password
	docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt

run-once:      ## run the whole pipeline once without Airflow
	docker compose exec airflow bash -c "cd /opt/project && \
	  python -m lakehouse.jobs.bronze_ingest && \
	  python -m lakehouse.jobs.silver_metrics && \
	  python -m lakehouse.jobs.silver_node_events && \
	  python -m lakehouse.jobs.gold_dim_node && \
	  python -m lakehouse.jobs.gold_fact_node_hourly && \
	  python -m lakehouse.jobs.gold_cluster_capacity_daily && \
	  python -m lakehouse.jobs.dq_checks"

show:          ## print gold tables
	docker compose exec airflow python -m lakehouse.jobs.show_gold

test:          ## unit tests (no Delta jars needed)
	pytest -m "not integration" -q

lint:
	ruff check .

clean:         ## wipe lakehouse data and checkpoints (keeps Kafka topics)
	rm -rf data/lakehouse

test-integration:  ## Delta MERGE/SCD2 tests (downloads Delta jars first time)
	WITH_DELTA=1 pytest -m integration -q
