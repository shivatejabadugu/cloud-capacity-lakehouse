# One image for Airflow + Spark (local mode) + the producer.
FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements.txt /tmp/requirements.txt
# Pin airflow in the same install so pip can't up/downgrade it underneath us
RUN pip install --no-cache-dir "apache-airflow==2.10.4" -r /tmp/requirements.txt

# Pre-fetch the Delta + Kafka jars into the Ivy cache at build time, so jobs
# don't hit Maven Central on every run.
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]') \
.config('spark.jars.packages','io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
.getOrCreate().stop()"

ENV PYTHONPATH=/opt/project
WORKDIR /opt/project
