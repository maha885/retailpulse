"""
retailpulse_pipeline.py
Airflow DAG for the RetailPulse local ELT pipeline.

Full scope, now fully orchestrated end-to-end:
    generate synthetic data -> land in MinIO (raw)
    -> Spark transform, raw -> silver (SCD2 dimension included)
    -> load silver Parquet into Postgres staging tables
    -> dbt deps / dbt run (staging -> gold marts)
    -> dbt test (data quality checks, incl. dbt-expectations, on the gold layer)

The Spark step runs inside a custom Airflow image (see Dockerfile.airflow) that
bakes in a JRE -- the stock Airflow image has no JVM at all, so spark-submit
couldn't run in it. Building a custom image also sidestepped a class of pandas/
SQLAlchemy version conflicts that runtime pip installs (_PIP_ADDITIONAL_REQUIREMENTS)
had been running into against Airflow's own constraints file.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "deba",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retailpulse_pipeline",
    description="End-to-end local ELT pipeline: generate -> land -> transform -> load -> model -> test",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retailpulse", "portfolio"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_synthetic_orders",
        bash_command=(
            "python /opt/airflow/ingestion/generate_data.py "
            "--rows 5000 --out /tmp/orders_raw.csv"
        ),
    )

    land_to_minio = BashOperator(
        task_id="land_raw_to_minio",
        bash_command=(
            "python /opt/airflow/ingestion/load_to_minio.py "
            "--file /tmp/orders_raw.csv --bucket raw --key orders/{{ ds }}/orders_raw.csv"
        ),
    )

    spark_transform = BashOperator(
        task_id="spark_transform_raw_to_silver",
        bash_command=(
            "spark-submit "
            "/opt/airflow/spark_jobs/transform_orders.py --run-date {{ ds }}"
        ),
    )

    load_silver_to_postgres = BashOperator(
        task_id="load_silver_to_postgres",
        bash_command=(
            "python /opt/airflow/ingestion/load_silver_to_postgres.py --run-date {{ ds }}"
        ),
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps",
    )

    dbt_run = BashOperator(
        task_id="dbt_run_staging_to_gold",
        bash_command="cd /opt/airflow/dbt && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_gold_layer",
        bash_command="cd /opt/airflow/dbt && dbt test",
    )

    (
        generate_data
        >> land_to_minio
        >> spark_transform
        >> load_silver_to_postgres
        >> dbt_deps
        >> dbt_run
        >> dbt_test
    )
