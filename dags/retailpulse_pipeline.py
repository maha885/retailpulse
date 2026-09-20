"""
retailpulse_pipeline.py
Airflow DAG for the RetailPulse local ELT pipeline.

Current scope:
    generate synthetic data -> land in MinIO (raw)
    -> [run manually for now: Spark transform, raw -> silver -- see spark_jobs/]
    -> load silver Parquet into Postgres staging tables
    -> dbt run (staging -> gold marts)
    -> dbt test (data quality checks on the gold layer)

The Spark transform step is NOT in this DAG yet because the official Airflow
image has no JVM, and installing one needs a custom Dockerfile (a good Week 4+
follow-up). Everything else here runs fine inside the stock Airflow image since
dbt-core talks to Postgres directly with no JVM involved.
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
    description="End-to-end local ELT pipeline: generate -> land -> load -> model -> test",
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

    # --- Placeholder: run manually for now (see spark_jobs/transform_orders.py) ---
    # spark_transform = BashOperator(
    #     task_id="spark_transform_raw_to_silver",
    #     bash_command="spark-submit /opt/airflow/spark_jobs/transform_orders.py --run-date {{ ds }}",
    # )

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

    # --- Placeholder for Week 4 ---
    # ge_validate = BashOperator(
    #     task_id="validate_with_great_expectations",
    #     bash_command="great_expectations checkpoint run gold_layer_checkpoint",
    # )

    generate_data >> land_to_minio >> load_silver_to_postgres >> dbt_deps >> dbt_run >> dbt_test
