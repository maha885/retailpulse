"""
retailpulse_pipeline.py
Airflow DAG for the RetailPulse local ELT pipeline.

Week 1 scope: generate synthetic data -> land in MinIO raw bucket.
Later weeks will extend this DAG with:
    - PySpark transform task (raw -> silver)
    - dbt run task (silver -> gold, in Postgres)
    - Great Expectations validation task between each layer
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
    description="End-to-end local ELT pipeline: generate -> land -> transform -> model -> validate",
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

    # --- Placeholders for Week 2 onward ---
    # spark_transform = BashOperator(
    #     task_id="spark_transform_raw_to_silver",
    #     bash_command="spark-submit /opt/airflow/spark_jobs/transform_orders.py",
    # )
    #
    # dbt_run = BashOperator(
    #     task_id="dbt_run_silver_to_gold",
    #     bash_command="cd /opt/airflow/dbt && dbt run",
    # )
    #
    # ge_validate = BashOperator(
    #     task_id="validate_gold_layer",
    #     bash_command="great_expectations checkpoint run gold_layer_checkpoint",
    # )

    generate_data >> land_to_minio
    # generate_data >> land_to_minio >> spark_transform >> dbt_run >> ge_validate
