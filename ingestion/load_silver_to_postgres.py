"""
load_silver_to_postgres.py
Week 3 — loads the Parquet output of the Spark transform job (living in MinIO's
'silver' bucket) into PostgreSQL staging tables, so dbt can build the gold layer
on top of them.

Uses psycopg2 + COPY directly rather than pandas.to_sql()/SQLAlchemy. This is
partly to sidestep a pandas/SQLAlchemy version-detection issue that surfaced
when running inside the Airflow container (where SQLAlchemy is pinned to an
older version Airflow itself needs), and partly because COPY is the standard,
fast way to bulk-load into Postgres in real pipelines anyway -- row-by-row
INSERTs (which is what to_sql does under the hood) don't scale.

Usage:
    python ingestion/load_silver_to_postgres.py --run-date 2026-09-20
"""

import argparse
import io
import os

import pandas as pd
import psycopg2
from psycopg2 import sql

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "retailpulse")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "retailpulse")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "retailpulse")

STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
}

# Simple pandas dtype -> Postgres type mapping. Good enough for this project;
# a production loader would be more careful about precision/timezone handling.
PANDAS_TO_PG_TYPE = {
    "object": "TEXT",
    "int64": "BIGINT",
    "int32": "INTEGER",
    "float64": "DOUBLE PRECISION",
    "float32": "REAL",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "uint64": "NUMERIC",  # customer_sk can come out as uint64 from Spark's monotonically_increasing_id
}


def get_pg_type(dtype) -> str:
    return PANDAS_TO_PG_TYPE.get(str(dtype), "TEXT")


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=5432,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )


def load_dataframe(df: pd.DataFrame, table_name: str, schema: str, conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )

        full_table = sql.Identifier(schema, table_name)

        columns_sql = sql.SQL(", ").join(
            sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(get_pg_type(dtype)))
            for col, dtype in df.dtypes.items()
        )

        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(full_table))
        cur.execute(
            sql.SQL("CREATE TABLE {} ({})").format(full_table, columns_sql)
        )

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep="")
        buffer.seek(0)

        copy_sql = sql.SQL("COPY {} FROM STDIN WITH (FORMAT csv, NULL '')").format(
            full_table
        )
        cur.copy_expert(copy_sql.as_string(conn), buffer)

    conn.commit()
    print(f"  -> loaded {len(df)} rows into {schema}.{table_name}")


def load_parquet_dir_to_postgres(s3_path: str, table_name: str, schema: str, conn) -> None:
    print(f"Reading {s3_path}")
    df = pd.read_parquet(s3_path, storage_options=STORAGE_OPTIONS)
    print(f"  -> {len(df)} rows, {len(df.columns)} columns")
    load_dataframe(df, table_name, schema, conn)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="Partition date, format YYYY-MM-DD")
    parser.add_argument("--silver-bucket", default="silver")
    args = parser.parse_args()

    conn = get_connection()

    orders_path = f"s3://{args.silver_bucket}/orders/{args.run_date}/"
    customer_dim_path = f"s3://{args.silver_bucket}/dim_customer/"

    try:
        load_parquet_dir_to_postgres(orders_path, "raw_orders", "staging", conn)
        load_parquet_dir_to_postgres(customer_dim_path, "raw_dim_customer", "staging", conn)
    finally:
        conn.close()

    print("Load complete.")


if __name__ == "__main__":
    main()
