"""
transform_orders.py
Week 2 — Spark transform job for RetailPulse.

Reads raw order CSVs from MinIO's 'raw' bucket, cleans/dedupes them, and writes
Parquet into the 'silver' bucket. Also builds an SCD Type 2 customer dimension,
so the same customer's attribute changes (e.g. city, state) over time are
tracked with is_current / valid_from / valid_to columns, instead of overwritten.

Run with:
    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        transform_orders.py --run-date 2026-09-20

The AWS/Hadoop packages above give Spark the ability to talk to any S3-compatible
endpoint (including MinIO) via the s3a:// protocol.
"""

import argparse
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("RetailPulse-Transform")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Bronze -> Silver: clean the raw orders
# ---------------------------------------------------------------------------
def clean_orders(spark: SparkSession, raw_path: str) -> DataFrame:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)

    df = (
        df
        # Drop exact duplicate rows (e.g. from a retried ingestion run)
        .dropDuplicates(["order_id"])
        # Drop rows missing anything essential
        .dropna(subset=["order_id", "customer_id", "order_timestamp", "total_amount"])
        # Normalize types
        .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        # Guard against bad data: negative or zero quantity/price shouldn't exist
        .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
        # Standardize text fields
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .withColumn("product_category", F.trim(F.col("product_category")))
        .withColumn("customer_email", F.lower(F.trim(F.col("customer_email"))))
        # Derived column, handy for later marts
        .withColumn("order_date", F.to_date("order_timestamp"))
        .withColumn("processed_at", F.current_timestamp())
    )

    return df


# ---------------------------------------------------------------------------
# Silver: SCD Type 2 customer dimension
# ---------------------------------------------------------------------------
def build_customer_scd2(
    spark: SparkSession,
    new_orders: DataFrame,
    existing_dim_path: str,
) -> DataFrame:
    """
    Builds/updates a customer dimension with SCD Type 2 semantics:
    - Each customer's attribute set (city, state) is tracked over time.
    - When a customer's attributes change, the old row is closed out
      (is_current=False, valid_to=now) and a new row is inserted
      (is_current=True, valid_to=NULL).
    - First-time customers get a fresh row with is_current=True.
    """
    # Latest attributes seen for each customer in this batch
    incoming = (
        new_orders.select(
            "customer_id", "customer_name", "customer_email",
            "customer_city", "customer_state",
        )
        .dropDuplicates(["customer_id"])
    )

    try:
        existing_dim = spark.read.parquet(existing_dim_path)
        has_existing = True
    except Exception:
        has_existing = False

    if not has_existing:
        # First run ever: everyone is a brand-new current record
        dim = (
            incoming
            .withColumn("valid_from", F.current_timestamp())
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .withColumn("customer_sk", F.monotonically_increasing_id())
        )
        return dim

    # Only keep currently-active rows to compare against
    current_rows = existing_dim.filter(F.col("is_current") == True)  # noqa: E712

    # Detect changes: join incoming attributes against current dim rows
    joined = incoming.alias("inc").join(
        current_rows.alias("cur"), on="customer_id", how="left"
    )

    changed_or_new = joined.filter(
        F.col("cur.customer_id").isNull()
        | (F.col("inc.customer_city") != F.col("cur.customer_city"))
        | (F.col("inc.customer_state") != F.col("cur.customer_state"))
    ).select("inc.*")

    # Close out old versions of customers whose attributes changed
    changed_customer_ids = [row.customer_id for row in changed_or_new.select("customer_id").collect()]

    closed_dim = existing_dim.withColumn(
        "is_current",
        F.when(
            (F.col("customer_id").isin(changed_customer_ids)) & (F.col("is_current") == True),  # noqa: E712
            F.lit(False),
        ).otherwise(F.col("is_current")),
    ).withColumn(
        "valid_to",
        F.when(
            (F.col("customer_id").isin(changed_customer_ids)) & (F.col("valid_to").isNull()),
            F.current_timestamp(),
        ).otherwise(F.col("valid_to")),
    )

    new_versions = (
        changed_or_new
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "customer_sk",
            F.monotonically_increasing_id() + F.lit(1_000_000),  # avoid key collisions with existing rows
        )
    )

    return closed_dim.unionByName(new_versions)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="Partition date to process, format YYYY-MM-DD")
    parser.add_argument("--raw-bucket", default="raw")
    parser.add_argument("--silver-bucket", default="silver")
    args = parser.parse_args()

    spark = get_spark()

    raw_path = f"s3a://{args.raw_bucket}/orders/{args.run_date}/orders_raw.csv"
    orders_silver_path = f"s3a://{args.silver_bucket}/orders/{args.run_date}/"
    customer_dim_path = f"s3a://{args.silver_bucket}/dim_customer/"

    print(f"Reading raw orders from {raw_path}")
    clean_df = clean_orders(spark, raw_path)

    print(f"Writing cleaned orders to {orders_silver_path}")
    clean_df.write.mode("overwrite").parquet(orders_silver_path)

    print("Building SCD Type 2 customer dimension")
    dim_df = build_customer_scd2(spark, clean_df, customer_dim_path)

    dim_df = dim_df.persist()
    dim_row_count = dim_df.count()

    print(f"Writing customer dimension to {customer_dim_path}")
    dim_df.write.mode("overwrite").parquet(customer_dim_path)

    print("Transform complete.")
    print(f"  Orders processed : {clean_df.count()}")
    print(f"  Customer dim rows: {dim_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
