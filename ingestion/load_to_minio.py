"""
load_to_minio.py
Uploads a local file into the MinIO 'raw' bucket (bronze layer),
using the same boto3 S3 client interface you'd use for real AWS S3.

Usage:
    python ingestion/load_to_minio.py --file data/orders_raw.csv --bucket raw --key orders/orders_raw.csv
"""

import argparse
import os

import boto3
from botocore.client import Config

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_file(local_path: str, bucket: str, key: str):
    client = get_client()
    # Bucket is normally pre-created by the createbuckets service in docker-compose,
    # but this makes the script safe to run standalone too.
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)

    client.upload_file(local_path, bucket, key)
    print(f"Uploaded {local_path} -> s3://{bucket}/{key}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Local file path to upload")
    parser.add_argument("--bucket", default="raw", help="Target MinIO bucket")
    parser.add_argument("--key", required=True, help="Destination object key (path inside bucket)")
    args = parser.parse_args()

    upload_file(args.file, args.bucket, args.key)


if __name__ == "__main__":
    main()
