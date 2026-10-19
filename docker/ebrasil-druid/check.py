"""Verify the eBrasil bronze copy, silver model and Druid datasources."""
from __future__ import annotations

import hashlib
import json
import os
import sys

import boto3
import requests
from botocore.config import Config

BUCKET = "lakehouse"
ENDPOINT = os.getenv("S3_ENDPOINT", "http://objectstore:9000")
DRUID = os.getenv("DRUID_URL", "http://druid-router:8888")
S3 = boto3.client("s3", endpoint_url=ENDPOINT,
    aws_access_key_id=os.getenv("S3_ACCESS_KEY", "labadmin"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY", "TRAINING_ONLY_S3_PASSWORD"),
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
HTTP = requests.Session()

def check_bronze():
    manifest = json.loads(S3.get_object(Bucket=BUCKET,
        Key="bronze/ebrasil/_manifest.json")["Body"].read())
    if len(manifest) != 8:
        raise AssertionError(f"Expected 8 bronze source files, found {len(manifest)}")
    for filename, expected in manifest.items():
        body = S3.get_object(Bucket=BUCKET, Key=f"bronze/ebrasil/{filename}")["Body"]
        digest = hashlib.file_digest(body, "sha256").hexdigest()
        body.close()
        if digest != expected["sha256"]:
            raise AssertionError(f"Bronze SHA-256 mismatch: {filename}")
    print(f"EBRASIL_BRONZE_CHECK_OK files={len(manifest)}")

def query(sql):
    response = HTTP.post(DRUID + "/druid/v2/sql",
                         json={"query": sql}, timeout=120)
    if not response.ok:
        raise RuntimeError(
            f"Druid SQL HTTP {response.status_code}: {response.text[:2000]}"
        )
    return response.json()

def check_silver_and_druid():
    model = json.loads(S3.get_object(Bucket=BUCKET,
        Key="silver/ebrasil/model-manifest.json")["Body"].read())
    tables = model["tables"]
    expected_tables = {
        "fact_order_item", "fact_order", "fact_payment",
        "fact_logistics", "fact_review", "mart_customer_rfm",
    }
    if set(tables) != expected_tables:
        raise AssertionError(
            f"Expected the six denormalized star tables, found {sorted(tables)}")
    for table, definition in tables.items():
        prefix = f"silver/ebrasil/{table}/"
        objects = S3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])
        parquet = [x for x in objects if x["Key"].endswith(".parquet")]
        if not parquet:
            raise AssertionError(f"No silver Parquet part file for {table}")
        rows = query(f'SELECT COUNT(*) AS n FROM "{table}"')[0]["n"]
        if int(rows) != int(definition["rows"]):
            raise AssertionError(
                f"Druid row count mismatch for {table}: {rows} != {definition['rows']}")
    print(f"EBRASIL_SILVER_DRUID_CHECK_OK tables={len(tables)}")

def main():
    check_bronze()
    check_silver_and_druid()
    print("EBRASIL_ALL_CHECKS_OK")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"EBRASIL_CHECK_FAIL: {exc}", file=sys.stderr, flush=True)
        raise
