from __future__ import annotations

import hashlib
import json
from pathlib import Path
import argparse
import os
import sys
import time
from typing import Iterable

def _aws_exception_types(*names):
    """Load botocore exception classes only when handling AWS operations."""
    try:
        from botocore import exceptions
    except ImportError:
        return ()
    return tuple(getattr(exceptions, name) for name in names)


MARKER_KEY = ".bigdata-init"
MARKER_BODY = b"bigdata-training-objectstore-init\n"
LAKEHOUSE_PREFIX_MARKERS = ("spark-events/", "spark-warehouse/")


def required_buckets(value: str | None = None) -> list[str]:
    raw = value if value is not None else os.getenv("REQUIRED_BUCKETS", "warehouse,lakehouse,checkpoints")
    buckets = [item.strip() for item in raw.split(",") if item.strip()]
    if not buckets:
        raise ValueError("No required buckets configured")
    return buckets


def make_client():
    try:
        import boto3
        from botocore.client import Config
    except ImportError as exc:
        raise RuntimeError("boto3 and botocore are required for S3 operations") from exc
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://objectstore:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "bigdataadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "bigdatasecret2026"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def bucket_names(client) -> set[str]:
    response = client.list_buckets()
    return {item["Name"] for item in response.get("Buckets", [])}


def create_bucket_if_missing(client, bucket: str) -> None:
    if bucket in bucket_names(client):
        return
    try:
        client.create_bucket(Bucket=bucket)
    except Exception as exc:
        if not isinstance(exc, _aws_exception_types("ClientError")):
            raise
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise


def verify_bucket(client, bucket: str) -> None:
    client.head_bucket(Bucket=bucket)
    client.put_object(Bucket=bucket, Key=MARKER_KEY, Body=MARKER_BODY, ContentType="text/plain")
    client.head_object(Bucket=bucket, Key=MARKER_KEY)


def ensure_lakehouse_prefixes(client) -> None:
    for key in LAKEHOUSE_PREFIX_MARKERS:
        client.put_object(Bucket="lakehouse", Key=key, Body=b"", ContentType="application/x-directory")
        client.head_object(Bucket="lakehouse", Key=key)


def ensure_buckets(client, buckets: Iterable[str], attempts: int = 30, delay_seconds: float = 1.0) -> None:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            for bucket in buckets:
                create_bucket_if_missing(client, bucket)
                verify_bucket(client, bucket)
            if "lakehouse" in buckets:
                ensure_lakehouse_prefixes(client)
            return
        except Exception as exc:
            if not isinstance(exc, _aws_exception_types("ClientError", "EndpointConnectionError")):
                raise
            last_error = exc
            print(f"[objectstore-init] attempt={attempt}/{attempts} error={exc}", file=sys.stderr)
            if attempt < attempts:
                time.sleep(delay_seconds)
    raise RuntimeError(f"Unable to initialize object storage after {attempts} attempts: {last_error}")


def verify_only(client, buckets: Iterable[str]) -> None:
    existing = bucket_names(client)
    missing = [bucket for bucket in buckets if bucket not in existing]
    if missing:
        raise RuntimeError(f"Missing buckets: {','.join(missing)}")
    for bucket in buckets:
        client.head_bucket(Bucket=bucket)
        client.head_object(Bucket=bucket, Key=MARKER_KEY)
    if "lakehouse" in buckets:
        for key in LAKEHOUSE_PREFIX_MARKERS:
            client.head_object(Bucket="lakehouse", Key=key)


def sync_data(client, source: Path, verify_only: bool = False) -> dict:
    """Upload every source file with its relative path; verify the returned bytes."""
    if not source.is_dir():
        raise FileNotFoundError(source)
    manifest = {}
    for path in sorted(source.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"Symlink in data: {path}")
        if not path.is_file():
            continue
        key = "donnees/" + path.relative_to(source).as_posix()
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        if not verify_only:
            client.upload_file(str(path), "lakehouse", key)
        received = client.get_object(Bucket="lakehouse", Key=key)["Body"]
        try:
            remote_digest = hashlib.file_digest(received, "sha256").hexdigest()
        finally:
            received.close()
        if remote_digest != digest:
            raise RuntimeError(f"S3 content mismatch: {key}")
        manifest[key] = {"size": path.stat().st_size, "sha256": digest}
    if not manifest:
        raise ValueError("Empty data directory")
    if not verify_only:
        client.put_object(Bucket="lakehouse", Key="donnees-manifest.json",
                          Body=json.dumps(manifest, sort_keys=True).encode())
    saved = json.loads(client.get_object(Bucket="lakehouse", Key="donnees-manifest.json")["Body"].read())
    if saved != manifest:
        raise RuntimeError("S3 manifest differs from source data")
    print(f"S3_DATA_VERIFIED files={len(manifest)}")
    return manifest



def sync_ebrasil_bronze(client, source: Path, verify_only: bool = False) -> dict:
    """Copy the prepared Olist Parquet files to lakehouse/bronze/ebrasil."""
    ebrasil = source / "db_ebrasil"
    if not ebrasil.is_dir():
        raise FileNotFoundError(f"Missing eBrasil source directory: {ebrasil}")
    files = sorted(path for path in ebrasil.iterdir() if path.is_file() and path.suffix.lower() == ".parquet")
    if len(files) != 9:
        raise ValueError(f"Expected 9 eBrasil Parquet files in {ebrasil}, found {len(files)}")
    manifest = {}
    for path in files:
        key = "bronze/ebrasil/" + path.name
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if not verify_only:
            client.upload_file(str(path), "lakehouse", key)
        remote = client.get_object(Bucket="lakehouse", Key=key)["Body"]
        try:
            remote_digest = hashlib.file_digest(remote, "sha256").hexdigest()
        finally:
            remote.close()
        if remote_digest != digest:
            raise RuntimeError(f"RustFS bronze content mismatch: {key}")
        manifest[path.name] = {"size": path.stat().st_size, "sha256": digest}
    manifest_key = "bronze/ebrasil/_manifest.json"
    encoded = json.dumps(manifest, sort_keys=True).encode()
    if not verify_only:
        client.put_object(Bucket="lakehouse", Key=manifest_key, Body=encoded,
                          ContentType="application/json")
    saved = json.loads(client.get_object(Bucket="lakehouse", Key=manifest_key)["Body"].read())
    if saved != manifest:
        raise RuntimeError("RustFS bronze eBrasil manifest differs from source files")
    print(f"EBRASIL_BRONZE_VERIFIED files={len(manifest)} prefix=lakehouse/bronze/ebrasil")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize and verify the training S3 buckets")
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()

    buckets = required_buckets()
    client = make_client()

    if args.verify_only:
        verify_only(client, buckets)
        print("OBJECTSTORE_VERIFY_OK " + ",".join(buckets))
    else:
        ensure_buckets(client, buckets)
        verify_only(client, buckets)
        print("OBJECTSTORE_INIT_OK " + ",".join(buckets))
    source = Path(os.environ["DATA_DIR"])
    sync_data(client, source, args.verify_only)
    sync_ebrasil_bronze(client, source, args.verify_only)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
