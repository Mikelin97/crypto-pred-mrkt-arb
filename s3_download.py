#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError


def _build_s3_client(profile: str | None, region: str | None):
    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    if region:
        session_kwargs["region_name"] = region
    session = boto3.Session(**session_kwargs)
    return session.client("s3")


def _list_keys(s3_client, bucket: str, prefix: str | None) -> List[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
        for item in page.get("Contents", []):
            keys.append(item["Key"])
    return keys


def _download_objects(
    s3_client,
    bucket: str,
    keys: Iterable[str],
    destination_root: Path,
) -> List[Path]:
    downloaded: List[Path] = []
    for key in keys:
        target = destination_root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(bucket, key, str(target))
        downloaded.append(target)
    return downloaded


def _apply_start_from(keys: List[str], start_from: str | None) -> List[str]:
    if not start_from:
        return keys
    try:
        start_index = keys.index(start_from)
    except ValueError:
        raise ValueError(f"Start-from key '{start_from}' not found in listed objects.")
    return keys[start_index:]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download one object or an entire prefix from S3."
    )
    parser.add_argument("--bucket", required=True, help="Bucket name to read from.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--key", help="Exact object key to download.")
    group.add_argument(
        "--prefix",
        help="Prefix to download (all objects under this path will be pulled).",
    )
    parser.add_argument(
        "--dest",
        default="downloads",
        help="Local directory to write files into (default: %(default)s).",
    )
    parser.add_argument(
        "--start-from",
        help=(
            "For prefix downloads, skip until this exact key is reached, then continue "
            "from there (inclusive). Useful for resuming interrupted runs."
        ),
    )
    parser.add_argument(
        "--profile",
        help="Optional AWS profile name to use (falls back to default credentials).",
    )
    parser.add_argument(
        "--region",
        help="Optional AWS region; only set if you need to override your defaults.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be downloaded without writing any files.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    destination_root = Path(args.dest)

    if args.start_from and args.key:
        print("--start-from only applies when using --prefix.", file=sys.stderr)
        return 1

    try:
        s3_client = _build_s3_client(args.profile, args.region)
    except (BotoCoreError, ClientError) as exc:
        print(f"Failed to create S3 client: {exc}", file=sys.stderr)
        return 1

    try:
        keys = [args.key] if args.key else _list_keys(s3_client, args.bucket, args.prefix)
        keys = _apply_start_from(keys, args.start_from)
        if not keys:
            print("No objects found; nothing to download.")
            return 0

        print(f"Found {len(keys)} object(s) in {args.bucket}:")
        for key in keys:
            print(f" - {key}")

        if args.dry_run:
            print("Dry run complete; no files were downloaded.")
            return 0

        downloaded = _download_objects(s3_client, args.bucket, keys, destination_root)
        print("Downloaded files:")
        for path in downloaded:
            print(f" - {path}")
        return 0
    except NoCredentialsError:
        print("AWS credentials not configured; set them before running this script.", file=sys.stderr)
    except (BotoCoreError, ClientError) as exc:
        print(f"S3 error: {exc}", file=sys.stderr)
    except Exception as exc:  # pragma: no cover - defensive catch for unexpected issues
        print(f"Unexpected error: {exc}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
