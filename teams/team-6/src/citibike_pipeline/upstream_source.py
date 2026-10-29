"""Read-only acquisition and audit helpers for the shared public S3 export."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
import re
import xml.etree.ElementTree as ET

import duckdb
import pyarrow.parquet as pq
import requests

from .db import sql_literal

BUCKET = "cuny-sps-citibike-archive"
BASE_URL = f"https://{BUCKET}.s3.amazonaws.com"
STATUS_PREFIX = "exports/station_status/"
INFO_KEY = "exports/station_info.parquet"
UTC = timezone.utc


def file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def list_status_objects(session=requests) -> tuple[bytes, list[dict]]:
    response = session.get(
        BASE_URL + "/", params={"list-type": "2", "prefix": STATUS_PREFIX}, timeout=60
    )
    response.raise_for_status()
    root = ET.fromstring(response.content)
    namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    objects = []
    for item in root.findall("s3:Contents", namespace):
        key = item.findtext("s3:Key", namespaces=namespace)
        match = re.search(r"nyc_date=(\d{4}-\d{2}-\d{2})/.*\.parquet$", key or "")
        if not match:
            continue
        objects.append(
            {
                "key": key,
                "date": match.group(1),
                "etag": (item.findtext("s3:ETag", namespaces=namespace) or "").strip('"'),
                "last_modified": item.findtext("s3:LastModified", namespaces=namespace),
                "bytes": int(item.findtext("s3:Size", namespaces=namespace) or 0),
            }
        )
    return response.content, sorted(objects, key=lambda value: (value["date"], value["key"]))


def download(key: str, destination: Path, session=requests) -> dict:
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".partial")
    digest = sha256()
    size = 0
    with session.get(BASE_URL + "/" + key, stream=True, timeout=120) as response:
        response.raise_for_status()
        with partial.open("wb") as target:
            for block in response.iter_content(1024 * 1024):
                if block:
                    target.write(block)
                    digest.update(block)
                    size += len(block)
        headers = response.headers
    partial.replace(destination)
    return {
        "key": key,
        "url": BASE_URL + "/" + key,
        "path": str(destination),
        "bytes": size,
        "sha256": digest.hexdigest(),
        "etag": headers.get("ETag", "").strip('"') or None,
        "last_modified": headers.get("Last-Modified"),
        "s3_version_id": headers.get("x-amz-version-id"),
        "downloaded_at_utc": datetime.now(UTC).isoformat(),
    }


def parquet_profile(path: Path, kind: str) -> dict:
    schema = pq.ParquetFile(path).schema_arrow
    con = duckdb.connect()
    source = f"read_parquet({sql_literal(str(path))})"
    if kind == "status":
        row = con.execute(
            f"""SELECT count(*) AS row_count, count(DISTINCT station_id) AS stations,
                count(*)-count(DISTINCT (station_id,fetched_at)) duplicate_keys,
                min(fetched_at),max(fetched_at),
                count(*) FILTER(WHERE coalesce(_has_parse_error,false)
                    OR coalesce(_required_value_missing,false)
                    OR coalesce(_has_negative_count,false)) invalid_rows
                FROM {source}"""
        ).fetchone()
        metrics = {
            "rows": row[0], "stations": row[1], "duplicate_keys": row[2],
            "min_fetched_at": row[3], "max_fetched_at": row[4], "invalid_rows": row[5],
        }
    elif kind == "information":
        row = con.execute(
            f"""SELECT count(*) AS row_count, count(DISTINCT station_id) AS stations,
                count(*)-count(DISTINCT (station_id,fetched_at)) duplicate_keys,
                min(fetched_at),max(fetched_at),min(snapshot_date),max(snapshot_date),
                count(*) FILTER(WHERE coalesce(_has_parse_error,false)
                    OR coalesce(_required_value_missing,false)
                    OR coalesce(_has_negative_capacity,false)
                    OR coalesce(_has_invalid_coordinates,false)) invalid_rows
                FROM {source}"""
        ).fetchone()
        metrics = {
            "rows": row[0], "stations": row[1], "duplicate_keys": row[2],
            "min_fetched_at": row[3], "max_fetched_at": row[4],
            "min_snapshot_date": row[5], "max_snapshot_date": row[6], "invalid_rows": row[7],
        }
    else:
        raise ValueError(f"Unknown profile kind: {kind}")
    con.close()
    return {
        **{key: value.isoformat() if hasattr(value, "isoformat") else value for key, value in metrics.items()},
        "schema": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in schema
        ],
    }


def information_valid_until(latest_fetched_at: datetime, freshness_hours: int = 49) -> datetime:
    if latest_fetched_at.tzinfo is None:
        latest_fetched_at = latest_fetched_at.replace(tzinfo=UTC)
    return latest_fetched_at.astimezone(UTC) + timedelta(hours=freshness_hours)


def information_is_fresh(
    latest_fetched_at: datetime, origin_utc: datetime, freshness_hours: int = 49
) -> bool:
    if origin_utc.tzinfo is None:
        origin_utc = origin_utc.replace(tzinfo=UTC)
    return origin_utc.astimezone(UTC) < information_valid_until(latest_fetched_at, freshness_hours)
