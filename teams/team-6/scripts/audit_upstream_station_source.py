"""Download and profile pinned shared-pipeline station exports without modifying them."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path

from citibike_pipeline.upstream_source import (
    INFO_KEY,
    download,
    information_valid_until,
    list_status_objects,
    parquet_profile,
)

UTC = timezone.utc


def write_json(path: Path, value: dict) -> None:
    partial = path.with_suffix(path.suffix + ".partial")
    partial.write_text(json.dumps(value, indent=2, default=str) + "\n")
    partial.replace(path)


def audit(root: Path, dates: list[str]) -> dict:
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"Refusing to overwrite nonempty audit root: {root}")
    root.mkdir(parents=True, exist_ok=True)
    listing, available = list_status_objects()
    (root / "station_status_listing.xml").write_bytes(listing)
    by_date: dict[str, list[dict]] = {}
    for item in available:
        by_date.setdefault(item["date"], []).append(item)
    missing = [date for date in dates if len(by_date.get(date, [])) != 1]
    if missing:
        raise ValueError(f"Expected exactly one Parquet object for dates: {missing}")
    records = []
    for date in dates:
        listed = by_date[date][0]
        target = root / "station_status" / f"{date}.parquet"
        record = download(listed["key"], target)
        record.update({"kind": "status", "date": date, "listed": listed})
        record["profile"] = parquet_profile(target, "status")
        records.append(record)
    info_path = root / "station_information" / "station_info.parquet"
    info = download(INFO_KEY, info_path)
    info.update({"kind": "information", "profile": parquet_profile(info_path, "information")})
    records.append(info)
    max_info = datetime.fromisoformat(info["profile"]["max_fetched_at"]).replace(tzinfo=UTC)
    status_dates = sorted(by_date)
    result = {
        "schema_version": "shared-station-source-audit-v1",
        "status": "complete",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "root": str(root),
        "read_only_source": True,
        "listing": {
            "sha256": sha256(listing).hexdigest(),
            "parquet_objects": len(available),
            "first_date": status_dates[0],
            "last_date": status_dates[-1],
        },
        "requested_dates": dates,
        "objects": records,
        "station_information_freshness": {
            "latest_fetched_at": max_info.isoformat(),
            "freshness_hours": 49,
            "valid_through_exclusive_utc": information_valid_until(max_info).isoformat(),
            "policy": "Reject capacity, location and normalized-count predictors at or after the validity cutoff unless a newer independently audited information snapshot exists.",
        },
    }
    write_json(root / "manifest.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--dates", nargs="+", default=["2026-08-21", "2026-08-22", "2026-08-23", "2026-08-24"])
    args = parser.parse_args()
    print(json.dumps(audit(args.root, args.dates), indent=2, default=str))
