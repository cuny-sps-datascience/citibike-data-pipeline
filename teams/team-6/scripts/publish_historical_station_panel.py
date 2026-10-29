#!/usr/bin/env python3
"""Publish a validated panel to new shared-volume paths, without replacing tables."""

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from citibike_pipeline.forecast_sources import digest
from citibike_pipeline.historical_station_panel import checked_path, validate_historical_panel
from deploy_three_hour_pilot import upload_immutable


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--profile", default="DATA622")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    root = args.package.resolve()
    manifest = validate_historical_panel(root)
    project = Path(__file__).resolve().parents[1]
    os.environ["PATH"] = str(project/".local/bin") + os.pathsep + os.environ["PATH"]
    client = WorkspaceClient(profile=args.profile)
    if client.config.host.rstrip("/") != "https://dbc-d00cbd4c-c74c.cloud.databricks.com":
        raise ValueError("Profile must point to the existing Team 6 integration workspace")
    destination = "/Volumes/workspace/citibike_3h/inputs/historical_station_panels/" + root.name
    items = list(manifest["artifacts"]) + [{"path": "manifest.json", "sha256": digest(root/"manifest.json")}]
    total = 0
    for n, item in enumerate(items, 1):
        path = checked_path(root, item["path"])
        if digest(path) != item["sha256"]:
            raise ValueError("Source changed after validation")
        remote = destination + "/" + item["path"]
        upload_immutable(client, remote, path.read_bytes())
        downloaded = client.files.download(remote).contents.read()
        if len(downloaded) != path.stat().st_size or hashlib.sha256(downloaded).hexdigest() != item["sha256"]:
            raise ValueError("Remote read-back verification failed")
        total += len(downloaded)
        print(f"{n}/{len(items)} uploaded and read-back verified", flush=True)
    receipt = {"status": "uploaded_and_readback_verified", "remote_root": destination,
               "verified_at_utc": datetime.now(timezone.utc).isoformat(),
               "objects": len(items), "bytes": total, "manifest_sha256": digest(root/"manifest.json"),
               "stable_views_changed": False, "models_changed": False, "jobs_or_schedules_changed": False}
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2)+"\n")
    print(destination)


if __name__ == "__main__":
    main()
