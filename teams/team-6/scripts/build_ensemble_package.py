"""Derive an immutable ensemble experiment package from a validated pilot package."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil

import pyarrow.parquet as pq

from citibike_pipeline.forecast_sources import digest


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, indent=2, default=str) + "\n")
    temporary.replace(path)


def verified_base(root, expected_schema):
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest["schema_version"] != expected_schema or manifest["status"] != "built":
        raise ValueError("Wrong or incomplete base package")
    for artifact in manifest["artifacts"]:
        path = root / artifact["path"]
        if path.stat().st_size != artifact["bytes"] or digest(path) != artifact["sha256"]:
            raise ValueError("Base package integrity failed: " + artifact["path"])
    return manifest


def build(kind, base, root):
    project = Path(__file__).resolve().parents[1]
    if kind == "station":
        base_manifest = verified_base(base, "station-pilot-package-v1")
        protocol = project / "docs/station_ensemble_protocol.json"
        sources = {
            "features.parquet": base / "features.parquet",
            "cohort.json": base / "cohort.json",
            "station_ensemble_runtime.py": project / "src/citibike_pipeline/station_ensemble_runtime.py",
            "protocol.json": protocol,
        }
        schema = "station-ensemble-package-v1"
    else:
        base_manifest = verified_base(base, "availability-package-v1")
        protocol = project / "docs/availability_ensemble_protocol.json"
        sources = {
            "features.parquet": base / "features.parquet",
            "labels.parquet": base / "labels.parquet",
            "cohort.json": base / "cohort.json",
            "availability_ensemble_runtime.py": project / "src/citibike_pipeline/availability_ensemble_runtime.py",
            "availability_live.py": project / "src/citibike_pipeline/availability_live.py",
            "protocol.json": protocol,
        }
        schema = "availability-ensemble-package-v1"
    if root.exists():
        raise ValueError("Output package already exists")
    root.mkdir(parents=True)
    artifacts = []
    for name, source in sources.items():
        destination = root / name
        shutil.copyfile(source, destination)
        artifacts.append({"path": name, "bytes": destination.stat().st_size, "sha256": digest(destination)})
    save(root / "base_package.json", {
        "root": str(base),
        "manifest_sha256": digest(base / "manifest.json"),
        "schema_version": base_manifest["schema_version"],
        "features_sha256": digest(base / "features.parquet"),
        **({"labels_sha256": digest(base / "labels.parquet")} if kind == "availability" else {}),
    })
    artifacts.append({"path": "base_package.json", "bytes": (root / "base_package.json").stat().st_size,
                      "sha256": digest(root / "base_package.json")})
    rows = pq.read_metadata(root / "features.parquet").num_rows
    manifest = {
        "schema_version": schema,
        "status": "built",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "base_package_root": str(base),
        "base_manifest_sha256": digest(base / "manifest.json"),
        "protocol_sha256": digest(root / "protocol.json"),
        "rows": rows,
        "cohort_stations": base_manifest["cohort_stations"],
        "artifacts": artifacts,
        "original_artifacts_changed": False,
        "active_model_changed": False,
        "production_activation": False,
    }
    save(root / "manifest.json", manifest)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("station", "availability"))
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    arguments = parser.parse_args()
    build(arguments.kind, arguments.base, arguments.root)
