"""Audit a stockout-onset package and result on independent reload."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path

import pyarrow.parquet as pq

from citibike_pipeline.stockout_onset_runtime import OUTCOMES, candidate_names, partition
from citibike_pipeline.upstream_source import file_sha256

UTC = timezone.utc


def audit(package: Path, result: Path) -> dict:
    manifest = json.loads((package / "manifest.json").read_text())
    for item in manifest["artifacts"]:
        if file_sha256(package / item["path"]) != item["sha256"]:
            raise ValueError("Package artifact checksum mismatch")
    features = pq.read_table(package / "features.parquet")
    labels = pq.read_table(package / "labels.parquet")
    protocol = json.loads((package / "protocol.json").read_text())
    masks = partition(features, labels, protocol)
    selection = json.loads((result / "selection.json").read_text())
    report = json.loads((result / "report.json").read_text())
    if selection["selected"] != report["selected"] or selection["test_metrics_viewed"]:
        raise ValueError("Selection freeze mismatch")
    predictions = pq.read_table(result / "predictions.parquet")
    if predictions.num_rows != masks["test"].sum():
        raise ValueError("Prediction row mismatch")
    if len(set(predictions["example_id"].to_pylist())) != predictions.num_rows:
        raise ValueError("Duplicate prediction keys")
    for outcome in OUTCOMES:
        for candidate in candidate_names():
            values = predictions[outcome + "_" + candidate].to_pylist()
            if any(value is None or not math.isfinite(value) or not 0 <= value <= 1 for value in values):
                raise ValueError("Invalid probability")
    return {
        "schema_version": "stockout-onset-validation-v1",
        "validated_at_utc": datetime.now(UTC).isoformat(),
        "package_root": str(package),
        "result_root": str(result),
        "package_manifest_sha256": file_sha256(package / "manifest.json"),
        "result_report_sha256": file_sha256(result / "report.json"),
        "model_sha256": file_sha256(result / "models.joblib"),
        "package_rows": features.num_rows,
        "prediction_rows": predictions.num_rows,
        "split_rows": {name: int(mask.sum()) for name, mask in masks.items()},
        "source_audit": json.loads((package / "source_audit.json").read_text()),
        "selected": selection["selected"],
        "validation": selection["validation"],
        "test": report["test"],
        "checks": {
            "artifact_checksums": "passed",
            "predictor_label_keys": "passed",
            "chronological_cutoffs": "passed",
            "selection_frozen_before_test": "passed",
            "unique_test_keys": "passed",
            "probabilities_finite_and_bounded": "passed"
        },
        "production_activation": False
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    value = audit(args.package, args.result)
    text = json.dumps(value, indent=2) + "\n"
    if args.output:
        args.output.write_text(text)
    print(text, end="")
