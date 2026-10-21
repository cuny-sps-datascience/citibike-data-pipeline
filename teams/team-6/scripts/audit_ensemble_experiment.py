"""Independently validate ensemble packages and optional persisted results."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from citibike_pipeline.forecast_sources import digest


def audit(kind, package, result=None):
    manifest = json.loads((package / "manifest.json").read_text())
    expected = kind + "-ensemble-package-v1"
    if manifest["schema_version"] != expected or manifest["status"] != "built":
        raise ValueError("Wrong package")
    for artifact in manifest["artifacts"]:
        path = package / artifact["path"]
        if path.stat().st_size != artifact["bytes"] or digest(path) != artifact["sha256"]:
            raise ValueError("Artifact integrity failed")
    base = json.loads((package / "base_package.json").read_text())
    base_root = Path(base["root"])
    checks = {
        "base_manifest_changed": int(digest(base_root / "manifest.json") != base["manifest_sha256"]),
        "base_features_changed": int(digest(base_root / "features.parquet") != base["features_sha256"]),
    }
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'; SET memory_limit='1GB'; SET threads=2")
    features = str(package / "features.parquet")
    if kind == "station":
        checks.update({
            "duplicate_feature_keys": connection.execute("""SELECT count(*) FROM (
                SELECT station_id,origin_utc,horizon,count(*) n FROM read_parquet(?) GROUP BY ALL HAVING n<>1)""", [features]).fetchone()[0],
            "invalid_feature_rows": connection.execute("""SELECT count(*) FROM read_parquet(?) WHERE
                departures<0 OR target_start_utc<>origin_utc+horizon*INTERVAL '1 hour'
                OR target_end_utc<>target_start_utc+INTERVAL '1 hour'
                OR weather_assumed_available_at_utc>=issue_cutoff_utc
                OR information_assumed_available_at_utc>=issue_cutoff_utc""", [features]).fetchone()[0],
        })
    else:
        labels = str(package / "labels.parquet")
        checks["base_labels_changed"] = int(digest(base_root / "labels.parquet") != base["labels_sha256"])
        columns = {row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [features]).fetchall()}
        checks.update({
            "future_labels_in_features": len({"bikes", "docks", "target_utc", "label_observed_at_utc"} & columns),
            "duplicate_example_keys": connection.execute("""SELECT count(*) FROM (
                SELECT example_id,count(*) n FROM read_parquet(?) GROUP BY ALL HAVING n<>1)""", [features]).fetchone()[0],
            "label_key_disagreement": connection.execute("""SELECT count(*) FROM read_parquet(?) f FULL JOIN read_parquet(?) l
                USING(example_id) WHERE f.example_id IS NULL OR l.example_id IS NULL""", [features, labels]).fetchone()[0],
        })
    report = {
        "kind": kind, "package_root": str(package), "manifest_sha256": digest(package / "manifest.json"),
        "rows": manifest["rows"], "checks": checks, "active_model_changed": False, "production_activation": False,
    }
    if result:
        saved = json.loads((result / "report.json").read_text())
        predictions = str(result / "predictions.parquet")
        if kind == "station":
            recomputed = connection.execute("""SELECT model,count(*) AS row_count,avg(abs(prediction-departures)) AS mae
                FROM read_parquet(?) GROUP BY model ORDER BY model""", [predictions]).fetchall()
            for model, rows, mae in recomputed:
                if rows != saved["test"][model]["rows"] or abs(mae - saved["test"][model]["mae"]) > 1e-9:
                    raise ValueError("Station result metric mismatch")
            checks["duplicate_predictions"] = connection.execute("""SELECT count(*) FROM (
                SELECT model,station_id,origin_utc,horizon,count(*) n FROM read_parquet(?) GROUP BY ALL HAVING n<>1)""", [predictions]).fetchone()[0]
        else:
            recomputed = []
            for horizon, rows, bike_mae, dock_mae in connection.execute("""SELECT horizon_minutes,count(*),
                avg(abs(bikes_prediction-bikes)),avg(abs(docks_prediction-docks)) FROM read_parquet(?)
                GROUP BY horizon_minutes ORDER BY horizon_minutes""", [predictions]).fetchall():
                expected_row = saved["test_by_horizon"][str(horizon)]
                if rows != expected_row["rows"] or abs(bike_mae - expected_row["bikes"]["model"]["mae"]) > 1e-9 or abs(dock_mae - expected_row["docks"]["model"]["mae"]) > 1e-9:
                    raise ValueError("Availability result metric mismatch")
                recomputed.append((horizon, rows, bike_mae, dock_mae))
            checks["duplicate_predictions"] = connection.execute("""SELECT count(*) FROM (
                SELECT station_id,origin_utc,horizon_minutes,count(*) n FROM read_parquet(?) GROUP BY ALL HAVING n<>1)""", [predictions]).fetchone()[0]
        report["result_root"] = str(result)
        report["result_schema_version"] = saved["schema_version"]
        report["independently_recomputed_metrics"] = recomputed
    report["passed"] = not any(checks.values())
    connection.close()
    if not report["passed"]:
        raise ValueError(json.dumps(report))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("station", "availability"))
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--result", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    value = audit(arguments.kind, arguments.package, arguments.result)
    arguments.output.write_text(json.dumps(value, indent=2) + "\n")
    print(json.dumps(value, indent=2))
