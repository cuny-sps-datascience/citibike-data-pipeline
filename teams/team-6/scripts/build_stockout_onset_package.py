"""Build strict 30-minute stockout-onset labels on frozen availability origins."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil

import duckdb
import pyarrow.parquet as pq

from citibike_pipeline.db import sql_literal
from citibike_pipeline.upstream_source import file_sha256

UTC = timezone.utc


def write_json(path: Path, value: dict) -> None:
    partial = path.with_suffix(path.suffix + ".partial")
    partial.write_text(json.dumps(value, indent=2, default=str) + "\n")
    partial.replace(path)


def artifact(path: Path, root: Path) -> dict:
    result = {"path": str(path.relative_to(root)), "bytes": path.stat().st_size, "sha256": file_sha256(path)}
    if path.suffix == ".parquet":
        result["rows"] = pq.read_metadata(path).num_rows
    return result


def build(base_package: Path, sources: Path, output: Path) -> dict:
    if output.exists():
        raise ValueError(f"Refusing to overwrite output: {output}")
    base_manifest = json.loads((base_package / "manifest.json").read_text())
    if base_manifest.get("schema_version") != "availability-package-v1" or base_manifest.get("status") != "built":
        raise ValueError("Wrong base availability package")
    for item in base_manifest["artifacts"]:
        if file_sha256(base_package / item["path"]) != item["sha256"]:
            raise ValueError("Base package checksum mismatch")
    source_manifest = json.loads((sources / "manifest.json").read_text())
    if source_manifest.get("status") != "downloaded":
        raise ValueError("Incomplete station source acquisition")
    protocol_path = Path("docs/stockout_onset_protocol.json")
    protocol = json.loads(protocol_path.read_text())
    source_items = [item for item in source_manifest["objects"] if item["source"] == "station_status"]
    source_files = [sources / item["path"] for item in source_items]
    if not source_files:
        raise ValueError("No station-status sources")
    for item, path in zip(source_items, source_files):
        if file_sha256(path) != item["sha256"]:
            raise ValueError(f"Station-status checksum mismatch: {item['path']}")
    output.mkdir(parents=True)
    shutil.copyfile(protocol_path, output / "protocol.json")
    shutil.copyfile(base_package / "cohort.json", output / "cohort.json")
    cohort = json.loads((output / "cohort.json").read_text())
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; SET memory_limit='3GB'; SET threads=4")
    (output / "duckdb_tmp").mkdir()
    con.execute("SET temp_directory=" + sql_literal(str(output / "duckdb_tmp")))
    con.execute("CREATE TABLE cohort(uuid VARCHAR,station_id VARCHAR)")
    con.executemany("INSERT INTO cohort VALUES (?,?)", [(row["uuid"], row["station_id"]) for row in cohort])
    files = "[" + ",".join(sql_literal(str(path)) for path in source_files) + "]"
    con.execute(
        f"""CREATE TABLE raw_polls AS SELECT c.station_id,to_timestamp(s.polled_at) observed_at_utc,
            s.polled_at,s.ts,s.last_reported,s.num_bikes_available,s.num_docks_available,
            s.num_ebikes_available,s.num_bikes_disabled,s.num_docks_disabled,
            s.is_installed,s.is_renting,s.is_returning
            FROM read_parquet({files}) s JOIN cohort c ON s.station_id=c.uuid"""
    )
    conflicts = con.execute(
        """SELECT count(*) FROM (SELECT station_id,polled_at,count(DISTINCT (
            ts,last_reported,num_bikes_available,num_docks_available,num_ebikes_available,
            num_bikes_disabled,num_docks_disabled,is_installed,is_renting,is_returning)) versions
            FROM raw_polls GROUP BY ALL HAVING versions>1)"""
    ).fetchone()[0]
    if conflicts:
        raise ValueError("Conflicting station/poll values")
    con.execute(
        """CREATE TABLE polls AS SELECT *,
            polled_at-ts BETWEEN -300 AND 120 AND polled_at-last_reported BETWEEN -300 AND 600
            AND least(num_bikes_available,num_docks_available,num_ebikes_available,
                num_bikes_disabled,num_docks_disabled)>=0
            AND is_installed=1 AND is_renting=1 AND is_returning=1 AS eligible
            FROM (SELECT DISTINCT * FROM raw_polls)"""
    )
    con.execute(
        """CREATE TABLE ordered_polls AS SELECT *,epoch(observed_at_utc-lag(observed_at_utc)
            OVER(PARTITION BY station_id ORDER BY observed_at_utc)) gap_seconds FROM polls"""
    )
    base_features = sql_literal(str(base_package / "features.parquet"))
    con.execute(
        f"""CREATE TABLE origins AS SELECT * FROM read_parquet({base_features})
            WHERE horizon_minutes={int(protocol['horizon_minutes'])}
            AND current_bikes>0 AND current_docks>0"""
    )
    horizon = int(protocol["horizon_minutes"])
    endpoint = int(protocol["endpoint_tolerance_seconds"])
    con.execute(
        f"""CREATE TABLE windows AS SELECT o.example_id,
            min(p.observed_at_utc) first_future_at,
            max(p.observed_at_utc) FILTER(WHERE p.observed_at_utc<=o.origin_utc+INTERVAL '{horizon} minutes') last_event_poll_at,
            min(p.observed_at_utc) FILTER(WHERE p.observed_at_utc>=o.origin_utc+INTERVAL '{horizon} minutes') label_observed_at_utc,
            max(p.gap_seconds) maximum_gap_seconds,
            count(*) FILTER(WHERE NOT p.eligible) invalid_polls,
            bool_or(p.num_bikes_available=0) FILTER(WHERE p.observed_at_utc<=o.origin_utc+INTERVAL '{horizon} minutes') bike_onset,
            bool_or(p.num_docks_available=0) FILTER(WHERE p.observed_at_utc<=o.origin_utc+INTERVAL '{horizon} minutes') dock_onset
            FROM origins o LEFT JOIN ordered_polls p ON o.station_id=p.station_id
                AND p.observed_at_utc>o.origin_utc
                AND p.observed_at_utc<=o.origin_utc+INTERVAL '{horizon} minutes {endpoint} seconds'
            GROUP BY o.example_id"""
    )
    max_gap = int(protocol["maximum_gap_seconds"])
    con.execute(
        f"""CREATE TABLE accepted AS SELECT o.*,w.label_observed_at_utc,
            coalesce(w.bike_onset,false) bike_onset,coalesce(w.dock_onset,false) dock_onset
            FROM origins o JOIN windows w USING(example_id)
            WHERE epoch(w.first_future_at-o.origin_utc) BETWEEN 0 AND {max_gap}
            AND w.maximum_gap_seconds<={max_gap} AND w.invalid_polls=0
            AND w.last_event_poll_at IS NOT NULL
            AND w.label_observed_at_utc BETWEEN o.origin_utc+INTERVAL '{horizon} minutes'
                AND o.origin_utc+INTERVAL '{horizon} minutes {endpoint} seconds'"""
    )
    feature_columns = [
        "example_id", "station_id", "station_index", "origin_utc", "horizon_minutes",
        "input_observed_at_utc", "information_available_at_utc", "current_bikes", "current_docks",
        "current_ebikes", "disabled_bikes", "disabled_docks", "capacity", "local_hour", "dow",
        "hour_sin", "hour_cos",
    ]
    features_path = output / "features.parquet"
    labels_path = output / "labels.parquet"
    con.execute(
        "COPY (SELECT " + ",".join(feature_columns) + " FROM accepted ORDER BY example_id) TO "
        + sql_literal(str(features_path)) + " (FORMAT PARQUET,COMPRESSION ZSTD)"
    )
    con.execute(
        "COPY (SELECT example_id,origin_utc+INTERVAL '30 minutes' target_utc,label_observed_at_utc,"
        "bike_onset,dock_onset FROM accepted ORDER BY example_id) TO "
        + sql_literal(str(labels_path)) + " (FORMAT PARQUET,COMPRESSION ZSTD)"
    )
    audit = con.execute(
        """SELECT (SELECT count(*) FROM origins) candidate_rows,count(*) accepted_rows,
            count(*) FILTER(WHERE bike_onset) bike_events,count(*) FILTER(WHERE dock_onset) dock_events,
            count(DISTINCT station_id) stations,min(origin_utc),max(origin_utc) FROM accepted"""
    ).fetchone()
    con.close()
    source_audit = {
        "candidate_rows": audit[0], "accepted_rows": audit[1], "bike_events": audit[2],
        "dock_events": audit[3], "stations": audit[4], "first_origin_utc": audit[5],
        "last_origin_utc": audit[6], "conflicting_poll_keys": conflicts,
        "continuous_observation_required": True, "current_stockouts_excluded": True,
    }
    write_json(output / "source_audit.json", source_audit)
    shutil.copyfile("src/citibike_pipeline/stockout_onset_runtime.py", output / "stockout_onset_runtime.py")
    artifacts = [artifact(path, output) for path in (
        features_path, labels_path, output / "protocol.json", output / "cohort.json",
        output / "source_audit.json", output / "stockout_onset_runtime.py",
    )]
    result = {
        "schema_version": "stockout-onset-package-v1", "status": "built",
        "created_at_utc": datetime.now(UTC).isoformat(), "base_package": str(base_package),
        "base_manifest_sha256": file_sha256(base_package / "manifest.json"),
        "source_root": str(sources), "source_manifest_sha256": file_sha256(sources / "manifest.json"),
        "protocol_sha256": file_sha256(output / "protocol.json"), "rows": audit[1],
        "cohort_stations": len(cohort), "artifacts": artifacts, "production_activation": False,
    }
    write_json(output / "manifest.json", result)
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-package", required=True, type=Path)
    parser.add_argument("--sources", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    build(args.base_package, args.sources, args.output)
