"""Compare pinned shared S3 station observations with the frozen ctbk.dev cohort."""
from __future__ import annotations

import argparse
from datetime import date, timedelta
import json
from pathlib import Path

import duckdb

from citibike_pipeline.db import sql_literal
from citibike_pipeline.prospective_evidence import distance_m
from citibike_pipeline.station_history import information
from citibike_pipeline.upstream_source import file_sha256


def write_json(path: Path, value: dict) -> None:
    partial = path.with_suffix(path.suffix + ".partial")
    partial.write_text(json.dumps(value, indent=2, default=str) + "\n")
    partial.replace(path)


def compare(audit_root: Path, ctbk_root: Path, cohort_path: Path, output: Path) -> dict:
    manifest = json.loads((audit_root / "manifest.json").read_text())
    if manifest.get("status") != "complete":
        raise ValueError("Upstream audit is incomplete")
    cohort = json.loads(cohort_path.read_text())
    uuids = [row["uuid"] for row in cohort]
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; SET memory_limit='2GB'; SET threads=2")
    con.execute("CREATE TABLE cohort(station_id VARCHAR)")
    con.executemany("INSERT INTO cohort VALUES (?)", [(value,) for value in uuids])
    comparisons = []
    for item in manifest["objects"]:
        if item["kind"] != "status":
            continue
        day = item["date"]
        upstream = audit_root / "station_status" / f"{day}.parquet"
        next_day = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        references = [
            ctbk_root / "station_status" / f"{day}.parquet",
            ctbk_root / "station_status" / f"{next_day}.parquet",
        ]
        if file_sha256(upstream) != item["sha256"]:
            raise ValueError(f"Upstream checksum changed: {day}")
        if not all(reference.exists() for reference in references):
            raise ValueError(f"Missing ctbk.dev overlap file for {day} or {next_day}")
        up = sql_literal(str(upstream))
        ref = "[" + ",".join(sql_literal(str(reference)) for reference in references) + "]"
        con.execute(
            f"""CREATE OR REPLACE TEMP TABLE upstream AS
                SELECT s.*,epoch(fetched_at) observed_seconds
                FROM read_parquet({up}) s JOIN cohort USING(station_id)"""
        )
        con.execute(
            f"""CREATE OR REPLACE TEMP TABLE reference AS
                SELECT s.*,polled_at::DOUBLE observed_seconds
                FROM read_parquet({ref}) s JOIN cohort USING(station_id)"""
        )
        con.execute(
            """CREATE OR REPLACE TEMP TABLE paired AS
                SELECT u.station_id,u.observed_seconds upstream_seconds,
                    r.observed_seconds reference_seconds,
                    u.num_bikes_available upstream_bikes,r.num_bikes_available reference_bikes,
                    u.num_docks_available upstream_docks,r.num_docks_available reference_docks,
                    u.num_ebikes_available upstream_ebikes,r.num_ebikes_available reference_ebikes,
                    u.num_bikes_disabled upstream_bikes_disabled,r.num_bikes_disabled reference_bikes_disabled,
                    u.num_docks_disabled upstream_docks_disabled,r.num_docks_disabled reference_docks_disabled,
                    u.is_installed upstream_installed,r.is_installed reference_installed,
                    u.is_renting upstream_renting,r.is_renting reference_renting,
                    u.is_returning upstream_returning,r.is_returning reference_returning
                FROM upstream u LEFT JOIN reference r
                ON u.station_id=r.station_id AND abs(u.observed_seconds-r.observed_seconds)<=150
                QUALIFY row_number() OVER(PARTITION BY u.station_id,u.observed_seconds
                    ORDER BY abs(u.observed_seconds-r.observed_seconds) NULLS LAST)=1"""
        )
        source_counts = con.execute(
            """SELECT
                (SELECT count(*) FROM upstream),(SELECT count(DISTINCT station_id) FROM upstream),
                (SELECT count(*) FROM reference),(SELECT count(DISTINCT station_id) FROM reference)"""
        ).fetchone()
        match = con.execute(
            """SELECT count(*) AS row_count,
                count(*) FILTER(WHERE reference_seconds IS NOT NULL
                    AND abs(upstream_seconds-reference_seconds)<=150) AS paired_rows,
                median(abs(upstream_seconds-reference_seconds)) FILTER(WHERE reference_seconds IS NOT NULL
                    AND abs(upstream_seconds-reference_seconds)<=150) median_seconds,
                approx_quantile(abs(upstream_seconds-reference_seconds),.95) FILTER(WHERE reference_seconds IS NOT NULL
                    AND abs(upstream_seconds-reference_seconds)<=150) p95_seconds,
                count(*) FILTER(WHERE reference_seconds IS NOT NULL AND abs(upstream_seconds-reference_seconds)<=150
                    AND upstream_bikes=reference_bikes)::DOUBLE/nullif(count(*) FILTER(WHERE reference_seconds IS NOT NULL
                    AND abs(upstream_seconds-reference_seconds)<=150),0) bikes_exact_share,
                count(*) FILTER(WHERE reference_seconds IS NOT NULL AND abs(upstream_seconds-reference_seconds)<=150
                    AND upstream_docks=reference_docks)::DOUBLE/nullif(count(*) FILTER(WHERE reference_seconds IS NOT NULL
                    AND abs(upstream_seconds-reference_seconds)<=150),0) docks_exact_share,
                count(*) FILTER(WHERE reference_seconds IS NOT NULL AND abs(upstream_seconds-reference_seconds)<=150
                    AND upstream_ebikes=reference_ebikes)::DOUBLE/nullif(count(*) FILTER(WHERE reference_seconds IS NOT NULL
                    AND abs(upstream_seconds-reference_seconds)<=150),0) ebikes_exact_share,
                count(*) FILTER(WHERE reference_seconds IS NOT NULL AND abs(upstream_seconds-reference_seconds)<=150
                    AND upstream_bikes_disabled=reference_bikes_disabled AND upstream_docks_disabled=reference_docks_disabled
                    AND upstream_installed=(reference_installed=1) AND upstream_renting=(reference_renting=1)
                    AND upstream_returning=(reference_returning=1))::DOUBLE/nullif(count(*) FILTER(WHERE reference_seconds IS NOT NULL
                    AND abs(upstream_seconds-reference_seconds)<=150),0) flags_exact_share
                FROM paired"""
        ).fetchone()
        gaps = {}
        for name in ("upstream", "reference"):
            row = con.execute(
                f"""WITH ordered AS (SELECT observed_seconds-lag(observed_seconds) OVER
                    (PARTITION BY station_id ORDER BY observed_seconds) gap FROM {name})
                    SELECT median(gap) FILTER(WHERE gap>0),approx_quantile(gap,.95) FILTER(WHERE gap>0),
                    count(*) FILTER(WHERE gap>600) FROM ordered"""
            ).fetchone()
            gaps[name] = {"median_seconds": row[0], "p95_seconds": row[1], "gaps_over_600_seconds": row[2]}
        comparisons.append(
            {
                "date": day,
                "upstream_rows": source_counts[0], "upstream_stations": source_counts[1],
                "reference_rows": source_counts[2], "reference_stations": source_counts[3],
                "paired_rows": match[1], "upstream_rows_considered": match[0],
                "paired_share": match[1] / match[0] if match[0] else None,
                "pair_delta_seconds": {"median": match[2], "p95": match[3], "maximum_allowed": 150},
                "exact_value_share": {
                    "bikes": match[4], "docks": match[5], "ebikes": match[6], "service_and_disabled": match[7]
                },
                "gaps": gaps,
            }
        )
    info_item = next(item for item in manifest["objects"] if item["kind"] == "information")
    info_path = audit_root / "station_information" / "station_info.parquet"
    if file_sha256(info_path) != info_item["sha256"]:
        raise ValueError("Station-information checksum changed")
    last_day = max(item["date"] for item in comparisons)
    rows = con.execute(
        f"""SELECT station_id,short_name,capacity,lat,lon,fetched_at FROM (
            SELECT i.*,row_number() OVER(PARTITION BY station_id ORDER BY fetched_at DESC) rank
            FROM read_parquet({sql_literal(str(info_path))}) i JOIN cohort USING(station_id)
            WHERE snapshot_date<=DATE {sql_literal(last_day)}) WHERE rank=1"""
    ).fetchall()
    upstream_info = {
        row[0]: {"short_name": row[1], "capacity": row[2], "lat": row[3], "lon": row[4], "fetched_at": row[5]}
        for row in rows
    }
    reference_info = information(ctbk_root / "station_information" / f"{last_day}.json", date.fromisoformat(last_day))
    info_matches = []
    for uuid in uuids:
        left, right = upstream_info.get(uuid), reference_info.get(uuid)
        if left and right:
            info_matches.append(
                {
                    "uuid": uuid,
                    "short_name_equal": left["short_name"] == right["station_id"],
                    "capacity_equal": left["capacity"] == right["capacity"],
                    "distance_m": distance_m(left["lat"], left["lon"], right["latitude"], right["longitude"]),
                }
            )
    con.close()
    result = {
        "schema_version": "shared-station-overlap-comparison-v1",
        "upstream_manifest_sha256": file_sha256(audit_root / "manifest.json"),
        "ctbk_manifest_sha256": file_sha256(ctbk_root / "manifest.json"),
        "cohort_sha256": file_sha256(cohort_path),
        "cohort_stations": len(uuids),
        "status_comparisons": comparisons,
        "station_information": {
            "comparison_date": last_day,
            "matched_stations": len(info_matches),
            "short_name_exact": sum(row["short_name_equal"] for row in info_matches),
            "capacity_exact": sum(row["capacity_equal"] for row in info_matches),
            "maximum_coordinate_difference_m": max((row["distance_m"] for row in info_matches), default=None),
        },
        "decision": "Keep the shared S3 export as a separately versioned candidate source. Expansion beyond the frozen cohort requires the Team 6 freshness and station-eligibility gates; overlap agreement does not establish continuous operation.",
    }
    write_json(output, result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-root", required=True, type=Path)
    parser.add_argument("--ctbk-root", required=True, type=Path)
    parser.add_argument("--cohort", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(compare(args.audit_root, args.ctbk_root, args.cohort, args.output), indent=2, default=str))
