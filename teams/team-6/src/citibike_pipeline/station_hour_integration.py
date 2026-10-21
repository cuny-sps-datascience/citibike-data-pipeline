"""Leakage-aware station-hour assembly for the bounded Team 6 pilot.

The integration keeps predictors and outcomes in separate artifacts. It joins
the GBFS UUID used by status/weather sources to the Citi Bike ``short_name``
used by trip files through dated station-information snapshots.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re

import duckdb

from .db import sql_literal
from .station_history import information


UTC = timezone.utc
START_DST = ("start_dst_ambiguous", "start_dst_nonexistent")
END_DST = ("end_dst_ambiguous", "end_dst_nonexistent")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(2**20), b""):
            digest.update(block)
    return digest.hexdigest()


def split_for(origin: datetime, protocol: dict) -> tuple[str, bool]:
    """Return the frozen station-model split and independent refit membership."""
    if origin.tzinfo is None:
        raise ValueError("Origin must include a timezone")
    from zoneinfo import ZoneInfo

    local_day = origin.astimezone(ZoneInfo("America/New_York")).date()

    def inside(name: str) -> bool:
        first, last = (date.fromisoformat(value) for value in protocol[name])
        return first <= local_day < last

    primary = next(
        (
            label
            for label, key in (
                ("train", "train_target_local"),
                ("validation", "validation_target_local"),
                ("test", "test_target_local"),
            )
            if inside(key)
        ),
        "excluded",
    )
    return primary, inside("refit_target_local")


def dated_crosswalk(cohort_path: Path, information_root: Path) -> list[dict]:
    """Build the audited UUID/short-name history used for as-of joins."""
    cohort = json.loads(Path(cohort_path).read_text())
    expected = {str(row["uuid"]): str(row["station_id"]) for row in cohort}
    if len(expected) != len(cohort) or len(set(expected.values())) != len(cohort):
        raise ValueError("Cohort identifiers are not one-to-one")
    rows: list[dict] = []
    for path in sorted(Path(information_root).glob("*.json")):
        try:
            object_day = date.fromisoformat(path.stem)
        except ValueError as exc:
            raise ValueError(f"Unexpected station-information filename: {path.name}") from exc
        snapshot = information(path, object_day)
        for uuid, short_name in expected.items():
            row = snapshot.get(uuid)
            if row is None:
                continue
            if row["station_id"] != short_name:
                raise ValueError(f"Station identifier changed for cohort UUID {uuid}")
            rows.append(
                {
                    "station_uuid": uuid,
                    "station_id": short_name,
                    "station_name": row["name"],
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                    "capacity": row["capacity"],
                    "provider_at_utc": row["provider_at_utc"],
                    "available_at_utc": row["assumed_available_at_utc"],
                    "object_date": object_day,
                }
            )
    if not rows:
        raise ValueError("No dated station crosswalk rows")
    covered = {row["station_uuid"] for row in rows}
    if covered != set(expected):
        raise ValueError("Dated station crosswalk does not cover the cohort")
    return rows


def selected_trip_paths(cfg, end_exclusive: date) -> list[Path]:
    """Return all selected start-time partitions capable of supplying valid returns."""
    with duckdb.connect(str(cfg.manifest_db), read_only=True) as ledger:
        records = ledger.execute(
            """SELECT p.path FROM published_files p JOIN sources s USING(source_key)
            WHERE p.dataset='trips' AND s.status='selected' ORDER BY p.path"""
        ).fetchall()
    paths: list[Path] = []
    for (relative,) in records:
        match = re.search(r"trip_year=(\d{4})/trip_month=(\d{2})/", relative)
        if not match:
            raise ValueError(f"Unrecognized trip partition: {relative}")
        first = date(int(match[1]), int(match[2]), 1)
        if first >= end_exclusive:
            continue
        path = (cfg.processed_root / relative).resolve()
        if not path.is_relative_to(cfg.processed_root.resolve()) or not path.is_file():
            raise FileNotFoundError(path)
        paths.append(path)
    if not paths:
        raise ValueError("No selected trip inputs")
    return paths


def create_flow_tables(
    con: duckdb.DuckDBPyConnection,
    trips_relation: str,
    first_origin: datetime,
    last_origin: datetime,
) -> None:
    """Aggregate complete modern departure and return flows over a UTC hour range."""
    start_flags = "[" + ",".join(sql_literal(value) for value in START_DST) + "]"
    end_flags = "[" + ",".join(sql_literal(value) for value in END_DST) + "]"
    first_sql = sql_literal(first_origin.astimezone(UTC).isoformat())
    last_sql = sql_literal(last_origin.astimezone(UTC).isoformat())
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE departures AS
        SELECT start_station_id AS station_id,
            date_trunc('hour',timezone('America/New_York',started_at_local)) AS origin_utc,
            count(*)::BIGINT AS departure_count
        FROM {trips_relation}
        WHERE schema_family='modern' AND eligible_departure
          AND started_at_local IS NOT NULL
          AND NOT list_has_any(coalesce(quality_flags,[]::VARCHAR[]),{start_flags}::VARCHAR[])
          AND date_trunc('hour',timezone('America/New_York',started_at_local))
                BETWEEN {first_sql}::TIMESTAMPTZ AND {last_sql}::TIMESTAMPTZ
        GROUP BY 1,2"""
    )
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE returns AS
        SELECT end_station_id AS station_id,
            date_trunc('hour',timezone('America/New_York',ended_at_local)) AS origin_utc,
            count(*)::BIGINT AS return_count
        FROM {trips_relation}
        WHERE schema_family='modern' AND eligible_arrival
          AND ended_at_local IS NOT NULL
          AND NOT list_has_any(coalesce(quality_flags,[]::VARCHAR[]),{end_flags}::VARCHAR[])
          AND date_trunc('hour',timezone('America/New_York',ended_at_local))
                BETWEEN {first_sql}::TIMESTAMPTZ AND {last_sql}::TIMESTAMPTZ
        GROUP BY 1,2"""
    )


def create_return_table(
    con: duckdb.DuckDBPyConnection,
    trips_relation: str,
    first_origin: datetime,
    last_origin: datetime,
) -> None:
    """Aggregate return counts without rescanning already validated departures."""
    end_flags = "[" + ",".join(sql_literal(value) for value in END_DST) + "]"
    first_sql = sql_literal(first_origin.astimezone(UTC).isoformat())
    last_sql = sql_literal(last_origin.astimezone(UTC).isoformat())
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE returns AS
        SELECT end_station_id AS station_id,
            date_trunc('hour',timezone('America/New_York',ended_at_local)) AS origin_utc,
            count(*)::BIGINT AS return_count
        FROM {trips_relation}
        WHERE schema_family='modern' AND eligible_arrival
          AND ended_at_local IS NOT NULL
          AND NOT list_has_any(coalesce(quality_flags,[]::VARCHAR[]),{end_flags}::VARCHAR[])
          AND date_trunc('hour',timezone('America/New_York',ended_at_local))
                BETWEEN {first_sql}::TIMESTAMPTZ AND {last_sql}::TIMESTAMPTZ
        GROUP BY 1,2"""
    )


def _copy(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM {table}) TO {sql_literal(str(path))} "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    return {
        "path": path.name,
        "rows": con.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
    }


def build_station_hour_integration(
    cfg,
    station_package: Path,
    availability_package: Path,
    weather_root: Path,
    output_root: Path,
) -> Path:
    """Assemble and validate one immutable station-hour integration package."""
    station_package = Path(station_package).resolve()
    availability_package = Path(availability_package).resolve()
    weather_root = Path(weather_root).resolve()
    output_root = Path(output_root).resolve()
    if output_root.exists():
        raise FileExistsError(output_root)
    output_root.mkdir(parents=True)

    station_manifest = json.loads((station_package / "manifest.json").read_text())
    availability_manifest = json.loads((availability_package / "manifest.json").read_text())
    weather_manifest = json.loads((weather_root / "transfer_manifest.json").read_text())
    weather_receipt_path = weather_root.parent.parent / "receipt.json"
    weather_receipt = (
        json.loads(weather_receipt_path.read_text()) if weather_receipt_path.is_file() else None
    )
    if station_manifest.get("status") != "built":
        raise ValueError("Station package is not complete")
    if availability_manifest.get("status") != "built":
        raise ValueError("Availability package is not complete")
    if weather_manifest.get("format_version") != 1:
        raise ValueError("Unsupported weather handoff")

    sources_root = Path(station_manifest["sources_root"]).resolve()
    cohort_path = availability_package / "cohort.json"
    protocol_path = station_package / "protocol.json"
    protocol = json.loads(protocol_path.read_text())
    crosswalk_rows = dated_crosswalk(cohort_path, sources_root / "station_information")

    station_features = station_package / "features.parquet"
    availability_features = availability_package / "features.parquet"
    weather_glob = weather_root / "weather_classes" / "**" / "*.parquet"
    mapping_glob = weather_root / "station_mapping" / "*.parquet"
    for path in (station_features, availability_features):
        if not path.is_file():
            raise FileNotFoundError(path)
    if not list((weather_root / "weather_classes").rglob("*.parquet")):
        raise FileNotFoundError(weather_glob)
    if not list((weather_root / "station_mapping").glob("*.parquet")):
        raise FileNotFoundError(mapping_glob)

    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='UTC'")
        con.execute(f"SET memory_limit={sql_literal(cfg.memory_limit)}")
        con.execute(f"SET threads={int(cfg.threads)}")
        cfg.tmp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={sql_literal(str(cfg.tmp_dir))}")
        con.execute(
            """CREATE TABLE crosswalk(
            station_uuid VARCHAR, station_id VARCHAR, station_name VARCHAR,
            latitude DOUBLE, longitude DOUBLE, capacity BIGINT,
            provider_at_utc TIMESTAMPTZ, available_at_utc TIMESTAMPTZ,
            object_date DATE)"""
        )
        con.executemany(
            "INSERT INTO crosswalk VALUES (?,?,?,?,?,?,?,?,?)",
            [tuple(row.values()) for row in crosswalk_rows],
        )
        con.execute(
            """CREATE TEMP TABLE base AS
            SELECT row_number() OVER (ORDER BY station_id,origin_utc)::BIGINT AS example_id,
                station_id,origin_utc,issue_cutoff_utc,label_available_at_utc,
                information_assumed_available_at_utc,
                temperature AS proxy_temperature_c,
                precipitation AS proxy_precipitation_mm,
                wind AS proxy_wind_speed_ms,
                wet AS proxy_wet,
                advisory_covered,advisory_active,advisory_next3h,advisory_unspecified,
                advisory_observed_at_utc,advisory_feed_age_seconds,
                gtfs_archive_available_at_utc,subway_parent_count_500m,
                subway_parent_count_250m,advisory_active_250m,departures
            FROM read_parquet(?) WHERE horizon=0""",
            [str(station_features)],
        )
        duplicate = con.execute(
            """SELECT count(*) FROM (
            SELECT station_id,origin_utc,count(*) n FROM base GROUP BY ALL HAVING n<>1)"""
        ).fetchone()[0]
        if duplicate:
            raise ValueError("Duplicate station-hour base keys")
        first_origin, last_origin = con.execute(
            "SELECT min(origin_utc),max(origin_utc) FROM base"
        ).fetchone()

        con.execute(
            """CREATE TEMP TABLE keyed AS
            SELECT b.*,x.station_uuid,x.station_name,x.latitude,x.longitude,
                x.capacity AS dated_capacity,x.provider_at_utc AS station_provider_at_utc,
                x.available_at_utc AS crosswalk_available_at_utc,
                x.object_date AS crosswalk_object_date
            FROM base b ASOF LEFT JOIN crosswalk x
              ON b.station_id=x.station_id AND b.origin_utc>x.available_at_utc"""
        )
        missing_crosswalk = con.execute(
            "SELECT count(*) FROM keyed WHERE station_uuid IS NULL"
        ).fetchone()[0]
        if missing_crosswalk:
            raise ValueError(f"Missing as-of crosswalk for {missing_crosswalk} station-hours")

        con.execute(
            """CREATE TEMP TABLE inventory AS
            SELECT station_id,origin_utc,
                min(input_observed_at_utc) AS inventory_observed_at_utc,
                min(information_available_at_utc) AS inventory_information_available_at_utc,
                any_value(current_bikes) AS current_bikes,
                any_value(current_docks) AS current_docks,
                any_value(current_ebikes) AS current_ebikes,
                any_value(disabled_bikes) AS disabled_bikes,
                any_value(disabled_docks) AS disabled_docks,
                any_value(capacity) AS inventory_capacity,
                count(DISTINCT (current_bikes,current_docks,current_ebikes,
                    disabled_bikes,disabled_docks,capacity,input_observed_at_utc,
                    information_available_at_utc)) AS state_versions
            FROM read_parquet(?) GROUP BY station_id,origin_utc""",
            [str(availability_features)],
        )
        if con.execute("SELECT count(*) FROM inventory WHERE state_versions<>1").fetchone()[0]:
            raise ValueError("Availability state differs across horizons")

        mapping_sql = sql_literal(str(mapping_glob))
        weather_sql = sql_literal(str(weather_glob))
        weather_rows, weather_keys, mapping_rows = con.execute(
            f"""SELECT
                (SELECT count(*) FROM read_parquet({weather_sql},hive_partitioning=true)),
                (SELECT count(DISTINCT (forecast_origin_utc,forecast_horizon,weather_request_class))
                    FROM read_parquet({weather_sql},hive_partitioning=true)),
                (SELECT count(*) FROM read_parquet({mapping_sql}))"""
        ).fetchone()
        if weather_rows != weather_manifest["class_rows"] or weather_keys != weather_rows:
            raise ValueError("Weather class rows do not reconcile")
        if mapping_rows != weather_manifest["station_count"]:
            raise ValueError("Weather station mapping count does not reconcile")
        con.execute(
            f"""CREATE TEMP TABLE spatial_weather AS
            SELECT m.station_id AS station_uuid,
                timezone('UTC',w.forecast_origin_utc) AS origin_utc,
                timezone('UTC',w.target_hour_utc) AS target_hour_utc,
                timezone('UTC',w.selected_model_run_utc) AS selected_model_run_utc,
                timezone('UTC',w.selected_run_available_utc) AS selected_run_available_utc,
                w.forecast_temperature_f,w.forecast_precipitation_in,w.forecast_rain_in,
                w.forecast_snowfall_in,w.forecast_wind_speed_mph,
                w.weather_request_class,w.is_fallback_weather_run,
                w.weather_forecast_has_missing,w.weather_model,
                w.assumed_availability_delay_hours,w.dataset_build_id,
                m.weather_mapping_distance_km,m.station_geography_policy,
                timezone('UTC',m.station_snapshot_cutoff_utc) AS station_snapshot_cutoff_utc
            FROM read_parquet({mapping_sql}) m
            JOIN (SELECT DISTINCT station_uuid FROM keyed) c
              ON m.station_id=c.station_uuid
            JOIN read_parquet({weather_sql},hive_partitioning=true) w
              USING(weather_request_class,dataset_build_id)
            JOIN (SELECT DISTINCT origin_utc FROM base) b
              ON timezone('UTC',w.forecast_origin_utc)=b.origin_utc
            WHERE w.forecast_horizon=1"""
        )
        weather_duplicates = con.execute(
            """SELECT count(*) FROM (SELECT station_uuid,origin_utc,count(*) n
                FROM spatial_weather GROUP BY ALL HAVING n<>1)"""
        ).fetchone()
        if weather_duplicates[0]:
            raise ValueError("Duplicate station-hour spatial weather rows")

        end_exclusive = date(last_origin.year, last_origin.month, 1)
        if end_exclusive.month == 12:
            end_exclusive = date(end_exclusive.year + 1, 1, 1)
        else:
            end_exclusive = date(end_exclusive.year, end_exclusive.month + 1, 1)
        trip_paths = selected_trip_paths(cfg, end_exclusive)
        paths_sql = "[" + ",".join(sql_literal(str(path)) for path in trip_paths) + "]"
        con.execute(
            f"""CREATE TEMP VIEW trips AS
            SELECT * FROM read_parquet({paths_sql},hive_partitioning=false)"""
        )
        create_return_table(con, "trips", first_origin, last_origin)

        con.execute(
            "CREATE TABLE split_tags(origin_utc TIMESTAMPTZ, model_split VARCHAR, refit_eligible BOOLEAN)"
        )
        origins = [
            row[0]
            for row in con.execute("SELECT DISTINCT origin_utc FROM base ORDER BY 1").fetchall()
        ]
        con.executemany(
            "INSERT INTO split_tags VALUES (?,?,?)",
            [(origin, *split_for(origin, protocol)) for origin in origins],
        )

        con.execute(
            """CREATE TEMP TABLE features AS
            SELECT k.example_id,k.station_id,k.station_uuid,k.origin_utc,k.issue_cutoff_utc,
                s.model_split,s.refit_eligible,
                k.station_name,k.latitude,k.longitude,k.dated_capacity,
                k.station_provider_at_utc,k.crosswalk_available_at_utc,k.crosswalk_object_date,
                i.inventory_observed_at_utc,i.inventory_information_available_at_utc,
                i.current_bikes,i.current_docks,i.current_ebikes,i.disabled_bikes,i.disabled_docks,
                i.inventory_capacity,(i.station_id IS NOT NULL) AS inventory_covered,
                k.proxy_temperature_c,k.proxy_precipitation_mm,k.proxy_wind_speed_ms,k.proxy_wet,
                sw.target_hour_utc,sw.selected_model_run_utc,sw.selected_run_available_utc,
                sw.forecast_temperature_f,sw.forecast_precipitation_in,sw.forecast_rain_in,
                sw.forecast_snowfall_in,sw.forecast_wind_speed_mph,sw.weather_request_class,
                sw.is_fallback_weather_run,sw.weather_forecast_has_missing,sw.weather_model,
                sw.assumed_availability_delay_hours,sw.dataset_build_id AS weather_dataset_build_id,
                sw.weather_mapping_distance_km,sw.station_geography_policy,
                sw.station_snapshot_cutoff_utc,(sw.station_uuid IS NOT NULL) AS spatial_weather_covered,
                (sw.station_uuid IS NOT NULL AND NOT sw.weather_forecast_has_missing
                    AND sw.selected_run_available_utc<k.issue_cutoff_utc)
                    AS spatial_weather_sensitivity_eligible,
                (sw.station_uuid IS NOT NULL AND NOT sw.weather_forecast_has_missing
                    AND sw.selected_run_available_utc<k.issue_cutoff_utc
                    AND sw.station_snapshot_cutoff_utc<k.issue_cutoff_utc)
                    AS spatial_weather_strict_asof_eligible,
                k.advisory_covered,k.advisory_active,k.advisory_next3h,k.advisory_unspecified,
                k.advisory_observed_at_utc,k.advisory_feed_age_seconds,
                k.gtfs_archive_available_at_utc,k.subway_parent_count_500m,
                k.subway_parent_count_250m,k.advisory_active_250m,
                (k.advisory_covered AND k.advisory_observed_at_utc<k.issue_cutoff_utc
                    AND k.gtfs_archive_available_at_utc<k.issue_cutoff_utc) AS disruption_asof_eligible
            FROM keyed k JOIN split_tags s USING(origin_utc)
            LEFT JOIN inventory i USING(station_id,origin_utc)
            LEFT JOIN spatial_weather sw USING(station_uuid,origin_utc)"""
        )
        con.execute(
            """CREATE TEMP TABLE labels AS
            SELECT k.example_id,k.station_id,k.origin_utc,k.label_available_at_utc,
                k.departures::BIGINT AS departures,
                coalesce(r.return_count,0)::BIGINT AS returns,
                true AS departure_operation_supported,
                (i.station_id IS NOT NULL) AS return_operation_supported
            FROM keyed k
            LEFT JOIN returns r USING(station_id,origin_utc)
            LEFT JOIN inventory i USING(station_id,origin_utc)"""
        )

        future_crosswalk = con.execute(
            "SELECT count(*) FROM features WHERE crosswalk_available_at_utc>=origin_utc"
        ).fetchone()[0]
        future_disruptions = con.execute(
            """SELECT count(*) FROM features
            WHERE advisory_covered AND (advisory_observed_at_utc>=issue_cutoff_utc
                OR gtfs_archive_available_at_utc>=issue_cutoff_utc)"""
        ).fetchone()[0]
        if future_crosswalk or future_disruptions:
            raise ValueError("Predictor availability violation")
        if con.execute("SELECT count(*) FROM features").fetchone()[0] != con.execute(
            "SELECT count(*) FROM labels"
        ).fetchone()[0]:
            raise ValueError("Feature/label row counts differ")

        artifacts = [
            _copy(con, "features", output_root / "features.parquet"),
            _copy(con, "labels", output_root / "labels.parquet"),
            _copy(con, "crosswalk", output_root / "station_crosswalk.parquet"),
        ]
        coverage_rows = con.execute(
            """SELECT model_split,count(*) AS row_count,
            count(*) FILTER(WHERE inventory_covered) inventory_rows,
            count(*) FILTER(WHERE spatial_weather_covered) spatial_weather_rows,
            count(*) FILTER(WHERE spatial_weather_sensitivity_eligible) spatial_weather_sensitivity_rows,
            count(*) FILTER(WHERE spatial_weather_strict_asof_eligible) spatial_weather_strict_rows,
            count(*) FILTER(WHERE advisory_covered) disruption_rows,
            count(*) FILTER(WHERE disruption_asof_eligible) disruption_asof_rows
            FROM features GROUP BY 1 ORDER BY 1"""
        ).fetchall()
        coverage = [
            {
                "split": row[0],
                "rows": row[1],
                "inventory_rows": row[2],
                "spatial_weather_rows": row[3],
                "spatial_weather_sensitivity_rows": row[4],
                "spatial_weather_strict_rows": row[5],
                "disruption_rows": row[6],
                "disruption_asof_rows": row[7],
            }
            for row in coverage_rows
        ]
        manifest = {
            "schema_version": "station-hour-integration-v1",
            "status": "validated_with_coverage_gates",
            "grain": "one row per supported station and forecast origin; next-hour flow labels",
            "first_origin_utc": first_origin.isoformat(),
            "last_origin_utc": last_origin.isoformat(),
            "rows": artifacts[0]["rows"],
            "stations": con.execute(
                "SELECT count(DISTINCT station_id) FROM features"
            ).fetchone()[0],
            "artifacts": artifacts,
            "coverage_by_split": coverage,
            "inputs": {
                "station_package": str(station_package),
                "station_manifest_sha256": sha256(station_package / "manifest.json"),
                "availability_package": str(availability_package),
                "availability_manifest_sha256": sha256(availability_package / "manifest.json"),
                "weather_root": str(weather_root),
                "weather_manifest_sha256": sha256(weather_root / "transfer_manifest.json"),
                "weather_dataset_build_id": weather_manifest["dataset_build_id"],
                "weather_zip_sha256": (
                    weather_receipt.get("zip_sha256") if weather_receipt else None
                ),
                "weather_receipt_sha256": (
                    sha256(weather_receipt_path) if weather_receipt else None
                ),
                "trip_files": len(trip_paths),
                "protocol_sha256": sha256(protocol_path),
            },
            "contracts": {
                "identifier": "dated GBFS station UUID to Citi Bike short_name; strict before origin",
                "flows": "validated station-pilot departures plus returns by end hour from every selected earlier trip partition",
                "weather": "forecast horizon 1 joined by UUID weather class; missing values retained",
                "disruptions": "null unless a semantically complete archived snapshot predates the issue cutoff",
                "split": "frozen station pilot train/validation/test dates; refit membership is separate",
                "label_separation": "features.parquet contains no departure or return outcomes",
            },
            "gates": [
                "The weather station mapping is a 2026-10-18 static retrospective snapshot, so strict as-of weather eligibility is expected to be zero for historical rows.",
                "The delivered spatial weather package ends 2026-06-30; September test coverage is absent.",
                "Legacy April-June subway archives lack complete selector/display semantics; disruption values remain null rather than zero.",
                "Return labels are supported only where a fresh inventory row also establishes return-enabled endpoint coverage.",
            ],
        }
        (output_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    finally:
        con.close()
    return output_root


def validate_station_hour_integration(root: Path) -> dict:
    root = Path(root).resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest.get("status") != "validated_with_coverage_gates":
        raise ValueError("Integration package is incomplete")
    for artifact in manifest["artifacts"]:
        path = (root / artifact["path"]).resolve()
        if not path.is_relative_to(root) or sha256(path) != artifact["sha256"]:
            raise ValueError("Integration artifact checksum mismatch")
        with duckdb.connect() as con:
            rows = con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(path)]
            ).fetchone()[0]
        if rows != artifact["rows"]:
            raise ValueError("Integration artifact row-count mismatch")
    return manifest
