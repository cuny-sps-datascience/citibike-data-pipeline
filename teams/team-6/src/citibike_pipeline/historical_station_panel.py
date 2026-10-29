"""Bounded historical departure panel; unknown station exposure never becomes zero.

This package is independent of the frozen 32-station experiments. Published
weather/subway archives use retrospective geography, explicitly authorized for
this study. Their origin and target timestamps are different concepts.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from zoneinfo import ZoneInfo

import duckdb
from shapely.geometry import Point, shape

from .db import sql_literal
from .forecast_sources import digest
from .borough_mvp import release_at


DATASETS = {
    "weather_classes", "station_weather_mapping", "subway_bridge",
    "subway_alert_features", "subway_retrospective",
}
WEATHER_VALUES = [
    "forecast_temperature_f", "forecast_precipitation_in", "forecast_rain_in",
    "forecast_snowfall_in", "forecast_wind_speed_mph",
]
EVENT_VALUES = [
    "recent_event_count", "recent_reroute_event_count", "recent_suspended_event_count",
    "recent_part_suspended_event_count", "recent_stops_skipped_event_count",
    "recent_severe_delays_event_count", "recent_delays_event_count",
    "recent_cancellations_event_count", "recent_express_to_local_event_count",
    "recent_boarding_change_event_count", "recent_information_outage_event_count",
]


def checked_path(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if Path(relative).is_absolute() or not path.is_relative_to(root.resolve()):
        raise ValueError(f"Manifest path escapes package: {relative}")
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


def parquet_view(con, name: str, paths: list[Path]) -> None:
    if not paths:
        raise ValueError(f"No manifest-listed files for {name}")
    con.read_parquet([str(p) for p in paths], hive_partitioning=False).create_view(name, replace=True)


def load_transfer(con, root: Path) -> dict:
    """Verify all producer-listed files before reading any Parquet table."""
    manifest = json.loads((root / "transfer_manifest.json").read_text())
    if set(manifest["datasets"]) != DATASETS:
        raise ValueError("Unexpected transfer table contract")
    listed = manifest["files"]
    if len({f["path"] for f in listed}) != len(listed):
        raise ValueError("Duplicate transfer manifest file")
    for item in listed:
        path = checked_path(root, item["path"])
        if path.stat().st_size != item["size_bytes"] or digest(path) != item["sha256"]:
            raise ValueError(f"Transfer checksum/size mismatch: {item['path']}")
    report = {"build_id": manifest["build_id"], "files_verified": len(listed), "tables": {}}
    for name, dataset in manifest["datasets"].items():
        paths = [checked_path(root, f["path"]) for f in listed
                 if f["path"].startswith(dataset["path"].rstrip("/") + "/")]
        parquet_view(con, name, paths)
        columns = {r[0] for r in con.execute(f"DESCRIBE {name}").fetchall()}
        if columns != {f["name"] for f in dataset["schema"]["fields"]}:
            raise ValueError(f"Transfer schema mismatch: {name}")
        keys = dataset["primary_key"]
        key_sql = ",".join(keys)
        null_sql = " OR ".join(f"{k} IS NULL" for k in keys)
        rows, unique, nulls = con.execute(
            f"SELECT count(*),count(DISTINCT ({key_sql})),"
            f"count(*) FILTER(WHERE {null_sql}) FROM {name}"
        ).fetchone()
        if rows != dataset["rows"] or rows != unique or nulls:
            raise ValueError(f"Transfer row/key mismatch: {name}")
        report["tables"][name] = {"rows": rows, "unique_keys": unique}
    invalid = con.execute("""SELECT count(*) FROM weather_classes WHERE
        target_hour_utc<>forecast_origin_utc+forecast_horizon*INTERVAL '1 hour'
        OR valid_time_utc<>target_hour_utc OR selected_run_available_utc IS NULL
        OR selected_model_run_utc IS NULL OR assumed_availability_delay_hours<>7
        OR selected_run_available_utc<>selected_model_run_utc+INTERVAL '7 hours'
        OR selected_run_available_utc>forecast_origin_utc""").fetchone()[0]
    if invalid:
        raise ValueError("Invalid weather target/availability contract")
    invalid = con.execute("""SELECT count(*) FROM subway_alert_features WHERE
        latest_publication_time_utc>=forecast_origin_utc
        OR latest_publication_time_utc<forecast_origin_utc-INTERVAL '3 hours'
        OR publication_window_hours<>3 OR assumed_ingestion_delay_minutes<>0
        OR (archive_silence_last_3h AND recent_event_count IS NOT NULL)""").fetchone()[0]
    if invalid:
        raise ValueError("Invalid subway publication contract")
    return report


def reference_input(root: Path, name: str) -> tuple[Path, dict]:
    manifest = json.loads((root / "manifest.json").read_text())
    items = [a for a in manifest["artifacts"] if a["name"] == name and a["path"].startswith("raw/")]
    if len(items) != 1:
        raise ValueError(f"Missing unique pinned reference: {name}")
    item = items[0]
    path = checked_path(root, item["path"])
    if digest(path) != item["sha256"]:
        raise ValueError(f"Reference checksum mismatch: {name}")
    return path, item


def station_cohort(con, reference_root: Path, borough: str) -> dict:
    information_path, info_receipt = reference_input(reference_root, "gbfs_station_information")
    borough_path, borough_receipt = reference_input(reference_root, "boroughs")
    information = json.loads(information_path.read_text())
    stations = information["data"]["stations"]
    uuid_counts = Counter(str(s.get("station_id", "")) for s in stations)
    short_counts = Counter(str(s.get("short_name", "")) for s in stations)
    polygons = [shape(f["geometry"]) for f in json.loads(borough_path.read_text())["features"]
                if f["properties"]["boroname"] == borough]
    if len(polygons) != 1:
        raise ValueError("Unknown/ambiguous borough boundary")
    rows = []
    for s in stations:
        uuid, short = str(s.get("station_id", "")), str(s.get("short_name", ""))
        if uuid and short and uuid_counts[uuid] == short_counts[short] == 1:
            rows.append((uuid, short, float(s["lat"]), float(s["lon"])))
    con.execute("""CREATE TEMP TABLE identity_reference(
        station_uuid VARCHAR,station_id VARCHAR,reference_latitude DOUBLE,reference_longitude DOUBLE)""")
    con.executemany("INSERT INTO identity_reference VALUES (?,?,?,?)", rows)
    mapping = con.execute("SELECT station_id,station_latitude,station_longitude FROM station_weather_mapping").fetchall()
    in_borough = [(uuid,) for uuid, lat, lon in mapping if polygons[0].covers(Point(lon, lat))]
    con.execute("CREATE TEMP TABLE borough_uuids(station_uuid VARCHAR)")
    con.executemany("INSERT INTO borough_uuids VALUES (?)", in_borough)
    con.execute("""CREATE TEMP TABLE cohort AS
        SELECT i.station_id,i.station_uuid,m.station_name,m.station_latitude,m.station_longitude,
            m.weather_request_class,m.dataset_build_id,m.weather_mapping_distance_km,
            m.station_snapshot_cutoff_utc::TIMESTAMPTZ AS station_snapshot_cutoff_utc,
            sqrt(pow((m.station_latitude-i.reference_latitude)*111195,2)+
            pow(cos(radians(m.station_latitude))*(m.station_longitude-i.reference_longitude)*111195,2))
                AS identity_distance_m
        FROM station_weather_mapping m JOIN identity_reference i ON m.station_id=i.station_uuid
        JOIN borough_uuids b ON m.station_id=b.station_uuid
        WHERE identity_distance_m<=25""")
    count = con.execute("SELECT count(*) FROM cohort").fetchone()[0]
    if not count:
        raise ValueError("No unique compatible borough station identities")
    return {"stations": count, "mapped_borough_stations": len(in_borough),
            "excluded_identity_or_location": len(in_borough)-count,
            "information": info_receipt, "borough_boundary": borough_receipt,
            "policy": "Unique October GBFS UUID/short_name; <=25m reference agreement; retrospective snapshot cohort"}


def copy_table(con, query: str, root: Path, relative: str, table: str) -> dict:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO {sql_literal(str(path))} (FORMAT PARQUET,COMPRESSION ZSTD)")
    rows = con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
    return {"table": table, "path": relative, "rows": rows, "bytes": path.stat().st_size,
            "sha256": digest(path)}


def monthly_frames(con, first: datetime, end: datetime, cutoff_minutes: int) -> None:
    """Inputs are verified source views, cohort, splits and monthly observations.

    Source weather horizons 1/2/3 at T-1 correspond to our target hours
    T/T+1/T+2. With a five-minute cutoff we also use subway origin T-1;
    a top-of-hour study may use subway origin T. No publication is shifted
    forward and no retrospective exposure table enters this query.
    """
    if cutoff_minutes not in (0, 5):
        raise ValueError("Supported issue cutoffs are zero or five minutes")
    subway_offset = 1 if cutoff_minutes else 0
    con.execute("""CREATE OR REPLACE TEMP TABLE origins AS
        SELECT origin_utc,s.model_split,s.end_utc AS split_end_utc
        FROM generate_series(?::TIMESTAMPTZ,?::TIMESTAMPTZ-INTERVAL '1 hour',INTERVAL '1 hour') g(origin_utc)
        JOIN splits s ON origin_utc>=s.start_utc AND origin_utc<s.end_utc
        WHERE origin_utc+INTERVAL '3 hours'<=s.end_utc""", [first, end])
    w_values = ",".join("w." + col for col in WEATHER_VALUES)
    s_values = ",".join("s." + col for col in EVENT_VALUES)
    weather_complete = " AND ".join(f"w.{col} IS NOT NULL" for col in WEATHER_VALUES)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE panel_features AS
        SELECT c.station_id,c.station_uuid,o.origin_utc,h.horizon::SMALLINT AS horizon,
            o.origin_utc+(h.horizon-1)*INTERVAL '1 hour' AS target_hour_utc,
            o.origin_utc-INTERVAL '{cutoff_minutes} minutes' AS issue_cutoff_utc,
            o.model_split,o.split_end_utc,
            isodow(timezone('America/New_York',o.origin_utc+(h.horizon-1)*INTERVAL '1 hour'))::SMALLINT AS target_weekday,
            hour(timezone('America/New_York',o.origin_utc+(h.horizon-1)*INTERVAL '1 hour'))::SMALLINT AS target_local_hour,
            month(timezone('America/New_York',o.origin_utc+(h.horizon-1)*INTERVAL '1 hour'))::SMALLINT AS target_local_month,
            c.station_latitude,c.station_longitude,c.station_snapshot_cutoff_utc,
            true AS retrospective_geography,
            (c.station_snapshot_cutoff_utc<issue_cutoff_utc) AS strict_asof_geography_eligible,
            c.weather_mapping_distance_km,c.weather_request_class,
            w.dataset_build_id AS weather_dataset_build_id,
            w.forecast_origin_utc::TIMESTAMPTZ AS weather_source_origin_utc,
            w.target_hour_utc::TIMESTAMPTZ AS weather_target_hour_utc,
            w.selected_model_run_utc::TIMESTAMPTZ AS selected_model_run_utc,
            w.selected_run_available_utc::TIMESTAMPTZ AS selected_run_available_utc,
            w.is_fallback_weather_run,w.weather_forecast_has_missing,
            {w_values},
            coalesce(w.target_hour_utc::TIMESTAMPTZ=o.origin_utc+(h.horizon-1)*INTERVAL '1 hour'
                AND w.selected_run_available_utc::TIMESTAMPTZ<issue_cutoff_utc
                AND NOT w.weather_forecast_has_missing AND {weather_complete},false) AS weather_eligible,
            s.forecast_origin_utc::TIMESTAMPTZ AS subway_source_origin_utc,
            s.latest_publication_time_utc::TIMESTAMPTZ AS latest_publication_time_utc,
            s.latest_publication_age_minutes,s.archive_silence_last_3h,
            s.archive_publications_last_3h,s.has_recent_route_publication,
            s.has_ambiguous_shuttle_link,s.source_transfer_build_id AS subway_source_build_id,
            (s.station_id IS NOT NULL) AS subway_mapped,
            {s_values},
            coalesce(NOT s.archive_silence_last_3h AND s.recent_event_count IS NOT NULL
                AND s.forecast_origin_utc::TIMESTAMPTZ<=issue_cutoff_utc
                AND (s.latest_publication_time_utc IS NULL
                    OR s.latest_publication_time_utc::TIMESTAMPTZ<issue_cutoff_utc),false) AS subway_eligible
        FROM cohort c CROSS JOIN origins o CROSS JOIN (VALUES (1),(2),(3)) h(horizon)
        LEFT JOIN weather_classes w ON w.weather_request_class=c.weather_request_class
            AND w.dataset_build_id=c.dataset_build_id AND w.forecast_horizon=h.horizon
            AND w.forecast_origin_utc::TIMESTAMPTZ=o.origin_utc-INTERVAL '1 hour'
            AND w.target_hour_utc::TIMESTAMPTZ=o.origin_utc+(h.horizon-1)*INTERVAL '1 hour'
        LEFT JOIN subway_alert_features s ON s.station_id=c.station_uuid
            AND s.forecast_origin_utc::TIMESTAMPTZ=o.origin_utc-INTERVAL '{subway_offset} hour'
    """)
    # Historical monthly trip geography is a label-consistency diagnostic only;
    # it must never become a predictor derived from the target month's outcomes.
    con.execute("""CREATE OR REPLACE TEMP TABLE panel_labels AS
        SELECT f.station_id,f.origin_utc,f.horizon,f.target_hour_utc,
            CASE WHEN timezone('America/New_York',f.target_hour_utc)
                IN (timezone('America/New_York',f.target_hour_utc-INTERVAL '1 hour'),
                    timezone('America/New_York',f.target_hour_utc+INTERVAL '1 hour'))
                THEN NULL ELSE coalesce(o.departures,
                    CASE WHEN p.departure_operation_supported THEN p.departures END) END::BIGINT AS departures,
            r.label_available_at_utc,
            coalesce(p.departure_operation_supported,false) AS conditional_operation_supported,
            false AS historical_operation_verified,false AS main_evaluation_eligible,
            sqrt(pow((g.latitude-f.station_latitude)*111195,2)+
                pow(cos(radians(f.station_latitude))*(g.longitude-f.station_longitude)*111195,2))
                AS observed_month_geography_distance_m,
            coalesce(o.departures,p.departures) IS NOT NULL AS source_count_present,
            bool_and(coalesce(p.departure_operation_supported,false) AND f.weather_eligible
                AND f.subway_eligible) OVER(PARTITION BY f.station_id,f.origin_utc)
                AS three_horizon_conditional_inputs_supported
        FROM panel_features f
        LEFT JOIN observations o ON o.station_id=f.station_id AND o.id_scheme='short_name'
            AND o.hour_utc=f.target_hour_utc
        LEFT JOIN pilot_labels p ON p.station_id=f.station_id AND p.origin_utc=f.target_hour_utc
        LEFT JOIN monthly_geography g ON g.station_id=f.station_id AND g.id_scheme='short_name'
            AND g.month_local=date_trunc('month',timezone('America/New_York',f.target_hour_utc))::DATE
        JOIN release_availability r
            ON r.month_local=date_trunc('month',timezone('America/New_York',f.target_hour_utc))::DATE
    """)
    disagreement = con.execute("""SELECT count(*) FROM panel_features f
        JOIN observations o ON o.station_id=f.station_id AND o.id_scheme='short_name' AND o.hour_utc=f.target_hour_utc
        JOIN pilot_labels p ON p.station_id=f.station_id AND p.origin_utc=f.target_hour_utc
        WHERE p.departure_operation_supported AND o.departures<>p.departures""").fetchone()[0]
    if disagreement:
        raise ValueError("Observed and pilot departure counts disagree")
    assert_frames(con)


def assert_frames(con) -> dict:
    """Check joins, targets, boundary purging and the unknown-zero policy."""
    n, keys = con.execute("""SELECT count(*),count(DISTINCT(station_id,origin_utc,horizon))
        FROM panel_features""").fetchone()
    labels, label_keys = con.execute("""SELECT count(*),count(DISTINCT(station_id,origin_utc,horizon))
        FROM panel_labels""").fetchone()
    mismatch = con.execute("""SELECT count(*) FROM panel_features f FULL JOIN panel_labels l
        USING(station_id,origin_utc,horizon)
        WHERE f.target_hour_utc IS NULL OR l.target_hour_utc IS NULL
            OR f.target_hour_utc<>l.target_hour_utc""").fetchone()[0]
    if n != keys or labels != label_keys or n != labels or mismatch:
        raise ValueError("Feature/label keys or row counts differ")
    bad = con.execute("""SELECT count(*) FROM panel_features WHERE
        horizon NOT BETWEEN 1 AND 3
        OR target_hour_utc<>origin_utc+(horizon-1)*INTERVAL '1 hour'
        OR origin_utc+INTERVAL '3 hours'>split_end_utc
        OR (weather_target_hour_utc IS NOT NULL AND weather_target_hour_utc<>target_hour_utc)
        OR (weather_eligible AND selected_run_available_utc>=issue_cutoff_utc)
        OR (subway_eligible AND latest_publication_time_utc>=issue_cutoff_utc)""").fetchone()[0]
    incomplete = con.execute("""SELECT count(*) FROM (SELECT station_id,origin_utc,count(*) n
        FROM panel_features GROUP BY ALL HAVING n<>3)""").fetchone()[0]
    zeros = con.execute("""SELECT count(*) FROM panel_labels WHERE
        (departures=0 AND NOT conditional_operation_supported) OR departures<0
        OR main_evaluation_eligible OR historical_operation_verified""").fetchone()[0]
    if bad or incomplete or zeros:
        raise ValueError("Target/cutoff, three-horizon or unsupported-zero violation")
    return {"rows": n, "unique_keys": keys, "target_cutoff_violations": bad,
            "unsupported_zeros": zeros, "feature_label_key_mismatches": mismatch}


def build_historical_panel(
    observed_root: Path, transfer_root: Path, reference_root: Path,
    pilot_root: Path, protocol_path: Path, output: Path,
) -> dict:
    """Write a new immutable panel without moving any existing package pointer."""
    from .hourly_export import validate_hourly_package
    began = time.monotonic()
    protocol = json.loads(protocol_path.read_text())
    if (protocol["horizons"] != [1, 2, 3] or protocol["issue_cutoff_minutes"] != 5
            or protocol["panel_mode"] != "full_with_unknowns"
            or not protocol["allow_retrospective_geography"]):
        raise ValueError("Protocol does not match the authorized study")
    validate_hourly_package(observed_root)
    observed = json.loads((observed_root / "manifest.json").read_text())
    pilot = json.loads((pilot_root / "manifest.json").read_text())
    pilot_artifacts = [a for a in pilot["artifacts"] if a["path"] == "labels.parquet"]
    if len(pilot_artifacts) != 1:
        raise ValueError("Missing unique pilot-label artifact")
    pilot_artifact = pilot_artifacts[0]
    pilot_labels = checked_path(pilot_root, pilot_artifact["path"])
    if digest(pilot_labels) != pilot_artifact["sha256"]:
        raise ValueError("Pilot label checksum mismatch")
    zone = ZoneInfo("America/New_York")
    def midnight(day):
        return datetime.fromisoformat(day).replace(tzinfo=zone).astimezone(timezone.utc)
    splits = [(s["name"], midnight(s["start_local"]), midnight(s["end_exclusive_local"]))
              for s in protocol["splits"]]
    if (len(splits) != 3 or [s[0] for s in splits] != ["train", "validation", "test"]
            or any(a >= b for _, a, b in splits)
            or any(splits[i][2] != splits[i+1][1] for i in range(2))):
        raise ValueError("Invalid chronological split boundaries")
    index = {(a["table"], Path(a["path"]).stem): a for a in observed["artifacts"]}
    # This original pilot supplies bounded operation evidence for a small subset
    # only. It cannot establish historical operation for the larger cohort.
    output.mkdir(parents=True, exist_ok=False)
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='UTC'")
        con.execute("SET memory_limit='3GB'")
        con.execute("SET threads=2")
        (output / "scratch").mkdir()
        con.execute("SET temp_directory=" + sql_literal(str(output / "scratch")))
        transfer_report = load_transfer(con, transfer_root)
        cohort_report = station_cohort(con, reference_root, protocol["borough"])
        con.execute("CREATE TEMP TABLE splits(model_split VARCHAR,start_utc TIMESTAMPTZ,end_utc TIMESTAMPTZ)")
        con.executemany("INSERT INTO splits VALUES (?,?,?)", splits)
        parquet_view(con, "pilot_labels", [pilot_labels])
        # Preserve conservative source-release maturity for every supplying
        # release in a target month, including later-month spillovers.
        releases = {}
        for item in observed["input_files"]:
            month = item["partition"][:7]
            releases[month] = max(releases.get(month, release_at(month)), release_at(item["nominal_release"]))
        con.execute("CREATE TEMP TABLE release_availability(month_local DATE,label_available_at_utc TIMESTAMPTZ)")
        con.executemany("INSERT INTO release_availability VALUES (?,?)",
                        [(m+"-01", d) for m, d in sorted(releases.items())])
        artifacts = [copy_table(con, "SELECT * FROM cohort ORDER BY station_id", output,
                                "station_crosswalk.parquet", "station_crosswalk")]
        coverage = []
        first, last = splits[0][1], splits[-1][2]
        current = first.astimezone(zone).replace(day=1)
        while current.astimezone(timezone.utc) < last:
            following = current.replace(year=current.year+1, month=1) if current.month == 12 else current.replace(month=current.month+1)
            batch_first = max(first, current.astimezone(timezone.utc))
            batch_end = min(last, following.astimezone(timezone.utc))
            # Three-hour origins may cross a month boundary within one split.
            months = [current.strftime("%Y-%m")]
            if following.astimezone(timezone.utc) < last:
                months.append(following.strftime("%Y-%m"))
            for table, view in [("hourly_observed", "observations"), ("station_geography_observed", "monthly_geography")]:
                items = [index[table, month] for month in months]
                parquet_view(con, view, [checked_path(observed_root, a["path"]) for a in items])
            monthly_frames(con, batch_first, batch_end, protocol["issue_cutoff_minutes"])
            month = current.strftime("%Y-%m")
            for table, view in [("features", "panel_features"), ("labels", "panel_labels")]:
                artifacts.append(copy_table(con, f"SELECT * FROM {view} ORDER BY station_id,origin_utc,horizon",
                    output, f"{table}/{month}.parquet", table))
            result = con.execute("""SELECT f.model_split,count(*) AS rows,
                count(DISTINCT f.station_id) AS stations,
                count(*) FILTER(WHERE f.weather_eligible) AS weather_eligible_rows,
                count(*) FILTER(WHERE f.subway_eligible) AS subway_eligible_rows,
                count(*) FILTER(WHERE f.weather_eligible AND f.subway_eligible) AS combined_predictor_rows,
                count(*) FILTER(WHERE l.departures IS NOT NULL) AS known_count_rows,
                count(*) FILTER(WHERE l.departures IS NULL) AS unknown_count_rows,
                count(*) FILTER(WHERE l.departures=0) AS supported_zero_rows,
                count(*) FILTER(WHERE l.conditional_operation_supported) AS conditional_operation_rows,
                count(*) FILTER(WHERE l.three_horizon_conditional_inputs_supported) AS conditional_common_rows,
                count(*) FILTER(WHERE f.strict_asof_geography_eligible) AS strict_geography_rows
                FROM panel_features f JOIN panel_labels l USING(station_id,origin_utc,horizon)
                GROUP BY f.model_split ORDER BY f.model_split""")
            names = [d[0] for d in result.description]
            coverage.extend({"month": month, **dict(zip(names, row))} for row in result.fetchall())
            print(f"{month}: {sum(a['rows'] for a in coverage if a['month']==month):,} station-origin-horizon rows", flush=True)
            current = following
        (output / "protocol.json").write_bytes(protocol_path.read_bytes())
        artifacts.append({"table": "protocol", "path": "protocol.json", "sha256": digest(output / "protocol.json"),
                          "bytes": (output / "protocol.json").stat().st_size})
        report = {
            "schema_version": "historical-station-panel-v1", "status": "validated_with_coverage_gates",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "rows": sum(a["rows"] for a in artifacts if a["table"] == "features"),
            "stations": cohort_report["stations"], "cohort": cohort_report,
            "transfer_audit": transfer_report, "coverage": coverage, "artifacts": artifacts,
            "inputs": {"observed_root": str(observed_root), "observed_manifest_sha256": digest(observed_root/"manifest.json"),
                       "transfer_root": str(transfer_root), "transfer_manifest_sha256": digest(transfer_root/"transfer_manifest.json"),
                       "reference_root": str(reference_root), "reference_manifest_sha256": digest(reference_root/"manifest.json"),
                       "pilot_labels": str(pilot_labels), "pilot_labels_sha256": digest(pilot_labels),
                       "protocol_sha256": digest(protocol_path)},
            "main_evaluation_ready": False, "model_activated": False, "existing_pointers_changed": False,
            "gates": ["Historical station operation is unverified across the full study; absent counts stay null.",
                      "The October snapshot cohort omits stations retired before that snapshot and may include stations not yet opened.",
                      "Static retrospective geography and assumed weather/trip-release availability prevent strict operational reconstruction.",
                      "Conditional zero labels come only from the preserved bounded 32-station pilot, never from missing trips or inventory.",
                      "Demand-profile means and unconditional model evaluation remain gated on valid historical exposure.",
                      "Earlier periods have been inspected; these splits are retrospective development, not untouched confirmation."],
            "seconds": round(time.monotonic()-began, 2),
        }
        (output / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")
    finally:
        con.close()
    return validate_historical_panel(output)


def validate_historical_panel(root: Path) -> dict:
    report = json.loads((root / "manifest.json").read_text())
    if report.get("schema_version") != "historical-station-panel-v1" or report.get("status") != "validated_with_coverage_gates":
        raise ValueError("Incomplete historical panel")
    with duckdb.connect() as con:
        con.execute("SET TimeZone='UTC'")
        con.execute("SET memory_limit='3GB'")
        con.execute("SET threads=2")
        by_month = {}
        for a in report["artifacts"]:
            p = checked_path(root, a["path"])
            if digest(p) != a["sha256"] or p.stat().st_size != a["bytes"]:
                raise ValueError("Historical panel checksum/size mismatch")
            if "rows" in a:
                if con.execute("SELECT count(*) FROM read_parquet(?)", [str(p)]).fetchone()[0] != a["rows"]:
                    raise ValueError("Historical panel row-count mismatch")
            if a["table"] in ("features", "labels"):
                by_month.setdefault(p.stem, {})[a["table"]] = p
        rows = 0
        for month, tables in by_month.items():
            if set(tables) != {"features", "labels"}:
                raise ValueError("Unpaired feature/label partition")
            for name, p in tables.items():
                parquet_view(con, "panel_"+name, [p])
            columns = {r[0] for r in con.execute("DESCRIBE panel_features").fetchall()}
            if columns & {"departures", "returns", "label_available_at_utc", "lost_minutes", "exposure_usable", "observed_month_geography_distance_m"}:
                raise ValueError("Outcomes or retrospective exposure leaked into features")
            rows += assert_frames(con)["rows"]
        if not by_month or rows != report["rows"]:
            raise ValueError("Historical panel total differs")
    return report
