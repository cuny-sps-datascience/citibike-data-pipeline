"""Normalizes one selected source into staged, partitioned Parquet plus a quarantine file.

Python streams records (exact source_row, one timestamp parser); DuckDB types and flags them.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from .db import sql_literal
from .discover import candidate_from_record, open_candidate
from .dst import crosses_dst
from .records import open_records, parse_local_ts
from .schemas import END_COLUMN, RAW_NAMES, START_COLUMN


class NormalizeError(RuntimeError):
    pass


TRIPS_DDL = """
CREATE OR REPLACE TABLE staged_trips (
  started_at_local TIMESTAMP, ended_at_local TIMESTAMP,
  duration_seconds DOUBLE, source_duration_seconds DOUBLE,
  start_station_id VARCHAR, start_station_name VARCHAR, start_lat DOUBLE, start_lng DOUBLE,
  end_station_id VARCHAR, end_station_name VARCHAR, end_lat DOUBLE, end_lng DOUBLE,
  ride_id VARCHAR, rideable_type VARCHAR, membership_group VARCHAR, original_membership VARCHAR,
  source_key VARCHAR, source_row BIGINT, schema_family VARCHAR, schema_version INTEGER, run_id VARCHAR,
  quality_flags VARCHAR[], eligible_departure BOOLEAN, eligible_arrival BOOLEAN,
  trip_year VARCHAR, trip_month VARCHAR
)
"""

# Rows not published because an earlier release already published the same trip (see _remove_spillover).
# The four compared fields are kept so validate can check the kept copy is still published.
REMOVED_SCHEMA = pa.schema([
    ("source_key", pa.string()), ("source_row", pa.int64()), ("ride_id", pa.string()),
    ("reason", pa.string()), ("kept_source_key", pa.string()),
    ("started_at_local", pa.timestamp("us")), ("ended_at_local", pa.timestamp("us")),
    ("start_station_id", pa.string()), ("end_station_id", pa.string()),
])

QUARANTINE_SCHEMA = pa.schema([
    ("source_key", pa.string()), ("source_row", pa.int64()),
    ("reason", pa.string()), ("raw_record", pa.string()),
])


def slug_for(source_key: str) -> str:
    base = source_key.split("::")[-1].rsplit("/", 1)[-1].removesuffix(".csv")
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base)
    return f"{base}-{hashlib.sha1(source_key.encode()).hexdigest()[:8]}"


def _nz(col: str) -> str:
    return f"CASE WHEN trim({col}) IN ('', '\\N', 'NULL') THEN NULL ELSE trim({col}) END"


def _legacy_station_id(col: str) -> str:
    """Legacy IDs are integers; some re-exported 2019 parts write them as '72.0'."""
    return f"regexp_replace({_nz(col)}, '^([0-9]+)\\.0$', '\\1')"


def _modern_station_id(col: str) -> str:
    """Exports sometimes drop a trailing zero from modern short-name IDs ('6948.10' as '6948.1')."""
    return (f"CASE WHEN regexp_matches({_nz(col)}, '^[0-9]+\\.[0-9]$') THEN {_nz(col)} || '0' "
            f"ELSE {_nz(col)} END")


def _coords_ok(lat: str, lng: str) -> str:
    return f"coalesce({lat} BETWEEN 40.4 AND 41.1 AND {lng} BETWEEN -74.4 AND -73.6, false)"


def transform_sql(family: str) -> str:
    if family in ("legacy_lower", "legacy_title"):
        src_duration = f"TRY_CAST({_nz('src_duration')} AS DOUBLE)"
        ride_id, rideable = "NULL::VARCHAR", "NULL::VARCHAR"
        group = f"CASE {_nz('membership')} WHEN 'Subscriber' THEN 'member' WHEN 'Customer' THEN 'casual' END"
        start_id, end_id = _legacy_station_id("start_station_id"), _legacy_station_id("end_station_id")
    elif family == "modern":
        src_duration = "NULL::DOUBLE"
        ride_id, rideable = _nz("ride_id"), _nz("rideable_type")
        group = f"CASE {_nz('membership')} WHEN 'member' THEN 'member' WHEN 'casual' THEN 'casual' END"
        start_id, end_id = _modern_station_id("start_station_id"), _modern_station_id("end_station_id")
    else:
        raise NormalizeError(f"no adapter for schema family {family!r}")
    return f"""
INSERT INTO staged_trips
SELECT
  started_at_local, ended_at_local, duration_seconds, source_duration_seconds,
  start_station_id, start_station_name, start_lat, start_lng,
  end_station_id, end_station_name, end_lat, end_lng,
  ride_id, rideable_type, membership_group, original_membership,
  ?::VARCHAR, source_row, ?::VARCHAR, ?::INTEGER, ?::VARCHAR,
  list_filter([
    CASE WHEN start_station_id IS NULL THEN 'start_station_missing' END,
    CASE WHEN end_station_id IS NULL THEN 'end_station_missing' END,
    CASE WHEN ended_at_local IS NULL THEN 'end_ts_invalid' END,
    CASE WHEN duration_seconds <= 0 THEN 'duration_nonpositive' END,
    CASE WHEN abs(duration_seconds - source_duration_seconds) > 2 THEN 'duration_mismatch' END,
    CASE WHEN NOT {_coords_ok('start_lat', 'start_lng')} THEN 'start_coords_invalid' END,
    CASE WHEN NOT {_coords_ok('end_lat', 'end_lng')} THEN 'end_coords_invalid' END,
    CASE WHEN original_membership IS NULL THEN 'membership_missing'
         WHEN membership_group IS NULL THEN 'membership_unrecognized' END,
    CASE WHEN start_dst IS NOT NULL THEN 'start_dst_' || start_dst END,
    CASE WHEN end_dst IS NOT NULL THEN 'end_dst_' || end_dst END,
    CASE WHEN crosses_dst THEN 'crosses_dst' END
  ], lambda f: f IS NOT NULL),
  start_station_id IS NOT NULL,
  ended_at_local IS NOT NULL AND end_station_id IS NOT NULL AND coalesce(duration_seconds >= 0, false),
  strftime(started_at_local, '%Y'),
  strftime(started_at_local, '%m')
FROM (
  SELECT
    start_local AS started_at_local,
    end_local AS ended_at_local,
    date_diff('microsecond', start_local, end_local) / 1e6 AS duration_seconds,
    {src_duration} AS source_duration_seconds,
    {start_id} AS start_station_id,
    {_nz('start_station_name')} AS start_station_name,
    TRY_CAST({_nz('start_lat')} AS DOUBLE) AS start_lat,
    TRY_CAST({_nz('start_lng')} AS DOUBLE) AS start_lng,
    {end_id} AS end_station_id,
    {_nz('end_station_name')} AS end_station_name,
    TRY_CAST({_nz('end_lat')} AS DOUBLE) AS end_lat,
    TRY_CAST({_nz('end_lng')} AS DOUBLE) AS end_lng,
    {ride_id} AS ride_id,
    {rideable} AS rideable_type,
    {group} AS membership_group,
    {_nz('membership')} AS original_membership,
    source_row, start_dst, end_dst, crosses_dst
  FROM raw
) t
"""


@dataclass
class SourceResult:
    source_key: str
    slug: str
    source_rows: int
    staged_rows: int
    quarantined_rows: int
    staged_dir: Path
    removed_rows: int = 0
    spillover_conflicts: int = 0


class _Batch:
    def __init__(self, names: tuple[str, ...]):
        self.names = names
        self.cols: list[list[str]] = [[] for _ in names]
        self.rows: list[int] = []
        self.starts, self.ends, self.start_dst, self.end_dst, self.crosses_dst = [], [], [], [], []

    def add(self, source_row, fields, start, end, start_dst, end_dst, crosses):
        for i, value in enumerate(fields):
            self.cols[i].append(value)
        self.rows.append(source_row)
        self.starts.append(start)
        self.ends.append(end)
        self.start_dst.append(start_dst)
        self.end_dst.append(end_dst)
        self.crosses_dst.append(crosses)

    def table(self) -> pa.Table:
        data = {name: pa.array(self.cols[i], pa.string()) for i, name in enumerate(self.names)}
        data["source_row"] = pa.array(self.rows, pa.int64())
        data["start_local"] = pa.array(self.starts, pa.timestamp("us"))
        data["end_local"] = pa.array(self.ends, pa.timestamp("us"))
        data["start_dst"] = pa.array(self.start_dst, pa.string())
        data["end_dst"] = pa.array(self.end_dst, pa.string())
        data["crosses_dst"] = pa.array(self.crosses_dst, pa.bool_())
        return pa.table(data)


_SPILLOVER_SQL = """
WITH spill AS (
  SELECT source_row, ride_id, started_at_local, ended_at_local, start_station_id, end_station_id,
         trip_year || '-' || trip_month AS start_release
  FROM staged_trips WHERE ride_id IS NOT NULL AND trip_year || '-' || trip_month <> ?
),
pub AS (
  SELECT p.ride_id, p.started_at_local, p.ended_at_local, p.start_station_id, p.end_station_id, p.source_key,
         f.release
  FROM read_parquet({files}, filename = true, hive_partitioning = false) p
  JOIN (SELECT unnest(?::VARCHAR[]) AS filename, unnest(?::VARCHAR[]) AS release) f USING (filename)
  WHERE p.ride_id IN (SELECT ride_id FROM spill)
)
SELECT spill.source_row, spill.ride_id, spill.start_release,
       spill.started_at_local, spill.ended_at_local, spill.start_station_id, spill.end_station_id,
       min(pub.source_key) FILTER (WHERE
         pub.started_at_local IS NOT DISTINCT FROM spill.started_at_local
         AND pub.ended_at_local IS NOT DISTINCT FROM spill.ended_at_local
         AND pub.start_station_id IS NOT DISTINCT FROM spill.start_station_id
         AND pub.end_station_id IS NOT DISTINCT FROM spill.end_station_id) AS kept_source_key
FROM spill JOIN pub ON pub.ride_id = spill.ride_id AND pub.release = spill.start_release
GROUP BY ALL ORDER BY spill.source_row
"""


def _remove_spillover(con, rec: dict, out: Path, published_trips: Callable[[str], list[str]]) -> tuple[int, int]:
    """Drops staged rows that an earlier release already published identically; returns (removed, conflicts).

    A spillover row is a modern row whose start month is not the source's release. If the release of
    that start month published the same ride_id with the same start/end times and station IDs, the row
    is removed and listed in removed.parquet. A published copy that differs is a conflict: the row is kept.
    """
    release = rec["nominal_release"]
    months = [r[0] for r in con.execute(
        "SELECT DISTINCT trip_year || '-' || trip_month FROM staged_trips "
        "WHERE ride_id IS NOT NULL AND trip_year || '-' || trip_month <> ? ORDER BY 1", [release]).fetchall()]
    files, releases = [], []
    for month in months:
        paths = published_trips(month)
        files += paths
        releases += [month] * len(paths)
    if not files:
        return 0, 0
    file_list = "[" + ", ".join(sql_literal(f) for f in files) + "]"
    matches = con.execute(_SPILLOVER_SQL.format(files=file_list), [release, files, releases]).fetchall()
    removed = [m for m in matches if m[-1] is not None]
    if removed:
        pq.write_table(pa.Table.from_pylist([
            {"source_key": rec["source_key"], "source_row": row, "ride_id": ride_id,
             "reason": f"duplicate_of_release:{start_release}", "kept_source_key": kept,
             "started_at_local": start, "ended_at_local": end, "start_station_id": start_id,
             "end_station_id": end_id}
            for row, ride_id, start_release, start, end, start_id, end_id, kept in removed
        ], REMOVED_SCHEMA), out / "removed.parquet")
        con.execute("DELETE FROM staged_trips WHERE list_contains(?, source_row)", [[m[0] for m in removed]])
    return len(removed), len(matches) - len(removed)


def normalize_source(cfg, con, rec: dict, run_id: str, staging_root: Path, dst: dict,
                     published_trips: Callable[[str], list[str]] | None = None) -> SourceResult:
    """Stages one source. `published_trips(release)` lists the published trips files of a release (absolute
    paths); when given, modern spillover rows already published by their start month's release are removed."""
    family = rec["schema_family"]
    sql = transform_sql(family)
    names = RAW_NAMES[family]
    width = len(names)
    start_idx, end_idx = START_COLUMN[family], END_COLUMN[family]
    slug = slug_for(rec["source_key"])
    out = staging_root / slug
    if out.exists():
        raise NormalizeError(f"staging dir {out} already exists; retry with a new run_id")
    out.mkdir(parents=True)
    params = [rec["source_key"], family, cfg.schema_version, run_id]
    tz = ZoneInfo(cfg.timezone)
    # A same-day trip can only cross a DST change on a transition date, so the offset comparison is
    # skipped for same-day trips on other dates inside the years `dst` covers. Multi-day trips and
    # dates outside those years are always compared, so the flags match comparing every row.
    dst_dates = {day for day, _ in dst}
    dst_years = {day.year for day in dst_dates}

    def crosses(start, end) -> bool:
        day = start.date()
        if day == end.date() and day.year in dst_years and day not in dst_dates:
            return False
        return crosses_dst(start, end, tz)

    con.execute(TRIPS_DDL)

    seen = quarantined = 0
    q_buffer: list[dict] = []
    q_writer = None

    def flush(batch: _Batch):
        con.register("raw", batch.table())
        try:
            con.execute(sql, params)
        finally:
            con.unregister("raw")

    def flush_quarantine():
        nonlocal q_writer
        if not q_buffer:
            return
        if q_writer is None:
            q_writer = pq.ParquetWriter(out / "quarantine.parquet", QUARANTINE_SCHEMA)
        q_writer.write_table(pa.Table.from_pylist(q_buffer, QUARANTINE_SCHEMA))
        q_buffer.clear()

    def reject(source_row, reason, fields):
        nonlocal quarantined
        quarantined += 1
        q_buffer.append({"source_key": rec["source_key"], "source_row": source_row,
                         "reason": reason, "raw_record": json.dumps(fields)})

    try:
        with open_candidate(cfg, candidate_from_record(rec)) as fh:
            header, records = open_records(fh)
            if list(header) != json.loads(rec["header_json"]):
                raise NormalizeError("header differs from discovery; re-run inventory discover")
            batch = _Batch(names)
            for source_row, fields in records:
                seen += 1
                if len(fields) != width:
                    reject(source_row, "field_count", fields)
                else:
                    start = parse_local_ts(fields[start_idx])
                    if start is None:
                        reject(source_row, "start_ts_invalid", fields)
                    else:
                        end = parse_local_ts(fields[end_idx])
                        batch.add(source_row, fields, start, end, dst.get((start.date(), start.hour)),
                                  dst.get((end.date(), end.hour)) if end else None,
                                  end is not None and crosses(start, end))
                if len(batch.rows) >= cfg.batch_rows:
                    flush(batch)
                    batch = _Batch(names)
                if len(q_buffer) >= cfg.batch_rows:
                    flush_quarantine()
            flush(batch)
            flush_quarantine()
    finally:
        if q_writer is not None:
            q_writer.close()

    if seen != rec["rows"]:
        raise NormalizeError(f"read {seen} rows but profiling saw {rec['rows']}; re-run inventory profile")
    if seen and quarantined / seen > cfg.max_quarantine_fraction:
        raise NormalizeError(f"quarantine share {quarantined}/{seen} exceeds {cfg.max_quarantine_fraction}")
    removed = conflicts = 0
    if family == "modern" and published_trips is not None:
        removed, conflicts = _remove_spillover(con, rec, out, published_trips)
    staged = con.execute("SELECT count(*) FROM staged_trips").fetchone()[0]
    if staged + quarantined + removed != seen:
        raise NormalizeError(f"staged {staged} + quarantined {quarantined} + removed {removed} != {seen} source rows")
    if staged:
        trips_dir = out / "trips"
        con.execute(f"COPY staged_trips TO {sql_literal(str(trips_dir))} "
                    "(FORMAT parquet, PARTITION_BY (trip_year, trip_month), COMPRESSION zstd)")
        files = [str(f) for f in sorted(trips_dir.rglob("*.parquet"))]
        written = con.execute("SELECT count(*) FROM read_parquet(?)", [files]).fetchone()[0]
        if written != staged:
            raise NormalizeError(f"readback found {written} rows, expected {staged}")
    return SourceResult(rec["source_key"], slug, seen, staged, quarantined, out, removed, conflicts)
