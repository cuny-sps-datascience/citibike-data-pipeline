"""Reconciles published rows to source rows and summarizes quality for one or more releases."""

from __future__ import annotations

import math
import os
import re
from collections import defaultdict
from pathlib import Path

import duckdb

from .db import connect, sql_literal
from .manifest import Manifest
from .report import table_row
from .sets import selection_problems

# Validation only scans and aggregates published Parquet; it needs far less than normalize. A smaller
# memory_limit makes DuckDB spill sooner, and fewer threads means fewer per-thread scan buffers, which
# keeps a long multi-release run well inside 16 GiB alongside everything else on the machine.
VALIDATE_MEMORY_CAP = 3 * 1000**3  # "3GB" in DuckDB's decimal units
VALIDATE_MAX_THREADS = 4
# The ride_id duplicate check groups by ride_id, and over the whole archive (~232M modern rows) that
# hash aggregate ran out of memory at the 3GB cap even with spilling. It is run in hash buckets of
# about this many non-null ride_ids each; 20M fit comfortably in that cap on synthetic data.
RIDE_IDS_PER_BUCKET = 20_000_000
_UNITS = {"": 1, "b": 1, "kb": 1000, "mb": 1000**2, "gb": 1000**3, "tb": 1000**4,
          "kib": 2**10, "mib": 2**20, "gib": 2**30, "tib": 2**40}


def _limit_bytes(text: str) -> int | None:
    """Bytes for a DuckDB memory_limit ("6GB", "512MiB", "50%" of physical RAM); None if unparseable."""
    m = re.fullmatch(r"\s*([0-9.]+)\s*([A-Za-z%]*)\s*", text)
    if not m:
        return None
    unit = m[2].lower()
    if unit == "%":
        ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        return int(ram * float(m[1]) / 100)
    if unit not in _UNITS:
        return None
    return int(float(m[1]) * _UNITS[unit])


def validate_connection(cfg) -> duckdb.DuckDBPyConnection:
    """One in-memory connection for a whole validate run: min(memory_limit, 3GB), at most 4 threads.

    A memory_limit this cannot parse is left as connect() set it rather than guessed at.
    """
    con = connect(cfg)
    limit = _limit_bytes(cfg.memory_limit)
    if limit is not None:
        con.execute(f"SET memory_limit = '{min(limit, VALIDATE_MEMORY_CAP)}B'")
    con.execute(f"SET threads = {min(int(cfg.threads), VALIDATE_MAX_THREADS)}")
    return con


def _unlisted_files(cfg, manifest) -> int:
    """Parquet files under trips/, quarantine/ or removed/ that the published-file ledger does not list."""
    listed = {cfg.processed_root / p for p in manifest.published_paths()}
    roots = [d for d in (cfg.trips_dir, cfg.quarantine_dir, cfg.removed_dir) if d.exists()]
    return sum(1 for d in roots for f in d.rglob("*.parquet") if f not in listed)


def validate_releases(cfg, manifest, releases: list[str], con=None) -> dict:
    records = manifest.all()
    status_by_key = {r["source_key"]: r["status"] for r in records}
    sources = [r for r in records if r["nominal_release"] in releases and r["status"] == "selected"]
    ledger = manifest.published_for_sources([r["source_key"] for r in sources])
    trips = [cfg.processed_root / e["path"] for e in ledger if e["dataset"] == "trips"]
    res = {
        "releases": list(releases),
        "sources": len(sources),
        "source_rows": sum(r["rows"] or 0 for r in sources),
        "published_rows": sum(e["rows"] for e in ledger if e["dataset"] == "trips"),
        "quarantined_rows": sum(e["rows"] for e in ledger if e["dataset"] == "quarantine"),
        "removed_rows": sum(e["rows"] for e in ledger if e["dataset"] == "removed"),
        "months": [], "flags": [], "membership": [], "rideable": [], "duration": None,
        "selection_problems": [p for release in releases for p in selection_problems(records, release)],
        "unlisted_files": _unlisted_files(cfg, manifest),
        "stale_ledger_sources": [k for k in manifest.published_keys_for_releases(releases)
                                 if status_by_key.get(k) != "selected"],
        "start_stations": 0, "ride_id_identical": 0, "ride_id_conflicting": 0, "date_range": None,
    }
    rows_by = {"trips": defaultdict(int), "quarantine": defaultdict(int), "removed": defaultdict(int)}
    for e in ledger:
        rows_by[e["dataset"]][e["source_key"]] += e["rows"]
    # Per source: profiled, published, quarantined and removed rows, and the quarantine the profile
    # predicts (malformed + invalid start). Normalize quarantines exactly those two kinds of record.
    res["per_source"] = [
        (r["source_key"], r["rows"] or 0, rows_by["trips"][r["source_key"]], rows_by["quarantine"][r["source_key"]],
         rows_by["removed"][r["source_key"]], (r["malformed_rows"] or 0) + (r["invalid_start_rows"] or 0))
        for r in sorted(sources, key=lambda r: r["source_key"])
    ]
    res["quarantine_mismatches"] = [key for key, _, _, q, _, expected in res["per_source"] if q != expected]
    res["row_mismatches"] = [key for key, rows, p, q, removed, _ in res["per_source"] if rows != p + q + removed]
    # Rows neither published nor quarantined; each must be a removed duplicate listed in the ledger.
    res["record_removals"] = res["source_rows"] - res["published_rows"] - res["quarantined_rows"]
    res["selection_complete"] = not res["selection_problems"]
    res["orphaned_removals"], res["orphaned_ride_ids"] = 0, []
    removed_files = [str(cfg.processed_root / e["path"]) for e in ledger if e["dataset"] == "removed"]
    own = con is None
    if own:
        con = validate_connection(cfg)
    try:
        _profile_trips(con, res, [str(f) for f in trips])
        _check_removals(con, cfg, manifest, res, removed_files)
    except BaseException:
        try:
            con.execute("DROP VIEW IF EXISTS trips")
        except Exception:
            pass  # keep the original error; the view goes with the connection anyway
        if own:
            con.close()
        raise
    con.execute("DROP VIEW IF EXISTS trips")
    if own:
        con.close()
    res["reconciled"] = res["selection_complete"] and res["published_rows"] > 0 and \
        res["unlisted_files"] == 0 and not res["stale_ledger_sources"] and \
        res["record_removals"] == res["removed_rows"] and not res["row_mismatches"] and \
        not res["quarantine_mismatches"] and res["orphaned_removals"] == 0
    return res


ORPHAN_SAMPLE = 10
_REASON_RELEASE = "regexp_extract(reason, '^duplicate_of_release:(.*)$', 1)"


def _check_removals(con, cfg, manifest, res: dict, removed_files: list[str]) -> None:
    """Counts removed rows whose kept copy is not among the published trips of the release in their reason.

    The kept copy must still be published by a selected source of that release with the same ride_id,
    start/end times and station IDs; otherwise removing the row lost the trip.
    """
    if not removed_files:
        return
    removed = "read_parquet([" + ", ".join(sql_literal(f) for f in removed_files) + "])"
    releases = [r[0] for r in con.execute(f"SELECT DISTINCT {_REASON_RELEASE} FROM {removed} ORDER BY 1").fetchall()]
    files, tags = [], []
    for release in releases:
        paths = [str(cfg.processed_root / p) for p in manifest.published_trip_paths(release)]
        files += paths
        tags += [release] * len(paths)
    if files:
        pub = f"""
          SELECT p.ride_id, p.started_at_local, p.ended_at_local, p.start_station_id, p.end_station_id, f.release
          FROM read_parquet([{", ".join(sql_literal(f) for f in files)}], filename = true, hive_partitioning = false) p
          JOIN (SELECT unnest(?::VARCHAR[]) AS filename, unnest(?::VARCHAR[]) AS release) f USING (filename)
          WHERE p.ride_id IN (SELECT ride_id FROM r)"""
        params = [files, tags]
    else:
        pub = ("SELECT NULL::VARCHAR AS ride_id, NULL::TIMESTAMP AS started_at_local, NULL::TIMESTAMP AS "
               "ended_at_local, NULL::VARCHAR AS start_station_id, NULL::VARCHAR AS end_station_id, "
               "NULL::VARCHAR AS release WHERE false")
        params = []
    orphans = con.execute(f"""
        WITH r AS (
          SELECT ride_id, started_at_local, ended_at_local, start_station_id, end_station_id,
                 {_REASON_RELEASE} AS release
          FROM {removed}
        ),
        pub AS ({pub})
        SELECT r.ride_id FROM r WHERE NOT EXISTS (
          SELECT 1 FROM pub WHERE pub.ride_id = r.ride_id AND pub.release = r.release
            AND pub.started_at_local IS NOT DISTINCT FROM r.started_at_local
            AND pub.ended_at_local IS NOT DISTINCT FROM r.ended_at_local
            AND pub.start_station_id IS NOT DISTINCT FROM r.start_station_id
            AND pub.end_station_id IS NOT DISTINCT FROM r.end_station_id)
        ORDER BY r.ride_id
    """, params).fetchall()
    res["orphaned_removals"] = len(orphans)
    res["orphaned_ride_ids"] = [r[0] for r in orphans[:ORPHAN_SAMPLE]]


def _profile_trips(con, res: dict, files: list[str]) -> None:
    def q(sql: str) -> list[tuple]:
        return con.execute(sql).fetchall()

    # One metadata query for every file's column layout; no relation object per file.
    res["schema_consistent"] = not files or con.execute("""
        SELECT count(DISTINCT layout) <= 1 FROM (
          SELECT string_agg(name || ':' || coalesce(duckdb_type, repetition_type), ',' ORDER BY column_id) AS layout
          FROM parquet_schema(?) GROUP BY file_name
        )
    """, [files]).fetchone()[0]
    if not files or not res["schema_consistent"]:
        return  # files with different columns cannot be read as one table
    # A plain SQL view stays lazy. A relation built with con.sql(..., params=...) is executed
    # immediately and holds every selected row in memory, which is what made validate grow with
    # the number of rows in a unit.
    file_list = "[" + ", ".join(sql_literal(f) for f in files) + "]"
    con.execute(f"CREATE OR REPLACE TEMP VIEW trips AS SELECT * FROM read_parquet({file_list}, "
                "hive_partitioning = true, hive_types = {'trip_year': INTEGER, 'trip_month': INTEGER})")
    res["months"] = q("SELECT trip_year, trip_month, count(*) FROM trips GROUP BY ALL ORDER BY ALL")
    res["flags"] = q("SELECT f, count(*) FROM (SELECT unnest(quality_flags) AS f FROM trips) GROUP BY f ORDER BY f")
    res["membership"] = q("SELECT membership_group, original_membership, count(*) FROM trips GROUP BY ALL ORDER BY ALL")
    res["rideable"] = q("SELECT rideable_type, count(*) FROM trips GROUP BY ALL ORDER BY ALL")
    res["duration"] = q("SELECT quantile_cont(duration_seconds, 0.5), quantile_cont(duration_seconds, 0.99), "
                        "max(duration_seconds), count(*) FILTER (WHERE duration_seconds <= 0) FROM trips")[0]
    # Aggregates below are plain GROUP BYs, which DuckDB can spill to temp_directory; count(DISTINCT ...)
    # keeps per-group distinct state in memory and ran out of memory over the whole archive.
    res["start_stations"] = q("SELECT count(*) FROM (SELECT start_station_id FROM trips "
                              "WHERE start_station_id IS NOT NULL GROUP BY start_station_id)")[0][0]
    res["date_range"] = q("SELECT min(started_at_local), max(started_at_local) FROM trips")[0]
    res["ride_id_identical"], res["ride_id_conflicting"] = _ride_id_duplicates(con)


def _ride_id_bucket(con, bucket: int, buckets: int) -> tuple[int, int]:
    """(identical, conflicting) duplicate ride_ids among those with hash(ride_id) % buckets = bucket."""
    return con.execute("""
        WITH v AS (  -- one row per distinct (ride_id, variant), with how many trips carry it
          SELECT ride_id, started_at_local, ended_at_local, start_station_id, end_station_id, count(*) AS n
          FROM trips WHERE ride_id IS NOT NULL AND hash(ride_id) % ? = ? GROUP BY ALL
        ),
        d AS (
          SELECT ride_id, count(*) AS variants FROM v GROUP BY ride_id HAVING sum(n) > 1
        )
        SELECT count(*) FILTER (WHERE variants = 1), count(*) FILTER (WHERE variants > 1) FROM d
    """, [buckets, bucket]).fetchone()


def _ride_id_duplicates(con) -> tuple[int, int]:
    """Duplicate ride_ids, counted per hash bucket. A ride_id always lands in one bucket, so the sums are exact."""
    rides = con.execute("SELECT count(ride_id) FROM trips").fetchone()[0]
    buckets = max(1, math.ceil(rides / RIDE_IDS_PER_BUCKET))
    identical = conflicting = 0
    for bucket in range(buckets):
        i, c = _ride_id_bucket(con, bucket, buckets)
        identical, conflicting = identical + i, conflicting + c
    return identical, conflicting


def write_validation_report(cfg, res: dict) -> Path:
    cfg.reports_dir.mkdir(parents=True, exist_ok=True)
    rels = sorted(res["releases"])
    name = rels[0] if len(rels) == 1 else f"{rels[0]}_{rels[-1]}"
    path = cfg.reports_dir / f"validate_{name}.md"
    lines = [
        f"# Validation: {', '.join(rels)}", "",
        f"- reconciled: {res['reconciled']}",
        f"- schema_consistent: {res['schema_consistent']}",
        f"- selection_complete: {res['selection_complete']}",
        *[f"  - {p}" for p in res["selection_problems"]],
        f"- unlisted files under trips/, quarantine/ and removed/: {res['unlisted_files']}",
        f"- stale ledger sources (published but not selected): {len(res['stale_ledger_sources'])}",
        *[f"  - {k}" for k in res["stale_ledger_sources"]],
        f"- selected sources: {res['sources']}",
        f"- source rows: {res['source_rows']}",
        f"- published rows: {res['published_rows']}",
        f"- quarantined rows: {res['quarantined_rows']}",
        f"- removed rows: {res['removed_rows']}",
        f"- record-level removals: {res['record_removals']} (source - published - quarantined; "
        "must equal the removed rows)",
        f"- per-source row mismatches (profiled != published + quarantined + removed): {len(res['row_mismatches'])}",
        *[f"  - {key}: profiled {rows}, published {p} + quarantined {q} + removed {r}"
          for key, rows, p, q, r, _ in res["per_source"] if key in res["row_mismatches"]],
        f"- orphaned removals (kept copy no longer published): {res['orphaned_removals']}",
        *[f"  - {ride_id}" for ride_id in res["orphaned_ride_ids"]],
        f"- quarantine cross-check mismatches: {len(res['quarantine_mismatches'])}",
        *[f"  - {key}: quarantined {q}, profile malformed + invalid start {expected}"
          for key, _, _, q, _, expected in res["per_source"] if key in res["quarantine_mismatches"]],
        f"- start date range: {res['date_range']}",
        f"- distinct start stations: {res['start_stations']}",
        f"- duration p50 / p99 / max / nonpositive: {res['duration']}",
        f"- duplicate ride_id: identical {res['ride_id_identical']}, conflicting {res['ride_id_conflicting']}",
        "", "## Sources", "",
        "| source_key | profiled rows | published rows | quarantined rows | removed rows "
        "| profile malformed + invalid start |",
        "|---|---|---|---|---|---|",
        *[table_row(*row) for row in res["per_source"]],
        "", "## Rows by trip month", "", "| year | month | trips |", "|---|---|---|",
        *[table_row(y, f"{m:02d}", n) for y, m, n in res["months"]],
        "", "## Quality flags", "", "| flag | rows |", "|---|---|",
        *[table_row(f, n) for f, n in res["flags"]],
        "", "## Membership", "", "| group | original | rows |", "|---|---|---|",
        *[table_row(g, o, n) for g, o, n in res["membership"]],
        "", "## Rideable type", "", "| type | rows |", "|---|---|",
        *[table_row(t, n) for t, n in res["rideable"]],
    ]
    path.write_text("\n".join(lines) + "\n")
    return path


def run_validate(cfg, releases: list[str], out) -> int:
    units = [[r] for r in releases] + ([list(releases)] if len(releases) > 1 else [])
    rc = 0
    with Manifest(cfg) as manifest:
        con = validate_connection(cfg)
        try:
            for unit in units:
                rc |= _validate_unit(cfg, manifest, con, unit, out)
        finally:
            con.close()
    return rc


def _validate_unit(cfg, manifest, con, unit: list[str], out) -> int:
    res = validate_releases(cfg, manifest, unit, con=con)
    path = write_validation_report(cfg, res)
    out(f"{', '.join(unit)}: reconciled={res['reconciled']} schema_consistent={res['schema_consistent']} "
        f"published={res['published_rows']} quarantined={res['quarantined_rows']} "
        f"removed={res['removed_rows']} -> {path}")
    return 0 if res["reconciled"] and res["schema_consistent"] else 1
