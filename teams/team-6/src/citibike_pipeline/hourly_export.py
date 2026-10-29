"""Ledger-backed hourly observations with explicit operation and DST eligibility gates."""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path
import time

import duckdb
from shapely.geometry import Point, shape

from .db import sql_literal
from .forecast_sources import digest, stamp
from .model_export import month_starts, select_inputs


START_DST = ['start_dst_ambiguous', 'start_dst_nonexistent']


def aggregate_hourly(con, start: date, end: date):
    """Input: trips + source_releases. Output keeps release cohorts for as-of profiles."""
    con.execute("SET TimeZone='UTC'")
    con.execute('''CREATE OR REPLACE TEMP TABLE hourly_release AS
        SELECT start_station_id AS station_id,
            CASE WHEN schema_family='modern' THEN 'short_name' ELSE 'legacy' END AS id_scheme,
            date_trunc('hour', timezone('America/New_York',started_at_local)) AS hour_utc,
            nominal_release, count(*)::BIGINT AS departures
        FROM trips JOIN source_releases USING(source_key)
        WHERE eligible_departure AND started_at_local >= ?::TIMESTAMP
            AND started_at_local < ?::TIMESTAMP
            AND NOT list_has_any(coalesce(quality_flags,[]::VARCHAR[]),?::VARCHAR[])
        GROUP BY ALL''', [start, end, START_DST])
    con.execute('''CREATE OR REPLACE TEMP TABLE hourly_observed AS
        SELECT station_id,id_scheme,hour_utc,sum(departures)::BIGINT AS departures,
            false AS operation_verified,false AS main_evaluation_eligible
        FROM hourly_release GROUP BY ALL''')


def eligible_hours(con):
    """Build only fully supported hours from an explicit operation-coverage contract.

    Input operation_intervals must have station_id, id_scheme, start_utc, end_utc,
    operation_evidence, and source_coverage_complete. Intervals must not overlap.
    """
    invalid = con.execute('''SELECT count(*) FROM operation_intervals
        WHERE station_id IS NULL OR id_scheme IS NULL OR start_utc IS NULL OR end_utc IS NULL
            OR start_utc>=end_utc OR coalesce(operation_evidence,'')='' ''').fetchone()[0]
    overlaps = con.execute('''SELECT count(*) FROM (
        SELECT start_utc,max(end_utc) OVER (PARTITION BY station_id,id_scheme ORDER BY start_utc
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_end
        FROM operation_intervals) WHERE start_utc<previous_end''').fetchone()[0]
    if invalid or overlaps:
        raise ValueError('Invalid or overlapping operation intervals')
    first,last=con.execute("SELECT date_trunc('hour',min(start_utc)),max(end_utc) FROM operation_intervals").fetchone()
    con.execute('''CREATE OR REPLACE TEMP TABLE operation_hour_grid AS
        SELECT * FROM generate_series(?::TIMESTAMPTZ,?::TIMESTAMPTZ,INTERVAL '1 hour') g(hour_utc)''',
        [first,last])
    con.execute('''CREATE OR REPLACE TEMP TABLE covered_hours AS
        SELECT station_id,id_scheme,hour_utc,true AS operation_verified,
            operation_evidence FROM operation_intervals JOIN operation_hour_grid
        ON source_coverage_complete AND hour_utc>=start_utc
            AND hour_utc+INTERVAL '1 hour'<=end_utc''')
    con.execute('''CREATE OR REPLACE TEMP TABLE supported_hourly AS
        SELECT c.station_id,c.id_scheme,c.hour_utc,coalesce(o.departures,0)::BIGINT AS departures,
            true AS operation_verified,true AS main_evaluation_eligible,c.operation_evidence
        FROM covered_hours c LEFT JOIN hourly_observed o USING(station_id,id_scheme,hour_utc)''')


def _copy(con, query, path: Path) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute('COPY (' + query + ') TO ' + sql_literal(str(path)) + ' (FORMAT PARQUET,COMPRESSION ZSTD)')
    rows = con.execute('SELECT count(*) FROM read_parquet(?)', [str(path)]).fetchone()[0]
    return {'path': str(path), 'rows': rows, 'bytes': path.stat().st_size, 'sha256': digest(path)}


def run_hourly_export(cfg, start: date, end: date, source_package: Path | None = None, out=print) -> Path:
    if start >= end or start.day != 1 or end.day != 1:
        raise ValueError('Hourly export requires increasing month boundaries')
    began = time.monotonic()
    records = select_inputs(cfg, start, end)
    root = cfg.processed_root / 'modeling_exports' / 'three_hour' / 'builds' / stamp()
    root.mkdir(parents=True)
    manifest = {'schema_version': 'observed-station-hour-v1', 'start_local': str(start),
        'end_exclusive_local': str(end), 'input_files': records, 'artifacts': [], 'months': [],
        'main_evaluation_ready': False,
        'remaining_gates': ['historical station operation and zero-hour exposure',
            'source release availability evidence', 'effective-dated station geography',
            'historical alert timezone and continuous coverage'],
        'source_package': str(source_package) if source_package else None}
    boroughs = []
    if source_package:
        source_manifest = json.loads((source_package / 'manifest.json').read_text())
        if source_manifest.get('status') != 'collected_with_documented_coverage_gaps':
            raise ValueError('Source package is incomplete')
        for item in source_manifest['artifacts']:
            if item.get('name') == 'boroughs':
                path = source_package / item['path']
                if digest(path) != item['sha256']:
                    raise ValueError('Borough source checksum mismatch')
                for f in json.loads(path.read_text())['features']:
                    boroughs.append((f['properties']['boroname'], shape(f['geometry'])))
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit='3GB'")
        con.execute('SET threads=4')
        cfg.tmp_dir.mkdir(parents=True, exist_ok=True)
        con.execute('SET temp_directory=' + sql_literal(str(cfg.tmp_dir)))
        con.execute('CREATE TABLE source_releases(source_key VARCHAR,nominal_release VARCHAR)')
        con.executemany('INSERT INTO source_releases VALUES (?,?)', sorted({
            (r['source_key'], r['nominal_release']) for r in records}))
        months = month_starts(start, end)
        for index, first in enumerate(months):
            following = months[index+1] if index+1 < len(months) else str(end)
            selected = [r for r in records if r['partition'] == first]
            paths = ','.join(sql_literal(str(cfg.processed_root / r['path'])) for r in selected)
            con.execute('CREATE OR REPLACE VIEW trips AS SELECT * FROM read_parquet([' + paths + '],hive_partitioning=false)')
            audit = con.execute('''SELECT count(*) FILTER(WHERE eligible_departure),
                count(*) FILTER(WHERE eligible_departure AND list_has_any(
                    coalesce(quality_flags,[]::VARCHAR[]),?::VARCHAR[]))
                FROM trips WHERE started_at_local>=?::TIMESTAMP AND started_at_local<?::TIMESTAMP''',
                [START_DST, first, following]).fetchone()
            aggregate_hourly(con, date.fromisoformat(first), date.fromisoformat(following))
            counted = con.execute('SELECT coalesce(sum(departures),0) FROM hourly_observed').fetchone()[0]
            if counted + audit[1] != audit[0]:
                raise ValueError('Hourly/DST-exclusion count reconciliation failed')
            duplicate = con.execute('''SELECT count(*) FROM (SELECT station_id,id_scheme,hour_utc,
                count(*) AS n FROM hourly_observed GROUP BY ALL HAVING n>1)''').fetchone()[0]
            if duplicate:
                raise ValueError('Duplicate hourly key')
            for table in ('hourly_observed', 'hourly_release'):
                artifact = _copy(con, 'SELECT * FROM ' + table + ' ORDER BY station_id,id_scheme,hour_utc',
                                 root / 'tables' / table / (first[:7] + '.parquet'))
                artifact['path'] = str(Path(artifact['path']).relative_to(root))
                artifact['table'] = table
                manifest['artifacts'].append(artifact)
            geo = con.execute('''SELECT start_station_id,
                CASE WHEN schema_family='modern' THEN 'short_name' ELSE 'legacy' END AS id_scheme,
                avg(start_lat),avg(start_lng),count(*),min(start_station_name)
                FROM trips WHERE eligible_departure AND start_lat BETWEEN -90 AND 90
                    AND start_lng BETWEEN -180 AND 180 GROUP BY ALL''').fetchall()
            con.execute('''CREATE OR REPLACE TEMP TABLE geography(station_id VARCHAR,id_scheme VARCHAR,
                month_local DATE,latitude DOUBLE,longitude DOUBLE,coordinate_observations BIGINT,
                station_name VARCHAR,borough VARCHAR,geography_verified BOOLEAN,evidence VARCHAR)''')
            rows = []
            for station, scheme, lat, lon, n, name in geo:
                point = Point(lon, lat)
                matches = [borough for borough, geom in boroughs if geom.covers(point)]
                rows.append((station, scheme, date.fromisoformat(first), lat, lon, n, name,
                    matches[0] if len(matches) == 1 else None, False,
                    'retrospective monthly mean of trip endpoint coordinates'))
            if rows:
                con.executemany('INSERT INTO geography VALUES (?,?,?,?,?,?,?,?,?,?)', rows)
            artifact = _copy(con, 'SELECT * FROM geography ORDER BY station_id,id_scheme',
                root / 'tables' / 'station_geography_observed' / (first[:7] + '.parquet'))
            artifact.update(path=str(Path(artifact['path']).relative_to(root)), table='station_geography_observed')
            manifest['artifacts'].append(artifact)
            manifest['months'].append({'month': first[:7], 'eligible_departures': audit[0],
                'dst_unassignable_departures': audit[1], 'assigned_departures': counted})
            out(f'{first[:7]}: {counted:,} departures assigned, {audit[1]:,} DST-uncertain excluded')
        manifest.update(status='validated_observations_operation_gate_pending',
                        seconds=round(time.monotonic()-began, 2))
        (root / 'manifest.json').write_text(json.dumps(manifest, indent=2, default=str) + '\n')
        validate_hourly_package(root)
        pointer = root.parent.parent / 'CURRENT_OBSERVED'
        temporary = pointer.with_suffix('.tmp')
        temporary.write_text(root.name + '\n')
        temporary.replace(pointer)
        out(f'Hourly observation package: {root}; main evaluation gated')
    finally:
        con.close()
    return root


def validate_hourly_package(root: Path) -> dict:
    root = Path(root).resolve()
    manifest = json.loads((root / 'manifest.json').read_text())
    if manifest.get('status') != 'validated_observations_operation_gate_pending':
        raise ValueError('Incomplete hourly package')
    for item in manifest['artifacts']:
        path = (root / item['path']).resolve()
        if not path.is_relative_to(root) or digest(path) != item['sha256']:
            raise ValueError('Hourly artifact path/checksum mismatch')
    for month in manifest['months']:
        if month['assigned_departures'] + month['dst_unassignable_departures'] != month['eligible_departures']:
            raise ValueError('Hourly count reconciliation mismatch')
    return manifest
