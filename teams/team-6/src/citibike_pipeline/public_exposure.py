"""Conditional public-archive operating exposure, distinct from verified full coverage.

CityBikes saves inventory changes. Short active-to-active brackets are a declared
interpolation assumption, never proof of continuous operation or collection.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb

from .db import sql_literal
from .forecast_sources import digest, stamp


POLICY = 'citybikes-active-brackets-10min-fresh10min-stable25m-v1'


def normalize_status(con, path):
    """The archive switched last_updated from epoch seconds to ISO text in 2026."""
    con.execute('''CREATE OR REPLACE TEMP TABLE raw_status AS
        SELECT nuid,name,latitude,longitude,timezone('UTC',timestamp) AS ts,
            try_cast(extra::JSON->>'renting' AS BOOLEAN) AS active,
            epoch(timestamp)-coalesce(try_cast(extra::JSON->>'last_updated' AS BIGINT),
                epoch(try_cast(extra::JSON->>'last_updated' AS TIMESTAMPTZ))) AS age_seconds
        FROM read_parquet(?)''',[str(path)])


def bracket_hours(con, gap_seconds=600):
    """Input status: station_id, ts (UTC), active, fresh, stable.

    Include a whole hour only when a chain of short eligible brackets covers it.
    Never fill over a missing, inactive, stale or moved observation. Duplicates
    must be resolved conservatively before this function, not arbitrarily kept.
    """
    if gap_seconds <= 0:
        raise ValueError('gap_seconds must be positive')
    con.execute('''CREATE OR REPLACE TEMP TABLE brackets AS
        WITH ordered AS (
            SELECT *,lag(ts) OVER w AS previous_ts,
                lag(active AND fresh AND stable) OVER w AS previous_good
            FROM status WINDOW w AS (PARTITION BY station_id ORDER BY ts)),
        marked AS (
            SELECT *,CASE WHEN active AND fresh AND stable AND previous_good
                AND ts>previous_ts AND epoch(ts-previous_ts)<=? THEN 0 ELSE 1 END AS broken
            FROM ordered),
        islands AS (
            SELECT *,sum(broken) OVER(PARTITION BY station_id ORDER BY ts) AS island FROM marked)
        SELECT station_id,min(ts) AS start_utc,max(ts) AS end_utc
        FROM islands WHERE active AND fresh AND stable GROUP BY station_id,island
        HAVING max(ts)>min(ts)''', [gap_seconds])
    # Materialize one grid. Correlated TIMESTAMPTZ generate_series returned
    # inconsistent empty results on DuckDB 1.5.6 with real archive intervals.
    first,last=con.execute("SELECT date_trunc('hour',min(start_utc)),max(end_utc) FROM brackets").fetchone()
    con.execute('''CREATE OR REPLACE TEMP TABLE exposure_grid AS
        SELECT * FROM generate_series(?::TIMESTAMPTZ,?::TIMESTAMPTZ,INTERVAL '1 hour') g(hour_utc)''',
        [first,last])
    con.execute('''CREATE OR REPLACE TEMP TABLE bracket_hours AS
        SELECT station_id,hour_utc FROM brackets JOIN exposure_grid
            ON hour_utc>=start_utc AND hour_utc+INTERVAL '1 hour'<=end_utc''')


def monthly_crosswalk(con):
    """Retrospective identity audit: unique nearby IDs in the same month's trip data.

    Reject another candidate within 50m and many-to-one assignments. This is label
    alignment evidence, not a historically published station-information feed.
    """
    con.execute('''CREATE OR REPLACE TEMP TABLE points AS
        SELECT nuid,median(latitude) AS latitude,median(longitude) AS longitude,
            min(name) AS station_name FROM raw_status GROUP BY nuid''')
    con.execute('''CREATE OR REPLACE TEMP TABLE candidates AS
        SELECT p.*,g.station_id,g.borough,
            sqrt(pow((p.latitude-g.latitude)*111195,2)+
                pow(cos(radians(p.latitude))*(p.longitude-g.longitude)*111195,2)) AS distance_m
        FROM points p JOIN geography g ON g.id_scheme='short_name'
            AND abs(p.latitude-g.latitude)<.002 AND abs(p.longitude-g.longitude)<.003''')
    con.execute('''CREATE OR REPLACE TEMP TABLE crosswalk AS
        WITH ranked AS (SELECT *,row_number() OVER(PARTITION BY nuid ORDER BY distance_m,station_id) AS r,
            lead(distance_m) OVER(PARTITION BY nuid ORDER BY distance_m,station_id) AS second_distance
            FROM candidates), nearest AS (SELECT * FROM ranked
                WHERE r=1 AND distance_m<=25 AND coalesce(second_distance,10000)>=50),
        unique_ids AS (SELECT *,count(*) OVER(PARTITION BY station_id) AS mappings FROM nearest)
        SELECT nuid,station_id,station_name,latitude,longitude,borough,distance_m
            FROM unique_ids WHERE mappings=1''')


def build_public_exposure(source_root: Path, observed_root: Path, output_root: Path, out=print):
    manifest = json.loads((source_root / 'manifest.json').read_text())
    observed = json.loads((observed_root / 'manifest.json').read_text())
    indexed = {(a['table'], Path(a['path']).stem): observed_root/a['path'] for a in observed['artifacts']}
    root = output_root / stamp()
    root.mkdir(parents=True)
    report = {'schema_version':'public-exposure-v1', 'policy':POLICY,
              'created_at_utc':datetime.now(timezone.utc).isoformat(),
              'source_manifest_sha256':digest(source_root/'manifest.json'),
              'observed_manifest_sha256':digest(observed_root/'manifest.json'),
              'source_root':str(source_root), 'observed_root':str(observed_root),
              'operation_verified':False, 'main_evaluation_eligible':False,
              'scope':'retrospective Manhattan hours covered under bounded public change-record interpolation',
              'selection_bias':'coverage depends on inventory activity; accuracy is conditional, not citywide',
              'identity_policy':'same-month unique nearest trip mean <=25m; second candidate >=50m; one-to-one',
              'geography_policy':'each endpoint <=25m from archived monthly median; retrospective label alignment',
              'source_timezone':'UTC inferred from SQLite CURRENT_TIMESTAMP and epoch last_updated comparison',
              'attribution':'CityBikes / data.citybik.es, ODbL-1.0; original Citi Bike operator terms apply',
              'artifacts':[], 'months':[]}
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; SET memory_limit='3GB'; SET threads=4")

    def save(table, month):
        path=root/table/(month+'.parquet');path.parent.mkdir(exist_ok=True)
        con.execute('COPY (SELECT * FROM '+table+') TO '+sql_literal(str(path))+' (FORMAT PARQUET,COMPRESSION ZSTD)')
        report['artifacts'].append({'table':table,'path':str(path.relative_to(root)),
            'rows':con.execute('SELECT count(*) FROM '+table).fetchone()[0],
            'sha256':digest(path),'bytes':path.stat().st_size})

    try:
        for artifact in sorted(manifest['artifacts'], key=lambda a:a['path']):
            path=Path(artifact['path']); month=path.name[:4]+'-'+path.name[4:6]
            if digest(path)!=artifact['sha256']:
                raise ValueError(f'Station archive checksum mismatch: {path}')
            normalize_status(con,path)
            con.execute('CREATE OR REPLACE TEMP TABLE geography AS SELECT * FROM read_parquet(?)',
                        [str(indexed['station_geography_observed',month])])
            monthly_crosswalk(con)
            con.execute('''CREATE OR REPLACE TEMP TABLE status AS
                SELECT station_id,ts,bool_and(coalesce(active,false)) AS active,
                    bool_and(coalesce(age_seconds BETWEEN 0 AND 600,false)) AS fresh,
                    bool_and(sqrt(pow((r.latitude-c.latitude)*111195,2)+
                        pow(cos(radians(c.latitude))*(r.longitude-c.longitude)*111195,2))<=25) AS stable
                FROM raw_status r JOIN crosswalk c USING(nuid) WHERE c.borough='Manhattan'
                GROUP BY station_id,ts''')
            sensitivity={}
            for gap in (300,600,900):
                bracket_hours(con,gap)
                sensitivity[str(gap)]=con.execute('SELECT count(*) FROM bracket_hours').fetchone()[0]
            bracket_hours(con,600)
            con.execute('CREATE OR REPLACE TEMP TABLE counts AS SELECT * FROM read_parquet(?)',
                        [str(indexed['hourly_observed',month])])
            con.execute('''CREATE OR REPLACE TEMP TABLE public_hours AS
                SELECT h.station_id,'short_name' AS id_scheme,h.hour_utc,
                    coalesce(o.departures,0)::BIGINT AS departures,
                    timezone('America/New_York',h.hour_utc) AS target_local,
                    false AS operation_verified,false AS main_evaluation_eligible,
                    true AS public_coverage_evaluation_eligible,? AS coverage_policy
                FROM bracket_hours h LEFT JOIN counts o
                    ON h.station_id=o.station_id AND h.hour_utc=o.hour_utc AND o.id_scheme='short_name'
                WHERE strftime(timezone('America/New_York',h.hour_utc),'%Y-%m')=?
                    AND timezone('America/New_York',h.hour_utc)<>
                        timezone('America/New_York',h.hour_utc-INTERVAL '1 hour')
                    AND timezone('America/New_York',h.hour_utc)<>
                        timezone('America/New_York',h.hour_utc+INTERVAL '1 hour')''',[POLICY,month])
            stats=con.execute('''SELECT count(*),count(DISTINCT station_id),sum(departures),
                count(*) FILTER(WHERE departures=0) FROM public_hours''').fetchone()
            save('crosswalk',month);save('public_hours',month)
            audit={'month':month,'hours':stats[0],'stations':stats[1],'departures':stats[2],
                   'zero_hours':stats[3],'gap_sensitivity_hours':sensitivity,
                   'status_records':con.execute('SELECT count(*) FROM raw_status').fetchone()[0],
                   'matched_stations':con.execute('SELECT count(*) FROM crosswalk').fetchone()[0]}
            report['months'].append(audit);out(json.dumps(audit),flush=True)
        report['status']='conditional_exposure_built'
    except Exception:
        report['status']='failed'
        raise
    finally:
        con.close();(root/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
    (output_root/'CURRENT_PUBLIC_EXPOSURE').write_text(str(root)+'\n')
    return root
