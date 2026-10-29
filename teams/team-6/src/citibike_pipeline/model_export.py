"""Ledger-backed, provisional modeling handoff; never invent station-operation evidence."""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import resource
import time
from uuid import uuid4
import zipfile

import duckdb
import pyarrow.parquet as pq

from .db import sql_literal

VERSION = "observed-station-day-v1"
DST_UNCERTAIN = "['start_dst_ambiguous','end_dst_ambiguous','start_dst_nonexistent','end_dst_nonexistent']"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(2**20), b''):
            h.update(block)
    return h.hexdigest()


def select_inputs(cfg, start: date, end: date) -> list[dict]:
    """Select exact timestamp partitions, including supplying releases outside the period."""
    with duckdb.connect(str(cfg.manifest_db), read_only=True) as ledger:
        cur = ledger.execute("""SELECT p.path, p.rows, p.source_key, p.run_id,
            s.nominal_release, s.schema_family, s.sha256 AS original_source_sha256,
            s.row_digest AS original_source_row_digest, s.status
            FROM published_files p JOIN sources s USING(source_key)
            WHERE p.dataset='trips' ORDER BY p.path""")
        records = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]
    selected = []
    seen = set()
    for rec in records:
        match = re.search(r'trip_year=(\d{4})/trip_month=(\d{2})/', rec['path'])
        if not match:
            raise ValueError(f"Unrecognized trip partition: {rec['path']}")
        first = date(int(match[1]), int(match[2]), 1)
        last = first + timedelta(days=calendar.monthrange(first.year, first.month)[1])
        if first >= end or last <= start:
            continue
        if rec['status'] != 'selected':
            raise ValueError(f"Stale published source: {rec['source_key']}")
        if rec['path'] in seen:
            raise ValueError(f"Duplicate ledger path: {rec['path']}")
        seen.add(rec['path'])
        path = (cfg.processed_root / rec['path']).resolve()
        if not path.is_relative_to(cfg.processed_root.resolve()):
            raise ValueError('Published path escapes processed root')
        if not path.is_file():
            raise FileNotFoundError(path)
        meta = pq.read_metadata(path)
        if meta.num_rows != rec['rows']:
            raise ValueError(f"Ledger/Parquet row count mismatch: {path}")
        rec.update(partition=first.isoformat(), bytes=path.stat().st_size)
        selected.append(rec)
    if not selected:
        raise ValueError('No published inputs for requested period')
    wanted = {d[:7] for d in month_starts(start, end)}
    if wanted != {r['partition'][:7] for r in selected}:
        raise ValueError('Requested period has missing published month partitions')
    return selected


def month_starts(start: date, end: date) -> list[str]:
    out = []
    d = start.replace(day=1)
    while d < end:
        out.append(d.isoformat())
        d += timedelta(days=calendar.monthrange(d.year, d.month)[1])
    return out


def aggregate_departures(con, start: date, end: date) -> None:
    """Creates departure_batch from trips joined to the selected ledger's source_releases."""
    con.execute("""CREATE OR REPLACE TEMP TABLE departure_batch AS
        WITH timed AS (
            SELECT t.*, r.nominal_release,
                CASE WHEN schema_family='modern' THEN 'short_name' ELSE 'legacy_id' END AS id_scheme,
                NOT coalesce(list_has_any(quality_flags, """ + DST_UNCERTAIN + """),false)
                    AND started_at_local IS NOT NULL AND ended_at_local IS NOT NULL AS unambiguous,
                epoch(timezone('America/New_York',ended_at_local)) -
                    epoch(timezone('America/New_York',started_at_local)) AS elapsed_seconds
            FROM trips t JOIN source_releases r USING(source_key)
            WHERE eligible_departure AND started_at_local >= $start::TIMESTAMP
                AND started_at_local < $end::TIMESTAMP
        ), reliable AS (
            SELECT *, unambiguous AND elapsed_seconds > 0 AND
                (source_duration_seconds IS NULL OR
                 abs(source_duration_seconds-elapsed_seconds) <= 1) AS duration_reliable
            FROM timed
        )
        SELECT start_station_id AS station_id, id_scheme, started_at_local::DATE AS service_date,
            nominal_release, count(*)::BIGINT AS departures_published,
            count(*) FILTER(WHERE duration_reliable AND elapsed_seconds >= 30)::BIGINT AS departures_ge30,
            count(*) FILTER(WHERE duration_reliable AND elapsed_seconds >= 60)::BIGINT AS departures_ge60,
            count(*) FILTER(WHERE duration_reliable AND elapsed_seconds >= 120)::BIGINT AS departures_ge120,
            count(*) FILTER(WHERE duration_reliable)::BIGINT AS reliable_duration_departures,
            count(*) FILTER(WHERE NOT coalesce(duration_reliable,false))::BIGINT AS uncertain_duration_departures,
            count(*) FILTER(WHERE duration_seconds IS NULL)::BIGINT AS missing_duration_departures,
            count(*) FILTER(WHERE duration_seconds <= 0)::BIGINT AS wall_nonpositive_departures,
            count(*) FILTER(WHERE list_has_any(quality_flags,""" + DST_UNCERTAIN + """))::BIGINT AS dst_uncertain_departures,
            count(*) FILTER(WHERE list_contains(quality_flags,'crosses_dst'))::BIGINT AS crosses_dst_departures,
            count(*) FILTER(WHERE hour(started_at_local) >= 7 AND hour(started_at_local) < 10)::BIGINT AS morning_departures,
            count(*) FILTER(WHERE hour(started_at_local) >= 16 AND hour(started_at_local) < 19)::BIGINT AS evening_departures,
            count(*) FILTER(WHERE membership_group='member')::BIGINT AS member_departures,
            count(*) FILTER(WHERE membership_group='casual')::BIGINT AS casual_departures,
            count(*) FILTER(WHERE membership_group IS NULL OR membership_group NOT IN ('member','casual'))::BIGINT AS unknown_member_departures,
            count(*) FILTER(WHERE rideable_type='classic_bike')::BIGINT AS classic_departures,
            count(*) FILTER(WHERE rideable_type='electric_bike')::BIGINT AS electric_departures,
            count(*) FILTER(WHERE rideable_type IS NULL OR rideable_type NOT IN ('classic_bike','electric_bike'))::BIGINT AS unknown_other_bike_departures
        FROM reliable GROUP BY ALL
    """, {'start': start, 'end': end})


def build_features(con, start: date, end: date) -> None:
    """No observed-day mean: without operation evidence its exposure denominator is unknown."""
    con.execute("""CREATE TEMP TABLE origins AS SELECT DISTINCT service_date AS target_date,
        timezone('America/New_York', (service_date-1)::TIMESTAMP) AS forecast_origin_utc,
        (SELECT max(release_end_date) FROM release_availability r
         WHERE r.available_at_utc < timezone('America/New_York',(service_date-1)::TIMESTAMP)) AS profile_as_of
        FROM labels WHERE service_date >= $start AND service_date < $end""", {'start': start, 'end': end})
    # Every forecast with the same available month-end has the same historical window.
    # Aggregate these cohorts once, rather than repeating the history join for every day.
    con.execute("""CREATE TEMP TABLE profile_history AS
        SELECT d.*, c.profile_as_of FROM departure_daily d
        JOIN release_availability r USING(nominal_release)
        JOIN (SELECT DISTINCT profile_as_of FROM origins WHERE profile_as_of IS NOT NULL) c
            ON r.release_end_date <= c.profile_as_of
            AND d.service_date <= c.profile_as_of AND d.service_date > c.profile_as_of-56""")
    con.execute("""CREATE TEMP TABLE profiles AS
        SELECT station_id, id_scheme, profile_as_of,
            sum(departures_published)::BIGINT AS historical_departures,
            count(DISTINCT service_date)::INTEGER AS historical_observed_departure_days,
            sum(member_departures)::DOUBLE/nullif(sum(member_departures+casual_departures),0) AS historical_member_share_known,
            sum(electric_departures)::DOUBLE/nullif(sum(classic_departures+electric_departures),0) AS historical_electric_share_known,
            sum(member_departures+casual_departures)::DOUBLE/nullif(sum(departures_published),0) AS historical_member_coverage,
            sum(classic_departures+electric_departures)::DOUBLE/nullif(sum(departures_published),0) AS historical_bike_coverage,
            max(nominal_release) AS latest_included_release
        FROM profile_history GROUP BY station_id,id_scheme,profile_as_of""")
    con.execute("""CREATE TEMP TABLE weekday_profiles AS SELECT station_id,id_scheme,profile_as_of,
        isodow(service_date)::INTEGER AS weekday,
        sum(departures_published)::BIGINT AS historical_weekday_departures,
        count(DISTINCT service_date)::INTEGER AS historical_weekday_observed_days
        FROM profile_history GROUP BY ALL""")
    con.execute('DROP TABLE profile_history')
    con.execute("""CREATE TEMP TABLE features AS
        SELECT l.station_id,l.id_scheme,l.service_date AS target_date,o.forecast_origin_utc,
            isodow(l.service_date)::INTEGER AS weekday, month(l.service_date)::INTEGER AS month,
            dayofyear(l.service_date)::INTEGER AS day_of_year, isodow(l.service_date) >= 6 AS is_weekend,
            o.profile_as_of, o.profile_as_of-55 AS profile_window_start,
            p.historical_departures,p.historical_observed_departure_days,
            w.historical_weekday_departures,
            CASE WHEN p.historical_departures IS NOT NULL THEN
                coalesce(w.historical_weekday_observed_days,0) END AS historical_weekday_observed_days,
            p.historical_member_share_known,p.historical_electric_share_known,
            p.historical_member_coverage,p.historical_bike_coverage,p.latest_included_release,
            NULL::DOUBLE AS historical_weekday_departure_mean,
            NULL::DOUBLE AS pooled_departure_mean,
            NULL::DOUBLE AS population_density, NULL::DOUBLE AS renter_share,
            NULL::DOUBLE AS no_vehicle_household_share, NULL::DOUBLE AS median_household_income,
            false AS census_available, false AS operation_denominator_verified,
            false AS release_availability_verified, false AS main_evaluation_eligible,
            '""" + VERSION + """' AS feature_version
        FROM labels l JOIN origins o ON l.service_date=o.target_date
        LEFT JOIN profiles p ON l.station_id=p.station_id AND l.id_scheme=p.id_scheme AND o.profile_as_of=p.profile_as_of
        LEFT JOIN weekday_profiles w ON l.station_id=w.station_id AND l.id_scheme=w.id_scheme
            AND o.profile_as_of=w.profile_as_of AND isodow(l.service_date)=w.weekday
        WHERE l.service_date >= $start AND l.service_date < $end
    """, {'start': start, 'end': end})


def copy_table(con, query: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO {sql_literal(str(target))} (FORMAT PARQUET, COMPRESSION ZSTD)")


def validate_package(path: Path) -> dict:
    manifest = json.loads((path/'manifest.json').read_text())
    for artifact in manifest['artifacts']:
        file = path/artifact['path']
        if not file.is_file() or file.stat().st_size != artifact['bytes'] or sha256(file) != artifact['sha256']:
            raise ValueError(f"Artifact checksum/size failed: {file}")
        if artifact['path'].endswith('.parquet') and pq.read_metadata(file).num_rows != artifact['rows']:
            raise ValueError(f"Artifact row count failed: {file}")
    return json.loads((path/'validation.json').read_text())


def run_model_export(cfg, start: date, end: date, release_lag_days: int = 45,
                     dry_run: bool = False, out=print) -> Path | None:
    if start >= end or release_lag_days < 1:
        raise ValueError('start must precede end; release-lag-days must be positive')
    history_start = (start-timedelta(days=release_lag_days+90)).replace(day=1)
    inputs = select_inputs(cfg, history_start, end)
    out(f"Selected {len(inputs)} ledger files; targets {start} to {end} exclusive; history from {history_start}")
    if dry_run:
        out(f"Input bytes: {sum(r['bytes'] for r in inputs):,}; no export written")
        return None
    began = time.monotonic()
    build_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8]
    root = cfg.processed_root/'modeling_exports'
    stage = root/'staging'/build_id
    stage.mkdir(parents=True, exist_ok=False)
    spill = cfg.tmp_dir/('model-export-'+build_id)
    spill.mkdir(parents=True, exist_ok=False)
    sources = {r['source_key']: r['nominal_release'] for r in inputs}
    with duckdb.connect() as con:
        con.execute("SET memory_limit='3GB'")
        con.execute(f"SET threads={min(cfg.threads,4)}")
        con.execute(f"SET temp_directory={sql_literal(str(spill))}")
        con.execute("SET TimeZone='UTC'")
        con.execute('CREATE TEMP TABLE source_releases(source_key VARCHAR PRIMARY KEY, nominal_release VARCHAR)')
        con.executemany('INSERT INTO source_releases VALUES (?,?)', sorted(sources.items()))
        con.execute('''CREATE TEMP TABLE release_availability(nominal_release VARCHAR PRIMARY KEY,
            release_end_date DATE, available_at_utc TIMESTAMPTZ, evidence_type VARCHAR, verified BOOLEAN)''')
        for release in sorted(set(sources.values())):
            d = date.fromisoformat(release+'-01')
            last = d+timedelta(days=calendar.monthrange(d.year,d.month)[1]-1)
            # Deliberate reconstruction assumption; never label it provider publication evidence.
            assumed = datetime.combine(last+timedelta(days=release_lag_days),datetime.min.time(),timezone.utc)
            con.execute('INSERT INTO release_availability VALUES (?,?,?,?,false)',
                        [release,last,assumed,f'assumed month-end + {release_lag_days} days; not verified'])
        audit = []
        for partition in sorted({r['partition'] for r in inputs}):
            records = [r for r in inputs if r['partition']==partition]
            paths = ','.join(sql_literal(str(cfg.processed_root/r['path'])) for r in records)
            con.execute(f'CREATE OR REPLACE TEMP VIEW trips AS SELECT * FROM read_parquet([{paths}],hive_partitioning=false)')
            observed = con.execute('''SELECT count(*),count(*) FILTER(WHERE eligible_departure),
                count(*) FILTER(WHERE eligible_arrival AND ended_at_local >= ? AND ended_at_local < ?),
                count(*) FILTER(WHERE source_key NOT IN (SELECT source_key FROM source_releases))
                FROM trips WHERE started_at_local >= ? AND started_at_local < ?''',
                [start,end,history_start,end]).fetchone()
            if observed[3]:
                raise ValueError('Trip rows reference an unselected source')
            aggregate_departures(con,history_start,end)
            if partition==min(r['partition'] for r in inputs):
                con.execute('CREATE TEMP TABLE departure_daily AS SELECT * FROM departure_batch WHERE false')
                con.execute('''CREATE TEMP TABLE arrival_daily(station_id VARCHAR,id_scheme VARCHAR,
                    service_date DATE, arrivals_from_selected_start_cohort BIGINT)''')
                con.execute('''CREATE TEMP TABLE locations(station_id VARCHAR,id_scheme VARCHAR,
                    observation_month DATE, station_name VARCHAR, latitude DOUBLE, longitude DOUBLE,
                    min_latitude DOUBLE,max_latitude DOUBLE,min_longitude DOUBLE,max_longitude DOUBLE,
                    coordinate_observations BIGINT)''')
            con.execute('INSERT INTO departure_daily SELECT * FROM departure_batch')
            con.execute('''INSERT INTO arrival_daily SELECT end_station_id,
                CASE WHEN schema_family='modern' THEN 'short_name' ELSE 'legacy_id' END,
                ended_at_local::DATE,count(*) FROM trips WHERE eligible_arrival
                AND started_at_local >= ? AND started_at_local < ?
                AND ended_at_local >= ? AND ended_at_local < ? GROUP BY 1,2,3''',[history_start,end,start,end])
            con.execute('''INSERT INTO locations SELECT start_station_id,
                CASE WHEN schema_family='modern' THEN 'short_name' ELSE 'legacy_id' END,
                date_trunc('month',started_at_local)::DATE, min(start_station_name),
                avg(start_lat),avg(start_lng),min(start_lat),max(start_lat),min(start_lng),max(start_lng),count(*)
                FROM trips WHERE eligible_departure AND started_at_local >= ? AND started_at_local < ?
                AND start_lat BETWEEN -90 AND 90 AND start_lng BETWEEN -180 AND 180
                AND NOT coalesce(list_contains(quality_flags,'start_coords_invalid'),false)
                GROUP BY 1,2,3''',[history_start,end])
            counted = con.execute('SELECT coalesce(sum(departures_published),0) FROM departure_batch').fetchone()[0]
            if counted != observed[1]:
                raise ValueError('Departure reconciliation failed')
            audit.append({'partition':partition,'trip_rows_in_start_cohort':observed[0],
                          'eligible_departures_in_start_cohort':observed[1],
                          'eligible_arrivals_in_target_period_from_start_cohort':observed[2]})
            out(f"Aggregated {partition[:7]}: {observed[0]:,} trips; {counted:,} departures")
        sums = ','.join(f'sum({d[0]})::BIGINT AS {d[0]}' for d in con.execute('DESCRIBE departure_daily').fetchall()
                        if d[0] not in ('station_id','id_scheme','service_date','nominal_release'))
        con.execute(f'''CREATE TEMP TABLE labels AS SELECT station_id,id_scheme,service_date,{sums},
            min(nominal_release) AS earliest_supplying_release,max(nominal_release) AS latest_supplying_release,
            true AS observed_departure_day,false AS station_operation_verified,
            false AS zero_day_grid_complete,false AS main_evaluation_eligible,
            '{VERSION}' AS label_version FROM departure_daily
            WHERE service_date >= ? AND service_date < ? GROUP BY 1,2,3''',[start,end])
        con.execute('''CREATE TEMP TABLE arrivals AS SELECT station_id,id_scheme,service_date,
            sum(arrivals_from_selected_start_cohort)::BIGINT AS arrivals_from_selected_start_cohort,
            false AS full_arrival_coverage_verified FROM arrival_daily GROUP BY 1,2,3''')
        out('Building historical summaries with explicit assumed publication cutoffs')
        build_features(con,start,end)
        rows = con.execute('SELECT count(*),sum(departures_published) FROM labels').fetchone()
        total_dep = con.execute('SELECT sum(departures_published) FROM departure_daily WHERE service_date >= ? AND service_date < ?',[start,end]).fetchone()[0]
        checks = {
            'departure_counts_reconcile': rows[1]==total_dep,
            'arrival_counts_reconcile': con.execute('SELECT coalesce(sum(arrivals_from_selected_start_cohort),0) FROM arrivals').fetchone()[0] == sum(a['eligible_arrivals_in_target_period_from_start_cohort'] for a in audit),
            'unique_label_keys': con.execute('SELECT count(*) FROM (SELECT station_id,id_scheme,service_date FROM labels GROUP BY ALL HAVING count(*)>1)').fetchone()[0]==0,
            'feature_label_keys_match': con.execute('''SELECT count(*) FROM (
                (SELECT station_id,id_scheme,service_date FROM labels EXCEPT ALL
                 SELECT station_id,id_scheme,target_date FROM features)
                UNION ALL
                (SELECT station_id,id_scheme,target_date FROM features EXCEPT ALL
                 SELECT station_id,id_scheme,service_date FROM labels))''').fetchone()[0]==0,
            'duration_variants_ordered': con.execute('''SELECT count(*) FROM labels WHERE NOT
                (departures_ge120<=departures_ge60 AND departures_ge60<=departures_ge30
                AND departures_ge30<=reliable_duration_departures
                AND reliable_duration_departures+uncertain_duration_departures=departures_published)''').fetchone()[0]==0,
            'member_and_bike_counts_reconcile': con.execute('''SELECT count(*) FROM labels WHERE
                member_departures+casual_departures+unknown_member_departures<>departures_published OR
                classic_departures+electric_departures+unknown_other_bike_departures<>departures_published''').fetchone()[0]==0,
            'no_invented_zero_days': con.execute('SELECT count(*) FROM labels WHERE departures_published<=0 OR station_operation_verified OR main_evaluation_eligible').fetchone()[0]==0,
            'historical_release_cutoffs_respected': con.execute('''SELECT count(*) FROM features f JOIN release_availability r
                ON f.latest_included_release=r.nominal_release WHERE r.available_at_utc >= f.forecast_origin_utc''').fetchone()[0]==0,
        }
        if not all(checks.values()):
            raise ValueError(f'Export checks failed: {checks}')
        for m in month_starts(start,end):
            where = f"service_date >= DATE '{m}' AND service_date < DATE '{m}'+INTERVAL 1 MONTH"
            copy_table(con,f'SELECT * FROM labels WHERE {where} ORDER BY station_id,id_scheme,service_date',stage/'labels'/'station_day_labels'/f'{m[:7]}.parquet')
            copy_table(con,f"SELECT * FROM features WHERE {where.replace('service_date','target_date')} ORDER BY station_id,id_scheme,target_date",stage/'features'/'forecast_features'/f'{m[:7]}.parquet')
        for table, dest in [('arrivals','labels/arrivals_from_start_cohort.parquet'),
                             ('release_availability','audit/source_release_availability.parquet'),
                             ('departure_daily','audit/station_day_release_history.parquet'),
                             ('locations','dimensions/station_location_observations.parquet')]:
            copy_table(con,f'SELECT * FROM {table}',stage/dest)
        copy_table(con,'''SELECT date_trunc('month',service_date)::DATE AS month,
            sum(departures_published) AS departures_published,sum(departures_ge30) AS departures_ge30,
            sum(departures_ge60) AS departures_ge60,sum(departures_ge120) AS departures_ge120,
            sum(uncertain_duration_departures) AS uncertain_duration_departures,
            sum(dst_uncertain_departures) AS dst_uncertain_departures FROM labels GROUP BY 1 ORDER BY 1''',stage/'audit'/'duration_policy_impact.parquet')
        uncompressed = sum(pq.read_metadata(p).row_group(i).total_byte_size
                           for p in stage.rglob('*.parquet') for i in range(pq.read_metadata(p).num_row_groups))
        cur = con.execute('''SELECT sum(departures_published) AS departures_published,sum(departures_ge30) AS departures_ge30,
            sum(departures_ge60) AS departures_ge60,sum(departures_ge120) AS departures_ge120,
            sum(uncertain_duration_departures) AS uncertain_duration_departures FROM labels''')
        totals = dict(zip([d[0] for d in cur.description],cur.fetchone()))
        schemas = {}
        for table in ('labels','features','arrivals','locations','departure_daily','release_availability'):
            schemas[table] = [(r[0],r[1]) for r in con.execute(f'DESCRIBE {table}').fetchall()]
    dictionary = '''# Observed station-day export v1

This provisional all-station handoff is for import, exploration and further feature development.
It is not a completed Census/borough pilot and is not valid for unconditional demand evaluation.
Only station-days with recorded eligible departures enter the target keys. No zero-trip days are invented.
Station IDs are strings; short_name and legacy_id are distinct namespaces.

## Labels and duration

departures_published is the unchanged reference label. ge30/ge60/ge120 count reliable elapsed
durations at inclusive boundaries. NYC timezone offset changes are corrected for unambiguous
endpoints. Ambiguous/nonexistent timestamps, missing/nonpositive elapsed duration, and legacy
source-duration disagreement above one second enter uncertain_duration_departures; they remain
in the reference label and are excluded from all threshold variants. No threshold is selected.
Commute windows are 07:00–10:00 and 16:00–19:00 NYC local time, end-exclusive.
Arrivals are in a separate table, limited to trips starting within the scanned history/target cohort;
they cannot be represented as complete calendar-day arrivals.

## Features and availability

Forecast origin is midnight NYC local time on the day before target_date, stored as a UTC instant.
Release availability is ASSUMED month-end plus the configured lag, not verified historical evidence.
Profiles cover 56 calendar days ending at the latest release month-end available under that assumption;
each included trip must precede the origin and its supplying release must also precede the origin.
Historical counts and observed-day counts are descriptive. Departure means are NULL because station
operation exposure is unknown; dividing only by observed days would bias the baseline.
Member/electric shares use known-category denominators, with coverage columns supplied separately.
Null history means no available station history, never zero. Calendar features require no trip outcomes.
Do not use target labels, quality counts, target membership/bike shares or target coordinates as predictors.
Census predictors are NULL and census_available=false. All main_evaluation_eligible flags are false.
Split dates can be assigned downstream only after release maturation and operation evidence are resolved.

## Geography and remaining requirements

station_location_observations are retrospective monthly trip-endpoint summaries, not official
effective-dated station locations or borough assignments. Coordinates include bounds so movement/noise
can be reviewed; a monthly mean must not be assumed to locate a station for historical predictions.
Verified historical station operation, effective-dated station-to-tract crosswalk, released Census
profiles and publication-date evidence are required before the full proposal pilot is complete.

## Physical schemas

'''
    for table, columns in schemas.items():
        dictionary += f'### {table}\n\n| Column | Type |\n|---|---|\n'
        dictionary += ''.join(f'| {name} | {kind} |\n' for name,kind in columns)+'\n'
    (stage/'data_dictionary.md').write_text(dictionary)
    (stage/'databricks_import.py').write_text('''# Databricks notebook source
# Upload the entire build directory into a Unity Catalog volume first.
# Select an existing writable catalog and schema. This notebook does not create them.
dbutils.widgets.text("export_root", "/Volumes/<catalog>/<schema>/<volume>/<build_id>")
dbutils.widgets.text("target_schema", "<catalog>.<schema>")
root = dbutils.widgets.get("export_root").rstrip("/")
schema = dbutils.widgets.get("target_schema")
assert "<" not in root + schema, "Set both widgets before running"
for name, relative in {
    "station_day_labels": "labels/station_day_labels/*.parquet",
    "forecast_features": "features/forecast_features/*.parquet",
    "arrivals_from_start_cohort": "labels/arrivals_from_start_cohort.parquet",
    "station_location_observations": "dimensions/station_location_observations.parquet",
    "source_release_availability": "audit/source_release_availability.parquet",
    "duration_policy_impact": "audit/duration_policy_impact.parquet",
}.items():
    table = f"{schema}.citibike_{name}"
    # Unique table names per build/schema; fail if one already exists.
    spark.read.parquet(f"{root}/{relative}").write.format("delta").mode("errorifexists").saveAsTable(table)
    print(table)
# This package has no Census profile. It does not satisfy the class shared-Census-table requirement.
# All main_evaluation_eligible flags are false pending operation/coverage evidence.
''')
    validation = {'checks':checks,'label_rows':rows[0],'totals':totals,'monthly_input_audit':audit,
                  'parquet_uncompressed_row_group_bytes':uncompressed,
                  'seconds':round(time.monotonic()-began,2),
                  'peak_process_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                  'peak_rss_platform':'macOS bytes',
                  'model_training_memory_measured':False,
                  'main_evaluation_ready':False,
                  'missing_required_inputs':['verified Census profile','effective-dated station/tract crosswalk',
                      'historical station operation and complete zero-day coverage','verified historical release availability']}
    (stage/'validation.json').write_text(json.dumps(validation,indent=2)+'\n')
    out('Fingerprinting selected inputs and exported artifacts')
    for rec in inputs:
        rec['published_parquet_sha256'] = sha256(cfg.processed_root/rec['path'])
    artifacts=[]
    for file in sorted(stage.rglob('*')):
        if file.is_file():
            artifact={'path':file.relative_to(stage).as_posix(),'bytes':file.stat().st_size,'sha256':sha256(file)}
            if file.suffix=='.parquet':
                artifact['rows']=pq.read_metadata(file).num_rows
            artifacts.append(artifact)
    parquet_bytes=sum(a['bytes'] for a in artifacts if a['path'].endswith('.parquet'))
    manifest={'build_id':build_id,'version':VERSION,'status':'provisional_observed_days',
              'target_start':str(start),'target_end_exclusive':str(end),'history_start':str(history_start),
              'scope':'all observed stations; borough unknown','release_lag_assumption_days':release_lag_days,
              'label_policy':'published baseline retained; reliable elapsed threshold sensitivity only',
              'input_files':inputs,'artifacts':artifacts,'parquet_bytes':parquet_bytes,
              'within_500MB_project_budget':parquet_bytes < 500_000_000,
              'source_code_sha256':sha256(Path(__file__)),
              'duckdb_version':duckdb.__version__,
              'publication':'local export only; no Databricks upload or shared-catalog publication'}
    (stage/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    validate_package(stage)
    builds=root/'builds'
    builds.mkdir(parents=True,exist_ok=True)
    target=builds/build_id
    stage.rename(target)
    handoffs=root/'handoffs'
    handoffs.mkdir(parents=True,exist_ok=True)
    zip_stage=handoffs/(build_id+'.zip.partial')
    with zipfile.ZipFile(zip_stage,'w',compression=zipfile.ZIP_STORED,allowZip64=True) as archive:
        for file in sorted(target.rglob('*')):
            if file.is_file():
                archive.write(file,build_id+'/'+file.relative_to(target).as_posix())
    zip_target=handoffs/(build_id+'.zip')
    zip_stage.rename(zip_target)
    pointer=root/f'.CURRENT-{build_id}'
    pointer.write_text(build_id+'\n')
    pointer.replace(root/'CURRENT')
    if not any(spill.iterdir()):
        spill.rmdir()
    out(f'Published: {target}')
    out(f'{rows[0]:,} observed station-day labels; {parquet_bytes:,} compressed Parquet bytes')
    out(f'Portable ZIP: {zip_target} ({zip_target.stat().st_size:,} bytes; extract before notebook import)')
    out('Provisional: Census, operation/zero-day coverage, geography and release evidence still required')
    return target
