"""Audit downloaded public archive samples without promoting station eligibility.

Reads only the explicit files in a research package; does not modify the canonical
manifest, live model, forecasts, source archive or dataset pointers.
"""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import json
import csv,io,zipfile
from pathlib import Path
import duckdb
from citibike_pipeline.forecast_sources import digest
from citibike_pipeline.db import sql_literal


def row(con,sql,parameters=()):
    cur=con.execute(sql,list(parameters))
    return dict(zip([column[0] for column in cur.description],cur.fetchone()))


def audit(root):
    manifest=json.loads((root/'manifest.json').read_text())
    for item in manifest['artifacts']:
        if item.get('sha256'):
            assert digest(root/item['path'])==item['sha256'],item['path']
    con=duckdb.connect();con.execute("SET memory_limit='1GB'; SET threads=3; SET TimeZone='UTC'")
    result={'checked_at_utc':datetime.now(timezone.utc).isoformat(),'research_root':str(root),
        'source_artifacts':manifest['artifacts'],'ctbk':[],'station_operation_eligibility_promoted':False}
    try:
        for day in ('2026-04-07','2026-06-15','2026-09-15'):
            path=root/('ctbk_status_'+day+'.parquet')
            con.execute('CREATE OR REPLACE VIEW status AS SELECT * FROM read_parquet('+sql_literal(str(path))+')')
            stats=row(con,'''SELECT count(*) AS rows,count(DISTINCT station_id) AS stations,
                count(DISTINCT ts) AS distinct_feed_times,count(DISTINCT polled_at) AS distinct_poll_times,
                count(DISTINCT date_trunc('minute',to_timestamp(polled_at))) AS observed_utc_minutes,
                min(to_timestamp(ts)) AS first_feed_utc,max(to_timestamp(ts)) AS last_feed_utc,
                min(to_timestamp(polled_at)) AS first_poll_utc,max(to_timestamp(polled_at)) AS last_poll_utc,
                median(polled_at-ts) AS median_feed_age_seconds,max(polled_at-ts) AS max_feed_age_seconds,
                count(*) FILTER(WHERE polled_at-ts>120) AS rows_feed_age_over_120s,
                count(*) FILTER(WHERE polled_at<ts-300) AS rows_feed_clock_skew_over_300s,
                count(*) FILTER(WHERE is_installed=1 AND is_renting=1) AS installed_and_renting_rows,
                count(*) FILTER(WHERE num_bikes_available=0 AND is_installed=1 AND is_renting=1) AS renting_zero_bike_rows,
                count(*) FILTER(WHERE is_installed NOT IN (0,1) OR is_renting NOT IN (0,1) OR is_returning NOT IN (0,1)) AS invalid_operation_flags,
                median(polled_at-last_reported) AS median_station_report_age_seconds,
                quantile_cont(polled_at-last_reported,.99) AS p99_station_report_age_seconds,
                count(*) FILTER(WHERE polled_at-last_reported>600) AS station_report_age_over_600s,
                count(*) FILTER(WHERE last_reported<=0) AS station_report_timestamp_missing_or_zero,
                count(*) FILTER(WHERE is_installed=1 AND is_renting=1 AND polled_at-last_reported BETWEEN -300 AND 600) AS renting_fresh_report_rows,
                count(*) FILTER(WHERE num_bikes_available<0 OR num_docks_available<0) AS negative_inventory_rows
                FROM status''')
            stats['date']=day
            stats['duplicate_station_feed_keys']=con.execute('''SELECT count(*) FROM (
                SELECT station_id,ts,count(*) AS n FROM status GROUP BY ALL HAVING n>1)''').fetchone()[0]
            stats['duplicate_station_poll_keys']=con.execute('''SELECT count(*) FROM (
                SELECT station_id,polled_at,count(*) AS n FROM status GROUP BY ALL HAVING n>1)''').fetchone()[0]
            stats['poll_gaps']=row(con,'''WITH times AS (SELECT DISTINCT polled_at FROM status),
                gaps AS (SELECT polled_at-lag(polled_at) OVER(ORDER BY polled_at) AS seconds FROM times)
                SELECT max(seconds) AS max_seconds,count(*) FILTER(WHERE seconds>120) AS over_120s,
                count(*) FILTER(WHERE seconds>600) AS over_600s FROM gaps''')
            info=json.loads((root/('ctbk_info_'+day+'.json')).read_text())
            stations=info['data']['stations'];ids={str(s['station_id']) for s in stations}
            observed_ids={r[0] for r in con.execute('SELECT DISTINCT station_id FROM status').fetchall()}
            stats['same_date_information']={'stations':len(stations),'unique_ids':len(ids),
                'status_ids_missing_information':sorted(observed_ids-ids),
                'with_short_name':sum(bool(s.get('short_name')) for s in stations),
                'provider_last_updated_utc':datetime.fromtimestamp(info['last_updated'],timezone.utc).isoformat(),
                'retrieval_time_embedded':False}
            result['ctbk'].append(stats)
        path=root/'gtfsrt_subway_alerts_2026-09-15.parquet'
        con.execute('CREATE VIEW alerts AS SELECT * FROM read_parquet('+sql_literal(str(path))+')')
        result['gtfsrt']=row(con,'''SELECT count(*) AS flattened_rows,count(DISTINCT source_file) AS raw_snapshots,
            count(DISTINCT fetch_timestamp) AS fetch_times,count(DISTINCT entity_id) AS distinct_alert_ids,
            min(fetch_timestamp) AS first_fetch_utc,max(fetch_timestamp) AS last_fetch_utc,
            median(epoch(fetch_timestamp)-feed_timestamp) AS median_feed_age_seconds,
            max(epoch(fetch_timestamp)-feed_timestamp) AS max_feed_age_seconds,
            count(*) FILTER(WHERE epoch(fetch_timestamp)-feed_timestamp>900) AS rows_provider_age_over_900s,
            count(*) FILTER(WHERE agency_id='MTASBWY') AS rows_subway_agency,
            count(*) FILTER(WHERE route_id IS NOT NULL) AS rows_route_selector,
            count(*) FILTER(WHERE stop_id IS NOT NULL) AS rows_stop_selector,
            count(*) FILTER(WHERE trip_id IS NOT NULL OR trip_route_id IS NOT NULL OR trip_direction_id IS NOT NULL) AS rows_trip_restricted,
            count(*) FILTER(WHERE incrementality<>0 OR is_deleted) AS rows_differential_or_deleted
            FROM alerts''')
        result['gtfsrt']['fetch_gaps']=row(con,'''WITH times AS (SELECT DISTINCT fetch_timestamp FROM alerts),
            gaps AS (SELECT epoch(fetch_timestamp-lag(fetch_timestamp) OVER(ORDER BY fetch_timestamp)) AS seconds FROM times)
            SELECT max(seconds) AS max_seconds,count(*) FILTER(WHERE seconds>120) AS over_120s,
            count(*) FILTER(WHERE seconds>900) AS over_900s FROM gaps''')
        result['gtfsrt']['stale_fetches_over_900s']=con.execute('''SELECT count(DISTINCT fetch_timestamp)
            FROM alerts WHERE epoch(fetch_timestamp)-feed_timestamp>900''').fetchone()[0]
        result['gtfsrt']['sample_raw_source_file']=con.execute('SELECT source_file FROM alerts LIMIT 1').fetchone()[0]
        result['gtfsrt']['sample_selectors']=[dict(zip(['agency_id','rows'],r)) for r in con.execute('SELECT agency_id,count(*) FROM alerts GROUP BY agency_id ORDER BY count(*) DESC').fetchall()]
        result['ctbk_date_heads']=json.loads((root/'ctbk_date_probes.json').read_text())
        inventory=json.loads((root/'gtfsrt_inventory.json').read_text())
        result['gtfsrt_inventory']=next(x for x in inventory if x.get('system_id')=='subway-alerts')
        tree=json.loads((root/'comptroller_sample_tree.json').read_text())
        status=[x for x in tree['tree'] if x['path'].startswith('data/') and x['path'].endswith('_status.json')]
        timestamps=sorted(int(Path(x['path']).name.split('_')[0]) for x in status)
        result['comptroller_sample_branch']={'git_sha':tree['sha'],'status_files':len(status),
            'information_files':sum(x['path'].endswith('_stations.json') for x in tree['tree']),
            'first_filename_timestamp_utc':datetime.fromtimestamp(min(timestamps),timezone.utc).isoformat(),
            'last_filename_timestamp_utc':datetime.fromtimestamp(max(timestamps),timezone.utc).isoformat(),
            'bytes_status_information':sum(x.get('size',0) for x in tree['tree'] if x['path'].startswith('data/')),
            'sample_only_not_downloaded':True}
        result['historical_subway_gtfs']=[]
        for version in ('202602270137','202605140138','202605270017','202608280038'):
            path=root/('mta_gtfs_'+version+'.zip')
            with zipfile.ZipFile(path) as archive:
                assert archive.testzip() is None
                def records(name):
                    return list(csv.DictReader(io.TextIOWrapper(archive.open(name),encoding='utf-8-sig')))
                routes=records('routes.txt');stops=records('stops.txt');cal=records('calendar.txt')
                result['historical_subway_gtfs'].append({'version':version,'sha256':digest(path),
                    'bytes':path.stat().st_size,'routes':len(routes),'stops':len(stops),
                    'calendar_start':min(r['start_date'] for r in cal),
                    'calendar_end':max(r['end_date'] for r in cal),'zip_crcs_passed':True})
        result['gtfsrt_raw_snapshot_access']={'public_gcs_read':'HTTP 403; raw bucket private per publisher reconciliation documentation',
            'public_hostname_https':'TLS hostname verification failed; verification was not disabled',
            'public_parquet_download_verified':True,'flattening':'one row per informed entity; deduplicate entities after selector resolution'}
        coverage=root/'daily_archive_coverage_summary.json'
        if coverage.exists():result['daily_object_coverage']=json.loads(coverage.read_text())
        return result
    finally:con.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();result=audit(args.root)
    args.output.write_text(json.dumps(result,indent=2,default=str)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ['source_artifacts','ctbk_date_heads']},indent=2,default=str))
