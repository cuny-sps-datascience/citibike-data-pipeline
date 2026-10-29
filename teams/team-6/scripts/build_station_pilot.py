"""Build a narrow, independent, timestamp-audited station forecasting package.

Full public archives remain on the RAID. Only selected station observations and
latest pre-cutoff advisory snapshots are materialized. No live pointers change.
"""
from __future__ import annotations
import argparse
from bisect import bisect_left
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import json, shutil
from pathlib import Path
from zoneinfo import ZoneInfo
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point, shape
from citibike_pipeline.db import sql_literal
from citibike_pipeline.forecast_sources import digest, stamp
from citibike_pipeline.station_history import (information,stable,DatedGTFS,distance_m,
    advisory_snapshot,label_available,TRIP_FIELDS)
from citibike_pipeline.borough_mvp import weather_initialization
from citibike_pipeline.model_runtime import weather_inputs

UTC=timezone.utc;HOUR=timedelta(hours=1);ZONE=ZoneInfo('America/New_York')
BASE=Path('/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour')


def write_json(path,value):
    temp=path.with_suffix('.json.partial');temp.write_text(json.dumps(value,indent=2,default=str)+'\n');temp.replace(path)


def arrow(path,rows):
    pq.write_table(pa.Table.from_pylist(rows),path,compression='zstd')


def starts(first,last):
    t=datetime.fromisoformat(first).replace(tzinfo=ZONE).astimezone(UTC)
    end=datetime.fromisoformat(last).replace(tzinfo=ZONE).astimezone(UTC)
    while t+3*HOUR<=end:yield t;t+=HOUR


def prepare(sources,root,protocol_path,status_only=False):
    root.mkdir(parents=True,exist_ok=True)
    protocol=json.loads(protocol_path.read_text())
    if (root/'protocol.json').exists() and digest(root/'protocol.json')!=digest(protocol_path):
        raise ValueError('Protocol changed after package creation')
    shutil.copyfile(protocol_path,root/'protocol.json')
    source_manifest=json.loads((sources/'manifest.json').read_text())
    if source_manifest['status']!='downloaded' and not status_only:raise ValueError('Source acquisition is incomplete')
    lock={'sources_root':str(sources),'sources_manifest_sha256':digest(sources/'manifest.json'),
          'protocol_sha256':digest(protocol_path)}
    if not status_only and (root/'build_inputs.json').exists() and json.loads((root/'build_inputs.json').read_text())!=lock:
        raise ValueError('Cannot resume a build with different input versions')
    if not status_only:write_json(root/'build_inputs.json',lock)
    bykind=defaultdict(list)
    for r in source_manifest['objects']:
        path=sources/r['path']
        if path.stat().st_size!=r['bytes']:raise ValueError('Source size mismatch')
        bykind[r['source']].append((r,path))
    source_root=BASE/'sources/20261015T234334Z-d29974e2'
    sm=json.loads((source_root/'manifest.json').read_text())
    boundary=next(r for r in sm['artifacts'] if r.get('name')=='boroughs')
    if digest(source_root/boundary['path'])!=boundary['sha256']:raise ValueError('Boundary mismatch')
    geometry=shape(next(f['geometry'] for f in json.loads((source_root/boundary['path']).read_text())['features'] if f['properties']['boroname']=='Manhattan'))
    info={};metadata_hashes=[]
    extra=next(r for r in source_manifest['references'] if '2026-04-06' in r['path'])
    all_info=[(dict(date='2026-04-06',**{k:v for k,v in extra.items() if k!='date'}),sources/extra['path'])]+bykind['station_information']
    for item,path in all_info:
        if digest(path)!=item['sha256']:raise ValueError('Information checksum mismatch')
        info[date.fromisoformat(item['date'])]=information(path,date.fromisoformat(item['date']))
        metadata_hashes.append({'path':str(path),'sha256':item['sha256']})
    a=info[date(2026,4,6)];b=info[date(2026,4,7)]
    cohort=sorted([v for k,v in b.items() if stable(a.get(k),v) and v['capacity']>0
        and geometry.covers(Point(v['longitude'],v['latitude']))],key=lambda r:r['station_id'])[:32]
    if len(cohort)<8:raise ValueError('Insufficient pre-outcome cohort')
    write_json(root/'cohort.json',cohort)
    stations={r['uuid']:r for r in cohort}
    uuid_to_short={r['uuid']:r['station_id'] for r in cohort}
    short_to_uuid={v:k for k,v in uuid_to_short.items()}
    histories=defaultdict(list)
    for day,records in sorted(info.items()):
        for uuid in stations:
            current=records.get(uuid);previous=info.get(day-timedelta(days=1),{}).get(uuid)
            if current:
                # Stability is measured against the previous snapshot AND the
                # frozen cohort identity; observed moves do not get backfilled.
                histories[uuid].append(current|{'stable':stable(previous,current) and stable(stations[uuid],current)})
            else:
                # An observed missing/ambiguous identity invalidates the prior
                # mapping as soon as that information snapshot is assumed known.
                histories[uuid].append({'stable':False,'assumed_available_at_utc':
                    datetime.combine(day+timedelta(days=1),datetime.min.time(),UTC)+HOUR})
    for history in histories.values():history.sort(key=lambda r:r['assumed_available_at_utc'])
    history_times={uuid:[r['assumed_available_at_utc'] for r in rows] for uuid,rows in histories.items()}
    def meta(uuid,cutoff):
        rows=histories[uuid];times=history_times[uuid]
        index=bisect_left(times,cutoff)-1
        if index<0:return None
        row=rows[index]
        if not row['stable'] or (cutoff-row['assumed_available_at_utc']).total_seconds()>49*3600:return None
        return row
    con=duckdb.connect();con.execute("SET TimeZone='UTC';SET memory_limit='2GB';SET threads=2")
    (root/'duckdb_tmp').mkdir(exist_ok=True)
    con.execute('SET temp_directory='+sql_literal(str(root/'duckdb_tmp')))
    con.execute('CREATE TABLE cohort(uuid VARCHAR,station_id VARCHAR)')
    con.executemany('INSERT INTO cohort VALUES (?,?)',uuid_to_short.items())
    narrow=root/'narrow_status';narrow.mkdir(exist_ok=True)
    status_audit=[]
    for i,(item,path) in enumerate(sorted(bykind['station_status'],key=lambda p:p[0]['date'])):
        target=narrow/(item['date']+'.parquet');receipt=target.with_suffix('.json')
        if receipt.exists():
            record=json.loads(receipt.read_text())
            if record['source_sha256']!=item['sha256'] or digest(target)!=record['output_sha256']:
                raise ValueError('Status checkpoint changed')
        else:
            if digest(path)!=item['sha256']:raise ValueError('Status checksum mismatch')
            con.execute('''COPY (SELECT s.station_id AS uuid,c.station_id,s.polled_at,s.ts,s.last_reported,
                s.is_installed,s.is_renting,s.num_bikes_available
                FROM read_parquet('''+sql_literal(str(path))+''') s JOIN cohort c ON s.station_id=c.uuid)
                TO '''+sql_literal(str(target))+''' (FORMAT PARQUET,COMPRESSION ZSTD)''')
            record={'source_path':str(path),'source_sha256':item['sha256'],
                'output_sha256':digest(target),'rows':pq.read_metadata(target).num_rows,'date':item['date']}
            write_json(receipt,record)
        status_audit.append(record)
        if (i+1)%14==0:print('Narrow station days',i+1,'/',len(bykind['station_status']),flush=True)
    files=','.join(sql_literal(str(narrow/(r['date']+'.parquet'))) for r in status_audit)
    con.execute('CREATE VIEW polls_raw AS SELECT * FROM read_parquet(['+files+'])')
    # A partition may contain a neighboring-day snapshot. Exact boundary copies
    # are deduplicated; conflicting values are rejected rather than averaged.
    conflict=con.execute('''SELECT count(*) FROM (SELECT uuid,polled_at,count(DISTINCT
        (ts,last_reported,is_installed,is_renting,num_bikes_available)) n FROM polls_raw GROUP BY ALL HAVING n>1)''').fetchone()[0]
    if conflict:raise ValueError('Conflicting station/poll boundary records')
    con.execute('''CREATE TABLE polls AS SELECT DISTINCT *,
        is_installed=1 AND is_renting=1 AND polled_at-ts BETWEEN -300 AND 120
        AND polled_at-last_reported BETWEEN -300 AND 600 AS good FROM polls_raw''')
    con.execute('''CREATE TABLE chains AS WITH prior AS (
        SELECT *,lag(polled_at) OVER w AS previous,lag(good) OVER w AS previous_good
        FROM polls WINDOW w AS (PARTITION BY uuid ORDER BY polled_at)), grouped AS (
        SELECT *,sum(CASE WHEN good AND previous_good AND polled_at-previous BETWEEN 1 AND 120
            THEN 0 ELSE 1 END) OVER (PARTITION BY uuid ORDER BY polled_at) AS chain FROM prior)
        SELECT uuid,station_id,min(polled_at) AS first,max(polled_at) AS last
        FROM grouped WHERE good GROUP BY uuid,station_id,chain''')
    hours=defaultdict(set)
    con.execute('COPY (SELECT * FROM chains ORDER BY uuid,first) TO '+
        sql_literal(str(root/'operation_intervals.parquet'))+' (FORMAT PARQUET,COMPRESSION ZSTD)')
    for uuid,short,first,last in con.execute('SELECT uuid,station_id,first,last FROM chains WHERE last-first>=3600').fetchall():
        t=((first+3599)//3600)*3600
        while t+3600<=last:hours[short].add(datetime.fromtimestamp(t,UTC));t+=3600
    poll_quality=con.execute('''SELECT count(*),count(*) FILTER(WHERE good),
        count(*) FILTER(WHERE good AND num_bikes_available=0) FROM polls''').fetchone()
    write_json(root/'operation_audit.json',{'acquisition_complete':source_manifest['status']=='downloaded',
        'narrow_days':status_audit,'unique_poll_rows':poll_quality[0],
        'fresh_renting_rows':poll_quality[1],'fresh_renting_empty_inventory_rows':poll_quality[2],
        'bounded_supported_hours':{k:len(v) for k,v in hours.items()},'max_gap_seconds':120,
        'assumption':'continuous operation only within bounded consecutive fresh installed/renting polls',
        'canonical_eligibility_promoted':False})
    print('Supported bracket hours',sum(map(len,hours.values())),flush=True)
    if status_only:con.close();return root
    observed=BASE/'builds'/(BASE/'CURRENT_OBSERVED').read_text().strip()
    shutil.copyfile(sources/'manifest.json',root/'public_source_manifest.json')
    shutil.copyfile(observed/'manifest.json',root/'trip_input_manifest.json')
    om=json.loads((observed/'manifest.json').read_text());counts=defaultdict(int);cohorts=defaultdict(set)
    trip_hashes=[];geographic_audit=[];geo_rejected=set()
    for item in om['artifacts']:
        month=Path(item['path']).stem
        if item['table']!='station_geography_observed' or month not in ['2026-04','2026-05','2026-06','2026-09']:continue
        p=observed/item['path']
        if digest(p)!=item['sha256']:raise ValueError('Trip geography audit checksum mismatch')
        for row in pq.read_table(p).to_pylist():
            short=row['station_id']
            if short not in short_to_uuid or row['id_scheme']!='short_name':continue
            baseline=stations[short_to_uuid[short]]
            distance=distance_m(row['latitude'],row['longitude'],baseline['latitude'],baseline['longitude'])
            geographic_audit.append({'station_id':short,'month':month,'mean_endpoint_distance_m':distance,
                'endpoint_observations':row['coordinate_observations'],'reference':'dated station information; endpoint mean is a diagnostic, not geography proof'})
            if distance>100:geo_rejected.add((short,month))
    write_json(root/'geography_audit.json',{'monthly_diagnostics':geographic_audit,
        'rejected_station_months':[list(x) for x in sorted(geo_rejected)]})
    for item in om['artifacts']:
        if item['table']!='hourly_release' or Path(item['path']).stem not in ['2026-04','2026-05','2026-06','2026-09']:continue
        p=observed/item['path']
        if digest(p)!=item['sha256']:raise ValueError('Trip label checksum mismatch')
        trip_hashes.append(item)
        for short,hour,release,n in con.execute('''SELECT r.station_id,r.hour_utc,r.nominal_release,r.departures
            FROM read_parquet(?) r JOIN cohort c ON r.station_id=c.station_id
            WHERE id_scheme='short_name' ''',[str(p)]).fetchall():
            counts[(short,hour)]+=n;cohorts[(short,hour)].add(release)
    schedules=[DatedGTFS(sources/r['path']) for r in source_manifest['references'] if Path(r['path']).name.startswith('mta_gtfs_')]
    origins={}
    for first,last in [protocol['refit_target_local'],protocol['test_target_local']]:
        for origin in starts(first,last):origins[origin]=None
    byday=defaultdict(list)
    for origin in origins:byday[(origin-timedelta(minutes=5)).date()].append(origin)
    alert_items={r['date']:(r,p) for r,p in bykind['subway_alerts']}
    alert_root=root/'advisories';alert_root.mkdir(exist_ok=True)
    alert_by_key={};alert_audit=[];mapping_cache={};mapping_rows={};schema_audit=[]
    for day,day_origins in sorted(byday.items()):
        checkpoint=alert_root/(str(day)+'.json')
        if checkpoint.exists():
            data=json.loads(checkpoint.read_text())
            if data['source_sha256']!=alert_items[str(day)][0]['sha256']:raise ValueError('Advisory checkpoint source changed')
            for row in data.get('mapping',[]):
                mapping_rows[(row['information_object_date'],row['uuid'],row['gtfs_archive'],row['subway_parent_id'])]=row
            for row in data['features']:
                for key in ('origin_utc','advisory_observed_at_utc','information_assumed_available_at_utc','gtfs_archive_available_at_utc'):
                    row[key]=datetime.fromisoformat(row[key])
                alert_by_key[(row['station_id'],row['origin_utc'])]=row
            alert_audit+=data['audit'];schema_audit.append(data['schema']);continue
        if str(day) not in alert_items:continue
        item,path=alert_items[str(day)];day_mapping={}
        if digest(path)!=item['sha256']:raise ValueError('Advisory checksum mismatch')
        snapshots=con.execute('SELECT DISTINCT source_file,fetch_timestamp FROM read_parquet(?) ORDER BY fetch_timestamp',[str(path)]).fetchall()
        times=[r[1] for r in snapshots];selected={};audits=[];features=[]
        for origin in sorted(day_origins):
            cutoff=origin-timedelta(minutes=5);ix=bisect_left(times,cutoff)-1
            if ix<0:audits.append({'origin_utc':origin,'reason':'missing_snapshot'});continue
            selected[origin]=snapshots[ix]
        chosen=sorted({r[1] for r in selected.values()})
        if not chosen:continue
        columns=['source_file','fetch_timestamp','feed_timestamp','incrementality','is_deleted','entity_id',
            'agency_id','route_id','route_type','stop_id','direction_id','active_periods_json',*TRIP_FIELDS]
        present=set(pq.read_schema(path).names)
        required={'active_periods_json','direction_id','incrementality','is_deleted',*TRIP_FIELDS}
        missing=sorted(required-present)
        schema={'date':str(day),'missing_columns':missing,'semantically_complete':not missing,
            'source_sha256':item['sha256']}
        schema_audit.append(schema)
        selections=','.join('"'+c+'"' if c in present else 'NULL AS "'+c+'"' for c in columns)
        placeholders=','.join('?' for _ in chosen)
        rows=[] if missing else con.execute('SELECT '+selections+' FROM read_parquet(?) WHERE fetch_timestamp IN ('+placeholders+')',[str(path),*chosen]).fetch_arrow_table().to_pylist()
        groups=defaultdict(list)
        for r in rows:groups[(r['source_file'],r['fetch_timestamp'])].append(r)
        for origin,snapshot in selected.items():
            cutoff=origin-timedelta(minutes=5)
            available=[s for s in schedules if s.available<cutoff and s.reference(origin.astimezone(ZONE).date())['route_stops']]
            if not available:audits.append({'origin_utc':origin,'reason':'no_dated_schedule'});continue
            schedule=max(available,key=lambda s:s.available)
            metadata={uuid:meta(uuid,cutoff) for uuid in stations}
            nearby={};nearby250={}
            for uuid,m in metadata.items():
                if not m:continue
                cache_key=(m['object_date'],uuid,schedule.path.name)
                if cache_key not in mapping_cache:
                    distances={stop:distance_m(m['latitude'],m['longitude'],*point) for stop,point in schedule.points.items()}
                    mapping_cache[cache_key]=({s for s,d in distances.items() if d<=500},{s for s,d in distances.items() if d<=250})
                    for stop,d in distances.items():
                        if d<=500:
                            mapping_rows[(*cache_key,stop)]={'information_object_date':m['object_date'],
                                'uuid':uuid,'station_id':m['station_id'],'gtfs_archive':schedule.path.name,
                                'subway_parent_id':stop,'distance_m':d,'within_250m':d<=250,
                                'information_assumed_available_at_utc':m['assumed_available_at_utc'],
                                'gtfs_archive_available_at_utc':schedule.available}
                for stop in mapping_cache[cache_key][0]:
                    key=(*cache_key,stop);day_mapping[key]=mapping_rows[key]
                nearby[m['station_id']],nearby250[m['station_id']]=mapping_cache[cache_key]
            reference=schedule.reference(origin.astimezone(ZONE).date())
            if missing:
                audits.append({'origin_utc':origin,'reason':'incomplete_legacy_alert_schema'});continue
            result,audit=advisory_snapshot(groups[snapshot],cutoff,reference,nearby,origin)
            audits.append({'origin_utc':origin,**audit})
            if result is None:continue
            sensitivity,_=advisory_snapshot(groups[snapshot],cutoff,reference,nearby250,origin)
            for short,values in result.items():
                m=metadata[short_to_uuid[short]]
                row={'station_id':short,'origin_utc':origin,**values,
                    'information_assumed_available_at_utc':m['assumed_available_at_utc'],
                    'gtfs_archive_available_at_utc':schedule.available,
                    'subway_parent_count_500m':len(nearby[short]),
                    'subway_parent_count_250m':len(nearby250[short]),
                    'advisory_active_250m':sensitivity[short]['advisory_active']}
                features.append(row);alert_by_key[(short,origin)]=row
        write_json(checkpoint,{'source_sha256':item['sha256'],'audit':audits,'features':features,
            'mapping':list(day_mapping.values()),'schema':schema})
        alert_audit+=audits
        if len(alert_audit)%240<24:print('Advisory origins audited',len(alert_audit),flush=True)
    weather_path=BASE/'weather_reference_builds/20261016T173331Z-84df9caa/weather_runs.parquet'
    weather=defaultdict(dict)
    for row in pq.read_table(weather_path).to_pylist():weather[row['initialization_utc']][row['valid_time_utc']]=row
    rows=[];exclusions=Counter();supported=[]
    for origin in sorted(origins):
        cutoff=origin-timedelta(minutes=5);init=weather_initialization(cutoff)
        if not init+timedelta(hours=7)<cutoff:raise ValueError('Weather availability cutoff')
        for short in short_to_uuid:
            alert=alert_by_key.get((short,origin));m=meta(short_to_uuid[short],cutoff)
            for horizon in range(3):
                target=origin+horizon*HOUR;key=(short,target)
                if target not in hours.get(short,set()):exclusions['unsupported_operation']+=1;continue
                if (short,target.astimezone(ZONE).strftime('%Y-%m')) in geo_rejected:
                    exclusions['trip_geography_mismatch']+=1;continue
                if not m:exclusions['missing_stable_asof_metadata']+=1;continue
                available=label_available(target,cohorts.get(key,[]))
                values,valid=weather_inputs(weather.get(init,{}),target)
                if not valid:exclusions['missing_weather']+=1;continue
                if not alert:exclusions['missing_or_unresolved_advisory_retained_for_weather_only']+=1
                local=target.astimezone(ZONE)
                rows.append({'station_id':short,'id_scheme':'short_name','origin_utc':origin,
                    'issue_cutoff_utc':cutoff,'target_start_utc':target,'target_end_utc':target+HOUR,
                    'horizon':horizon,'departures':counts.get(key,0),'label_available_at_utc':available,
                    'weather_assumed_available_at_utc':init+timedelta(hours=7),
                    'station_hour':short+'|'+str(local.hour),'dow_hour':str(local.weekday())+'|'+str(local.hour),
                    'temperature':values[0],'temperature_squared':values[0]**2,
                    'precipitation':values[1],'wind':values[2],'wet':int(values[1]>0),
                    'information_assumed_available_at_utc':m['assumed_available_at_utc'],
                    'advisory_covered':alert is not None,
                    **(alert or {k:None for k in ('advisory_active','advisory_next3h','advisory_unspecified',
                        'advisory_observed_at_utc','advisory_feed_age_seconds','gtfs_archive_available_at_utc',
                        'subway_parent_count_500m','subway_parent_count_250m','advisory_active_250m')})})
                supported.append({'station_id':short,'hour_utc':target,'departures':counts.get(key,0),
                    'label_available_at_utc':available,'bounded_operation_support':True})
    if not rows:raise ValueError('No common-key station examples; inspect audits')
    arrow(root/'features.parquet',rows)
    # Checkpoint resumption still preserves a complete mapping (derive directly
    # from all metadata/version combinations used, independent of alert coverage).
    if mapping_rows:
        for row in mapping_rows.values():
            for key in ('information_assumed_available_at_utc','gtfs_archive_available_at_utc'):
                if isinstance(row[key],str):row[key]=datetime.fromisoformat(row[key])
        arrow(root/'station_subway_mapping.parquet',list(mapping_rows.values()))
    unique_labels=list({(r['station_id'],r['hour_utc']):r for r in supported}.values())
    arrow(root/'supported_labels.parquet',unique_labels)
    write_json(root/'advisory_audit.json',{'origins':len(alert_audit),'status_counts':dict(Counter(r['reason'] for r in alert_audit)),
        'origins_detail':alert_audit,'feature_rows':len(alert_by_key),'daily_schemas':schema_audit})
    report={'schema_version':'station-pilot-package-v1','status':'built','sources_root':str(sources),
        'sources_manifest_sha256':digest(sources/'manifest.json'),'protocol_sha256':digest(protocol_path),
        'observed_root':str(observed),'observed_manifest_sha256':digest(observed/'manifest.json'),
        'weather_path':str(weather_path),'weather_sha256':digest(weather_path),
        'cohort_stations':len(cohort),'rows':len(rows),'zero_labels':sum(r['departures']==0 for r in rows),
        'unique_supported_label_hours':len(unique_labels),
        'unique_supported_zero_hours':sum(r['departures']==0 for r in unique_labels),
        'exclusions':dict(exclusions),'advisory_status':dict(Counter(r['reason'] for r in alert_audit)),
        'trip_artifacts':trip_hashes,'metadata_artifacts':metadata_hashes,
        'canonical_station_eligibility_promoted':False,'active_model_changed':False,'artifacts':[]}
    report['build_code_sha256']={str(p):digest(p) for p in [Path(__file__),
        Path('src/citibike_pipeline/station_history.py'),Path('src/citibike_pipeline/borough_mvp.py')]}
    report['label_predictor_contract']='supported_labels.parquet holds station-hour outcomes; features.parquet is a training join. The runtime uses an explicit predictor allowlist excluding departure counts, label availability, operation masks and future trip properties.'
    # Portable runtime is standalone and runs identically locally and in Free Edition.
    shutil.copyfile(Path('src/citibike_pipeline/station_pilot_runtime.py'),root/'station_pilot_runtime.py')
    for name in ['features.parquet','supported_labels.parquet','cohort.json','protocol.json',
                 'operation_audit.json','advisory_audit.json','geography_audit.json','build_inputs.json',
                 'operation_intervals.parquet',
                 'public_source_manifest.json','trip_input_manifest.json',
                 'station_pilot_runtime.py','station_subway_mapping.parquet']:
        p=root/name
        if p.exists():report['artifacts'].append({'path':name,'bytes':p.stat().st_size,'sha256':digest(p)})
    write_json(root/'manifest.json',report);con.close()
    print(json.dumps({k:v for k,v in report.items() if k not in ('artifacts','metadata_artifacts','trip_artifacts')},indent=2));return root


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',type=Path,required=True)
    p.add_argument('--root',type=Path,required=True);p.add_argument('--protocol',type=Path,default=Path('docs/station_pilot_protocol.json'))
    p.add_argument('--status-only',action='store_true',help='checkpoint immutable status partitions already acquired; no feature package or eligibility promotion')
    args=p.parse_args();prepare(args.sources,args.root,args.protocol,args.status_only)
