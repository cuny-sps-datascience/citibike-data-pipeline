"""Sequential RAID build of as-of inventory predictors and separate future labels."""
from __future__ import annotations
import argparse,json,shutil
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from citibike_pipeline.db import sql_literal
from citibike_pipeline.forecast_sources import digest
from citibike_pipeline.station_history import information,stable

UTC=timezone.utc


def save(path,value):
    tmp=path.with_suffix('.json.partial');tmp.write_text(json.dumps(value,indent=2,default=str)+'\n');tmp.replace(path)


def build(sources,cohort_root,root):
    root.mkdir(parents=True,exist_ok=True)
    protocol=Path('docs/availability_protocol.json')
    lock={'sources':str(sources),'source_manifest_sha256':digest(sources/'manifest.json'),
        'cohort_sha256':digest(cohort_root/'cohort.json'),'protocol_sha256':digest(protocol),
        'builder_sha256':digest(Path(__file__))}
    if (root/'build_inputs.json').exists() and json.loads((root/'build_inputs.json').read_text())!=lock:
        raise ValueError('Cannot resume with different code or pinned inputs')
    save(root/'build_inputs.json',lock)
    shutil.copyfile(protocol,root/'protocol.json');shutil.copyfile(cohort_root/'cohort.json',root/'cohort.json')
    manifest=json.loads((sources/'manifest.json').read_text())
    if manifest['status']!='downloaded':raise ValueError('Incomplete acquisition')
    cohort=json.loads((root/'cohort.json').read_text());frozen={s['uuid']:s for s in cohort}
    con=duckdb.connect();con.execute("SET TimeZone='UTC';SET memory_limit='2GB';SET threads=2")
    (root/'duckdb_tmp').mkdir(exist_ok=True);con.execute('SET temp_directory='+sql_literal(str(root/'duckdb_tmp')))
    con.execute('CREATE TABLE cohort(uuid VARCHAR,station_id VARCHAR,station_index INTEGER)')
    con.executemany('INSERT INTO cohort VALUES (?,?,?)',[(s['uuid'],s['station_id'],i) for i,s in enumerate(cohort)])
    # Keep only cohort metadata in memory, while validating each full source JSON.
    infos=[r for r in manifest['objects'] if r['source']=='station_information']
    first=next(r for r in manifest['references'] if '2026-04-06' in r['path'])
    infos=[first|{'date':'2026-04-06'}]+sorted(infos,key=lambda r:r['date'])
    metadata=[];previous={}
    for item in infos:
        path=sources/item['path']
        if digest(path)!=item['sha256']:raise ValueError('Information source checksum mismatch')
        day=date.fromisoformat(item['date']);allrows=information(path,day);current={k:allrows[k] for k in frozen if k in allrows}
        for uuid,initial in frozen.items():
            m=current.get(uuid);valid=stable(previous.get(uuid),m) and stable(initial,m)
            available=m['assumed_available_at_utc'] if m else datetime.combine(day+timedelta(days=1),datetime.min.time(),UTC)+timedelta(hours=1)
            metadata.append({'uuid':uuid,'information_available_at_utc':available,'stable_identity':valid,
                'capacity':m['capacity'] if m else None})
        previous=current
    con.register('metadata_arrow',pa.Table.from_pylist(metadata))
    con.execute('''CREATE TABLE metadata AS SELECT *,least(information_available_at_utc+INTERVAL '49 hours',
        coalesce(lead(information_available_at_utc) OVER (PARTITION BY uuid ORDER BY information_available_at_utc),
        information_available_at_utc+INTERVAL '49 hours')) AS valid_until_utc FROM metadata_arrow''')
    narrow=root/'narrow_status';narrow.mkdir(exist_ok=True);receipts=[]
    columns=['polled_at','ts','last_reported','num_bikes_available','num_docks_available','num_ebikes_available',
        'num_bikes_disabled','num_docks_disabled','is_installed','is_renting','is_returning']
    for i,item in enumerate(sorted([r for r in manifest['objects'] if r['source']=='station_status'],key=lambda r:r['date'])):
        path=sources/item['path'];target=narrow/(item['date']+'.parquet');receipt=target.with_suffix('.json')
        if receipt.exists():
            r=json.loads(receipt.read_text())
            if r['source_sha256']!=item['sha256'] or digest(target)!=r['sha256']:raise ValueError('Narrow checkpoint mismatch')
        else:
            if digest(path)!=item['sha256']:raise ValueError('Status checksum mismatch')
            selected=','.join('s.'+c for c in columns)
            con.execute('COPY (SELECT c.*, '+selected+' FROM read_parquet('+sql_literal(str(path))+''') s
                JOIN cohort c ON s.station_id=c.uuid) TO '''+sql_literal(str(target))+' (FORMAT PARQUET,COMPRESSION ZSTD)')
            r={'path':str(target.relative_to(root)),'sha256':digest(target),'source_sha256':item['sha256'],
                'rows':pq.read_metadata(target).num_rows,'date':item['date']};save(receipt,r)
        receipts.append(r)
        if (i+1)%20==0:print('Availability status days',i+1,'/ 178',flush=True)
    files=','.join(sql_literal(str(root/r['path'])) for r in receipts)
    con.execute('CREATE VIEW polls_raw AS SELECT * FROM read_parquet(['+files+'])')
    payload=','.join(columns[1:])
    conflicts=con.execute('SELECT count(*) FROM (SELECT uuid,polled_at,count(DISTINCT ('+payload+')) n FROM polls_raw GROUP BY ALL HAVING n>1)').fetchone()[0]
    if conflicts:raise ValueError('Conflicting station/poll values')
    con.execute('''CREATE TABLE polls AS SELECT DISTINCT *,to_timestamp(polled_at) AS observed_at_utc,
        polled_at-ts BETWEEN -300 AND 120 AND polled_at-last_reported BETWEEN -300 AND 600
        AND least(num_bikes_available,num_docks_available,num_ebikes_available,num_bikes_disabled,num_docks_disabled)>=0
        AND is_installed=1 AND is_renting=1 AND is_returning=1 AS endpoint_eligible FROM polls_raw''')
    # All polls enter ASOF selection before quality filtering. Never reach back
    # past a recent stale/closed observation to an older apparently good one.
    con.execute('''CREATE TABLE origin_grid AS SELECT c.*,g.origin_utc
        FROM cohort c CROSS JOIN generate_series(TIMESTAMPTZ '2026-04-08 04:00:00+00',
        TIMESTAMPTZ '2026-10-01 03:00:00+00',INTERVAL '1 hour') g(origin_utc)''')
    con.execute('''CREATE TABLE origin_current AS SELECT g.*,p.observed_at_utc AS input_observed_at_utc,
        p.polled_at,p.ts,p.last_reported,p.num_bikes_available AS current_bikes,
        p.num_docks_available AS current_docks,p.num_ebikes_available AS current_ebikes,
        p.num_bikes_disabled AS disabled_bikes,p.num_docks_disabled AS disabled_docks,
        p.endpoint_eligible AS input_eligible
        FROM origin_grid g ASOF LEFT JOIN polls p ON g.uuid=p.uuid AND g.origin_utc>p.observed_at_utc''')
    con.execute('''CREATE TABLE origin_info AS SELECT o.*,m.capacity,m.information_available_at_utc,
        m.valid_until_utc,m.stable_identity FROM origin_current o ASOF LEFT JOIN metadata m
        ON o.uuid=m.uuid AND o.origin_utc>m.information_available_at_utc''')
    for lag in (15,30,60):
        # Invalid/missing lags stay NULL predictors, rather than synthesized zero
        # inventory or invented customer arrivals/departures.
        con.execute(f'''CREATE TABLE lag_{lag} AS SELECT o.uuid,o.origin_utc,
            CASE WHEN p.endpoint_eligible AND epoch(o.origin_utc-INTERVAL '{lag} minutes'-p.observed_at_utc) BETWEEN 0 AND 120
            THEN o.current_bikes-p.num_bikes_available END AS bike_change_{lag},
            CASE WHEN p.endpoint_eligible AND epoch(o.origin_utc-INTERVAL '{lag} minutes'-p.observed_at_utc) BETWEEN 0 AND 120
            THEN o.current_docks-p.num_docks_available END AS dock_change_{lag},
            CASE WHEN p.endpoint_eligible AND epoch(o.origin_utc-INTERVAL '{lag} minutes'-p.observed_at_utc) BETWEEN 0 AND 120
            THEN p.observed_at_utc END AS lag_{lag}_observed_at_utc
            FROM origin_info o ASOF LEFT JOIN polls p ON o.uuid=p.uuid
            AND o.origin_utc-INTERVAL '{lag} minutes'>p.observed_at_utc''')
    con.execute('''CREATE TABLE examples AS SELECT o.*,
        horizon_minutes,origin_utc+horizon_minutes*INTERVAL '1 minute' AS target_utc
        FROM origin_info o CROSS JOIN (VALUES (15),(30),(60),(180)) h(horizon_minutes)''')
    con.execute('''CREATE TABLE matched AS SELECT e.*,p.observed_at_utc AS label_observed_at_utc,
        p.endpoint_eligible AS label_eligible,p.num_bikes_available AS bikes,p.num_docks_available AS docks
        FROM examples e ASOF LEFT JOIN polls p ON e.uuid=p.uuid AND e.target_utc<=p.observed_at_utc''')
    audit=con.execute('''SELECT count(*) AS potential_rows,
        count(*) FILTER(WHERE NOT coalesce(input_eligible,false)) AS invalid_input_status,
        count(*) FILTER(WHERE epoch(origin_utc-input_observed_at_utc) NOT BETWEEN 0 AND 120 OR input_observed_at_utc IS NULL) AS stale_or_missing_input,
        count(*) FILTER(WHERE NOT coalesce(stable_identity,false) OR origin_utc>=valid_until_utc) AS missing_stable_metadata,
        count(*) FILTER(WHERE NOT coalesce(label_eligible,false)) AS invalid_or_closed_target,
        count(*) FILTER(WHERE epoch(label_observed_at_utc-target_utc) NOT BETWEEN 0 AND 120 OR label_observed_at_utc IS NULL) AS missing_target_endpoint
        FROM matched''').fetch_arrow_table().to_pylist()[0]
    con.execute('''CREATE TABLE accepted AS SELECT row_number() OVER(ORDER BY station_id,origin_utc,horizon_minutes)::BIGINT AS example_id,
        m.*,l15.bike_change_15,l15.dock_change_15,l15.lag_15_observed_at_utc,
        l30.bike_change_30,l30.dock_change_30,l30.lag_30_observed_at_utc,
        l60.bike_change_60,l60.dock_change_60,l60.lag_60_observed_at_utc,
        hour(timezone('America/New_York',m.origin_utc))::DOUBLE AS local_hour,
        (isodow(timezone('America/New_York',m.origin_utc))-1)::DOUBLE AS dow,
        sin(2*pi()*hour(timezone('America/New_York',m.origin_utc))/24) AS hour_sin,
        cos(2*pi()*hour(timezone('America/New_York',m.origin_utc))/24) AS hour_cos
        FROM matched m LEFT JOIN lag_15 l15 USING(uuid,origin_utc)
        LEFT JOIN lag_30 l30 USING(uuid,origin_utc) LEFT JOIN lag_60 l60 USING(uuid,origin_utc)
        WHERE input_eligible AND label_eligible AND stable_identity AND m.origin_utc<valid_until_utc
            AND epoch(m.origin_utc-input_observed_at_utc) BETWEEN 0 AND 120
            AND epoch(label_observed_at_utc-target_utc) BETWEEN 0 AND 120 AND capacity>0''')
    feature_columns=['example_id','station_id','station_index','origin_utc','horizon_minutes','input_observed_at_utc',
        'information_available_at_utc','current_bikes','current_docks','current_ebikes','disabled_bikes','disabled_docks',
        'capacity','local_hour','dow','hour_sin','hour_cos','bike_change_15','dock_change_15',
        'bike_change_30','dock_change_30','bike_change_60','dock_change_60',
        'lag_15_observed_at_utc','lag_30_observed_at_utc','lag_60_observed_at_utc']
    artifacts=[]
    for name,query in [('features.parquet','SELECT '+','.join(feature_columns)+' FROM accepted ORDER BY example_id'),
        ('labels.parquet','SELECT example_id,target_utc,label_observed_at_utc,bikes,docks FROM accepted ORDER BY example_id')]:
        path=root/name;con.execute('COPY ('+query+') TO '+sql_literal(str(path))+' (FORMAT PARQUET,COMPRESSION ZSTD)')
        artifacts.append({'path':name,'bytes':path.stat().st_size,'sha256':digest(path),'rows':pq.read_metadata(path).num_rows})
    save(root/'source_audit.json',{'source_root':str(sources),'status_partitions':receipts,
        'input_exclusions_overlapping':audit,'accepted_rows':artifacts[0]['rows'],
        'direct_snapshot_labels':True,'continuous_operation_claim':False})
    shutil.copyfile('src/citibike_pipeline/availability_runtime.py',root/'availability_runtime.py')
    for name in ('protocol.json','cohort.json','build_inputs.json','source_audit.json','availability_runtime.py'):
        path=root/name;artifacts.append({'path':name,'bytes':path.stat().st_size,'sha256':digest(path)})
    result={'schema_version':'availability-package-v1','status':'built','sources_root':str(sources),
        'cohort_stations':32,'rows':artifacts[0]['rows'],'inputs':lock,'artifacts':artifacts,
        'original_models_changed':False,'canonical_eligibility_promoted':False}
    save(root/'manifest.json',result);con.close();print(json.dumps(result,indent=2),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',required=True,type=Path)
    p.add_argument('--cohort-root',required=True,type=Path);p.add_argument('--root',required=True,type=Path)
    args=p.parse_args();build(args.sources,args.cohort_root,args.root)
