"""Public-only Manhattan aggregate benchmark: released trip profiles and run weather.

The geographic aggregation is a reduced-scope benchmark, not a station model.
Complete published trip input coverage supports borough-hour totals without
inferring operation for any station. Geography is applied to each actual start.
"""
from __future__ import annotations

import calendar
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry import shape

from .forecast_sources import digest, stamp
from .hourly_export import START_DST
from .model_runtime import FEATURES, numerical_features, weather_inputs


def release_at(month: str) -> datetime:
    year, number=map(int,month.split('-'))
    last=date(year,number,calendar.monthrange(year,number)[1])
    return datetime.combine(last+timedelta(days=45),datetime.min.time(),timezone.utc)


def weather_initialization(issue: datetime, delay=7) -> datetime:
    eligible=issue.astimezone(timezone.utc)-timedelta(hours=delay)
    return eligible.replace(hour=(eligible.hour//6)*6,minute=0,second=0,microsecond=0)


def label_assignable(hour: datetime) -> bool:
    """Naive published trip times cannot distinguish either repeated fall hour."""
    from zoneinfo import ZoneInfo
    zone=ZoneInfo('America/New_York')
    local=hour.astimezone(zone).replace(tzinfo=None)
    return all(local!=(hour+timedelta(hours=delta)).astimezone(zone).replace(tzinfo=None)
               for delta in (-1,1))


def quality_revision(label_root: Path, output_root: Path):
    """Upgrade the initial assigned-count export without rescanning original trips."""
    import shutil
    original=json.loads((label_root/'manifest.json').read_text())
    for item in original['artifacts']:
        if digest(label_root/item['path'])!=item['sha256']:raise ValueError('Label checksum mismatch')
    root=output_root/stamp();root.mkdir(parents=True)
    labels=pq.read_table(label_root/'borough_hourly.parquet').to_pylist()
    for row in labels:
        row['label_assignable']=label_assignable(row['hour_utc'])
        row['assigned_departures']=row['departures']
        if not row['label_assignable']:row['departures']=None
    pq.write_table(pa.Table.from_pylist(labels),root/'borough_hourly.parquet',compression='zstd')
    shutil.copyfile(label_root/'borough_hourly_release.parquet',root/'borough_hourly_release.parquet')
    report=original|{'schema_version':'manhattan-aggregate-labels-v2',
        'parent_manifest_sha256':digest(label_root/'manifest.json'),
        'unassignable_repeated_clock_hours':sum(not r['label_assignable'] for r in labels),
        'label_policy':'Repeated fall-back local hours have null labels, excluded from training and profile exposure; assigned counts retained',
        'artifacts':[]}
    for name in ('borough_hourly.parquet','borough_hourly_release.parquet'):
        path=root/name;report['artifacts'].append({'path':name,'sha256':digest(path),'rows':pq.read_metadata(path).num_rows,'bytes':path.stat().st_size})
    (root/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
    (output_root/'CURRENT_BOROUGH_LABELS').write_text(str(root)+'\n')
    return root


def split_origin(origin: datetime) -> str | None:
    """Boundary in New York; require all three target hours within one split."""
    from zoneinfo import ZoneInfo
    zone=ZoneInfo('America/New_York')
    for name,first,last in [('train','2025-03-01','2026-04-01'),
                            ('validation','2026-04-01','2026-06-01'),
                            ('test','2026-06-01','2026-09-01')]:
        start=datetime.fromisoformat(first).replace(tzinfo=zone).astimezone(timezone.utc)
        end=datetime.fromisoformat(last).replace(tzinfo=zone).astimezone(timezone.utc)
        if start<=origin and origin+timedelta(hours=3)<=end:
            return name
    return None


def build_borough_labels(observed_root: Path, source_root: Path, output_root: Path, out=print):
    manifest=json.loads((observed_root/'manifest.json').read_text())
    sources=json.loads((source_root/'manifest.json').read_text())
    borough_file=next(a for a in sources['artifacts'] if a['name']=='boroughs')
    borough_path=source_root/borough_file['path']
    if digest(borough_path)!=borough_file['sha256']:
        raise ValueError('Borough boundary checksum mismatch')
    feature=next(f for f in json.loads(borough_path.read_text())['features'] if f['properties']['boroname']=='Manhattan')
    geometry=shape(feature['geometry']);shapely.prepare(geometry)
    root=output_root/stamp();root.mkdir(parents=True)
    processed=Path('/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed')
    con=duckdb.connect();con.execute("SET TimeZone='UTC';SET memory_limit='3GB';SET threads=4")
    records=[];counts=Counter();audits=[]
    for i,item in enumerate(manifest['input_files']):
        path=(processed/item['path']).resolve()
        if not path.is_relative_to(processed.resolve()):
            raise ValueError('Input escapes processed root')
        batch_count=Counter();eligible=0;missing=0;dst=0
        # Paths are exclusively the ledger-selected inputs saved in the validated export.
        stats=con.execute('''SELECT count(*) FILTER(WHERE eligible_departure),
            count(*) FILTER(WHERE eligible_departure AND list_has_any(coalesce(quality_flags,[]::VARCHAR[]),?))
            FROM read_parquet(?)''',[START_DST,str(path)]).fetchone()
        reader=con.execute('''SELECT date_trunc('hour',timezone('America/New_York',started_at_local)) AS hour_utc,
            start_lat,start_lng FROM read_parquet(?)
            WHERE eligible_departure AND NOT list_has_any(coalesce(quality_flags,[]::VARCHAR[]),?)''',
            [str(path),START_DST]).fetch_record_batch(250000)
        for batch in reader:
            latitude=batch.column('start_lat').to_numpy(zero_copy_only=False)
            longitude=batch.column('start_lng').to_numpy(zero_copy_only=False)
            valid=np.isfinite(latitude)&np.isfinite(longitude)&(np.abs(latitude)<=90)&(np.abs(longitude)<=180)
            missing+=int((~valid).sum());eligible+=len(valid)
            mask=valid & shapely.intersects_xy(geometry,longitude,latitude)
            hours=batch.column('hour_utc').cast(pa.int64()).to_numpy(zero_copy_only=False)[mask]
            unique,totals=np.unique(hours,return_counts=True)
            batch_count.update({int(h):int(n) for h,n in zip(unique,totals)})
        for hour,n in batch_count.items():
            records.append({'hour_utc':datetime.fromtimestamp(hour/1e6,timezone.utc),
                'nominal_release':item['nominal_release'],'departures':n})
            counts[hour]+=n
        if eligible+stats[1]!=stats[0]:
            raise ValueError('Eligible/DST reconciliation failed')
        audits.append({'input':item['path'],'eligible_departures':stats[0],'excluded_dst':stats[1],
            'invalid_or_missing_coordinate':missing,'manhattan_departures':sum(batch_count.values())})
        if (i+1)%10==0 or i+1==len(manifest['input_files']):
            out(f'Borough labels {i+1}/{len(manifest["input_files"])}',flush=True)
    con.close()
    # Full published period, including every hour (zero is a geographic total,
    # never an assertion that any particular station was operating).
    from zoneinfo import ZoneInfo
    zone=ZoneInfo('America/New_York')
    first=datetime.fromisoformat(manifest['start_local']).replace(tzinfo=zone).astimezone(timezone.utc)
    last=datetime.fromisoformat(manifest['end_exclusive_local']).replace(tzinfo=zone).astimezone(timezone.utc)
    labels=[];hour=first
    while hour<last:
        assigned=counts.get(int(hour.timestamp()*1e6),0)
        assignable=label_assignable(hour)
        labels.append({'hour_utc':hour,'departures':assigned if assignable else None,
            'assigned_departures':assigned,'label_assignable':assignable,
            'borough':'Manhattan','coverage':'complete selected published trip inputs; eligible start coordinates',
            'station_operation_required':False})
        hour+=timedelta(hours=1)
    pq.write_table(pa.Table.from_pylist(labels),root/'borough_hourly.parquet',compression='zstd')
    pq.write_table(pa.Table.from_pylist(records),root/'borough_hourly_release.parquet',compression='zstd')
    report={'schema_version':'manhattan-aggregate-labels-v2','status':'built',
        'spatial_scope':'Manhattan total; individual trip start coordinates intersect borough boundary',
        'boundary_source':borough_file,'observed_manifest_sha256':digest(observed_root/'manifest.json'),
        'input_files':manifest['input_files'],'input_audit':audits,
        'start':first.isoformat(),'end_exclusive':last.isoformat(),'label_hours':len(labels),
        'departures':sum(counts.values()),'missing_coordinate_policy':'excluded and counted in audit',
        'station_prediction':False,'artifacts':[]}
    report['unassignable_repeated_clock_hours']=sum(not r['label_assignable'] for r in labels)
    report['label_policy']='Repeated fall-back local hours have null labels, excluded from training and profile exposure; assigned counts retained'
    for name in ('borough_hourly.parquet','borough_hourly_release.parquet'):
        path=root/name;report['artifacts'].append({'path':name,'sha256':digest(path),'rows':pq.read_metadata(path).num_rows,'bytes':path.stat().st_size})
    (root/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
    (output_root/'CURRENT_BOROUGH_LABELS').write_text(str(root)+'\n')
    return root


def build_borough_features(label_root: Path, weather_root: Path, output_root: Path):
    from zoneinfo import ZoneInfo
    zone=ZoneInfo('America/New_York')
    root=output_root/stamp();root.mkdir(parents=True)
    label_manifest=json.loads((label_root/'manifest.json').read_text())
    for item in label_manifest['artifacts']:
        if digest(label_root/item['path'])!=item['sha256']:raise ValueError('Label package checksum mismatch')
    labels=pq.read_table(label_root/'borough_hourly.parquet').to_pylist()
    if any('label_assignable' not in row for row in labels):raise ValueError('Upgrade labels to v2 before modeling')
    release_rows=pq.read_table(label_root/'borough_hourly_release.parquet').to_pylist()
    months=sorted({r['nominal_release'] for r in release_rows})
    release_dates={m:release_at(m) for m in months}
    totals=Counter();exposure=Counter()
    # Denominator is complete borough hours in already released target months.
    for month in months:
        for row in labels:
            local=row['hour_utc'].astimezone(zone)
            if row['label_assignable'] and local.strftime('%Y-%m')<=month:
                exposure[month,local.weekday(),local.hour]+=1
        for row in release_rows:
            local=row['hour_utc'].astimezone(zone)
            if label_assignable(row['hour_utc']) and row['nominal_release']<=month and local.strftime('%Y-%m')<=month:
                totals[month,local.weekday(),local.hour]+=row['departures']
    profiles=[]
    for month in months:
        for dow in range(7):
            for hour in range(24):
                n=exposure[month,dow,hour]
                if n:
                    profiles.append({'nominal_release':month,'available_at_utc':release_dates[month],
                        'dow':dow,'hour':hour,'covered_hours':n,'departures':totals[month,dow,hour],
                        'rate':totals[month,dow,hour]/n})
    profile_map={(r['nominal_release'],r['dow'],r['hour']):r for r in profiles}
    # Pooled calendar baseline: target hour of day, pooling day of week.
    pooled={}
    for month in months:
        for hour in range(24):
            n=sum(exposure[month,d,hour] for d in range(7))
            pooled[month,hour]=sum(totals[month,d,hour] for d in range(7))/max(n,1)
    weather_manifest=json.loads((weather_root/'manifest.json').read_text())
    weather={};weather_rows=[]
    for artifact in weather_manifest['artifacts']:
        path=Path(artifact['path'])
        if digest(path)!=artifact['sha256']:
            raise ValueError('Weather checksum mismatch')
        payload=json.loads(path.read_text());init=datetime.fromisoformat(artifact['initialization_utc'])
        hourly=payload['hourly'];weather[init]={}
        for i,value in enumerate(hourly['time']):
            valid=datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
            values={k:hourly[k][i] for k in ('temperature_2m','precipitation','wind_speed_10m')}
            weather[init][valid]=values
            weather_rows.append({'initialization_utc':init,'valid_time_utc':valid,
                'assumed_available_at_utc':init+timedelta(hours=7),'availability_verified':False,
                'cell_latitude':payload['latitude'],'cell_longitude':payload['longitude'],
                'precipitation_interval_start_utc':valid-timedelta(hours=1),
                'precipitation_interval_end_utc':valid,**values})
    outcomes={r['hour_utc']:r['departures'] for r in labels if r['label_assignable']}
    rows=[];excluded=Counter()
    for origin in sorted(outcomes):
        split=split_origin(origin)
        if not split:continue
        if any(origin+timedelta(hours=h) not in outcomes for h in (0,1,2)):
            excluded['origin_with_unassignable_target']+=1;continue
        issue=origin-timedelta(minutes=5)
        eligible=[m for m in months if release_dates[m]<issue]
        if not eligible:
            excluded['no_released_profile']+=3;continue
        month=max(eligible);init=weather_initialization(issue)
        for horizon in (1,2,3):
            target=origin+timedelta(hours=horizon-1);local=target.astimezone(zone)
            values,covered=weather_inputs(weather.get(init,{}),target)
            if not covered:excluded['weather_missing']+=1
            profile=profile_map[month,local.weekday(),local.hour]['rate'];pool=pooled[month,local.hour]
            numeric=numerical_features(profile,pool,local,horizon,*[v if v is not None else 0. for v in values])
            rows.append({'origin_utc':origin,'issue_cutoff_utc':issue,'target_start_utc':target,
                'target_end_utc':target+timedelta(hours=1),'target_local':local.replace(tzinfo=None),
                'horizon':horizon,'split':split,'departures':outcomes[target],
                'profile_release':month,'profile_available_at_utc':release_dates[month],
                'profile_baseline':profile,'pooled_baseline':pool,'weather_initialization_utc':init,
                'weather_assumed_available_at_utc':init+timedelta(hours=7),
                'weather_covered':covered,'weather_missing_policy':'calendar model fallback; zero placeholders excluded from weather training/evaluation',
                **dict(zip(FEATURES,numeric))})
    tables={'features':rows,'profiles':profiles,'weather_runs':weather_rows}
    artifacts=[]
    for name,values in tables.items():
        path=root/(name+'.parquet');pq.write_table(pa.Table.from_pylist(values),path,compression='zstd')
        artifacts.append({'path':path.name,'sha256':digest(path),'rows':len(values),'bytes':path.stat().st_size})
    report={'schema_version':'manhattan-three-hour-features-v1','status':'built','artifacts':artifacts,
        'label_root':str(label_root),'label_manifest_sha256':digest(label_root/'manifest.json'),
        'weather_manifest_sha256':digest(weather_root/'manifest.json'),'weather_runs':len(weather),
        'weather_failures':weather_manifest['failures'],'exclusions':dict(excluded),
        'spatial_scope':'Manhattan total; one common reference weather point, not station-specific cells',
        'features':FEATURES,'issue_minutes_before_origin':5,'weather_delay_hours':7,
        'release_rule':'nominal source month end +45 days at 00:00 UTC, assumed and unverified',
        'weather_early_archive_hindcasts':True,'alert_features_included':False,
        'repeated_clock_hour_policy':'Null labels; omit any three-target origin touching an unassignable hour; omit those hours from historical exposure',
        'study':'retrospective model development; validation outcomes were not released at first test origin',
        'fixed_training_design':{'models':['pooled hour profile','hour-of-week profile','Poisson calendar','Poisson weather'],
            'poisson_alphas':[.1,1.0],'max_iter':300,'selection':'minimum validation MAE, tie calendar/profile',
            'test':'once after validation selection; never select using test'},
        'attribution':['Citi Bike trip histories','NYC borough boundaries',
            'Weather data by Open-Meteo.com, ECMWF IFS, CC BY 4.0']}
    (root/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
    (output_root/'CURRENT_BOROUGH_FEATURES').write_text(str(root)+'\n')
    return root
