"""Build a weather-only contribution without replacing frozen model features."""
from __future__ import annotations
import argparse
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from citibike_pipeline.forecast_sources import digest,stamp


def normalize(source, destination):
    manifest=json.loads((source/'manifest.json').read_text())
    rows=[];keys=set();initializations=set()
    for artifact in manifest['artifacts']:
        path=Path(artifact['path'])
        if digest(path)!=artifact['sha256']:raise ValueError('Weather checksum mismatch')
        data=json.loads(path.read_text());hourly=data['hourly'];units=data['hourly_units']
        if any(units[k]!=unit for k,unit in [('temperature_2m','°C'),('precipitation','mm'),('wind_speed_10m','m/s')]):
            raise ValueError('Unexpected weather units')
        init=datetime.fromisoformat(artifact['initialization_utc'])
        if init in initializations:raise ValueError('Duplicate initialization')
        initializations.add(init)
        for index,value in enumerate(hourly['time']):
            valid=datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
            key=(init,valid)
            if key in keys:raise ValueError('Duplicate weather key')
            keys.add(key)
            rows.append({'initialization_utc':init,'valid_time_utc':valid,
                'assumed_available_at_utc':init+timedelta(hours=7),'availability_verified':False,
                'cell_latitude':data['latitude'],'cell_longitude':data['longitude'],
                'precipitation_interval_start_utc':valid-timedelta(hours=1),
                'precipitation_interval_end_utc':valid,
                **{k:hourly[k][index] for k in ('temperature_2m','precipitation','wind_speed_10m')}})
    if not rows:raise ValueError('Empty weather dataset')
    root=destination/stamp();root.mkdir(parents=True,exist_ok=False)
    path=root/'weather_runs.parquet'
    pq.write_table(pa.Table.from_pylist(rows),path,compression='zstd')
    report={'schema_version':'weather-reference-normalized-v1','status':'built',
        'weather_manifest_sha256':digest(source/'manifest.json'),'weather_runs':len(initializations),
        'expected_runs':manifest['expected_runs'],'weather_failures':manifest['failures'],
        'first_initialization_utc':min(initializations).isoformat(),
        'last_initialization_utc':max(initializations).isoformat(),
        'rows':len(rows),'historical_availability_verified':False,'assumed_delay_hours':7,
        'model_or_feature_pointer_changed':False,
        'attribution':'Weather data by Open-Meteo.com; ECMWF IFS; CC BY 4.0',
        'artifacts':[{'path':path.name,'sha256':digest(path),'rows':len(rows),'bytes':path.stat().st_size}]}
    (root/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'root':str(root),'rows':len(rows),'runs':len(initializations)},indent=2))
    return root


if __name__=='__main__':
    base=Path('/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour')
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,default=base/'source_candidates/weather')
    parser.add_argument('--destination',type=Path,default=base/'weather_reference_builds')
    args=parser.parse_args();normalize(args.source,args.destination)
