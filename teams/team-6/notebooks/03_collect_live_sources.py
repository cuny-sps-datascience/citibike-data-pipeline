# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Collect prospective Citi Bike inputs
# MAGIC Saves timestamped raw GBFS, MTA alerts, and forecast weather. This job collects evidence; it does not issue departure forecasts.

# COMMAND ----------
import gzip
import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

spark.conf.set('spark.sql.session.timeZone','UTC')
began=time.monotonic()
observed=datetime.now(timezone.utc)
run_id=observed.strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8]
root=Path('/Volumes/workspace/citibike_3h/inputs/live')/run_id
root.mkdir(parents=True,exist_ok=False)
dbutils.jobs.taskValues.set(key='collection_run_id',value=run_id)
records=[]

def collect(name,url,suffix='json'):
    try:
        with urlopen(Request(url,headers={'User-Agent':'CUNY-DATA622-research/0.1'}),timeout=20) as response:
            raw=response.read()
        retrieved=datetime.now(timezone.utc)
        path=root/(name+'.'+suffix+'.gz')
        path.write_bytes(gzip.compress(raw))
        records.append({'run_id':run_id,'source':name,'observed_at_utc':retrieved,
            'success':True,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
            'path':str(path),'error':None})
        return raw,retrieved
    except Exception as exc:
        records.append({'run_id':run_id,'source':name,'observed_at_utc':datetime.now(timezone.utc),
            'success':False,'bytes':0,'sha256':None,'path':None,'error':str(exc)[:1000]})
        return None,None

discovery,_=collect('gbfs_discovery','https://gbfs.citibikenyc.com/gbfs/gbfs.json')
statuses=[]
if discovery:
    for feed in json.loads(discovery)['data']['en']['feeds']:
        if feed['name'] not in ('station_information','station_status'):
            continue
        raw,retrieved=collect('gbfs_'+feed['name'],feed['url'])
        if raw and feed['name']=='station_status':
            payload=json.loads(raw)
            for station in payload['data']['stations']:
                statuses.append((run_id,str(station['station_id']),retrieved,
                    int(payload['last_updated']),bool(station['is_installed']),
                    bool(station['is_renting']),bool(station['is_returning']),
                    int(station['num_bikes_available']),int(station['num_docks_available'])))
collect('mta_alerts','https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fall-alerts','pb')
initialization=observed-timedelta(hours=7)
initialization=initialization.replace(hour=(initialization.hour//6)*6,minute=0,second=0,microsecond=0)
query={'latitude':40.75,'longitude':-73.98,'models':'ecmwf_ifs',
    'run':initialization.replace(tzinfo=None).isoformat(timespec='minutes'),
    'forecast_days':2,'timezone':'UTC','wind_speed_unit':'ms',
    'hourly':'temperature_2m,precipitation,rain,snowfall,wind_speed_10m'}
collect('weather','https://single-runs-api.open-meteo.com/v1/forecast?'+urlencode(query))

# COMMAND ----------
if statuses:
    frame=spark.createDataFrame(statuses,'''run_id STRING,station_id STRING,observed_at_utc TIMESTAMP,
        feed_last_updated LONG,is_installed BOOLEAN,is_renting BOOLEAN,is_returning BOOLEAN,
        bikes_available LONG,docks_available LONG''')
    frame.write.format('delta').mode('append').saveAsTable('workspace.citibike_3h.gbfs_status_observations')
logs=spark.createDataFrame(records,'''run_id STRING,source STRING,observed_at_utc TIMESTAMP,
    success BOOLEAN,bytes LONG,sha256 STRING,path STRING,error STRING''')
logs.write.format('delta').mode('append').saveAsTable('workspace.citibike_3h.collection_log')
report={'run_id':run_id,'seconds':round(time.monotonic()-began,2),'status_rows':len(statuses),
    'weather_initialization_utc':initialization.isoformat(),
    'sources':[{'source':r['source'],'success':r['success'],'bytes':r['bytes'],'error':r['error']} for r in records],
    'main_forecast_ready':False}
Path('/Volumes/workspace/citibike_3h/exports/live_collection_latest.json').write_text(json.dumps(report,indent=2)+'\n')
(root/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
display(logs.select('source','observed_at_utc','success','bytes','error'))
if any(not r['success'] for r in records):
    raise RuntimeError('Source collection incomplete; see collection_log and saved report')
dbutils.notebook.exit(json.dumps(report))
