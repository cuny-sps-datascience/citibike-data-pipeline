# Databricks notebook source
# /// script
# dependencies = ["scikit-learn==1.7.2"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Issue three separate Manhattan hourly forecasts
# MAGIC Uses the frozen public-only benchmark. Stores first predictions immutably; publication completion determines whether a forecast was on time. Station forecasts and historical alert effects remain outside this benchmark.

# COMMAND ----------
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import joblib
import numpy as np
import pyarrow.parquet as pq

spark.conf.set('spark.sql.session.timeZone','UTC')
dbutils.widgets.text('collection_run_id','')
dbutils.widgets.text('trigger_ms','')
collection_id=dbutils.widgets.get('collection_run_id')
trigger=datetime.fromtimestamp(int(dbutils.widgets.get('trigger_ms'))/1000,timezone.utc)
origin=trigger.replace(minute=0,second=0,microsecond=0)+timedelta(hours=1)
source=Path('/Volumes/workspace/citibike_3h/inputs/live')/collection_id
metadata=json.loads(Path('/Volumes/workspace/citibike_3h/models/borough_active.json').read_text())
package=Path(metadata['package_root']);model_root=Path(metadata['model_root'])
assert hashlib.sha256((package/'manifest.json').read_bytes()).hexdigest()==metadata['package_manifest_sha256']
assert hashlib.sha256((package/'model_runtime.py').read_bytes()).hexdigest()==metadata['runtime_sha256']
assert hashlib.sha256((model_root/'model.joblib').read_bytes()).hexdigest()==metadata['model_sha256']
spec=importlib.util.spec_from_file_location('model_runtime',package/'model_runtime.py')
runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
bundle=joblib.load(model_root/'model.joblib')
profiles=pq.read_table(package/'profiles.parquet').to_pylist()
cutoff=datetime.now(timezone.utc)
hourly={};init=None;reason=None
try:
    collected=json.loads((source/'manifest.json').read_text())
    init=datetime.fromisoformat(collected['weather_initialization_utc'])
    expected=cutoff-timedelta(hours=7)
    expected=expected.replace(hour=expected.hour//6*6,minute=0,second=0,microsecond=0)
    assert init==expected and init+timedelta(hours=7)<cutoff,'stale/unavailable weather initialization'
    payload=json.loads(gzip.decompress((source/'weather.json.gz').read_bytes()))
    hourly={datetime.fromisoformat(t).replace(tzinfo=timezone.utc):
        {k:v[i] for k,v in payload['hourly'].items() if k!='time'}
        for i,t in enumerate(payload['hourly']['time'])}
except Exception as exc:
    reason='weather unavailable: '+str(exc)[:300]
rows=[]
for horizon in (1,2,3):
    target=origin+timedelta(hours=horizon-1);local=target.astimezone(ZoneInfo('America/New_York'))
    p,pooled=runtime.select_profile(profiles,cutoff,local)
    release=p['nominal_release']
    weather,covered=runtime.weather_inputs(hourly,target)
    valid=reason is None and covered
    model=bundle['selected_model'] if valid else bundle['fallback_model']
    features=runtime.numerical_features(p['rate'],pooled,local,horizon,*[float(v) if valid else 0. for v in weather])
    values=dict(zip(runtime.FEATURES,features))|{'profile_baseline':p['rate'],'pooled_baseline':pooled}
    prediction=float(runtime.predict(bundle,[values],model)[0])
    rows.append({'scope':'Manhattan','origin_utc':origin,'horizon':horizon,'target_start_utc':target,
        'target_end_utc':target+timedelta(hours=1),'prediction':prediction,'model_version':metadata['model_version'],
        'selected_model':model,'model_trained_at_utc':datetime.fromisoformat(metadata['trained_at_utc']),
        'profile_release':release,'profile_available_at_utc':p['available_at_utc'],
        'weather_initialization_utc':init,'weather_assumed_available_at_utc':init+timedelta(hours=7) if init else None,
        'weather_used':valid and model.startswith('weather_'),
        'fallback_reason':None if valid else (reason or 'null weather interval'),
        'input_cutoff_utc':cutoff,'collection_run_id':collection_id,
        'feature_package_sha256':metadata['package_manifest_sha256'],
        'source_snapshot_path':str(source),'score_run_id':uuid4().hex if horizon==1 else rows[0]['score_run_id']})
ready=datetime.now(timezone.utc)
for row in rows:
    row['prediction_ready_at_utc']=ready
    assert row['profile_available_at_utc']<row['input_cutoff_utc']<=ready
    assert row['model_trained_at_utc']<ready
schema='''scope STRING,origin_utc TIMESTAMP,horizon INT,target_start_utc TIMESTAMP,target_end_utc TIMESTAMP,
    prediction DOUBLE,model_version STRING,selected_model STRING,model_trained_at_utc TIMESTAMP,
    profile_release STRING,profile_available_at_utc TIMESTAMP,weather_initialization_utc TIMESTAMP,
    weather_assumed_available_at_utc TIMESTAMP,weather_used BOOLEAN,fallback_reason STRING,input_cutoff_utc TIMESTAMP,
    collection_run_id STRING,feature_package_sha256 STRING,source_snapshot_path STRING,score_run_id STRING,
    prediction_ready_at_utc TIMESTAMP'''
frame=spark.createDataFrame(rows,schema)
table='workspace.citibike_3h.borough_forecast_output'
if not spark.catalog.tableExists(table):
    frame.limit(0).write.format('delta').mode('error').saveAsTable(table)
frame.createOrReplaceTempView('borough_new_predictions')
spark.sql(f'''MERGE INTO {table} t USING borough_new_predictions s
    ON t.scope=s.scope AND t.origin_utc=s.origin_utc AND t.horizon=s.horizon AND t.model_version=s.model_version
    WHEN NOT MATCHED THEN INSERT *''')
completed=datetime.now(timezone.utc)
log=spark.createDataFrame([(rows[0]['score_run_id'],completed,'success')],
    'score_run_id STRING,publication_completed_at_utc TIMESTAMP,status STRING')
log.write.format('delta').mode('append').saveAsTable('workspace.citibike_3h.borough_publication_log')
spark.sql('''CREATE OR REPLACE VIEW workspace.citibike_3h.borough_forecasts AS
    SELECT f.*,p.publication_completed_at_utc,
        p.publication_completed_at_utc<f.target_start_utc AS published_before_target
    FROM workspace.citibike_3h.borough_forecast_output f LEFT JOIN (
        SELECT score_run_id,min(publication_completed_at_utc) AS publication_completed_at_utc
        FROM workspace.citibike_3h.borough_publication_log WHERE status='success' GROUP BY score_run_id) p USING(score_run_id)''')
forecast_view=spark.table('workspace.citibike_3h.borough_forecasts')
persisted=forecast_view.where((forecast_view.origin_utc==origin) &
    (forecast_view.model_version==metadata['model_version'])).orderBy('horizon').collect()
report={'scope':'Manhattan totals; not station forecasts','origin_utc':origin.isoformat(),
    'model_version':metadata['model_version'],
    'prediction_ready_at_utc':min(r['prediction_ready_at_utc'] for r in persisted).replace(tzinfo=timezone.utc).isoformat(),
    'publication_completed_at_utc':min(r['publication_completed_at_utc'] for r in persisted).replace(tzinfo=timezone.utc).isoformat(),
    'published_before_first_target':all(r['publication_completed_at_utc'].replace(tzinfo=timezone.utc)<origin for r in persisted),
    'attempt_publication_completed_at_utc':completed.isoformat(),
    'attempted_models':[r['selected_model'] for r in rows],
    'attempted_fallback_reasons':[r['fallback_reason'] for r in rows],
    'predictions':[{'horizon':r['horizon'],'prediction':r['prediction'],'model':r['selected_model'],
        'fallback_reason':r['fallback_reason'],'first_score_run_id':r['score_run_id'],
        'published_before_target':r['published_before_target']} for r in persisted]}
latest_path=Path('/Volumes/workspace/citibike_3h/exports/borough_forecast_latest.json')
previous=json.loads(latest_path.read_text()) if latest_path.exists() else None
if previous is None or datetime.fromisoformat(previous['origin_utc'])<=origin:
    latest_path.write_text(json.dumps(report,indent=2)+'\n')
display(spark.table('workspace.citibike_3h.borough_forecasts').orderBy('origin_utc',ascending=False).limit(9))
dbutils.notebook.exit(json.dumps(report))
