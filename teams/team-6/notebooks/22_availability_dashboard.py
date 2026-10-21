# Databricks notebook source
# /// script
# dependencies = ["scikit-learn==1.7.2"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Station availability
# MAGIC Run all to refresh current station readings and forecasts. Select a station
# MAGIC and horizon using the widgets. Historical evaluation uses hourly issue times;
# MAGIC this manual refresh issues experimental forecasts at its actual timestamp.
# MAGIC Empty/no-dock probabilities refer to the selected future timestamp and assume
# MAGIC the station continues accepting rentals and returns.

# COMMAND ----------
from pathlib import Path
from datetime import datetime,timezone
from uuid import uuid4
from collections import Counter
import hashlib,html,importlib.util,json,math
import joblib
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import functions as F

dbutils.widgets.text('package_root','')
dbutils.widgets.text('model_root','')
dbutils.widgets.text('live_helper_sha256','')
dbutils.widgets.text('model_sha256','')
package=Path(dbutils.widgets.get('package_root'));model_root=Path(dbutils.widgets.get('model_root'))
manifest=json.loads((package/'manifest.json').read_text())
for name in ('availability_runtime.py','cohort.json','protocol.json'):
    item=next(r for r in manifest['artifacts'] if r['path']==name)
    assert hashlib.sha256((package/name).read_bytes()).hexdigest()==item['sha256']
assert hashlib.sha256((package/'availability_live.py').read_bytes()).hexdigest()==dbutils.widgets.get('live_helper_sha256')
assert hashlib.sha256((model_root/'models.joblib').read_bytes()).hexdigest()==dbutils.widgets.get('model_sha256')
def load(name):
    spec=importlib.util.spec_from_file_location(name,package/(name+'.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
runtime=load('availability_runtime');live=load('availability_live')
bundle=joblib.load(model_root/'models.joblib');evaluation=json.loads((model_root/'report.json').read_text())
cohort=bundle['cohort']
station_choices={'All':'All'}|{
    f"{r.get('name') or r['station_id']} [{r['station_id']}]":r['station_id'] for r in cohort}
try:requested_station=dbutils.widgets.get('a_station')
except Exception:
    try:requested_station=dbutils.widgets.get('station')
    except Exception:requested_station='All'
default_station=next((label for label,value in station_choices.items() if value==requested_station),
    requested_station if requested_station in station_choices else 'All')
try:dbutils.widgets.remove('station')
except Exception:pass
dbutils.widgets.dropdown('a_station',default_station,list(station_choices),'Station (cross street / number)')
dbutils.widgets.dropdown('horizon_minutes','60',['15','30','60','180'])
active=Path('/Volumes/workspace/citibike_3h/models/borough_active.json')
before=hashlib.sha256(active.read_bytes()).hexdigest()

# COMMAND ----------
version=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8]
snapshot=Path('/Volumes/workspace/citibike_3h/exports/availability_live')/version
try:
    info,status,info_at,status_at=live.collect(snapshot)
except Exception as error:
    displayHTML('<div style="padding:20px;background:#fff3cd">Current source could not be refreshed. '
        'No new forecasts were issued. '+html.escape(str(error))+'</div>')
    raise
issue=datetime.now(timezone.utc)
rows,X,states=live.inputs(cohort,info,status,info_at,status_at,issue,runtime.FEATURES)
forecasts=[]
if rows:
    predictions=runtime.forecast(bundle,X)
    for index,row in enumerate(rows):
        values={target+'_'+key:float(array[index]) for target,items in predictions.items() for key,array in items.items()}
        forecasts.append({k:row[k] for k in ('station_id','station_name','origin_utc','target_utc','horizon_minutes',
            'input_observed_at_utc','current_bikes','current_docks')}|values)
state_schema=pa.schema([('station_id',pa.string()),('station_name',pa.string()),('status',pa.string()),
    ('source_observed_at_utc',pa.timestamp('us',tz='UTC')),('provider_at_utc',pa.timestamp('us',tz='UTC')),
    ('current_bikes',pa.int64()),('current_docks',pa.int64())])
pq.write_table(pa.Table.from_pylist(states,schema=state_schema),snapshot/'station_states.parquet',compression='zstd')
(snapshot/'forecast_rows.json').write_text(json.dumps(forecasts,default=str,indent=2)+'\n')
table='workspace.citibike_3h.availability_live_predictions'
spark.conf.set('spark.sql.session.timeZone','UTC')
if forecasts:
    pq.write_table(pa.Table.from_pylist(forecasts),snapshot/'forecasts.parquet',compression='zstd')
    spark.read.parquet(str(snapshot/'forecasts.parquet')).withColumn('dataset_version',F.lit(version)).withColumn(
        'model_version',F.lit(model_root.name)).write.format('delta').mode('overwrite').option(
        'overwriteSchema','true').saveAsTable(table)
published=datetime.now(timezone.utc)
assert not forecasts or published<min(r['target_utc'] for r in forecasts),'Forecast publication missed its first target'
assert hashlib.sha256(active.read_bytes()).hexdigest()==before
report={'schema_version':'availability-live-refresh-v1','status':'READY' if forecasts else 'NO_FRESH_STATIONS',
    'snapshot_root':str(snapshot),'model_root':str(model_root),'issue_time_utc':issue.isoformat(),
    'source_observed_at_utc':status_at.isoformat(),'source_provider_at_utc':live.epoch(status['last_updated']).isoformat(),
    'published_at_utc':published.isoformat(),'forecast_rows':len(forecasts),'cohort_stations':32,
    'fresh_eligible_stations':sum(r['status']=='ready' for r in states),
    'station_status_counts':dict(Counter(r['status'] for r in states)),
    'predictions_table':table if forecasts else None,'active_departure_pointer_sha256':before,
    'active_departure_model_changed':False,'scheduled':False,'nominal_interval_coverage':.9}
(snapshot/'report.json').write_text(json.dumps(report,indent=2)+'\n')
_ = Path('/Volumes/workspace/citibike_3h/exports/availability_dashboard_latest.json').write_text(json.dumps(report,indent=2)+'\n')

# COMMAND ----------
ready=report['fresh_eligible_stations']
displayHTML(f'''<div style="font-family:system-ui;padding:20px;border-radius:14px;background:#102b3f;color:white">
<h1 style="margin:0">Station availability</h1><p>{ready} of 32 stations have fresh usable readings ·
Issued {html.escape(issue.strftime('%Y-%m-%d %H:%M:%S UTC'))}</p>
<p>Current counts → 15 / 30 / 60 / 180 minute forecasts · nominal 90% calibrated ranges</p>
<small>Manual refresh. Model frozen before September evaluation. Ranges may lose coverage as conditions change.</small></div>''')
display(spark.read.parquet(str(snapshot/'station_states.parquet')).orderBy('status','station_id'))

# COMMAND ----------
station=station_choices[dbutils.widgets.get('a_station')];horizon=int(dbutils.widgets.get('horizon_minutes'))
selected=[r for r in forecasts if r['horizon_minutes']==horizon and (station=='All' or r['station_id']==station)]
selected.sort(key=lambda r:max(r['bikes_p_zero'],r['docks_p_zero']),reverse=True)
def interval(row,target):return f"{math.floor(row[target+'_lower'])}–{math.ceil(row[target+'_upper'])}"
def risk_text(value):return '&lt;0.01%' if 0<value<.0001 else f'{100*value:.2f}%'
body=''.join('<tr>'+''.join('<td style="padding:9px;border-bottom:1px solid #e2e8f0">'+str(v)+'</td>' for v in (
    html.escape(r['station_name']),r['current_bikes'],r['current_docks'],
    f"{r['bikes_prediction']:.1f} ({interval(r,'bikes')})",f"{r['docks_prediction']:.1f} ({interval(r,'docks')})",
    risk_text(r['bikes_p_zero']),risk_text(r['docks_p_zero'])))+'</tr>' for r in selected)
headers=['Station','Bikes now','Docks now','Bikes forecast (range)','Docks forecast (range)',
    'No bikes at target','No docks at target']
displayHTML('<h2>'+str(horizon)+' minutes ahead</h2><div style="overflow:auto"><table style="font-family:system-ui;border-collapse:collapse;width:100%"><thead><tr>'+''.join(
    '<th style="text-align:left;padding:9px;background:#eaf2f8">'+h+'</th>' for h in headers)+'</tr></thead><tbody>'+body+'</tbody></table></div>'+
    ('<p>No eligible forecasts for this selection. Check the station status table.</p>' if not selected else '')+
    '<p style="font-family:system-ui;color:#536273">Ranges are calibrated historical prediction intervals, not guaranteed availability. '
    'They are rounded outward and not hard-clipped to a possibly changing reported capacity. '
    'Probabilities describe the target timestamp, not every moment before it. Unknown/offline readings are not zero inventory.</p>')

# COMMAND ----------
if station!='All':
    chart_rows=sorted([r for r in forecasts if r['station_id']==station],key=lambda r:r['horizon_minutes'])
    for target,color in [('bikes','#067c87'),('docks','#cc6b22')]:
        if not chart_rows:continue
        ymax=max(1,max(r[target+'_upper'] for r in chart_rows),chart_rows[0]['current_'+target])*1.1
        def x(minutes):return 55+minutes/180*620
        def y(count):return 220-count/ymax*175
        path=' '.join(str(x(r['horizon_minutes']))+','+str(y(r[target+'_prediction'])) for r in chart_rows)
        marks=''.join(f'<line x1="{x(r["horizon_minutes"])}" y1="{y(r[target+"_lower"])}" x2="{x(r["horizon_minutes"])}" y2="{y(r[target+"_upper"])}" stroke="{color}" stroke-width="8" opacity=".2"/>'+
            f'<circle cx="{x(r["horizon_minutes"])}" cy="{y(r[target+"_prediction"])}" r="5" fill="{color}"/>'+
            f'<text x="{x(r["horizon_minutes"])}" y="243" text-anchor="middle">{r["horizon_minutes"]} min</text>' for r in chart_rows)
        baseline=y(chart_rows[0]['current_'+target])
        displayHTML(f'<h3>{html.escape(chart_rows[0]["station_name"])} — {target}</h3><svg viewBox="0 0 720 265" style="max-width:850px;font:12px system-ui">'+
            f'<line x1="55" y1="220" x2="690" y2="220" stroke="#8996a2"/><line x1="55" y1="{baseline}" x2="690" y2="{baseline}" stroke="#8996a2" stroke-dasharray="5 5"/>'+
            '<text x="5" y="40">'+str(round(ymax))+'</text><text x="28" y="224">0</text>'+f'<polyline points="{path}" stroke="{color}" fill="none" stroke-width="2"/>'+marks+'</svg>')

# COMMAND ----------
metrics=[]
for h,record in evaluation['test_by_horizon'].items():
    for target in ('bikes','docks'):
        r=record[target];metrics.append({'horizon_minutes':int(h),'outcome':target,
            'model_mae':r['model']['mae'],'persistence_mae':r['persistence']['mae'],
            'empirical_90pct_interval_coverage':r['interval_coverage'],'mean_interval_width':r['mean_interval_width'],
            'risk_brier':r['brier'],'persistence_risk_brier':r['persistence_brier'],'zero_events':r['zero_events']})
displayHTML('<h2>September historical evaluation</h2><p>Held-out station snapshots; hourly forecast origins. '
    'Lower MAE and Brier scores are better. Compare measured interval coverage with the nominal 90% target. '
    'The inventory model includes historical operator and maintenance effects; it does not identify unmet demand or prescribe movements.</p>')
display(spark.createDataFrame(metrics).orderBy('horizon_minutes','outcome'))

# COMMAND ----------
dbutils.notebook.exit(json.dumps(report))
