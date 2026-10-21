# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Import prepared validated outcomes and refresh live evaluation
# MAGIC Only local exports staged with a ready receipt qualify. This does not normalize newly listed ZIP files. Publication must precede the nominal origin for every evaluated horizon. Unassignable DST hours remain null. Empty evaluation is a valid pending-outcome state.

# COMMAND ----------
from datetime import datetime,timezone
import hashlib,importlib.util,json
from pathlib import Path
import re
from uuid import uuid4
from pyspark.sql import functions as F

dbutils.widgets.text('helper_path','');dbutils.widgets.text('helper_sha256','')
path=Path(dbutils.widgets.get('helper_path'))
assert hashlib.sha256(path.read_bytes()).hexdigest()==dbutils.widgets.get('helper_sha256')
spec=importlib.util.spec_from_file_location('release_watch',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
spark.conf.set('spark.sql.session.timeZone','UTC')
ready=Path('/Volumes/workspace/citibike_3h/inputs/approved_outcomes')
packages=[]
for receipt_path in sorted(ready.glob('*/ready.json')):
    package=receipt_path.parent;receipt=json.loads(receipt_path.read_text())
    result=module.validate_outcomes(package)
    assert receipt['manifest_sha256']==result['manifest_sha256']
    packages.append((result['latest_hour_utc'],receipt['approved_at_utc'],package,result))
if not packages:raise ValueError('No validated prepared outcome package')
_,_,package,result=max(packages,key=lambda x:(x[0],x[1]))
dataset_version=package.name
table='workspace.citibike_3h.borough_actual_hourly'
frame=spark.read.parquet(str(package/result['artifact']['path']))
assert 'dataset_version' not in frame.columns and frame.count()==result['rows']
if spark.catalog.tableExists(table):
    current_frame=spark.table(table)
    current=current_frame.selectExpr('max(hour_utc)').first()[0]
    current_version=current_frame.select('dataset_version').first()[0]
else:
    current=None;current_version=None
candidate=frame.selectExpr('max(hour_utc)').first()[0]
if current is not None and candidate<current:raise ValueError('Refusing older outcome coverage')
dataset_changed=current_version!=dataset_version
if dataset_changed:
    frame.withColumn('dataset_version',F.lit(dataset_version)).write.format('delta').mode(
        'overwrite').option('overwriteSchema','true').saveAsTable(table)
saved=spark.table(table)
assert saved.count()==result['rows'] and saved.filter(F.col('dataset_version')!=dataset_version).count()==0
evaluation_ddl='''CREATE OR REPLACE VIEW workspace.citibike_3h.borough_prospective_evaluation AS
    SELECT f.*,a.departures AS actual_departures,abs(f.prediction-a.departures) AS absolute_error,
        pow(f.prediction-a.departures,2) AS squared_error
    FROM workspace.citibike_3h.borough_forecasts f JOIN workspace.citibike_3h.borough_actual_hourly a
        ON f.target_start_utc=a.hour_utc AND f.scope=a.borough
    WHERE a.label_assignable AND a.departures IS NOT NULL AND f.publication_completed_at_utc<f.origin_utc'''
if dataset_changed or not spark.catalog.tableExists('workspace.citibike_3h.borough_prospective_evaluation'):
    spark.sql(evaluation_ddl)
metrics=spark.sql('''SELECT model_version,horizon,count(*) AS evaluated_forecasts,
    avg(absolute_error) AS mae,sqrt(avg(squared_error)) AS rmse
    FROM workspace.citibike_3h.borough_prospective_evaluation GROUP BY ALL''')
now=datetime.now(timezone.utc);run_id=now.strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8]
report={'run_id':run_id,'checked_at_utc':now.isoformat(),'prepared_package':str(package),
    'manifest_sha256':result['manifest_sha256'],'dataset_version':dataset_version,
    'dataset_changed':dataset_changed,'actual_hours':result['rows'],'latest_actual_hour_utc':result['latest_hour_utc'],
    'evaluated_forecasts':spark.table('workspace.citibike_3h.borough_prospective_evaluation').count(),
    'metrics':[r.asDict() for r in metrics.collect()]}
row=(run_id,now,str(package),result['manifest_sha256'],result['rows'],report['evaluated_forecasts'],json.dumps(report['metrics']))
spark.createDataFrame([row],'''run_id STRING,checked_at_utc TIMESTAMP,prepared_package STRING,manifest_sha256 STRING,
    actual_hours LONG,evaluated_forecasts LONG,metrics_json STRING''').write.format('delta').mode('append').saveAsTable(
    'workspace.citibike_3h.borough_evaluation_daily')
folder=Path('/Volumes/workspace/citibike_3h/exports/outcome_evaluation')/run_id;folder.mkdir(parents=True,exist_ok=False)
(folder/'report.json').write_text(json.dumps(report,indent=2)+'\n')
Path('/Volumes/workspace/citibike_3h/exports/borough_evaluation_status.json').write_text(json.dumps(report,indent=2)+'\n')
display(metrics)
dbutils.notebook.exit(json.dumps(report))
