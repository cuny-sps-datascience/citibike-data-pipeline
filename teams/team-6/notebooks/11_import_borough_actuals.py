# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Import released geographic outcomes and evaluate matured forecasts
# MAGIC Run after a validated borough-label export. Only forecasts published before target start qualify. Unassignable DST labels stay null. An empty evaluation means outcomes are not yet available; inventory changes are never substituted for departures.

# COMMAND ----------
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from pyspark.sql import functions as F

dbutils.widgets.text('package_root','')
root=Path(dbutils.widgets.get('package_root'));manifest=json.loads((root/'manifest.json').read_text())
assert manifest['schema_version']=='manhattan-aggregate-labels-v2' and manifest['status']=='built'
artifact=next(a for a in manifest['artifacts'] if a['path']=='borough_hourly.parquet')
assert hashlib.sha256((root/artifact['path']).read_bytes()).hexdigest()==artifact['sha256']
dataset_version=root.name
spark.conf.set('spark.sql.session.timeZone','UTC')
table='workspace.citibike_3h.borough_actual_hourly'
frame=spark.read.parquet(str(root/artifact['path']));assert frame.count()==artifact['rows']
candidate=frame.selectExpr('max(hour_utc)').first()[0]
current=spark.table(table).selectExpr('max(hour_utc)').first()[0] if spark.catalog.tableExists(table) else None
if current is not None and candidate<current:raise ValueError('Refusing older outcome coverage')
assert 'dataset_version' not in frame.columns
frame.withColumn('dataset_version',F.lit(dataset_version)).write.format('delta').mode(
    'overwrite').option('overwriteSchema','true').saveAsTable(table)
saved=spark.table(table)
assert saved.count()==artifact['rows'] and saved.filter(F.col('dataset_version')!=dataset_version).count()==0
spark.sql('''CREATE OR REPLACE VIEW workspace.citibike_3h.borough_prospective_evaluation AS
    SELECT f.*,a.departures AS actual_departures,abs(f.prediction-a.departures) AS absolute_error,
        pow(f.prediction-a.departures,2) AS squared_error
    FROM workspace.citibike_3h.borough_forecasts f JOIN workspace.citibike_3h.borough_actual_hourly a
        ON f.target_start_utc=a.hour_utc AND f.scope=a.borough
    WHERE a.label_assignable AND a.departures IS NOT NULL
        AND f.publication_completed_at_utc<f.origin_utc''')
summary=spark.sql('''SELECT model_version,horizon,count(*) AS evaluated_forecasts,
    avg(absolute_error) AS mae,sqrt(avg(squared_error)) AS rmse
    FROM workspace.citibike_3h.borough_prospective_evaluation GROUP BY ALL''')
report={'imported_at_utc':datetime.now(timezone.utc).isoformat(),'actual_package':str(root),
    'dataset_version':dataset_version,'actual_hours':artifact['rows'],
    'evaluated_forecasts':spark.table('workspace.citibike_3h.borough_prospective_evaluation').count(),
    'latest_actual_hour_utc':str(spark.table(table).selectExpr('max(hour_utc)').first()[0]),
    'note':'No prospective predictive accuracy is available until matching trip outcomes are published and imported.'}
Path('/Volumes/workspace/citibike_3h/exports/borough_evaluation_status.json').write_text(json.dumps(report,indent=2)+'\n')
display(summary)
dbutils.notebook.exit(json.dumps(report))
