# Databricks notebook source
# /// script
# dependencies = ["scikit-learn==1.7.2"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Release-aware monthly backtests
# MAGIC Each monthly fold gates training, validation selection and refitting by assumed source release times. Monthly outcomes previously inspected during development are reused, so this is additional retrospective evidence. No model pointer or live forecast is changed.

# COMMAND ----------
import hashlib
import importlib.util
import json
from pathlib import Path
from datetime import datetime,timezone
from uuid import uuid4
import pyarrow.parquet as pq
from pyspark.sql import functions as F

dbutils.widgets.text('package_root','')
root=Path(dbutils.widgets.get('package_root'));manifest=json.loads((root/'manifest.json').read_text())
assert manifest['schema_version']=='rolling-backtest-package-v1'
for artifact in manifest['artifacts']:
    assert hashlib.sha256((root/artifact['path']).read_bytes()).hexdigest()==artifact['sha256']
def load(name):
    spec=importlib.util.spec_from_file_location(name,root/(name+'.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
runtime=load('model_runtime');rolling=load('rolling_backtest')
output=Path('/Volumes/workspace/citibike_3h/exports/rolling_backtests')/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8])
active=Path('/Volumes/workspace/citibike_3h/models/borough_active.json')
before=hashlib.sha256(active.read_bytes()).hexdigest()
report=rolling.run(pq.read_table(root/'features.parquet').to_pylist(),
    pq.read_table(root/'borough_hourly_release.parquet').to_pylist(),output,runtime,manifest['months'])
report.update({'package_root':str(root),'output_root':str(output),'active_pointer_sha256':before})
assert hashlib.sha256(active.read_bytes()).hexdigest()==before
(output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
spark.conf.set('spark.sql.session.timeZone','UTC')
predictions=spark.read.parquet(str(output/'predictions.parquet'))
dataset_version=root.name
table='workspace.citibike_3h.borough_rolling_predictions'
assert 'dataset_version' not in predictions.columns
predictions.withColumn('dataset_version',F.lit(dataset_version)).write.format('delta').mode(
    'overwrite').option('overwriteSchema','true').saveAsTable(table)
saved=spark.table(table)
assert saved.count()==predictions.count() and saved.filter(F.col('dataset_version')!=dataset_version).count()==0
report['predictions_table']=table;report['dataset_version']=dataset_version
spark.sql('''CREATE OR REPLACE VIEW workspace.citibike_3h.borough_rolling_metrics AS
    SELECT fold_month,model,is_selected,count(*) AS rows,avg(abs(prediction-departures)) AS mae,
    sqrt(avg(pow(prediction-departures,2))) AS rmse,avg(prediction-departures) AS mean_bias
    FROM workspace.citibike_3h.borough_rolling_predictions GROUP BY ALL''')
Path('/Volumes/workspace/citibike_3h/exports/rolling_backtest_latest.json').write_text(json.dumps(report,indent=2)+'\n')
display(spark.table('workspace.citibike_3h.borough_rolling_metrics'))
dbutils.notebook.exit(json.dumps(report))
