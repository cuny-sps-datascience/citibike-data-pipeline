# Databricks notebook source
# /// script
# dependencies = ["scikit-learn==1.7.2"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Manhattan three-hour forecasting benchmark
# MAGIC Public trips and forecast weather. This predicts Manhattan totals, a reduced spatial scope from the station proposal. Validation selects the model; the test window is held out until selection. Historical input-release times remain assumptions.

# COMMAND ----------
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import tempfile
import time
from datetime import datetime, timezone
from uuid import uuid4

import mlflow
import platform
import sklearn
import numpy as np
import pyarrow
import joblib
from pyspark.sql import functions as F

dbutils.widgets.text('package_root','')
root=Path(dbutils.widgets.get('package_root'))
manifest=json.loads((root/'manifest.json').read_text())
dataset_version=root.name
qualified='workspace.citibike_3h'
spark.conf.set('spark.sql.session.timeZone','UTC')
began=time.monotonic()
for item in manifest['artifacts']:
    path=root/item['path']
    assert hashlib.sha256(path.read_bytes()).hexdigest()==item['sha256']
    name='borough_'+path.stem
    table=f'{qualified}.{name}'
    frame=spark.read.parquet(str(path))
    assert 'dataset_version' not in frame.columns and frame.count()==item['rows']
    frame.withColumn('dataset_version',F.lit(dataset_version)).write.format('delta').mode(
        'overwrite').option('overwriteSchema','true').saveAsTable(table)
    saved=spark.table(table)
    assert saved.count()==item['rows'] and saved.filter(F.col('dataset_version')!=dataset_version).count()==0
    if path.stem=='weather_runs':
        spark.sql(f'''COMMENT ON TABLE {table} IS
            'Open-Meteo ECMWF IFS single runs; CC BY 4.0, credit Open-Meteo and ECMWF.
            Common Manhattan reference point, not station-specific. Temperature C, precipitation mm,
            wind m/s. Precipitation is accumulated over the preceding hour. Initialization plus
            seven hours is assumed availability, unverified. Early archive includes hindcasts.' ''')

# COMMAND ----------
spec=importlib.util.spec_from_file_location('model_runtime',root/'model_runtime.py')
runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
model_version=root.name+'-'+uuid4().hex[:8]
model_root=Path('/Volumes/workspace/citibike_3h/models/borough')/model_version
model_root.parent.mkdir(parents=True,exist_ok=True)
with mlflow.start_run(run_name='Manhattan_3h_'+model_version) as run:
    report=runtime.train(root,model_root)
    mlflow.log_params({'spatial_scope':'Manhattan total','weather_delay_hours':7,
        'selected_model':report['selected_model'],'study':'retrospective development',
        'feature_package':root.name,'alerts':'excluded: archive coverage unresolved'})
    for split in ('validation','test'):
        for name,values in report[split].items():
            for metric in ('mae','rmse','poisson_deviance'):
                mlflow.log_metric(split+'_'+name+'_'+metric,values[metric])
    with tempfile.TemporaryDirectory() as scratch:
        path=Path(scratch)/'model_report.json';path.write_text(json.dumps(report,indent=2))
        mlflow.log_artifact(str(path))
    report['mlflow_run_id']=run.info.run_id
report.update({'model_version':model_version,'model_root':str(model_root),
    'package_root':str(root),'package_manifest_sha256':hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest(),
    'runtime_sha256':hashlib.sha256((root/'model_runtime.py').read_bytes()).hexdigest(),
    'model_sha256':hashlib.sha256((model_root/'model.joblib').read_bytes()).hexdigest(),
    'software_versions':{'python':platform.python_version(),'sklearn':sklearn.__version__,
        'numpy':np.__version__,'pyarrow':pyarrow.__version__,'joblib':joblib.__version__},
    'trained_at_utc':datetime.now(timezone.utc).isoformat(),'seconds':round(time.monotonic()-began,2)})
(model_root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
# This is a candidate pointer. Scheduled scoring activation is a separate step after verification.
Path('/Volumes/workspace/citibike_3h/models/borough_candidate.json').write_text(json.dumps(report,indent=2)+'\n')
metrics_rows=[]
for split in ('validation','test'):
    for name,values in report[split].items():
        metrics_rows.append((model_version,split,name,int(values['rows']),values['mae'],values['rmse'],values['poisson_deviance']))
frame=spark.createDataFrame(metrics_rows,'model_version STRING,split STRING,model STRING,rows LONG,mae DOUBLE,rmse DOUBLE,poisson_deviance DOUBLE')
frame.write.format('delta').mode('append').saveAsTable(qualified+'.borough_model_metrics')
display(frame)
dbutils.notebook.exit(json.dumps(report))
