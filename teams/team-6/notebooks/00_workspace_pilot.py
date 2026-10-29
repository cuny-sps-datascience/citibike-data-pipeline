# Databricks notebook source
# MAGIC %md
# MAGIC # Citi Bike three hour workspace pilot
# MAGIC Imports real source observations and checks Delta, Python model persistence, MLflow, and network access.
# MAGIC The small synthetic model below tests runtime capabilities only; it is not a Citi Bike forecast model.

# COMMAND ----------
import json
import hashlib
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

import joblib
import mlflow
import numpy as np
from sklearn.linear_model import PoissonRegressor
from pyspark.sql import functions as F

dbutils.widgets.text('catalog', 'workspace')
dbutils.widgets.text('schema', 'citibike_3h')
dbutils.widgets.text('source_root', '')
catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
source_root = Path(dbutils.widgets.get('source_root'))
assert re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', catalog)
assert re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', schema)
qualified = f'{catalog}.{schema}'
began = time.monotonic()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
manifest = json.loads((source_root / 'manifest.json').read_text())
assert manifest['status'] == 'collected_with_documented_coverage_gaps'
version = re.sub('[^a-zA-Z0-9_]', '_', source_root.name).lower()
dataset_version = source_root.name
imports = []

# COMMAND ----------
for artifact in manifest['artifacts']:
    if not artifact['path'].startswith('tables/'):
        continue
    name = artifact['name']
    assert re.fullmatch('[a-zA-Z_][a-zA-Z0-9_]*', name)
    table = f'{qualified}.{name}'
    assert hashlib.sha256((source_root/artifact['path']).read_bytes()).hexdigest()==artifact['sha256']
    frame = spark.read.parquet(str(source_root / artifact['path']))
    assert 'dataset_version' not in frame.columns and frame.count() == artifact['rows']
    frame.withColumn('dataset_version',F.lit(dataset_version)).write.format('delta').mode(
        'overwrite').option('overwriteSchema','true').saveAsTable(table)
    saved=spark.table(table)
    assert saved.count()==artifact['rows'] and saved.filter(F.col('dataset_version')!=dataset_version).count()==0
    imports.append({'table':table,'dataset_version':dataset_version,'rows':artifact['rows']})
display(spark.createDataFrame(imports))

# COMMAND ----------
X = np.array([[0., 0.], [1., 0.], [2., 1.], [3., 1.], [4., 0.], [5., 1.]])
y = np.array([0., 1., 2., 4., 3., 6.])
model = PoissonRegressor(alpha=1.0, max_iter=200).fit(X, y)
model_path = f'/Volumes/{catalog}/{schema}/models/capability_only.joblib'
joblib.dump(model, model_path)
restored = joblib.load(model_path)
reload_error = float(np.max(np.abs(model.predict(X)-restored.predict(X))))
assert reload_error < 1e-12
with mlflow.start_run(run_name='three_hour_runtime_capability_only') as run:
    mlflow.log_params({'purpose': 'runtime capability only', 'training_data': 'six synthetic examples'})
    mlflow.log_metric('reload_max_absolute_error', reload_error)
    # Log from local scratch as well as the durable volume artifact.
    with tempfile.TemporaryDirectory() as scratch:
        path = Path(scratch) / 'capability_only.joblib'
        joblib.dump(model, path)
        mlflow.log_artifact(str(path))
    mlflow_run_id = run.info.run_id

# COMMAND ----------
network = []
for name, url in (
    ('gbfs', 'https://gbfs.citibikenyc.com/gbfs/gbfs.json'),
    ('mta_alerts', 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fall-alerts'),
    ('weather', 'https://single-runs-api.open-meteo.com/v1/forecast?latitude=40.75&longitude=-73.98&models=ecmwf_ifs&run=2025-03-01T00:00&hourly=temperature_2m&forecast_days=1'),
):
    try:
        with urlopen(url, timeout=15) as response:
            sample = response.read(128)
            network.append({'source': name, 'accessible': response.status == 200, 'error': None})
    except Exception as error:
        network.append({'source': name, 'accessible': False, 'error': str(error)[:500]})
report = {'completed_at_utc': datetime.now(timezone.utc).isoformat(),
    'seconds': round(time.monotonic()-began, 2), 'imports': imports,
    'model_reload_error': reload_error, 'mlflow_run_id': mlflow_run_id, 'network': network,
    'main_forecast_ready': False,
    'remaining_gates': ['historical station operation', 'weather archive coverage', 'alert coverage and timing']}
report_path = f'/Volumes/{catalog}/{schema}/exports/capability_report_{version}.json'
Path(report_path).write_text(json.dumps(report, indent=2)+'\n')
display(spark.createDataFrame(network, 'source STRING, accessible BOOLEAN, error STRING'))
dbutils.notebook.exit(json.dumps(report))
