# Databricks notebook source
# /// script
# dependencies = ["scikit-learn==1.7.2"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Availability count ensemble development comparison
# MAGIC Horizon-routed persistence, random forest and histogram boosting candidates.
# MAGIC September is inspected development evidence. No activation or stable-table write.

# COMMAND ----------
from datetime import datetime,timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
from uuid import uuid4

import mlflow
import numpy
import sklearn

dbutils.widgets.text('package_root','')
root=Path(dbutils.widgets.get('package_root'))
manifest=json.loads((root/'manifest.json').read_text())
assert manifest['schema_version']=='availability-ensemble-package-v1'
for item in manifest['artifacts']:
    assert hashlib.sha256((root/item['path']).read_bytes()).hexdigest()==item['sha256']
active=Path('/Volumes/workspace/citibike_3h/models/borough_active.json')
before=hashlib.sha256(active.read_bytes()).hexdigest()
spec=importlib.util.spec_from_file_location('availability_ensemble_runtime',root/'availability_ensemble_runtime.py')
runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
version=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8]
output=Path('/Volumes/workspace/citibike_3h/exports/availability_ensembles')/version
user=spark.sql('SELECT current_user()').first()[0]
mlflow.set_experiment('/Users/'+user+'/DATA622_ThreeHour/Availability ensemble development')
with mlflow.start_run(run_name='availability-ensemble-'+root.name) as run:
    report=runtime.run(root,output)
    report.update({
        'mlflow_run_id':run.info.run_id,
        'dataset_version':version,
        'input_manifest_sha256':hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest(),
        'active_departure_pointer_sha256':before,
        'active_departure_model_changed':False,
        'stable_tables_changed':False,
        'runtime':{'python':platform.python_version(),'scikit_learn':sklearn.__version__,'numpy':numpy.__version__},
    })
    assert report['test_metrics_viewed'] is False
    assert hashlib.sha256(active.read_bytes()).hexdigest()==before
    for horizon,values in report['test_by_horizon'].items():
        for target in ('bikes','docks'):
            for metric in ('mae','rmse'):
                mlflow.log_metric(target+'_'+horizon+'_'+metric,values[target]['model'][metric])
    mlflow.log_params({'package_version':root.name,'routing':json.dumps(report['selected_counts_by_horizon']),
        'production_activation':False,'stable_tables_changed':False})
    (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    for name in ('selection.json','calibration.json','report.json'):mlflow.log_artifact(str(output/name))
display(report['test_by_horizon'])
dbutils.notebook.exit(json.dumps(report))
