# Databricks notebook source
# /// script
# dependencies = ["scikit-learn==1.7.2"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Station availability pilot: historical evaluation
# MAGIC Forecast observed bikes/docks and point-in-time empty/no-dock risk.
# MAGIC Separate chronological selection/calibration/test. No production activation.

# COMMAND ----------
from pathlib import Path
from datetime import datetime,timezone
from uuid import uuid4
import hashlib,importlib.util,json
import mlflow
from pyspark.sql import functions as F

dbutils.widgets.text('package_root','')
root=Path(dbutils.widgets.get('package_root'))
manifest=json.loads((root/'manifest.json').read_text())
active=Path('/Volumes/workspace/citibike_3h/models/borough_active.json')
before=hashlib.sha256(active.read_bytes()).hexdigest()
spec=importlib.util.spec_from_file_location('availability_runtime',root/'availability_runtime.py')
runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
version=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8]
output=Path('/Volumes/workspace/citibike_3h/exports/availability_pilots')/version
user=spark.sql('SELECT current_user()').first()[0]
mlflow.set_experiment('/Users/'+user+'/DATA622_ThreeHour/Station availability pilot')
with mlflow.start_run(run_name='availability-'+root.name) as run:
    report=runtime.run(root,output)
    report.update({'mlflow_run_id':run.info.run_id,'active_departure_pointer_sha256':before,
        'input_manifest_sha256':hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest()})
    mlflow.log_params({'cohort_stations':32,'package_version':root.name,
        'selection':json.dumps(report['selected_counts']),'production_activation':False})
    for horizon,metrics in report['test_by_horizon'].items():
        for target in ('bikes','docks'):
            for key in ('mae','rmse'):mlflow.log_metric(target+'_'+horizon+'_'+key,metrics[target]['model'][key])
            for key in ('brier','interval_coverage'):mlflow.log_metric(target+'_'+horizon+'_'+key,metrics[target][key])
    spark.conf.set('spark.sql.session.timeZone','UTC')
    table='workspace.citibike_3h.availability_test_predictions'
    spark.read.parquet(str(output/'predictions.parquet')).withColumn('dataset_version',F.lit(version)).write.format(
        'delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(table)
    frame=spark.table(table)
    assert frame.count()==report['test_forecast_rows']
    assert frame.groupBy('station_id','origin_utc','horizon_minutes').count().filter('count>1').count()==0
    assert frame.filter(F.col('dataset_version')!=version).count()==0
    report['predictions_table']=table;report['dataset_version']=version
    assert hashlib.sha256(active.read_bytes()).hexdigest()==before
    report['cloud_key_and_row_checks_passed']=True
    (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    for name in ('report.json','selection.json','calibration.json'):mlflow.log_artifact(str(output/name))
display(frame.groupBy('horizon_minutes').agg(F.count('*').alias('rows'),
    F.avg(F.abs(F.col('bikes_prediction')-F.col('bikes'))).alias('bike_mae'),
    F.avg(F.abs(F.col('docks_prediction')-F.col('docks'))).alias('dock_mae')))
dbutils.notebook.exit(json.dumps(report))
