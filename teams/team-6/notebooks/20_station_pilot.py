# Databricks notebook source
# /// script
# dependencies = ["scikit-learn==1.7.2"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Historical station forecast pilot
# MAGIC Independent, conditional station-hour study using timestamped public
# MAGIC history. Release and source quality gates apply. No production activation.

# COMMAND ----------
import hashlib
import importlib.util
import json
from pathlib import Path
from datetime import datetime,timezone
from uuid import uuid4
import mlflow
import pyarrow.parquet as pq
import numpy as np
from pyspark.sql import functions as F

dbutils.widgets.text('package_root','')
root=Path(dbutils.widgets.get('package_root'))
manifest=json.loads((root/'manifest.json').read_text())
assert manifest['schema_version']=='station-pilot-package-v1'
for item in manifest['artifacts']:
    assert hashlib.sha256((root/item['path']).read_bytes()).hexdigest()==item['sha256']
active=Path('/Volumes/workspace/citibike_3h/models/borough_active.json')
before=hashlib.sha256(active.read_bytes()).hexdigest()
spec=importlib.util.spec_from_file_location('station_pilot_runtime',root/'station_pilot_runtime.py')
runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
version=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8]
output=Path('/Volumes/workspace/citibike_3h/exports/station_pilots')/version
user=spark.sql('SELECT current_user()').first()[0]
mlflow.set_experiment('/Users/'+user+'/DATA622_ThreeHour/Station historical pilot')
with mlflow.start_run(run_name='public-station-'+root.name) as run:
    report=runtime.run(root,output)
    mlflow.log_params({'package_version':root.name,'cohort_stations':manifest['cohort_stations'],
        'alert_ablation_ready':report['alert_ablation_ready'],'production_activation':False,
        'scope':'conditional retrospective station-hours'})
    for family,result in report['test'].items():
        for key in ('mae','rmse','poisson_deviance','bias'):mlflow.log_metric('test_'+family+'_'+key,result[key])
    mlflow.log_artifact(str(output/'selection.json'))
    report['mlflow_run_id']=run.info.run_id
    # Saved-model parity is checked on independent held-out rows.
    import joblib
    bundle=joblib.load(output/'models.joblib')
    rows=pq.read_table(root/'features.parquet').to_pylist()
    parts,_,_=runtime.partition(rows,json.loads((root/'protocol.json').read_text()))
    if report['alert_ablation_ready']:
        sample=[r for r in parts['test'] if r['advisory_covered']][:100]
    else:sample=parts['test'][:100]
    matrix=runtime.matrix(sample)
    report['save_reload_checked_rows']=len(sample)
    persisted=pq.read_table(output/'predictions.parquet').to_pylist()
    expected={(r['model'],r['station_id'],r['origin_utc'],r['horizon']):r['prediction'] for r in persisted}
    report['save_reload_max_prediction_difference']=max(
        float(np.max(np.abs(model.predict(matrix)-np.array([
            expected[(family,r['station_id'],r['origin_utc'],r['horizon'])] for r in sample]))))
        for family,model in bundle['models'].items())
    assert report['save_reload_max_prediction_difference']==0
    assert hashlib.sha256(active.read_bytes()).hexdigest()==before
    report.update({'active_pointer_sha256':before,'active_model_changed':False})
    spark.conf.set('spark.sql.session.timeZone','UTC')
    table='workspace.citibike_3h.station_pilot_predictions'
    spark.read.parquet(str(output/'predictions.parquet')).withColumn('dataset_version',F.lit(version)).write.format(
        'delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(table)
    report['predictions_table']=table
    report['dataset_version']=version
    assert spark.table(table).count()==sum(v['rows'] for v in report['test'].values())
    assert spark.table(table).filter(F.col('dataset_version')!=version).count()==0
    assert spark.table(table).groupBy('model','station_id','origin_utc','horizon').count().filter('count>1').count()==0
    feature_table='workspace.citibike_3h.station_pilot_features'
    spark.read.parquet(str(root/'features.parquet')).withColumn('dataset_version',F.lit(version)).write.format(
        'delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(feature_table)
    report['features_table']=feature_table
    assert spark.table(feature_table).count()==manifest['rows']
    if (root/'station_subway_mapping.parquet').exists():
        mapping_table='workspace.citibike_3h.station_pilot_mapping'
        spark.read.parquet(str(root/'station_subway_mapping.parquet')).withColumn('dataset_version',F.lit(version)).write.format(
            'delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(mapping_table)
        report['mapping_table']=mapping_table
    report['cloud_key_and_row_checks_passed']=True
    (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    mlflow.log_artifact(str(output/'report.json'))
display(spark.table(table).groupBy('model').agg(F.count('*').alias('rows'),
    F.avg(F.abs(F.col('prediction')-F.col('departures'))).alias('mae')))
dbutils.notebook.exit(json.dumps(report))
