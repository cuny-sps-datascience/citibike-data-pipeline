# Databricks notebook source
# /// script
# dependencies = ["scikit-learn==1.7.2"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Public benchmark acceptance checks
# MAGIC Checks source keys, temporal availability, forecast persistence, conditional station flags, fallback parity and the validation-selection rule. Live demand accuracy remains unavailable until outcomes are published.

# COMMAND ----------
import hashlib
import importlib.util
import json
from pathlib import Path
import platform

import joblib
import numpy as np
import pyarrow
import pyarrow.parquet as pq
import sklearn

dbutils.widgets.text('expected_first_origin','')
dbutils.widgets.text('expected_first_score_run_id','')
spark.conf.set('spark.sql.session.timeZone','UTC')
checks={}

def zero(name,query):
    n=spark.sql(query).first()[0]
    checks[name]=int(n)
    assert n==0,(name,n)

zero('feature_duplicate_keys','''SELECT count(*) FROM (SELECT origin_utc,horizon,count(*) AS n
    FROM workspace.citibike_3h.borough_features GROUP BY ALL HAVING n<>1)''')
zero('feature_availability_violations','''SELECT count(*) FROM workspace.citibike_3h.borough_features
    WHERE profile_available_at_utc>=issue_cutoff_utc OR weather_assumed_available_at_utc>=issue_cutoff_utc
        OR issue_cutoff_utc>=origin_utc''')
zero('incomplete_three_horizon_origins','''SELECT count(*) FROM (SELECT origin_utc,count(*) AS n,
    count(DISTINCT horizon) AS horizons FROM workspace.citibike_3h.borough_features GROUP BY origin_utc
    HAVING n<>3 OR horizons<>3)''')
zero('unsupported_station_main_eligibility','''SELECT count(*) FROM workspace.citibike_3h.public_station_hours
    WHERE operation_verified OR main_evaluation_eligible''')
zero('weather_duplicate_keys','''SELECT count(*) FROM (SELECT initialization_utc,valid_time_utc,count(*) AS n
    FROM workspace.citibike_3h.borough_weather_runs GROUP BY ALL HAVING n<>1)''')
zero('forecast_duplicate_keys','''SELECT count(*) FROM (SELECT scope,origin_utc,horizon,model_version,count(*) AS n
    FROM workspace.citibike_3h.borough_forecast_output GROUP BY ALL HAVING n<>1)''')
zero('forecast_time_or_value_violations','''SELECT count(*) FROM workspace.citibike_3h.borough_forecasts
    WHERE prediction<0 OR isnan(prediction) OR profile_available_at_utc>=input_cutoff_utc
        OR model_trained_at_utc>=input_cutoff_utc OR prediction_ready_at_utc<input_cutoff_utc
        OR publication_completed_at_utc<prediction_ready_at_utc
        OR unix_timestamp(target_start_utc)-unix_timestamp(origin_utc)<>3600*(horizon-1)
        OR unix_timestamp(target_end_utc)-unix_timestamp(target_start_utc)<>3600''')
if spark.catalog.tableExists('workspace.citibike_3h.borough_prospective_evaluation'):
    zero('late_origins_in_main_evaluation','''SELECT count(*) FROM workspace.citibike_3h.borough_prospective_evaluation
        WHERE publication_completed_at_utc>=origin_utc''')

# COMMAND ----------
metadata=json.loads(Path('/Volumes/workspace/citibike_3h/models/borough_active.json').read_text())
package=Path(metadata['package_root']);model_root=Path(metadata['model_root'])
assert hashlib.sha256((package/'model_runtime.py').read_bytes()).hexdigest()==metadata['runtime_sha256']
assert hashlib.sha256((model_root/'model.joblib').read_bytes()).hexdigest()==metadata['model_sha256']
spec=importlib.util.spec_from_file_location('model_runtime',package/'model_runtime.py')
runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
bundle=joblib.load(model_root/'model.joblib')
features=pq.read_table(package/'features.parquet').to_pylist()
test=[r for r in features if r['split']=='test' and r['weather_covered']]
predicted=runtime.predict(bundle,test)
saved=pq.read_table(model_root/'test_predictions.parquet').to_pylist()
assert len(saved)==len(test)
checks['model_reload_max_difference']=float(np.max(np.abs(predicted-np.array([r['prediction'] for r in saved]))))
assert checks['model_reload_max_difference']<1e-8
fallback=runtime.predict(bundle,test,bundle['fallback_model'])
checks['fallback_test_mae']=runtime.metrics(np.array([r['departures'] for r in test]),fallback)['mae']
assert abs(checks['fallback_test_mae']-metadata['test'][bundle['fallback_model']]['mae'])<1e-8
eligible=['profile','pooled']+[n for n,v in metadata['convergence'].items() if v['converged']]
assert bundle['selected_model']==min(eligible,key=lambda n:metadata['validation'][n]['mae'])
expected=dbutils.widgets.get('expected_first_origin')
if expected:
    from pyspark.sql import functions as F
    original=spark.table('workspace.citibike_3h.borough_forecasts').where(
        F.col('origin_utc')==F.to_timestamp(F.lit(expected))).collect()
    assert len(original)==3
    assert all(r['score_run_id']==dbutils.widgets.get('expected_first_score_run_id') for r in original)
    assert all(r['selected_model']=='weather_0p1' and r['published_before_target'] for r in original)
    checks['first_forecast_rows_preserved']=len(original)
report={'status':'passed','scope':'public-only Manhattan aggregate benchmark','checks':checks,
    'model_version':metadata['model_version'],
    'versions':{'python':platform.python_version(),'sklearn':sklearn.__version__,
                'pyarrow':pyarrow.__version__,'numpy':np.__version__,'joblib':joblib.__version__},
    'prospective_departure_accuracy_available':spark.catalog.tableExists('workspace.citibike_3h.borough_prospective_evaluation')
        and spark.table('workspace.citibike_3h.borough_prospective_evaluation').count()>0,
    'class_catalog_publication_completed':False}
Path('/Volumes/workspace/citibike_3h/exports/public_mvp_verification.json').write_text(json.dumps(report,indent=2)+'\n')
dbutils.notebook.exit(json.dumps(report))
