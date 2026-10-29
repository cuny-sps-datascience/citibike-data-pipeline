# Databricks notebook source
# /// script
# dependencies = ["scikit-learn==1.7.2"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Weather availability sensitivity
# MAGIC Apply the frozen seven-hour model under hypothetical six-, seven- and eight-hour input delays. Compare identical covered keys; do not choose a release-time assumption from accuracy or retrain using test results.

# COMMAND ----------
from datetime import timedelta
import importlib.util
import json
from pathlib import Path

import joblib
import numpy as np
import pyarrow.parquet as pq

metadata=json.loads(Path('/Volumes/workspace/citibike_3h/models/borough_active.json').read_text())
root=Path(metadata['package_root'])
spec=importlib.util.spec_from_file_location('model_runtime',root/'model_runtime.py')
runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
bundle=joblib.load(Path(metadata['model_root'])/'model.joblib')
weather={}
for row in pq.read_table(root/'weather_runs.parquet').to_pylist():
    weather.setdefault(row['initialization_utc'],{})[row['valid_time_utc']]=row
features=pq.read_table(root/'features.parquet').to_pylist()
report={'model_version':metadata['model_version'],'method':'frozen seven-hour model, common keys; no refit or assumption selection',
        'delays_hours':[6,7,8],'splits':{}}
for split in ('validation','test'):
    original=[r for r in features if r['split']==split]
    scenarios={d:[] for d in (6,7,8)}
    for row in original:
        alternatives={}
        for delay in scenarios:
            eligible=row['issue_cutoff_utc']-timedelta(hours=delay)
            init=eligible.replace(hour=eligible.hour//6*6,minute=0,second=0,microsecond=0)
            assert init+timedelta(hours=delay)<row['issue_cutoff_utc']
            values,valid=runtime.weather_inputs(weather.get(init,{}),row['target_start_utc'])
            if valid:
                temp,precip,wind=values
                alternatives[delay]=row|{'temperature':temp,'temperature_squared':temp**2,
                    'precipitation':precip,'wind':wind,'wet':float(precip>0)}
        if len(alternatives)==3:
            for delay in scenarios:scenarios[delay].append(alternatives[delay])
    result={'original_rows':len(original),'common_rows':len(scenarios[7]),'weather_model':{}}
    for delay,rows in scenarios.items():
        result['weather_model'][str(delay)]=runtime.metrics(np.array([r['departures'] for r in rows]),runtime.predict(bundle,rows))
    result['calendar_control']=runtime.metrics(np.array([r['departures'] for r in scenarios[7]]),runtime.predict(bundle,scenarios[7],bundle['fallback_model']))
    report['splits'][split]=result
Path('/Volumes/workspace/citibike_3h/exports/borough_weather_delay_sensitivity.json').write_text(json.dumps(report,indent=2)+'\n')
dbutils.notebook.exit(json.dumps(report))
