# Databricks notebook source
# MAGIC %md
# MAGIC # Citi Bike Team 3 setup: compare the model files with the freeze record
# MAGIC The test-week notebook (step 04b) and the pilot (step 06) refuse to run unless these four files match the fingerprints in the approved freeze record (`docs/Citi_Bike_Freeze_Record_APPROVED_2026-11-10.md`). Run this after `setup_link_run_folder` has made the `acs_table` and `approved_models` folders. It only reads files.

# COMMAND ----------

## Expected fingerprints (the same values the pilot entry point checks)
import hashlib
from pathlib import Path

CANDIDATE_FOLDER = 'Citi Bike - ACS Controls and Neighborhood Audit - ea8df268'
ACS_RELATIVE = 'Citi Bike - ACS and July Trip Development - e1eb6e26/acs_station_features.parquet'
FROZEN = {
    CANDIDATE_FOLDER + '/bike_acs_candidate_train_before_Sep28.joblib': 'aa74b79e7c67276cc8d91895eefb56ec659c620182a6f021f41509eb5febaea4',
    CANDIDATE_FOLDER + '/dock_acs_candidate_train_before_Sep28.joblib': 'd5ec6ce138da03d111ca912f9523bda5659939a6585400a772ec3c7c4c875dd8',
    CANDIDATE_FOLDER + '/manifest.json': 'f130a24ae73e151826d184703f21a153508328a06bc5c7c019ab94ff0d77a196',
    ACS_RELATIVE: '3332babcb605f43c85f60b979bcda82c66e1c5c1199167118f1a333dbc9b84f9',
}

# COMMAND ----------

## Compare each file with its expected fingerprint
import sklearn
home = Path('/Workspace/Users') / spark.sql('select current_user() as u').first()['u']
rows = []
for rel, expected in FROZEN.items():
    p = home / rel
    got = hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None
    rows.append((rel, 'MATCH' if got == expected else ('MISSING' if got is None else 'DIFFERENT'), (got or '')[:16]))
# Display the result and what it means
for r in rows:
    print(*r)
print('scikit-learn', sklearn.__version__, '(the approved models were saved with 1.7.2)')
if all(r[1] == 'MATCH' for r in rows):
    print('ALL_MATCH: steps 04b and 06 can run unchanged.')
else:
    print('NOT_ALL_MATCH: these are not the approved files. Steps 04b and 06 will stop by design.',
          'Either copy the original approved files into these folders, or treat the rebuilt files as a new model',
          '(see "If the fingerprints differ" in RUNBOOK.md).')
