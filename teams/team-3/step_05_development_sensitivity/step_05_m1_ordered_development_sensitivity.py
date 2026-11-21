# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Citi Bike M1: ordered development sensitivity (corrected draft)
# MAGIC **Development sensitivity; cannot change the approved histogram gradient boosting model.** Every number here describes development weeks only. This notebook fits separate in-memory models, never replaces the frozen files, and does not use the independent test.
# MAGIC - Reads only the class S3 station-status partitions dated Aug 24 to Oct 4, 2026. It never reads the reserved Oct 5 to 11 test week (guards in cells 1 to 3).
# MAGIC - Makes no S3 writes and no shared-catalog writes. It writes only a new, uniquely named folder in your private workspace.
# MAGIC - Part 1: Risk Score and alert metrics on Sep 28 to Oct 4 for the approved histogram gradient boosting model, logistic regression, the low-count rule, persistence and a station hour-of-week rate.
# MAGIC - Part 2: bias-variance checks (training versus validation) for the boosted tree, one setting at a time, and a learning curve with 2 to 5 training weeks.
# MAGIC - Part 3: logistic regression with ridge (L2) and lasso (L1) penalties at four strengths, with and without the six ACS (American Community Survey) measures, on three forward folds.
# MAGIC - Expected run time: about 20 to 45 minutes on serverless (estimate; the local synthetic check took about 80 seconds on one-fifth the rows). Run cells individually; never use Run all.

# COMMAND ----------

# Reading copy with comments added. The code is unchanged from the version the team ran.
# Terms used in these comments (they match the v06 reports and the v06.1 deck):
#   Poll: one fetch of the public Citi Bike station feed.
#   Risk Score: a model's predicted probability of a shortage; the five highest Risk Scores per poll become alerts.
#   True positive: an alert (a flagged station observation) with a confirmed shortage at the target time.
#   Recall = true positives / all confirmed shortages among eligible stations.
#   Precision = true positives / alerts with known outcomes.
#   Methods: histogram gradient boosting (scikit-learn HistGradientBoostingClassifier), logistic regression,
#   and the low-count rule (the stations with the fewest bikes or open docks right now).
#   Code names such as `caught`, `probability` and `acs_candidate` are kept so saved results still load.
# Do not import this copy to rerun the analysis; the saved results came from the original.
#
# Cell 1: setup and guards
## Packages
# io, json, hashlib, uuid, base64: in-memory files, summaries, fingerprints, run ID, embedded logistic parameters.
# time, warnings: fit timing and catching convergence warnings.
import io, json, hashlib, uuid, base64, time, warnings
# boto3: read the class S3 bucket. pandas, numpy: tables and math. pyarrow.parquet: read Parquet files.
import boto3, pandas as pd, numpy as np, pyarrow.parquet as pq
# sklearn, joblib: model fitting and loading the approved models.
import sklearn, joblib
from pathlib import Path
from botocore.config import Config
# HistGradientBoostingClassifier: histogram gradient boosting (the approved model is this type).
# LogisticRegression, StandardScaler, SimpleImputer: Part 3 penalized logistic models and their preprocessing.
# average_precision_score, log_loss, brier_score_loss: probability metrics.
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, log_loss, brier_score_loss
from sklearn.exceptions import ConvergenceWarning
## Setup
# Connect to S3 with keys read from the Databricks secret scope (values are never printed).
scope = 'citibike-team3-secrets'; bucket = 'cuny-sps-citibike-archive'
client = boto3.client('s3', aws_access_key_id=dbutils.secrets.get(scope, 'aws_access_key_id'), aws_secret_access_key=dbutils.secrets.get(scope, 'aws_secret_access_key'), region_name='us-west-2', config=Config(connect_timeout=10, read_timeout=30, retries={'max_attempts': 2}))
## Development date range (UTC; 04:00Z is midnight in New York during daylight time)
# START = Aug 24, VALID = Sep 28 (start of the validation week), END = Oct 5 (exclusive).
START = pd.Timestamp('2026-08-24T04:00:00Z'); VALID = pd.Timestamp('2026-09-28T04:00:00Z'); END = pd.Timestamp('2026-10-05T04:00:00Z')
RESERVED_TEST_START = pd.Timestamp('2026-10-05T04:00:00Z')
# Confirm the analysis ends before the reserved test week (Oct 5 to 11) begins.
assert END <= RESERVED_TEST_START
user = spark.sql('select current_user() as u').first()['u']
home = Path('/Workspace/Users') / user
# Create a new, uniquely named private output folder. exist_ok=False means it fails rather than reuse an old folder.
RUN_ID = uuid.uuid4().hex[:8]
out_dir = home / ('Citi Bike - M1 Ordered Sensitivity - ' + RUN_ID)
out_dir.mkdir(parents=False, exist_ok=False)
LABEL = 'development sensitivity; cannot change the frozen model'
# SUMMARY collects every check and count; it is saved as summary.json in cell 8.
SUMMARY = {'label': LABEL, 'run_id': RUN_ID, 'output_folder': out_dir.name, 'sklearn_version': sklearn.__version__, 'reserved_test_read': False, 's3_writes': 0, 'started_utc': pd.Timestamp.now('UTC').isoformat()}
# Warn if scikit-learn differs from the version that saved the approved models; cell 5 shows whether scores still match.
if sklearn.__version__ != '1.7.2':
    print('WARNING: scikit-learn is', sklearn.__version__, 'but the frozen models were saved with 1.7.2. The frozen-model check in cell 5 will show whether scores still match.')
print('SETUP', json.dumps(SUMMARY), flush=True)

# COMMAND ----------

# Cell 2: inputs. Fixed cohort, dated development partitions only, ACS table, the approved histogram gradient boosting model and the logistic regression model (all hash-checked)
## Rebuild the 100-station cohort
from pyspark.sql import functions as F
spark.conf.set('spark.sql.session.timeZone', 'UTC')
# Load station information (capacity over time) from the class S3 export.
info = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket, Key='exports/station_info.parquet')['Body'].read()), columns=['station_id', 'fetched_at', 'capacity']).to_pandas()
info['fetched_at'] = pd.to_datetime(info.fetched_at, utc=True)
# Keep each station's last record on or before Aug 24 and drop stations with zero capacity.
pre = info.loc[info.fetched_at <= START].sort_values('fetched_at').drop_duplicates('station_id', keep='last')
pre = pre.loc[pre.capacity > 0, ['station_id']]
# Select 100 stations by a fixed hash order, then confirm the set matches the approved cohort fingerprint.
ids_df = spark.createDataFrame(pre.astype({'station_id': 'str'}))
C = {r.station_id for r in ids_df.orderBy(F.xxhash64('station_id'), 'station_id').limit(100).collect()}
COHORT_SHA = '3d5bf5cd0594981fb60b995a08f6c305b2927122ae1299021e413a1b35414ff3'
assert len(C) == 100 and hashlib.sha256('\n'.join(sorted(C)).encode()).hexdigest() == COHORT_SHA
# Keep cohort stations only; metadata_time is when each capacity record became known.
info = info.loc[info.station_id.isin(C)].rename(columns={'fetched_at': 'metadata_time'}).sort_values('metadata_time')
## Load station status, one Parquet file per New York date, Aug 24 to Oct 4 only
columns = ['station_id', 'fetched_at', 'station_reported_at', 'num_bikes_available', 'num_docks_available', 'is_installed', 'is_renting', 'is_returning']
days = pd.date_range('2026-08-24', '2026-10-04', freq='D')
# Confirm the last date read is before the reserved test week.
assert days.max() < pd.Timestamp('2026-10-05')
parts = []
for day in days:
    prefix = 'exports/station_status/nyc_date=' + day.strftime('%Y-%m-%d') + '/'
    # List the files for this date and confirm there is exactly one.
    items = client.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', [])
    keys = [o['Key'] for o in items if o['Key'].endswith('.parquet')]
    if len(keys) != 1: raise ValueError('Unexpected number of files for ' + day.strftime('%Y-%m-%d'))
    # Read only the needed columns and keep cohort stations.
    frame = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket, Key=keys[0])['Body'].read()), columns=columns).to_pandas()
    parts.append(frame[frame.station_id.isin(C)])
raw = pd.concat(parts, ignore_index=True)
for col in ['fetched_at', 'station_reported_at']: raw[col] = pd.to_datetime(raw[col], utc=True)
# Keep rows inside the development range, in time order.
raw = raw.loc[(raw.fetched_at >= START) & (raw.fetched_at < END)].sort_values('fetched_at')
# Confirm no duplicate (station, poll time) rows and nothing from the test week.
assert not raw.duplicated(['station_id', 'fetched_at']).any() and raw.fetched_at.max() < RESERVED_TEST_START
print('READ_COMPLETE', len(raw), 'days', len(days), 'last_day', str(days.max().date()), flush=True)

## Load the ACS table and confirm it is the frozen one
# Six ACS 2020-2024 estimates within 500 meters of each station.
acs_features = ['estimated_population_500m', 'bachelors_plus_pct_500m', 'households_no_vehicle_pct_500m', 'poverty_pct_500m', 'bicycle_commute_pct_500m', 'school_enrollment_pct_500m']
ACS_SHA = '3332babcb605f43c85f60b979bcda82c66e1c5c1199167118f1a333dbc9b84f9'
acs_bytes = (home / 'Citi Bike - ACS and July Trip Development - e1eb6e26' / 'acs_station_features.parquet').read_bytes()
assert hashlib.sha256(acs_bytes).hexdigest() == ACS_SHA, 'ACS table changed'
acs = pd.read_parquet(io.BytesIO(acs_bytes))
# Convert to numbers; unreadable values become NaN (the boosted tree handles missing values).
for k in acs_features: acs[k] = pd.to_numeric(acs[k], errors='coerce')
acs['station_id'] = acs.station_id.astype(str)
# Confirm exactly one row for each of the 100 cohort stations.
assert len(acs) == 100 and not acs.station_id.duplicated().any() and set(acs.station_id) == set(C)

## Load the approved histogram gradient boosting models (bike and dock) after a fingerprint check
FROZEN_DIR = home / 'Citi Bike - ACS Controls and Neighborhood Audit - ea8df268'
FROZEN_SHA = {'bike': 'aa74b79e7c67276cc8d91895eefb56ec659c620182a6f021f41509eb5febaea4', 'dock': 'd5ec6ce138da03d111ca912f9523bda5659939a6585400a772ec3c7c4c875dd8'}
frozen = {}
for o in ['bike', 'dock']:
    p = FROZEN_DIR / ('%s_acs_candidate_train_before_Sep28.joblib' % o)
    assert hashlib.sha256(p.read_bytes()).hexdigest() == FROZEN_SHA[o], 'frozen file changed: ' + p.name
    frozen[o] = joblib.load(p)   # loaded read-only; never refit or saved

# Logistic regression model (version development-2026-11-09): exact bytes of prototype model_parameters.json, embedded and hash-checked
LOGISTIC_B64 = 'ewogICJ2ZXJzaW9uIjogImRldmVsb3BtZW50LTIwMjYtMTEtMDkiLAogICJza2xlYXJuX3ZlcnNpb24iOiAiMS43LjIiLAogICJjb2hvcnRfc2hhMjU2X3NvcnRlZF9uZXdsaW5lIjogIjNkNWJmNWNkMDU5NDk4MWZiNjBiOTk1YTA4ZjZjMzA1YjI5MjcxMjJhZTEyOTkwMjFlNDEzYTFiMzU0MTRmZjMiLAogICJjb2hvcnRfbm90ZSI6ICJyZWNvbnN0cnVjdGVkIGZyb20gY3VycmVudCBtZXRhZGF0YTsgb3JpZ2luYWwgSURzIG5vdCBzYXZlZCIsCiAgInRyYWluIjogIjIwMjYtMDgtMjQgdGhyb3VnaCAyMDI2LTA5LTI3IEFtZXJpY2EvTmV3X1lvcmsiLAogICJob3Jpem9uX21pbnV0ZXMiOiA2MCwKICAibW9kZWxzIjogewogICAgImJpa2UiOiB7CiAgICAgICJmZWF0dXJlX29yZGVyIjogWwogICAgICAgICJudW1fYmlrZXNfYXZhaWxhYmxlIiwKICAgICAgICAibnVtX2RvY2tzX2F2YWlsYWJsZSIsCiAgICAgICAgImNhcGFjaXR5IiwKICAgICAgICAiYmlrZV9mcmFjdGlvbiIsCiAgICAgICAgImRvY2tfZnJhY3Rpb24iLAogICAgICAgICJob3VyX3NpbiIsCiAgICAgICAgImhvdXJfY29zIgogICAgICBdLAogICAgICAibWVhbiI6IFsKICAgICAgICAxNC43OTgxNTk4MDY2MDI5MzYsCiAgICAgICAgMTIuOTAwMDc0MzAyMDY4NjAzLAogICAgICAgIDMwLjU5NjMxNzQ5NjMzNzgxOCwKICAgICAgICAwLjQ4NzUyMTczNDA1OTczMDksCiAgICAgICAgMC40MDkxODk3MzUyNjY1OTMxNCwKICAgICAgICAtMC4wMDQ3MDA5MDA5MzY2NjQ5MDEsCiAgICAgICAgLTAuMDA3ODMwMDE3NTg4ODA2NTI0CiAgICAgIF0sCiAgICAgICJzY2FsZSI6IFsKICAgICAgICAxMi43Mjg5NDQ2NDc4MDMyOTIsCiAgICAgICAgMTMuNDQxNTU1MDcxODU4MTE3LAogICAgICAgIDE2LjA2ODQ5NDM5NTg4NjgyLAogICAgICAgIDAuMjkzMjQ5NjU2MDE5NDk1NDMsCiAgICAgICAgMC4zMDAzNTY3NDYzMzEwMjM1LAogICAgICAgIDAuNzA3NDQ1NzQ1NzY0MTYzNCwKICAgICAgICAwLjcwNjcwODY0NTE2Nzk1NwogICAgICBdLAogICAgICAiY29lZiI6IFsKICAgICAgICAxLjM3ODE5ODM5NTQ1MjE5ODgsCiAgICAgICAgLTAuMzgzMDgxMjM4Nzc0NTgxNCwKICAgICAgICAwLjE1Njc4Njg3MjU4MTY5NjUsCiAgICAgICAgLTcuMDY0ODcyNDY2NzQzOTA4LAogICAgICAgIDAuMTQ5NzUzNTI1ODQ2MTgwMDQsCiAgICAgICAgMC4wNjQwMDYxMjQ2MDkxNzA5LAogICAgICAgIDAuMDA3MjY1OTM5ODczNjczMDc0CiAgICAgIF0sCiAgICAgICJpbnRlcmNlcHQiOiAtMTAuMTkzODA0Njg5MDExOTI1LAogICAgICAiY2xhc3NlcyI6IFsKICAgICAgICAwLAogICAgICAgIDEKICAgICAgXQogICAgfSwKICAgICJkb2NrIjogewogICAgICAiZmVhdHVyZV9vcmRlciI6IFsKICAgICAgICAibnVtX2Jpa2VzX2F2YWlsYWJsZSIsCiAgICAgICAgIm51bV9kb2Nrc19hdmFpbGFibGUiLAogICAgICAgICJjYXBhY2l0eSIsCiAgICAgICAgImJpa2VfZnJhY3Rpb24iLAogICAgICAgICJkb2NrX2ZyYWN0aW9uIiwKICAgICAgICAiaG91cl9zaW4iLAogICAgICAgICJob3VyX2NvcyIKICAgICAgXSwKICAgICAgIm1lYW4iOiBbCiAgICAgICAgMTQuNzk4MTU5ODA2NjAyOTM2LAogICAgICAgIDEyLjkwMDA3NDMwMjA2ODYwMywKICAgICAgICAzMC41OTYzMTc0OTYzMzc4MTgsCiAgICAgICAgMC40ODc1MjE3MzQwNTk3MzA5LAogICAgICAgIDAuNDA5MTg5NzM1MjY2NTkzMTQsCiAgICAgICAgLTAuMDA0NzAwOTAwOTM2NjY0OTAxLAogICAgICAgIC0wLjAwNzgzMDAxNzU4ODgwNjUyNAogICAgICBdLAogICAgICAic2NhbGUiOiBbCiAgICAgICAgMTIuNzI4OTQ0NjQ3ODAzMjkyLAogICAgICAgIDEzLjQ0MTU1NTA3MTg1ODExNywKICAgICAgICAxNi4wNjg0OTQzOTU4ODY4MiwKICAgICAgICAwLjI5MzI0OTY1NjAxOTQ5NTQzLAogICAgICAgIDAuMzAwMzU2NzQ2MzMxMDIzNSwKICAgICAgICAwLjcwNzQ0NTc0NTc2NDE2MzQsCiAgICAgICAgMC43MDY3MDg2NDUxNjc5NTcKICAgICAgXSwKICAgICAgImNvZWYiOiBbCiAgICAgICAgMC4yNTIwNDI1NzAzMzAwMDU5LAogICAgICAgIDMuNjg1NDMyMDg2NDQ2NzkwNywKICAgICAgICAtMC43NzA1OTIzMTAyNTc5NDM0LAogICAgICAgIC0wLjMxODY2MTA5NzE2MzQwNTU3LAogICAgICAgIC04LjY5NTcxNTI4NTA1OTIxMywKICAgICAgICAtMC4wNjI4MzA0NTgyMDYzOTk2NCwKICAgICAgICAwLjE5MjEyNjk3NDQ3ODA3NDE3CiAgICAgIF0sCiAgICAgICJpbnRlcmNlcHQiOiAtNy45NDkxMjE1NzI3MDQ2MDMsCiAgICAgICJjbGFzc2VzIjogWwogICAgICAgIDAsCiAgICAgICAgMQogICAgICBdCiAgICB9CiAgfQp9Cg=='
LOGISTIC_SHA = '194bc9593a29d796eb192dcef7c065d6c4c362465268925609fe30cf15fada25'
# Decode the embedded logistic parameters and confirm they are byte-identical to the prototype file.
_lb = base64.b64decode(LOGISTIC_B64)
assert hashlib.sha256(_lb).hexdigest() == LOGISTIC_SHA, 'logistic parameters changed'
logit = json.loads(_lb)
# Record the input fingerprints and row count, then display them.
SUMMARY['inputs'] = {'acs_sha256': ACS_SHA, 'frozen_sha256': FROZEN_SHA, 'logistic_version': logit['version'], 'logistic_sha256': LOGISTIC_SHA, 'cohort_sha256': COHORT_SHA, 'raw_rows': len(raw)}
print('INPUTS_OK', json.dumps(SUMMARY['inputs']), flush=True)

# COMMAND ----------

# Cell 3: point-in-time metadata, labels and station features (identical logic to the ACS controls notebook ea8df268, cell 3)
## Attach the capacity record that was known at each poll time (point in time, no look-ahead)
df = pd.merge_asof(raw, info, by='station_id', left_on='fetched_at', right_on='metadata_time', direction='backward')
# Seconds between the poll and the station's own last report.
age = (df.fetched_at - df.station_reported_at).dt.total_seconds()
# A row is usable when the report is 0 to 300 seconds old, capacity is positive, counts are not negative, and the station is installed.
base = age.between(0, 300) & (df.capacity > 0) & (df.num_bikes_available >= 0) & (df.num_docks_available >= 0) & df.is_installed.fillna(False).astype(bool)
# Bike outcome also needs renting on; dock outcome needs returning on.
df['bike_ok'] = base & df.is_renting.fillna(False).astype(bool)
df['dock_ok'] = base & df.is_returning.fillna(False).astype(bool)
# both = usable for both outcomes.
df['both'] = df.bike_ok & df.dock_ok
## Require usable observations 15 and 30 minutes earlier too (the same rule the approved model was trained with)
for m in (15, 30):
    left = df[['station_id', 'fetched_at']].copy(); left['cutoff'] = left.fetched_at - pd.Timedelta(minutes=m)
    right = df[['station_id', 'fetched_at', 'bike_ok', 'dock_ok']].rename(columns={'fetched_at': 'lagtime', 'bike_ok': 'lagbike', 'dock_ok': 'lagdock'})
    lag = pd.merge_asof(left.sort_values('cutoff'), right.sort_values('lagtime'), by='station_id', left_on='cutoff', right_on='lagtime', direction='backward')
    # A lagged observation counts if it is 0 to 330 seconds before the cutoff and usable for both outcomes.
    gap = (lag.cutoff - lag.lagtime).dt.total_seconds(); lag['valid'] = gap.between(0, 330) & lag.lagbike.fillna(False) & lag.lagdock.fillna(False)
    df = df.merge(lag[['station_id', 'fetched_at', 'valid']], on=['station_id', 'fetched_at'], how='left', validate='one_to_one')
    df['both'] &= df.pop('valid').fillna(False)
## Build training and validation examples
# Leave a 3,750-second gap (60 minutes plus 150 seconds) before each boundary, so a label never uses data from the next period.
end_train = VALID - pd.Timedelta(seconds=3750); end_val = END - pd.Timedelta(seconds=3750)
within = ((df.fetched_at < end_train) | ((df.fetched_at >= VALID) & (df.fetched_at < end_val)))
examples = df.loc[within & df.both].copy()
# Each example looks 60 minutes ahead.
examples['target_at'] = examples.fetched_at + pd.Timedelta(minutes=60)
future = df[['station_id', 'fetched_at', 'bike_ok', 'dock_ok', 'num_bikes_available', 'num_docks_available']].rename(columns={'fetched_at': 'future_at', 'bike_ok': 'future_bike_ok', 'dock_ok': 'future_dock_ok', 'num_bikes_available': 'future_bikes', 'num_docks_available': 'future_docks'})
# Match the observation nearest the target time, within 150 seconds (same as the live outcome rule).
examples = pd.merge_asof(examples.sort_values('target_at'), future.sort_values('future_at'), by='station_id', left_on='target_at', right_on='future_at', direction='nearest', tolerance=pd.Timedelta(seconds=150))
# Label 1 = zero bikes (or zero docks) at the target time; NaN when either end is not usable.
for o in ['bike', 'dock']:
    examples[o + '_label'] = np.where(examples[o + '_ok'] & examples['future_' + o + '_ok'].fillna(False), (examples['future_' + o + 's'] == 0).astype(float), np.nan)
## Create the seven station features (same as the live scorer)
examples['bike_fraction'] = examples.num_bikes_available / examples.capacity
examples['dock_fraction'] = examples.num_docks_available / examples.capacity
# Time of day as a circle in New York time, plus hour of week and hour for later summaries.
local = examples.fetched_at.dt.tz_convert('America/New_York')
phase = (local.dt.hour + local.dt.minute / 60 + local.dt.second / 3600) * 2 * np.pi / 24
examples['hour_sin'] = np.sin(phase); examples['hour_cos'] = np.cos(phase)
examples['hour_of_week'] = local.dt.dayofweek * 24 + local.dt.hour
examples['ny_hour'] = local.dt.hour
# Confirm one example per station and poll, and that no label reaches into the test week.
assert not examples.duplicated(['station_id', 'fetched_at']).any()
assert examples.target_at.max() + pd.Timedelta(seconds=150) < RESERVED_TEST_START
features = ['num_bikes_available', 'num_docks_available', 'capacity', 'bike_fraction', 'dock_fraction', 'hour_sin', 'hour_cos']
FINAL_FEATURES = features + acs_features
# Confirm the seven station features have no missing or infinite values.
assert np.isfinite(examples[features].to_numpy()).all()
# Add the six ACS features by station.
ex = examples.merge(acs[['station_id'] + acs_features], on='station_id', how='left', validate='many_to_one')
assert len(ex) == len(examples)

# Persistence input: was the station at zero for this outcome about 60 minutes before the snapshot? (nearest observation within 150 s; past data only)
lagsrc = df[['station_id', 'fetched_at', 'bike_ok', 'dock_ok', 'num_bikes_available', 'num_docks_available']].rename(columns={'fetched_at': 'lag60_at', 'bike_ok': 'lag_bike_ok', 'dock_ok': 'lag_dock_ok', 'num_bikes_available': 'lag_bikes', 'num_docks_available': 'lag_docks'})
ex['lag60_target'] = ex.fetched_at - pd.Timedelta(minutes=60)
# Join the observation nearest 60 minutes earlier (within 150 seconds).
ex = pd.merge_asof(ex.sort_values('lag60_target'), lagsrc.sort_values('lag60_at'), by='station_id', left_on='lag60_target', right_on='lag60_at', direction='nearest', tolerance=pd.Timedelta(seconds=150))
# Confirm every lagged observation is from the past.
assert (ex.lag60_at.dropna() < ex.loc[ex.lag60_at.notna(), 'fetched_at']).all()
# Record the earlier state as zero, positive or unknown, for the persistence method.
for o, cnt in [('bike', 'lag_bikes'), ('dock', 'lag_docks')]:
    ok = ex['lag_' + o + '_ok'].fillna(False).astype(bool)
    ex[o + '_lag_state'] = np.where(ok, np.where(ex[cnt] == 0, 'zero', 'positive'), 'unknown')
# Restore the pre-lag-join examples order used by the original ACS fit.
# Confirm the row order and labels are identical to the examples before the lag join.
# The boosted tree is sensitive to row order, so this is what lets the approved model be reproduced exactly.
keys = ['station_id', 'fetched_at']
assert not examples.duplicated(keys).any() and not ex.duplicated(keys).any()
ex = examples[keys].merge(ex, on=keys, how='left', sort=False, validate='one_to_one', indicator=True)
assert len(ex) == len(examples) and (ex['_merge'] == 'both').all()
assert ex[keys].reset_index(drop=True).equals(examples[keys].reset_index(drop=True))
for label in ['bike_label', 'dock_label']:
    assert ex[label].reset_index(drop=True).equals(examples[label].reset_index(drop=True))
ex = ex.drop(columns='_merge')
# Record and display example counts.
SUMMARY['examples'] = {'rows': len(ex), 'train_period_rows': int((ex.fetched_at < VALID).sum()), 'validation_rows': int((ex.fetched_at >= VALID).sum())}
print('EXAMPLES', json.dumps(SUMMARY['examples']), flush=True)

# COMMAND ----------

# Cell 4: shared definitions
## Constants used by Parts 1 to 3
GAP = pd.Timedelta(seconds=3750)
# The three weekly validation folds; Sep 28 to Oct 4 is the final development week.
WEEKS = [('Sep14-20', pd.Timestamp('2026-09-14T04:00Z'), pd.Timestamp('2026-09-21T04:00Z')), ('Sep21-27', pd.Timestamp('2026-09-21T04:00Z'), pd.Timestamp('2026-09-28T04:00Z')), ('Sep28-Oct04', VALID, END)]
OUTCOMES = [('bike', 'num_bikes_available'), ('dock', 'num_docks_available')]
# The approved histogram gradient boosting settings.
HGB = dict(max_iter=100, max_leaf_nodes=15, min_samples_leaf=200, learning_rate=0.1, random_state=42, early_stopping=True)
# Alert counts per update to compare.
KS = [3, 5, 10]
# Probabilities are clipped to [EPS, 1 - EPS] so log loss never takes log(0).
EPS = 1e-15

## Helper functions
def train_rows(o, count, start, end):
    """Labeled rows with a positive current count, from start up to end minus the 3,750-second gap."""
    lab = o + '_label'
    return ex[(ex.fetched_at >= start) & (ex.fetched_at < end - GAP) & ex[lab].notna() & (ex[count] > 0)]

def pool_rows(o, count, start, end):
    """Alert candidates: service flag on and positive current count (labels may be unknown)."""
    return ex[(ex.fetched_at >= start) & (ex.fetched_at < end - GAP) & ex[o + '_ok'] & (ex[count] > 0)]

def topk(pool, lab, score, k, tie):
    """Top k per update by score (descending), then the tie columns ascending."""
    p = pool[['station_id', 'fetched_at', 'ny_hour', lab] + [c for c in tie if c not in ('station_id',)]].copy(); p['score'] = score
    return p.sort_values(['fetched_at', 'score'] + tie, ascending=[True, False] + [True] * len(tie)).groupby('fetched_at', sort=False).head(k)

def prob_metrics(y, p):
    """Probability metrics on known labels. Log loss and Brier score: lower is better. Average precision: higher is better; a random ranking scores about the prevalence."""
    y = np.asarray(y).astype(int); p = np.clip(np.asarray(p, dtype=float), EPS, 1 - EPS)
    return {'n_known': int(len(y)), 'prevalence': float(y.mean()), 'log_loss': float(log_loss(y, p, labels=[0, 1])), 'brier': float(brier_score_loss(y, p)), 'average_precision': float(average_precision_score(y, p)) if y.sum() > 0 else None, 'mean_predicted': float(p.mean())}

def calibration(y, p, bands=10):
    """Ten equal-count bands of predicted probability: average prediction versus observed shortage rate."""
    d = pd.DataFrame({'y': np.asarray(y).astype(int), 'p': np.asarray(p, dtype=float)})
    d['band'] = pd.qcut(d.p.rank(method='first'), bands, labels=list(range(1, bands + 1))).astype(int)
    t = d.groupby('band').agg(n=('y', 'size'), mean_predicted=('p', 'mean'), observed_rate=('y', 'mean'), p_min=('p', 'min'), p_max=('p', 'max')).reset_index()
    t['gap'] = t.observed_rate - t.mean_predicted
    return t

# Risk Score from the logistic regression model, applied to a whole table at once.
def logistic_prob(frame, o):
    m = logit['models'][o]; X = frame[m['feature_order']].to_numpy(dtype=float)
    z = m['intercept'] + ((X - np.array(m['mean'])) / np.array(m['scale'])) @ np.array(m['coef'])
    return 1 / (1 + np.exp(-z))

# Split the 100 stations into thirds (lower, middle, higher) on one ACS measure; stations without a value are "missing".
def tertiles(col):
    g = acs[['station_id', col]].copy(); g['group'] = 'missing'; k = g[col].notna()
    g.loc[k, 'group'] = pd.qcut(g.loc[k, col].rank(method='first'), 3, labels=['lower', 'middle', 'higher']).astype(str)
    return g.set_index('station_id').group
# Neighborhood groupings used for equity summaries: poverty share and households without a vehicle.
GROUPINGS = {'poverty_share': tertiles('poverty_pct_500m'), 'no_vehicle_share': tertiles('households_no_vehicle_pct_500m')}
# Five New York time-of-day blocks for capture by time of day.
BLOCKS = [('00-06 overnight', 0, 6), ('06-10 morning', 6, 10), ('10-15 midday', 10, 15), ('15-20 evening', 15, 20), ('20-24 night', 20, 24)]
def block_of(h):
    for name, a, b in BLOCKS:
        if a <= h < b: return name

# COMMAND ----------

# Cell 5: Part 1. Five methods on Sep 28 to Oct 4 (the week used for model selection; development, not test)
## Part 1: compare five ranking methods on the final development week
P1_metrics, P1_catch, P1_blocks, P1_groups, P1_cal, P1_checks = [], [], [], [], [], {}
for o, count in OUTCOMES:
    # Training rows before Sep 28; candidate pool from Sep 28 to Oct 4.
    lab = o + '_label'
    train = train_rows(o, count, START, VALID)
    pool = pool_rows(o, count, VALID, END).copy()
    # known = rows with a label; only these enter the Risk Score metrics.
    known = pool[lab].notna().to_numpy(); yk = pool.loc[known, lab].astype(int)
    # Persistence: training shortage rate for each state 60 minutes earlier (zero / positive / unknown)
    pers = train.groupby(o + '_lag_state')[lab].mean()
    # Station hour-of-week rate, shrunk toward the station rate (20 pseudo-rows), which is shrunk toward the overall rate (200 pseudo-rows)
    overall = train[lab].mean()
    st = train.groupby('station_id')[lab].agg(['sum', 'count']); st_rate = (st['sum'] + 200 * overall) / (st['count'] + 200)
    hw = train.groupby(['station_id', 'hour_of_week'])[lab].agg(['sum', 'count']).reset_index()
    hw['rate'] = (hw['sum'] + 20 * hw.station_id.map(st_rate)) / (hw['count'] + 20)
    hw_map = hw.set_index(['station_id', 'hour_of_week']).rate
    hw_p = pd.Series(list(zip(pool.station_id, pool.hour_of_week))).map(hw_map).to_numpy()
    hw_p = np.where(np.isnan(hw_p), pool.station_id.map(st_rate).fillna(overall).to_numpy(), hw_p)
    ## The five methods: (score for each pool row, tie-break columns)
    methods = {
        # Approved histogram gradient boosting model, loaded unchanged.
        'frozen_candidate_A': (frozen[o].predict_proba(pool[FINAL_FEATURES])[:, 1], ['station_id']),
        # Logistic regression baseline.
        'original_logistic': (logistic_prob(pool, o), ['station_id']),
        # Low count: fewer bikes (or docks) ranks higher.
        'low_count': (-pool[count].to_numpy(dtype=float), ['station_id']),
        # Persistence: training shortage rate for the station's state 60 minutes earlier.
        'persistence_60min': (pool[o + '_lag_state'].map(pers).fillna(overall).to_numpy(), [count, 'station_id']),
        # Station hour-of-week rate.
        'station_hour_of_week': (hw_p, ['station_id']),
    }
    # Record the inputs behind each method so they can be checked.
    P1_checks[o] = {'persistence_rates': pers.round(5).to_dict(), 'overall_training_rate': float(overall), 'train_rows': len(train), 'pool_rows': len(pool), 'pool_known': int(known.sum())}
    # Risk Score metrics and calibration for every method except the low-count rule (it is a ranking, not a probability).
    for name, (score, tie) in methods.items():
        if name != 'low_count':
            P1_metrics.append(dict(outcome=o, method=name, **prob_metrics(yk, score[known])))
            c = calibration(yk, score[known]); c['outcome'] = o; c['method'] = name; P1_cal.append(c)
        else:
            P1_metrics.append(dict(outcome=o, method=name, n_known=int(known.sum()), prevalence=float(yk.mean()), log_loss=None, brier=None, average_precision=float(average_precision_score(yk, score[known])), mean_predicted=None))
        # True positives at 3, 5 and 10 alerts per update.
        for k in KS:
            pk = topk(pool, lab, score, k, tie)
            P1_catch.append({'outcome': o, 'method': name, 'alerts_per_update': k, 'alerts': len(pk), 'caught': int(pk[lab].sum()), 'known_negative': int(pk[lab].notna().sum() - pk[lab].sum()), 'unknown': int(pk[lab].isna().sum()), 'precision_known': float(pk[lab].mean()) if pk[lab].notna().any() else None})
            # At 5 alerts per update, also break captures down by time block and by neighborhood group.
            if k == 5:
                pk = pk.assign(block=pk.ny_hour.map(block_of)); pb = pool.assign(block=pool.ny_hour.map(block_of))
                pos = pb.groupby('block')[lab].sum(); upd = pb.groupby('block').fetched_at.nunique()
                for b, sub in pk.groupby('block'):
                    P1_blocks.append({'outcome': o, 'method': name, 'block': b, 'updates': int(upd[b]), 'known_shortages': int(pos[b]), 'caught': int(sub[lab].sum()), 'capture': float(sub[lab].sum() / pos[b]) if pos[b] else None})
                for gname, gmap in GROUPINGS.items():
                    pg = pool.assign(group=pool.station_id.map(gmap)); kg = pk.assign(group=pk.station_id.map(gmap))
                    for g, sub in pg.groupby('group'):
                        alerts_g = kg[kg.group == g]
                        P1_groups.append({'outcome': o, 'method': name, 'grouping': gname, 'group': g, 'stations': int(sub.station_id.nunique()), 'candidates': len(sub), 'known': int(sub[lab].notna().sum()), 'known_shortages': int(sub[lab].sum()), 'shortage_rate': float(sub[lab].mean()), 'alerts': len(alerts_g), 'caught': int(alerts_g[lab].sum()), 'capture': float(alerts_g[lab].sum() / sub[lab].sum()) if sub[lab].sum() else None})
# Turn the result lists into tables.
P1_metrics, P1_catch, P1_blocks, P1_groups, P1_cal = pd.DataFrame(P1_metrics), pd.DataFrame(P1_catch), pd.DataFrame(P1_blocks), pd.DataFrame(P1_groups), pd.concat(P1_cal, ignore_index=True)
# Check: the approved model's true positives at five alerts must equal the validation replay (bike 1,814, dock 2,656)
fz = P1_catch[(P1_catch.method == 'frozen_candidate_A') & (P1_catch.alerts_per_update == 5)].set_index('outcome').caught.to_dict()
P1_checks['frozen_replay_match'] = {'observed': fz, 'expected': {'bike': 1814, 'dock': 2656}, 'match': fz == {'bike': 1814, 'dock': 2656}}
SUMMARY['part1_checks'] = P1_checks
print('PART1_FROZEN_MATCH', json.dumps(P1_checks['frozen_replay_match']), flush=True)
# Display the Risk Score metrics and the true positives at 5 alerts.
display(P1_metrics.round(4)); display(P1_catch[P1_catch.alerts_per_update == 5].round(4))

# COMMAND ----------

# Cell 6: Part 2. Bias-variance (training versus validation), one boosted-tree setting at a time, then a learning curve
## Part 2 helper: fit one boosted tree and measure it on training and validation rows
# Compares training and validation losses. Training and validation rows come from different weeks with
# different distributions, so the gap alone cannot diagnose overfitting or underfitting.
def fit_eval(o, count, train, pool, params):
    lab = o + '_label'; t0 = time.time()
    m = HistGradientBoostingClassifier(**params).fit(train[FINAL_FEATURES], train[lab].astype(int))
    secs = time.time() - t0
    ptr = m.predict_proba(train[FINAL_FEATURES])[:, 1]; ppo = m.predict_proba(pool[FINAL_FEATURES])[:, 1]
    known = pool[lab].notna().to_numpy()
    tr = prob_metrics(train[lab], ptr); va = prob_metrics(pool.loc[known, lab], ppo[known])
    pk = topk(pool, lab, ppo, 5, ['station_id'])
    return m, ppo, {'n_iter': int(m.n_iter_), 'fit_seconds': round(secs, 1), 'train_rows': len(train),
                    'train_log_loss': tr['log_loss'], 'val_log_loss': va['log_loss'], 'log_loss_gap': va['log_loss'] - tr['log_loss'],
                    'train_average_precision': tr['average_precision'], 'val_average_precision': va['average_precision'],
                    'train_brier': tr['brier'], 'val_brier': va['brier'], 'val_caught_at_5': int(pk[lab].sum())}

# Vary one setting at a time from the frozen settings: number of boosting rounds, then tree size.
SETTINGS = [('max_iter', v) for v in (25, 50, 100, 200)] + [('max_leaf_nodes', v) for v in (7, 31)]
P2_settings, P2_curve, P2_checks = [], [], {}
for o, count in OUTCOMES:
    lab = o + '_label'
    train = train_rows(o, count, START, VALID); pool = pool_rows(o, count, VALID, END)
    for param, value in SETTINGS:
        params = dict(HGB, **{param: value})
        m, ppo, r = fit_eval(o, count, train, pool, params)
        is_frozen_setting = params == HGB
        if is_frozen_setting:   # the frozen settings on the frozen window must reproduce frozen candidate A exactly
            P2_checks[o] = {'max_abs_prob_diff_vs_frozen': float(np.max(np.abs(ppo - frozen[o].predict_proba(pool[FINAL_FEATURES])[:, 1])))}
            assert P2_checks[o]['max_abs_prob_diff_vs_frozen'] == 0.0, 'Frozen candidate reproduction failed'
        # Record the result and display a progress line.
        P2_settings.append(dict(outcome=o, varied=param, value=value, frozen_setting=is_frozen_setting, max_iter=params['max_iter'], max_leaf_nodes=params['max_leaf_nodes'], **r))
        print('P2_SETTING', o, param, value, r['n_iter'], round(r['train_log_loss'], 5), round(r['val_log_loss'], 5), flush=True)
    # Learning curve: the frozen settings trained on 2, 3, 4 and 5 weeks before Sep 28.
    for weeks in (2, 3, 4, 5):
        start = VALID - pd.Timedelta(days=7 * weeks)
        tr = train_rows(o, count, max(start, START), VALID)
        m, ppo, r = fit_eval(o, count, tr, pool, HGB)
        P2_curve.append(dict(outcome=o, training_weeks=weeks, train_start_utc=str(max(start, START)), **r))
        print('P2_CURVE', o, weeks, r['train_rows'], round(r['train_log_loss'], 5), round(r['val_log_loss'], 5), flush=True)
# Turn the results into tables, record the reproduction check, and display both tables.
P2_settings, P2_curve = pd.DataFrame(P2_settings), pd.DataFrame(P2_curve)
SUMMARY['part2_checks'] = P2_checks
print('PART2_FROZEN_REPRODUCTION', json.dumps(P2_checks), flush=True)
display(P2_settings.round(5)); display(P2_curve.round(5))

# COMMAND ----------

# Cell 7: Part 3. Logistic regression with ridge (L2) and lasso (L1) at four strengths, with and without ACS, on three forward folds
# C is the inverse of penalty strength: smaller C means a stronger penalty. Features are standardized on the training rows.
# The one station with missing ACS values gets the training median (logistic regression cannot use missing values; the boosted tree handles them natively).
# Penalty strengths to try (C = 0.01 down to 0.00001).
CS = [1e-2, 1e-3, 1e-4, 1e-5]
# Two feature sets: the 7 station features alone, and with the 6 ACS features.
FEATURE_SETS = {'station_7': features, 'station_7_plus_acs_6': FINAL_FEATURES}
P3, P3_coef = [], []
# True positives at 5 that the histogram gradient boosting reference must reproduce on each fold (from earlier development runs).
expected_hgb = {('Sep14-20', 'bike'): 2197, ('Sep14-20', 'dock'): 3164, ('Sep21-27', 'bike'): 1592, ('Sep21-27', 'dock'): 2505, ('Sep28-Oct04', 'bike'): 1814, ('Sep28-Oct04', 'dock'): 2656}
# Three forward folds: train on everything before the fold week, evaluate on that week.
for week, start, end in WEEKS:
    for o, count in OUTCOMES:
        lab = o + '_label'
        train = train_rows(o, count, START, start); pool = pool_rows(o, count, start, end)
        known = pool[lab].notna().to_numpy(); y = train[lab].astype(int)
        # Reference: boosted tree with the frozen settings refit on this fold (reproduces earlier development counts)
        _, ppo, r = fit_eval(o, count, train, pool, HGB)
        P3.append({'fold': week, 'outcome': o, 'model': 'boosted_tree_reference', 'features': 'station_7_plus_acs_6', 'penalty': None, 'C': None, 'nonzero_coefs': None,
                   'train_log_loss': r['train_log_loss'], 'val_log_loss': r['val_log_loss'], 'val_brier': r['val_brier'], 'val_average_precision': r['val_average_precision'], 'val_caught_at_5': r['val_caught_at_5'], 'expected_caught_at_5': expected_hgb[week, o], 'converged': True, 'fit_seconds': r['fit_seconds']})
        for fs, cols in FEATURE_SETS.items():
            # Fill missing values with the training median, then standardize, both fitted on training rows only.
            imp = SimpleImputer(strategy='median').fit(train[cols]); sc = StandardScaler().fit(imp.transform(train[cols]))
            Xtr = sc.transform(imp.transform(train[cols])); Xpo = sc.transform(imp.transform(pool[cols]))
            for penalty in ('l2', 'l1'):
                for Cv in CS:
                    t0 = time.time()
                    with warnings.catch_warnings(record=True) as w:
                        warnings.simplefilter('always', ConvergenceWarning)
                        # L2 (ridge) uses the lbfgs solver.
                        if penalty == 'l2':
                            m = LogisticRegression(penalty='l2', C=Cv, solver='lbfgs', max_iter=1000)
                        # L1 (lasso) uses liblinear; intercept_scaling=100 keeps the intercept from being penalized much.
                        # Note: no random_state is set here, which is the likely cause of the small Part 3 differences seen in the step_05 review test.
                        else:
                            m = LogisticRegression(penalty='l1', C=Cv, solver='liblinear', intercept_scaling=100, max_iter=1000)
                        m.fit(Xtr, y)
                    # Record whether the solver converged.
                    converged = not any(issubclass(x.category, ConvergenceWarning) for x in w)
                    ptr = m.predict_proba(Xtr)[:, 1]; ppo = m.predict_proba(Xpo)[:, 1]
                    va = prob_metrics(pool.loc[known, lab], ppo[known]); trm = prob_metrics(y, ptr)
                    pk = topk(pool, lab, ppo, 5, ['station_id'])
                    # nonzero_coefs counts features the penalty kept (L1 can drop features to zero).
                    P3.append({'fold': week, 'outcome': o, 'model': 'logistic', 'features': fs, 'penalty': penalty, 'C': Cv, 'nonzero_coefs': int((np.abs(m.coef_[0]) > 1e-10).sum()),
                               'train_log_loss': trm['log_loss'], 'val_log_loss': va['log_loss'], 'val_brier': va['brier'], 'val_average_precision': va['average_precision'], 'val_caught_at_5': int(pk[lab].sum()), 'expected_caught_at_5': None, 'converged': converged, 'fit_seconds': round(time.time() - t0, 1)})
                    # Keep every standardized coefficient so feature effects can be compared.
                    P3_coef += [{'fold': week, 'outcome': o, 'features': fs, 'penalty': penalty, 'C': Cv, 'feature': f, 'standardized_coef': float(c)} for f, c in zip(cols, m.coef_[0])]
                    print('P3', week, o, fs, penalty, Cv, P3[-1]['nonzero_coefs'], round(va['log_loss'], 5), P3[-1]['val_caught_at_5'], flush=True)
# Turn results into tables and list any fold where the boosted reference did not reproduce its expected count.
P3, P3_coef = pd.DataFrame(P3), pd.DataFrame(P3_coef)
ref = P3[P3.model == 'boosted_tree_reference']
SUMMARY['part3_reference_mismatches'] = ref[ref.val_caught_at_5 != ref.expected_caught_at_5][['fold', 'outcome', 'val_caught_at_5', 'expected_caught_at_5']].to_dict('records')
# Average each model over the three folds and display the table.
P3_mean = P3.groupby(['outcome', 'model', 'features', 'penalty', 'C'], dropna=False)[['train_log_loss', 'val_log_loss', 'val_brier', 'val_average_precision', 'val_caught_at_5', 'nonzero_coefs']].mean().reset_index()
print('PART3_REFERENCE_MISMATCHES', SUMMARY['part3_reference_mismatches'], flush=True)
display(P3_mean.round(5))

# COMMAND ----------

# Cell 8: save tables to the new private folder (no S3) and print a compact summary
## Save every table as CSV in the new private folder, with row counts and fingerprints
tables = {'part1_probability_metrics': P1_metrics, 'part1_catches_at_3_5_10': P1_catch, 'part1_catches_by_time_block': P1_blocks, 'part1_shortage_rate_and_capture_by_group': P1_groups, 'part1_calibration_ten_bands': P1_cal,
          'part2_one_setting_at_a_time': P2_settings, 'part2_learning_curve': P2_curve, 'part3_folds': P3, 'part3_fold_means': P3_mean, 'part3_coefficients': P3_coef}
files = {}
for name, t in tables.items():
    p = out_dir / (name + '.csv'); t.to_csv(p, index=False)
    files[p.name] = {'rows': len(t), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
# Record that the approved models were not changed, and when the run finished.
SUMMARY['files'] = files
SUMMARY['frozen_model_changed'] = False
SUMMARY['finished_utc'] = pd.Timestamp.now('UTC').isoformat()
# Save the summary, list the saved files, and display the key checks.
(out_dir / 'summary.json').write_text(json.dumps(SUMMARY, indent=2, default=str))
print('SAVED', out_dir.name, sorted(p.name for p in out_dir.iterdir()), flush=True)
print(json.dumps({k: SUMMARY[k] for k in ['label', 'run_id', 'output_folder', 'sklearn_version', 'reserved_test_read', 's3_writes', 'part1_checks', 'part2_checks', 'part3_reference_mismatches']}, default=str, indent=1))
