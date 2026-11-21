# Databricks notebook source
# MAGIC %md
# MAGIC # Citi Bike: ONE-TIME reserved test week scoring, Oct 5 to 11, 2026
# MAGIC Runs the comparison fixed in the approved freeze record (SHA-256 0118cb65...). Run once with Run all. Once scoring has started, any rerun stops in cell 1 before reading test data.
# MAGIC Methods: histogram gradient boosting with ACS (the approved model), logistic regression, low-count rule. Same candidate pools. No tuning, no model changes. No S3 writes. Results go to one private folder.

# COMMAND ----------

# Cell 1: one-time guard and setup (stops if test-week scoring has already started)
# Team 3 commented copy (step 04b). Comments were added on November 17, 2026; the code is unchanged.
# Purpose: score the reserved test week (Oct 5 to 11, 2026) once, under the approved freeze record, with
# histogram gradient boosting, logistic regression and the low-count rule on the same eligible stations.
# Run it once, after step 04 and before step 05. It refuses to run a second time in the same workspace.
# It also stops if the model files or the ACS table do not match the freeze record fingerprints.
# Terms used in these comments (they match the v06 reports and the v06.1 deck):
#   Poll: one fetch of the public Citi Bike station feed.
#   Risk Score: a model's predicted probability of a shortage; the five highest Risk Scores per poll become alerts.
#   True positive: an alert (a flagged station observation) with a confirmed shortage at the target time.
#   Recall = true positives / all confirmed shortages among eligible stations.
#   Precision = true positives / alerts with known outcomes.
#   Methods: histogram gradient boosting (scikit-learn HistGradientBoostingClassifier, the approved model; code key
#   acs_candidate), logistic regression (key logistic) and the low-count rule (key low_count).
#   Code names such as `caught` and `p_acs` are kept so saved results still load.
## Packages
import io, json, hashlib, uuid, base64, time, sys, tempfile, math
from collections import Counter
import boto3, pandas as pd, numpy as np, pyarrow.parquet as pq
import sklearn, joblib
from pathlib import Path
from botocore.config import Config
user = spark.sql('select current_user() as u').first()['u']
home = Path('/Workspace/Users') / user
RESULT_PREFIX = 'Citi Bike - Reserved Test Week Oct 5-11 Result'
out_dir = home / RESULT_PREFIX
# Confirm this is the first run in this workspace (a marker file is written before scoring starts)
if (out_dir / 'SCORING_STARTED.txt').exists():
    raise RuntimeError('The reserved test week has already been scored (SCORING_STARTED.txt exists). Do not rerun.')
out_dir.mkdir(parents=False, exist_ok=True)
## Freeze record fingerprint and S3 connection (keys are read at run time from the Databricks secret scope)
FREEZE_RECORD_SHA256 = '0118cb65eac2d22dcc5a86306f78a1e56fc90dd5a21129821ba452b84ec9c324'
scope = 'citibike-team3-secrets'; bucket = 'cuny-sps-citibike-archive'
client = boto3.client('s3', aws_access_key_id=dbutils.secrets.get(scope, 'aws_access_key_id'), aws_secret_access_key=dbutils.secrets.get(scope, 'aws_secret_access_key'), region_name='us-west-2', config=Config(connect_timeout=10, read_timeout=30, retries={'max_attempts': 2}))
START = pd.Timestamp('2026-08-24T04:00:00Z')  # cohort reconstruction date, unchanged
## Date bounds: one development day before the test (for the 15- and 30-minute lag checks), then the test week
LOAD_START = pd.Timestamp('2026-10-04T04:00:00Z')  # one development day before the test, only for lag checks
TEST_START = pd.Timestamp('2026-10-05T04:00:00Z'); TEST_END = pd.Timestamp('2026-10-12T04:00:00Z')
GAP = pd.Timedelta(seconds=3750)
SUMMARY = {'scored_once_at_utc': pd.Timestamp.now('UTC').isoformat(), 'freeze_record_sha256': FREEZE_RECORD_SHA256, 'test_window_utc': [TEST_START.isoformat(), TEST_END.isoformat()], 'sklearn_version': sklearn.__version__, 's3_writes': 0, 'output_folder': out_dir.name}
print('SETUP', json.dumps(SUMMARY), flush=True)

# COMMAND ----------

# Cell 2: cohort (same reconstruction) and status partitions Oct 4 to Oct 11
from pyspark.sql import functions as F
spark.conf.set('spark.sql.session.timeZone', 'UTC')
info = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket, Key='exports/station_info.parquet')['Body'].read()), columns=['station_id', 'fetched_at', 'capacity']).to_pandas()
info['fetched_at'] = pd.to_datetime(info.fetched_at, utc=True)
pre = info.loc[info.fetched_at <= START].sort_values('fetched_at').drop_duplicates('station_id', keep='last')
pre = pre.loc[pre.capacity > 0, ['station_id']]
ids_df = spark.createDataFrame(pre.astype({'station_id': 'str'}))
# The fixed cohort: the same 100 stations as every other step (checked against its fingerprint)
C = {r.station_id for r in ids_df.orderBy(F.xxhash64('station_id'), 'station_id').limit(100).collect()}
COHORT_SHA = '3d5bf5cd0594981fb60b995a08f6c305b2927122ae1299021e413a1b35414ff3'
assert len(C) == 100 and hashlib.sha256('\n'.join(sorted(C)).encode()).hexdigest() == COHORT_SHA
info = info.loc[info.station_id.isin(C)].rename(columns={'fetched_at': 'metadata_time'}).sort_values('metadata_time')
columns = ['station_id', 'fetched_at', 'station_reported_at', 'num_bikes_available', 'num_docks_available', 'is_installed', 'is_renting', 'is_returning']
days = pd.date_range('2026-10-04', '2026-10-11', freq='D')
parts = []
for day in days:
    prefix = 'exports/station_status/nyc_date=' + day.strftime('%Y-%m-%d') + '/'
    keys = [o['Key'] for o in client.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', []) if o['Key'].endswith('.parquet')]
    if len(keys) != 1: raise ValueError('Unexpected number of files for ' + day.strftime('%Y-%m-%d'))
    frame = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket, Key=keys[0])['Body'].read()), columns=columns).to_pandas()
    parts.append(frame[frame.station_id.isin(C)])
raw = pd.concat(parts, ignore_index=True)
for col in ['fetched_at', 'station_reported_at']: raw[col] = pd.to_datetime(raw[col], utc=True)
raw = raw.loc[(raw.fetched_at >= LOAD_START) & (raw.fetched_at < TEST_END)].sort_values('fetched_at')
assert not raw.duplicated(['station_id', 'fetched_at']).any()
SUMMARY['raw_rows'] = len(raw)
print('READ_COMPLETE', len(raw), 'days', [str(d.date()) for d in days], flush=True)

# COMMAND ----------

# Cell 3: labels and features with the development definitions (300 s freshness, 15 and 30 min lag checks, nearest outcome within 150 s)
## Same usable-snapshot rules, 60-minute labels and features as development
df = pd.merge_asof(raw, info, by='station_id', left_on='fetched_at', right_on='metadata_time', direction='backward')
age = (df.fetched_at - df.station_reported_at).dt.total_seconds()
base = age.between(0, 300) & (df.capacity > 0) & (df.num_bikes_available >= 0) & (df.num_docks_available >= 0) & df.is_installed.fillna(False).astype(bool)
df['bike_ok'] = base & df.is_renting.fillna(False).astype(bool)
df['dock_ok'] = base & df.is_returning.fillna(False).astype(bool)
df['both'] = df.bike_ok & df.dock_ok
for m in (15, 30):
    left = df[['station_id', 'fetched_at']].copy(); left['cutoff'] = left.fetched_at - pd.Timedelta(minutes=m)
    right = df[['station_id', 'fetched_at', 'bike_ok', 'dock_ok']].rename(columns={'fetched_at': 'lagtime', 'bike_ok': 'lagbike', 'dock_ok': 'lagdock'})
    lag = pd.merge_asof(left.sort_values('cutoff'), right.sort_values('lagtime'), by='station_id', left_on='cutoff', right_on='lagtime', direction='backward')
    gap = (lag.cutoff - lag.lagtime).dt.total_seconds(); lag['valid'] = gap.between(0, 330) & lag.lagbike.fillna(False) & lag.lagdock.fillna(False)
    df = df.merge(lag[['station_id', 'fetched_at', 'valid']], on=['station_id', 'fetched_at'], how='left', validate='one_to_one')
    df['both'] &= df.pop('valid').fillna(False)
within = (df.fetched_at >= TEST_START) & (df.fetched_at < TEST_END - GAP)
examples = df.loc[within & df.both].copy()
examples['target_at'] = examples.fetched_at + pd.Timedelta(minutes=60)
future = df[['station_id', 'fetched_at', 'bike_ok', 'dock_ok', 'num_bikes_available', 'num_docks_available']].rename(columns={'fetched_at': 'future_at', 'bike_ok': 'future_bike_ok', 'dock_ok': 'future_dock_ok', 'num_bikes_available': 'future_bikes', 'num_docks_available': 'future_docks'})
examples = pd.merge_asof(examples.sort_values('target_at'), future.sort_values('future_at'), by='station_id', left_on='target_at', right_on='future_at', direction='nearest', tolerance=pd.Timedelta(seconds=150))
for o in ['bike', 'dock']:
    examples[o + '_label'] = np.where(examples[o + '_ok'] & examples['future_' + o + '_ok'].fillna(False), (examples['future_' + o + 's'] == 0).astype(float), np.nan)
examples['bike_fraction'] = examples.num_bikes_available / examples.capacity
examples['dock_fraction'] = examples.num_docks_available / examples.capacity
local = examples.fetched_at.dt.tz_convert('America/New_York')
phase = (local.dt.hour + local.dt.minute / 60 + local.dt.second / 3600) * 2 * np.pi / 24
examples['hour_sin'] = np.sin(phase); examples['hour_cos'] = np.cos(phase)
assert not examples.duplicated(['station_id', 'fetched_at']).any()
assert examples.fetched_at.min() >= TEST_START and examples.target_at.max() + pd.Timedelta(seconds=150) < TEST_END
features = ['num_bikes_available', 'num_docks_available', 'capacity', 'bike_fraction', 'dock_fraction', 'hour_sin', 'hour_cos']
assert np.isfinite(examples[features].to_numpy()).all()
SUMMARY['test_examples'] = len(examples)
print('EXAMPLES', len(examples), flush=True)

# COMMAND ----------

# Cell 4: load the approved histogram gradient boosting model and the logistic regression model through the v14 package (embedded, hash-checked)
PKG_FILES = {
    'scoring.py': ('IiIiRGV2ZWxvcG1lbnQtb25seSwgaW4tbWVtb3J5IDYwLW1pbnV0ZSBDaXRpIEJpa2Ugc2NvcmVyOyBubyBuZXR3b3JrIG9yIHdyaXRlcy4iIiIKaW1wb3J0IGNzdgppbXBvcnQgaGFzaGxpYgppbXBvcnQganNvbgppbXBvcnQgbWF0aApmcm9tIGRhdGV0aW1lIGltcG9ydCBkYXRldGltZSwgdGltZWRlbHRhLCB0aW1lem9uZQpmcm9tIHpvbmVpbmZvIGltcG9ydCBab25lSW5mbwpmcm9tIGNvbGxlY3Rpb25zIGltcG9ydCBDb3VudGVyCmZyb20gbm9ybWFsaXphdGlvbiBpbXBvcnQgZmllbGRzCgpGRUFUVVJFUyA9IFsnbnVtX2Jpa2VzX2F2YWlsYWJsZScsICdudW1fZG9ja3NfYXZhaWxhYmxlJywgJ2NhcGFjaXR5JywgJ2Jpa2VfZnJhY3Rpb24nLAogICAgICAgICAgICAnZG9ja19mcmFjdGlvbicsICdob3VyX3NpbicsICdob3VyX2NvcyddCk5ZID0gWm9uZUluZm8oJ0FtZXJpY2EvTmV3X1lvcmsnKQoKCmRlZiB0aW1lc3RhbXAodmFsdWUpOgogICAgaWYgbm90IGlzaW5zdGFuY2UodmFsdWUsIHN0cik6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcigndGltZXN0YW1wcyBtdXN0IGJlIElTTyA4NjAxIHN0cmluZ3Mgd2l0aCB0aW1lem9uZScpCiAgICByZXN1bHQgPSBkYXRldGltZS5mcm9taXNvZm9ybWF0KHZhbHVlLnJlcGxhY2UoJ1onLCAnKzAwOjAwJykpCiAgICBpZiByZXN1bHQudHppbmZvIGlzIE5vbmUgb3IgcmVzdWx0LnV0Y29mZnNldCgpIGlzIE5vbmU6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcigndGltZXN0YW1wIGxhY2tzIHRpbWV6b25lJykKICAgIHJldHVybiByZXN1bHQuYXN0aW1lem9uZSh0aW1lem9uZS51dGMpCgoKZGVmIGxvYWRfY29ob3J0KHBhdGgsIGV4cGVjdGVkX2RpZ2VzdCk6CiAgICB3aXRoIG9wZW4ocGF0aCwgbmV3bGluZT0nJywgZW5jb2Rpbmc9J3V0Zi04JykgYXMgZjoKICAgICAgICBpZHMgPSBbcm93WydzdGF0aW9uX2lkJ10gZm9yIHJvdyBpbiBjc3YuRGljdFJlYWRlcihmKV0KICAgIGlmIGxlbihpZHMpICE9IDEwMCBvciBsZW4oc2V0KGlkcykpICE9IDEwMCBvciBhbnkobm90IHggZm9yIHggaW4gaWRzKToKICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdjb2hvcnQgbXVzdCBjb250YWluIDEwMCBkaXN0aW5jdCBzdGF0aW9uIElEcycpCiAgICBkaWdlc3QgPSBoYXNobGliLnNoYTI1NignXG4nLmpvaW4oc29ydGVkKGlkcykpLmVuY29kZSgpKS5oZXhkaWdlc3QoKQogICAgaWYgZGlnZXN0ICE9IGV4cGVjdGVkX2RpZ2VzdDoKICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdjb2hvcnQgaGFzaCBkb2VzIG5vdCBtYXRjaCBtb2RlbCBhcnRpZmFjdCcpCiAgICByZXR1cm4gc2V0KGlkcykKCgpkZWYgbG9hZF9hcnRpZmFjdChwYXRoKToKICAgIHdpdGggb3BlbihwYXRoLCBlbmNvZGluZz0ndXRmLTgnKSBhcyBmOgogICAgICAgIGEgPSBqc29uLmxvYWQoZikKICAgIGlmIGFbJ2hvcml6b25fbWludXRlcyddICE9IDYwIG9yIHNldChhWydtb2RlbHMnXSkgIT0geydiaWtlJywnZG9jayd9OgogICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ3VuZXhwZWN0ZWQgbW9kZWwgYXJ0aWZhY3QnKQogICAgZm9yIG0gaW4gYVsnbW9kZWxzJ10udmFsdWVzKCk6CiAgICAgICAgaWYgbVsnZmVhdHVyZV9vcmRlciddICE9IEZFQVRVUkVTIG9yIG1bJ2NsYXNzZXMnXSAhPSBbMCwgMV06CiAgICAgICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ3VuZXhwZWN0ZWQgZmVhdHVyZSBvcmRlciBvciBjbGFzcyBlbmNvZGluZycpCiAgICAgICAgaWYgYW55KGxlbihtW2tdKSAhPSBsZW4oRkVBVFVSRVMpIGZvciBrIGluICgnbWVhbicsJ3NjYWxlJywnY29lZicpKToKICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignaW52YWxpZCBtb2RlbCBkaW1lbnNpb25zJykKICAgICAgICBpZiBub3QgYWxsKG1hdGguaXNmaW5pdGUoZmxvYXQoeCkpIGZvciBrIGluICgnbWVhbicsJ3NjYWxlJywnY29lZicpIGZvciB4IGluIG1ba10pOgogICAgICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdub24tZmluaXRlIG1vZGVsIHBhcmFtZXRlcicpCiAgICAgICAgaWYgbm90IGFsbChmbG9hdCh4KSA+IDAgZm9yIHggaW4gbVsnc2NhbGUnXSkgb3Igbm90IG1hdGguaXNmaW5pdGUoZmxvYXQobVsnaW50ZXJjZXB0J10pKToKICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignaW52YWxpZCBtb2RlbCBwYXJhbWV0ZXInKQogICAgcmV0dXJuIGEKCgpkZWYgcHJvYmFiaWxpdHkobW9kZWwsIGZlYXR1cmVzKToKICAgIHogPSBmbG9hdChtb2RlbFsnaW50ZXJjZXB0J10pICsgc3VtKGZsb2F0KGMpICogKGZsb2F0KHgpLWZsb2F0KG11KSkvZmxvYXQoc2QpCiAgICAgICAgZm9yIHgsbXUsc2QsYyBpbiB6aXAoZmVhdHVyZXMsIG1vZGVsWydtZWFuJ10sIG1vZGVsWydzY2FsZSddLCBtb2RlbFsnY29lZiddKSkKICAgIHJldHVybiAxLygxK21hdGguZXhwKC16KSkgaWYgeiA+PSAwIGVsc2UgbWF0aC5leHAoeikvKDErbWF0aC5leHAoeikpCgoKZGVmIHNjb3JlX3NuYXBzaG90KHJlY29yZHMsIG1vZGVsX2FydGlmYWN0LCBjb2hvcnQsIHNjb3JlZF9hdCwgKiwgcHJvYmFiaWxpdHlfZm49Tm9uZSwgbW9kZWxfdmVyc2lvbj1Ob25lLCBwb3NpdGl2ZV9vbmx5PUZhbHNlLCBzdGF0aW9uX3Byb2JhYmlsaXR5X2ZuPU5vbmUpOgogICAgIiIiU2NvcmUgb25lIHBvbGwuIFJlcXVpcmVkIGZpZWxkcyBkb2N1bWVudGVkIGluIFJFQURNRS4gUmV0dXJucyByZWNvcmRzIGFuZCBhbGVydHMuCgogICAgQ2FsbGVyIG11c3Qgc3VwcGx5IHBvaW50LWluLXRpbWUgbWV0YWRhdGEgYW5kIGNvbGxlY3Rpb24gY29tcGxldGlvbiB0aW1lLiBBIG1hbGZvcm1lZAogICAgc25hcHNob3QgZmFpbHMgY2xvc2VkLiBDdXJyZW50bHkgZW1wdHkgc3RhdGlvbnMgYXJlIHNjb3JlZCwgYnV0IGV4Y2x1ZGVkIGZyb20gYWxlcnRzLgogICAgIiIiCiAgICBub3cgPSB0aW1lc3RhbXAoc2NvcmVkX2F0KQogICAgc2Vlbiwgc2NvcmVkLCBjYW5kaWRhdGVzID0gc2V0KCksIFtdLCB7J2Jpa2UnOiBbXSwgJ2RvY2snOiBbXX0KICAgIHJlamVjdGVkID0gQ291bnRlcigpCiAgICBwb2xsX2tleSA9IE5vbmUKICAgIGZvciByIGluIHJlY29yZHM6CiAgICAgICAgc2lkID0gclsnc3RhdGlvbl9pZCddCiAgICAgICAgaWYgc2lkIGluIHNlZW46CiAgICAgICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ2R1cGxpY2F0ZSBzdGF0aW9uIElEIGluIHNuYXBzaG90JykKICAgICAgICBzZWVuLmFkZChzaWQpCiAgICAgICAgZmV0Y2hlZCA9IHRpbWVzdGFtcChyWydmZXRjaGVkX2F0J10pCiAgICAgICAgYXZhaWxhYmxlID0gdGltZXN0YW1wKHJbJ2F2YWlsYWJsZV9hdCddKQogICAgICAgIGlmIHBvbGxfa2V5IGlzIE5vbmU6CiAgICAgICAgICAgIHBvbGxfa2V5ID0gKGZldGNoZWQsIGF2YWlsYWJsZSkKICAgICAgICBlbGlmIHBvbGxfa2V5ICE9IChmZXRjaGVkLCBhdmFpbGFibGUpOgogICAgICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdhbGwgcm93cyBtdXN0IGJlbG9uZyB0byBvbmUgcG9sbCcpCiAgICAgICAgaWYgc2lkIG5vdCBpbiBjb2hvcnQ6CiAgICAgICAgICAgIHJlamVjdGVkWydvdXRzaWRlX2NvaG9ydCddICs9IDEKICAgICAgICAgICAgY29udGludWUKICAgICAgICB0cnk6CiAgICAgICAgICAgIHJlcG9ydGVkID0gdGltZXN0YW1wKHJbJ3N0YXRpb25fcmVwb3J0ZWRfYXQnXSkKICAgICAgICAgICAgbWV0YWRhdGFfYXQgPSB0aW1lc3RhbXAoclsnbWV0YWRhdGFfYXZhaWxhYmxlX2F0J10pCiAgICAgICAgZXhjZXB0IChLZXlFcnJvciwgVmFsdWVFcnJvciwgVHlwZUVycm9yLCBPdmVyZmxvd0Vycm9yKToKICAgICAgICAgICAgcmVqZWN0ZWRbJ2ludmFsaWRfc3RhdGlvbl9vcl9tZXRhZGF0YV90aW1lc3RhbXAnXSArPSAxCiAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgIyBUcmFpbmluZyB0aW1lc3RhbXBzIHVzZSB0aGUgcG9sbC1zdGFydCB0aW1lLiBBIGxhdGVyIGF2YWlsYWJpbGl0eSB0aW1lIGlzCiAgICAgICAgIyBuZWNlc3NhcnkgZm9yIGxpdmUgdXNlIGFuZCBpcyBzZXBhcmF0ZWx5IGNoZWNrZWQgYWdhaW5zdCB0aGUgc2NvcmluZyBjbG9jay4KICAgICAgICBpZiBub3QgKG1ldGFkYXRhX2F0IDw9IGZldGNoZWQgPD0gYXZhaWxhYmxlIDw9IG5vdyk6CiAgICAgICAgICAgIHJlamVjdGVkWydpbnZhbGlkX2F2YWlsYWJpbGl0eV9vcmRlciddICs9IDEKICAgICAgICAgICAgY29udGludWUKICAgICAgICBpZiAoZmV0Y2hlZC1yZXBvcnRlZCkudG90YWxfc2Vjb25kcygpIDwgMDoKICAgICAgICAgICAgcmVqZWN0ZWRbJ3JlcG9ydF9hZnRlcl9wb2xsX3N0YXJ0X2Nsb2NrX3NrZXcnXSArPSAxCiAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgaWYgbm90ICgwIDw9IChmZXRjaGVkLXJlcG9ydGVkKS50b3RhbF9zZWNvbmRzKCkgPD0gMzAwIGFuZAogICAgICAgICAgICAgICAgMCA8PSAobm93LXJlcG9ydGVkKS50b3RhbF9zZWNvbmRzKCkgPD0gMzAwKToKICAgICAgICAgICAgcmVqZWN0ZWRbJ3N0YWxlX3JlcG9ydCddICs9IDEKICAgICAgICAgICAgY29udGludWUKICAgICAgICB0cnk6CiAgICAgICAgICAgIGJpa2VzLCBkb2NrcywgY2FwYWNpdHksIGluc3RhbGxlZCwgcmVudGluZywgcmV0dXJuaW5nID0gZmllbGRzKHIpCiAgICAgICAgZXhjZXB0IChLZXlFcnJvciwgVmFsdWVFcnJvciwgVHlwZUVycm9yLCBPdmVyZmxvd0Vycm9yKToKICAgICAgICAgICAgcmVqZWN0ZWRbJ2ludmFsaWRfbnVtZXJpY19vcl9mbGFnJ10gKz0gMQogICAgICAgICAgICBjb250aW51ZQogICAgICAgIGlmIGNhcGFjaXR5IDw9IDAgb3IgYmlrZXMgPCAwIG9yIGRvY2tzIDwgMDoKICAgICAgICAgICAgcmVqZWN0ZWRbJ2ludmFsaWRfY291bnRzX29yX2NhcGFjaXR5J10gKz0gMQogICAgICAgICAgICBjb250aW51ZQogICAgICAgIGlmIG5vdCBpbnN0YWxsZWQ6CiAgICAgICAgICAgIHJlamVjdGVkWydub3RfaW5zdGFsbGVkJ10gKz0gMQogICAgICAgICAgICBjb250aW51ZQogICAgICAgIGxvY2FsID0gZmV0Y2hlZC5hc3RpbWV6b25lKE5ZKQogICAgICAgIHBoYXNlID0gKGxvY2FsLmhvdXIgKyBsb2NhbC5taW51dGUvNjAgKyBsb2NhbC5zZWNvbmQvMzYwMCkqMiptYXRoLnBpLzI0CiAgICAgICAgeCA9IFtiaWtlcyxkb2NrcyxjYXBhY2l0eSxiaWtlcy9jYXBhY2l0eSxkb2Nrcy9jYXBhY2l0eSxtYXRoLnNpbihwaGFzZSksbWF0aC5jb3MocGhhc2UpXQogICAgICAgIGZvciBvdXRjb21lLCBlbmFibGVkLCBjb3VudCBpbiBbKCdiaWtlJyxyZW50aW5nLGJpa2VzKSwoJ2RvY2snLHJldHVybmluZyxkb2NrcyldOgogICAgICAgICAgICBpZiBub3QgZW5hYmxlZDoKICAgICAgICAgICAgICAgIHJlamVjdGVkW291dGNvbWUgKyAnX3NlcnZpY2VfZGlzYWJsZWQnXSArPSAxCiAgICAgICAgICAgICAgICBjb250aW51ZQogICAgICAgICAgICBpZiBwb3NpdGl2ZV9vbmx5IGFuZCBjb3VudCA9PSAwOgogICAgICAgICAgICAgICAgcmVqZWN0ZWRbb3V0Y29tZSArICdfYWxyZWFkeV9lbXB0eSddICs9IDEKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAgIGlmIHN0YXRpb25fcHJvYmFiaWxpdHlfZm4gaXMgbm90IE5vbmU6CiAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgcmlzayA9IGZsb2F0KHN0YXRpb25fcHJvYmFiaWxpdHlfZm4ob3V0Y29tZSxzaWQseCkpCiAgICAgICAgICAgICAgICBleGNlcHQgU3RhdGlvbkZlYXR1cmVNaXNzaW5nOgogICAgICAgICAgICAgICAgICAgIHJlamVjdGVkW291dGNvbWUgKyAnX3N0YXRpb25fZmVhdHVyZXNfbWlzc2luZyddICs9IDEKICAgICAgICAgICAgICAgICAgICBjb250aW51ZQogICAgICAgICAgICBlbHNlOgogICAgICAgICAgICAgICAgcmlzayA9IHByb2JhYmlsaXR5KG1vZGVsX2FydGlmYWN0Wydtb2RlbHMnXVtvdXRjb21lXSx4KSBpZiBwcm9iYWJpbGl0eV9mbiBpcyBOb25lIGVsc2UgZmxvYXQocHJvYmFiaWxpdHlfZm4ob3V0Y29tZSx4KSkKICAgICAgICAgICAgaWYgbm90IG1hdGguaXNmaW5pdGUocmlzaykgb3Igbm90IDAgPD0gcmlzayA8PSAxOgogICAgICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignbW9kZWwgcmV0dXJuZWQgYW4gaW52YWxpZCBwcm9iYWJpbGl0eScpCiAgICAgICAgICAgIGl0ZW0gPSB7J3N0YXRpb25faWQnOnNpZCwnb3V0Y29tZSc6b3V0Y29tZSwnZmV0Y2hlZF9hdCc6ZmV0Y2hlZC5pc29mb3JtYXQoKSwKICAgICAgICAgICAgICAgICAgICAnYXZhaWxhYmxlX2F0JzphdmFpbGFibGUuaXNvZm9ybWF0KCksJ3Njb3JlZF9hdCc6bm93Lmlzb2Zvcm1hdCgpLAogICAgICAgICAgICAgICAgICAgICdzdGF0aW9uX3JlcG9ydGVkX2F0JzpyZXBvcnRlZC5pc29mb3JtYXQoKSwKICAgICAgICAgICAgICAgICAgICAndGFyZ2V0X2F0JzooZmV0Y2hlZCt0aW1lZGVsdGEobWludXRlcz02MCkpLmlzb2Zvcm1hdCgpLAogICAgICAgICAgICAgICAgICAgICdtb2RlbF92ZXJzaW9uJzptb2RlbF92ZXJzaW9uIG9yIG1vZGVsX2FydGlmYWN0Wyd2ZXJzaW9uJ10sCiAgICAgICAgICAgICAgICAgICAgJ3Byb2JhYmlsaXR5JzpyaXNrLAogICAgICAgICAgICAgICAgICAgICdjdXJyZW50X2NvdW50Jzpjb3VudH0KICAgICAgICAgICAgc2NvcmVkLmFwcGVuZChpdGVtKQogICAgICAgICAgICBpZiBjb3VudCA+IDA6CiAgICAgICAgICAgICAgICBjYW5kaWRhdGVzW291dGNvbWVdLmFwcGVuZChpdGVtKQogICAgIyBEZW1vbnN0cmF0aW9uIHBvbGljeSBvbmx5OiB1cCB0byBmaXZlIHBlciBvdXRjb21lLCBwZXIgcG9sbDsgdGllcyBieSBzdGF0aW9uIElELgogICAgYWxlcnRzID0ge291dGNvbWU6IHNvcnRlZChyb3dzLCBrZXk9bGFtYmRhIHY6KC12Wydwcm9iYWJpbGl0eSddLHZbJ3N0YXRpb25faWQnXSkpWzo1XQogICAgICAgICAgICAgIGZvciBvdXRjb21lLHJvd3MgaW4gY2FuZGlkYXRlcy5pdGVtcygpfQogICAgZGlhZ25vc3RpY3MgPSBkaWN0KHJlamVjdGVkKQogICAgaWYgbm90IHNjb3JlZDoKICAgICAgICByYWlzZSBaZXJvU2NvcmVFcnJvcihkaWFnbm9zdGljcykKICAgIHJldHVybiBzY29yZWQsIGFsZXJ0cywgZGlhZ25vc3RpY3MKCgpjbGFzcyBTdGF0aW9uRmVhdHVyZU1pc3NpbmcoS2V5RXJyb3IpOgogICAgIiIiUmFpc2VkIGJ5IGEgc3RhdGlvbi1hd2FyZSBwcm9iYWJpbGl0eSBmdW5jdGlvbiB3aGVuIGEgc3RhdGlvbiBoYXMgbm8gc3RhdGljIGZlYXR1cmUgcm93LiIiIgoKCmNsYXNzIFplcm9TY29yZUVycm9yKFZhbHVlRXJyb3IpOgogICAgZGVmIF9faW5pdF9fKHNlbGYsIHJlYXNvbnMpOgogICAgICAgIHNlbGYucmVhc29ucyA9IHJlYXNvbnMKICAgICAgICBzdXBlcigpLl9faW5pdF9fKCd6ZXJvIHNjb3JlczogJyArIHN0cihyZWFzb25zKSkK', 'f61d202a6689ce58dd35df0461b19665d5fe858b9b801499b8ec058b38592707'),
    'normalization.py': ('IiIiU3RyaWN0LCBsb3NzbGVzcyBub3JtYWxpemF0aW9uIGZvciBzY2FsYXIgZGF0YSBmcm9tIEpTT04sIHBhbmRhcyBvciBQYXJxdWV0LiIiIgppbXBvcnQgbWF0aAppbXBvcnQgbnVtYmVycwoKZGVmIGludGVnZXIodmFsdWUpOgogICAgaWYgaXNpbnN0YW5jZSh2YWx1ZSwgYm9vbCkgb3IgKHR5cGUodmFsdWUpLl9fbW9kdWxlX18uc3RhcnRzd2l0aCgnbnVtcHknKSBhbmQgdHlwZSh2YWx1ZSkuX19uYW1lX18gaW4gKCdib29sJywgJ2Jvb2xfJykpOgogICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ2Jvb2xlYW4gaXMgbm90IGEgY291bnQnKQogICAgaWYgbm90IGlzaW5zdGFuY2UodmFsdWUsIG51bWJlcnMuUmVhbCk6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignZXhwZWN0ZWQgYSBudW1lcmljIGludGVnZXInKQogICAgaWYgbm90IG1hdGguaXNmaW5pdGUodmFsdWUpIG9yIGludCh2YWx1ZSkgIT0gdmFsdWU6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignZXhwZWN0ZWQgYSBmaW5pdGUgaW50ZWdyYWwgdmFsdWUnKQogICAgcmV0dXJuIGludCh2YWx1ZSkKCmRlZiBmbGFnKHZhbHVlKToKICAgIGlmIGlzaW5zdGFuY2UodmFsdWUsIGJvb2wpIG9yICh0eXBlKHZhbHVlKS5fX21vZHVsZV9fLnN0YXJ0c3dpdGgoJ251bXB5JykgYW5kIHR5cGUodmFsdWUpLl9fbmFtZV9fIGluICgnYm9vbCcsICdib29sXycpKToKICAgICAgICByZXR1cm4gYm9vbCh2YWx1ZSkKICAgIHYgPSBpbnRlZ2VyKHZhbHVlKQogICAgaWYgdiBub3QgaW4gKDAsIDEpOgogICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ2ZsYWcgbXVzdCBiZSBib29sZWFuIG9yIDAvMScpCiAgICByZXR1cm4gYm9vbCh2KQoKZGVmIGZpZWxkcyhyb3cpOgogICAgcmV0dXJuIChpbnRlZ2VyKHJvd1snbnVtX2Jpa2VzX2F2YWlsYWJsZSddKSwgaW50ZWdlcihyb3dbJ251bV9kb2Nrc19hdmFpbGFibGUnXSksCiAgICAgICAgICAgIGludGVnZXIocm93WydjYXBhY2l0eSddKSwgZmxhZyhyb3dbJ2lzX2luc3RhbGxlZCddKSwKICAgICAgICAgICAgZmxhZyhyb3dbJ2lzX3JlbnRpbmcnXSksIGZsYWcocm93Wydpc19yZXR1cm5pbmcnXSkpCg==', 'f257fbbe6363c91de1727a228651287c1177da17053954e9fd5cd303a747d8e3'),
    'candidate_scorer_acs.py': ('IiIiUHJvdG90eXBlIHYxNDogQUNTIGNhbmRpZGF0ZSBzY29yZXIgd2l0aCBhIGxvZ2lzdGljIGZhbGxiYWNrLiBEZXZlbG9wbWVudCBvbmx5LgoKVGhlIGNhbmRpZGF0ZSBpcyB0aGUgcG9zaXRpdmUtY291bnQgYm9vc3RlZCB0cmVlIHVzaW5nIHRoZSBzZXZlbiBzdGF0aW9uIGZlYXR1cmVzIHBsdXMgc2l4CnN0YXRpYyBBQ1MgKEFtZXJpY2FuIENvbW11bml0eSBTdXJ2ZXkpIG1lYXN1cmVzIGpvaW5lZCBieSBzdGF0aW9uIElELiBUaGlzIG1vZHVsZSBwZXJmb3JtcwpubyBuZXR3b3JrIGNhbGxzLCBubyB3cml0ZXMgYW5kIG5vIHNjaGVkdWxpbmcuCgpPbmx5IGxvYWQgdHJ1c3RlZCBhcnRpZmFjdHMuIGpvYmxpYiBkZXNlcmlhbGl6YXRpb24gY2FuIGV4ZWN1dGUgY29kZSwgYW5kIGEgbWF0Y2hpbmcgaGFzaCBpbiBhbgphZGphY2VudCBtYW5pZmVzdCBjaGVja3MgaW50ZWdyaXR5LCBub3Qgb3JpZ2luLgoiIiIKaW1wb3J0IGhhc2hsaWIKaW1wb3J0IGlvCmltcG9ydCBqc29uCmltcG9ydCBtYXRoCmZyb20gcGF0aGxpYiBpbXBvcnQgUGF0aAoKaW1wb3J0IGpvYmxpYgppbXBvcnQgcGFuZGFzIGFzIHBkCmltcG9ydCBza2xlYXJuCgpmcm9tIHNjb3JpbmcgaW1wb3J0IEZFQVRVUkVTLCBTdGF0aW9uRmVhdHVyZU1pc3NpbmcsIGxvYWRfYXJ0aWZhY3QsIGxvYWRfY29ob3J0LCBzY29yZV9zbmFwc2hvdAoKQUNTX0ZFQVRVUkVTID0gWydlc3RpbWF0ZWRfcG9wdWxhdGlvbl81MDBtJywgJ2JhY2hlbG9yc19wbHVzX3BjdF81MDBtJywgJ2hvdXNlaG9sZHNfbm9fdmVoaWNsZV9wY3RfNTAwbScsCiAgICAgICAgICAgICAgICAncG92ZXJ0eV9wY3RfNTAwbScsICdiaWN5Y2xlX2NvbW11dGVfcGN0XzUwMG0nLCAnc2Nob29sX2Vucm9sbG1lbnRfcGN0XzUwMG0nXQpDQU5ESURBVEVfRkVBVFVSRVMgPSBGRUFUVVJFUyArIEFDU19GRUFUVVJFUwpPVVRDT01FUyA9ICgnYmlrZScsICdkb2NrJykKCgpkZWYgX3NoYTI1NihkYXRhKToKICAgIHJldHVybiBoYXNobGliLnNoYTI1NihkYXRhKS5oZXhkaWdlc3QoKQoKCmRlZiBsb2FkX2Fjc190YWJsZShwYXRoLCBleHBlY3RlZF9zaGEyNTYsIGNvaG9ydCk6CiAgICAiIiJSZXR1cm4ge3N0YXRpb25faWQ6IFtzaXggQUNTIHZhbHVlc119IGFmdGVyIGhhc2gsIHNjaGVtYSBhbmQgY292ZXJhZ2UgY2hlY2tzLgoKICAgIE1pc3NpbmcgQUNTIHZhbHVlcyBzdGF5IE5hTiwgbWF0Y2hpbmcgdHJhaW5pbmcsIHdoZXJlIHRoZSBib29zdGVkIHRyZWUgaGFuZGxlZCB0aGVtIG5hdGl2ZWx5LgogICAgVGhlIHRhYmxlIG11c3QgY292ZXIgZXhhY3RseSB0aGUgY29ob3J0LCBzbyBhIGNvaG9ydCBzdGF0aW9uIGNhbm5vdCBiZSBhYnNlbnQgYXQgc2NvcmluZyB0aW1lCiAgICB1bmxlc3MgdGhlIGluLW1lbW9yeSB0YWJsZSBpcyBhbHRlcmVkOyBzY29yaW5nIHN0aWxsIGd1YXJkcyB0aGF0IGNhc2UuCiAgICAiIiIKICAgIGRhdGEgPSBQYXRoKHBhdGgpLnJlYWRfYnl0ZXMoKQogICAgaWYgX3NoYTI1NihkYXRhKSAhPSBleHBlY3RlZF9zaGEyNTY6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignQUNTIHRhYmxlIGNoZWNrc3VtIG1pc21hdGNoJykKICAgIGlmIHN0cihwYXRoKS5lbmRzd2l0aCgnLnBhcnF1ZXQnKToKICAgICAgICB0YWJsZSA9IHBkLnJlYWRfcGFycXVldChpby5CeXRlc0lPKGRhdGEpKQogICAgZWxzZToKICAgICAgICB0YWJsZSA9IHBkLnJlYWRfY3N2KGlvLkJ5dGVzSU8oZGF0YSksIGR0eXBlPXsnc3RhdGlvbl9pZCc6IHN0cn0pCiAgICBtaXNzaW5nID0gW2MgZm9yIGMgaW4gWydzdGF0aW9uX2lkJ10gKyBBQ1NfRkVBVFVSRVMgaWYgYyBub3QgaW4gdGFibGUuY29sdW1uc10KICAgIGlmIG1pc3Npbmc6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignQUNTIHRhYmxlIGxhY2tzIGNvbHVtbnM6ICcgKyAnLCAnLmpvaW4obWlzc2luZykpCiAgICB0YWJsZSA9IHRhYmxlW1snc3RhdGlvbl9pZCddICsgQUNTX0ZFQVRVUkVTXS5jb3B5KCkKICAgIHRhYmxlWydzdGF0aW9uX2lkJ10gPSB0YWJsZS5zdGF0aW9uX2lkLmFzdHlwZShzdHIpCiAgICBpZiB0YWJsZS5zdGF0aW9uX2lkLmR1cGxpY2F0ZWQoKS5hbnkoKToKICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdBQ1MgdGFibGUgaGFzIGR1cGxpY2F0ZSBzdGF0aW9uIElEcycpCiAgICBpZiBzZXQodGFibGUuc3RhdGlvbl9pZCkgIT0gc2V0KGNvaG9ydCk6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignQUNTIHRhYmxlIHN0YXRpb25zIGRvIG5vdCBtYXRjaCB0aGUgY29ob3J0JykKICAgIGZvciBjIGluIEFDU19GRUFUVVJFUzoKICAgICAgICB0YWJsZVtjXSA9IHBkLnRvX251bWVyaWModGFibGVbY10sIGVycm9ycz0nY29lcmNlJykKICAgIHZhbHVlcyA9IHRhYmxlW0FDU19GRUFUVVJFU10udG9fbnVtcHkoZHR5cGU9ZmxvYXQpCiAgICBpZiBhbnkobWF0aC5pc2luZih2KSBmb3IgdiBpbiB2YWx1ZXMucmF2ZWwoKSk6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignQUNTIHRhYmxlIGNvbnRhaW5zIGluZmluaXRlIHZhbHVlcycpCiAgICByZXR1cm4ge3NpZDogW2Zsb2F0KHYpIGZvciB2IGluIHJvd10gZm9yIHNpZCwgcm93IGluIHppcCh0YWJsZS5zdGF0aW9uX2lkLCB2YWx1ZXMpfQoKCmRlZiBsb2FkX2NhbmRpZGF0ZShmb2xkZXIsIHdpbmRvdywgY29ob3J0X2ZpbGUsIGJhc2VsaW5lX2FydGlmYWN0X2ZpbGUsIGFjc19maWxlKToKICAgICIiIkxvYWQgYW5kIHZlcmlmeSB0aGUgZnJvemVuLWNhbmRpZGF0ZSBmaWxlcy4gUmFpc2VzIFZhbHVlRXJyb3Igb24gYW55IG1pc21hdGNoLiIiIgogICAgZm9sZGVyID0gUGF0aChmb2xkZXIpCiAgICBtYW5pZmVzdCA9IGpzb24ubG9hZHMoKGZvbGRlciAvICdtYW5pZmVzdC5qc29uJykucmVhZF90ZXh0KGVuY29kaW5nPSd1dGYtOCcpKQogICAgaWYgbWFuaWZlc3QuZ2V0KCdza2xlYXJuX3ZlcnNpb24nKSAhPSBza2xlYXJuLl9fdmVyc2lvbl9fOgogICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ3NjaWtpdC1sZWFybiB2ZXJzaW9uIG1pc21hdGNoOyByZWJ1aWxkIG9yIHZlcmlmeSBjb21wYXRpYmlsaXR5JykKICAgIGlmIG1hbmlmZXN0LmdldCgnZmVhdHVyZV9vcmRlcicpICE9IENBTkRJREFURV9GRUFUVVJFUzoKICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCd1bmV4cGVjdGVkIGNhbmRpZGF0ZSBmZWF0dXJlIG9yZGVyJykKICAgIGlmIG1hbmlmZXN0LmdldCgncGFyYW1zJywge30pLmdldCgnZWFybHlfc3RvcHBpbmcnKSBpcyBub3QgVHJ1ZToKICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdtYW5pZmVzdCBkb2VzIG5vdCByZWNvcmQgZXhwbGljaXQgZWFybHlfc3RvcHBpbmc9VHJ1ZScpCiAgICBiYXNlbGluZSA9IGxvYWRfYXJ0aWZhY3QoYmFzZWxpbmVfYXJ0aWZhY3RfZmlsZSkKICAgIGlmIG1hbmlmZXN0LmdldCgnY29ob3J0X3NoYTI1NicpICE9IGJhc2VsaW5lWydjb2hvcnRfc2hhMjU2X3NvcnRlZF9uZXdsaW5lJ106CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignY29ob3J0IG1hbmlmZXN0IG1pc21hdGNoJykKICAgIGNvaG9ydCA9IGxvYWRfY29ob3J0KGNvaG9ydF9maWxlLCBtYW5pZmVzdFsnY29ob3J0X3NoYTI1NiddKQogICAgYWNzID0gbG9hZF9hY3NfdGFibGUoYWNzX2ZpbGUsIG1hbmlmZXN0WydhY3NfdGFibGUnXVsnc2hhMjU2J10sIGNvaG9ydCkKICAgIG1vZGVscyA9IHt9CiAgICBmb3Igb3V0Y29tZSBpbiBPVVRDT01FUzoKICAgICAgICBuYW1lID0gJyVzX2Fjc19jYW5kaWRhdGVfJXMuam9ibGliJyAlIChvdXRjb21lLCB3aW5kb3cpCiAgICAgICAgbWV0YSA9IG1hbmlmZXN0Wydtb2RlbHMnXVtuYW1lXQogICAgICAgIGlmIG1ldGEuZ2V0KCdmZWF0dXJlcycpICE9IENBTkRJREFURV9GRUFUVVJFUzoKICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcigndW5leHBlY3RlZCBmZWF0dXJlIG9yZGVyIGZvciAnICsgbmFtZSkKICAgICAgICBwYXRoID0gZm9sZGVyIC8gbmFtZQogICAgICAgIGlmIF9zaGEyNTYocGF0aC5yZWFkX2J5dGVzKCkpICE9IG1ldGEuZ2V0KCdzaGEyNTYnKToKICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignbW9kZWwgY2hlY2tzdW0gbWlzbWF0Y2ggZm9yICcgKyBuYW1lKQogICAgICAgIG1vZGVsID0gam9ibGliLmxvYWQocGF0aCkKICAgICAgICBpZiBsaXN0KG1vZGVsLmNsYXNzZXNfKSAhPSBbMCwgMV0gb3IgbGlzdChtb2RlbC5mZWF0dXJlX25hbWVzX2luXykgIT0gQ0FORElEQVRFX0ZFQVRVUkVTOgogICAgICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCd1bmV4cGVjdGVkIG1vZGVsIGNsYXNzIG9yIGZpdHRlZCBmZWF0dXJlcyBmb3IgJyArIG5hbWUpCiAgICAgICAgaWYgbW9kZWwuZ2V0X3BhcmFtcygpLmdldCgnZWFybHlfc3RvcHBpbmcnKSBpcyBub3QgVHJ1ZToKICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignbW9kZWwgZWFybHlfc3RvcHBpbmcgaXMgbm90IGV4cGxpY2l0bHkgVHJ1ZSBmb3IgJyArIG5hbWUpCiAgICAgICAgbW9kZWxzW291dGNvbWVdID0gbW9kZWwKICAgIHZlcnNpb24gPSAnYWNzX2Jvb3N0ZWRfcG9zaXRpdmVfJXNfJXMnICUgKG1hbmlmZXN0LmdldCgncnVuX2lkJywgJ3Vua25vd24nKSwgd2luZG93KQogICAgcmV0dXJuIHsnbW9kZSc6ICdhY3NfY2FuZGlkYXRlJywgJ3ZlcnNpb24nOiB2ZXJzaW9uLCAnYmFzZWxpbmUnOiBiYXNlbGluZSwgJ2NvaG9ydCc6IGNvaG9ydCwKICAgICAgICAgICAgJ21vZGVscyc6IG1vZGVscywgJ2Fjcyc6IGFjcywgJ2ZhbGxiYWNrX3JlYXNvbic6IE5vbmV9CgoKZGVmIGxvYWRfc2NvcmVyKGZvbGRlciwgd2luZG93LCBjb2hvcnRfZmlsZSwgYmFzZWxpbmVfYXJ0aWZhY3RfZmlsZSwgYWNzX2ZpbGUpOgogICAgIiIiTG9hZCB0aGUgQUNTIGNhbmRpZGF0ZSwgb3IgZmFsbCBiYWNrIHRvIHRoZSBvcmlnaW5hbCBsb2dpc3RpYyBtb2RlbCBpZiBhbnkgYXJ0aWZhY3QgZmFpbHMuCgogICAgVGhlIGxvZ2lzdGljIGJhc2VsaW5lIGl0c2VsZiBtdXN0IGxvYWQ7IGlmIGl0IGNhbm5vdCwgdGhpcyByYWlzZXMgYW5kIHRoZSBydW4gZmFpbHMgY2xvc2VkLgogICAgIiIiCiAgICB0cnk6CiAgICAgICAgcmV0dXJuIGxvYWRfY2FuZGlkYXRlKGZvbGRlciwgd2luZG93LCBjb2hvcnRfZmlsZSwgYmFzZWxpbmVfYXJ0aWZhY3RfZmlsZSwgYWNzX2ZpbGUpCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGV4YzogICMgYW55IGNhbmRpZGF0ZS1hcnRpZmFjdCBmYWlsdXJlIHNlbGVjdHMgdGhlIGZhbGxiYWNrCiAgICAgICAgYmFzZWxpbmUgPSBsb2FkX2FydGlmYWN0KGJhc2VsaW5lX2FydGlmYWN0X2ZpbGUpCiAgICAgICAgY29ob3J0ID0gbG9hZF9jb2hvcnQoY29ob3J0X2ZpbGUsIGJhc2VsaW5lWydjb2hvcnRfc2hhMjU2X3NvcnRlZF9uZXdsaW5lJ10pCiAgICAgICAgcmV0dXJuIHsnbW9kZSc6ICdsb2dpc3RpY19mYWxsYmFjaycsICd2ZXJzaW9uJzogYmFzZWxpbmVbJ3ZlcnNpb24nXSwgJ2Jhc2VsaW5lJzogYmFzZWxpbmUsCiAgICAgICAgICAgICAgICAnY29ob3J0JzogY29ob3J0LCAnbW9kZWxzJzogTm9uZSwgJ2Fjcyc6IE5vbmUsCiAgICAgICAgICAgICAgICAnZmFsbGJhY2tfcmVhc29uJzogdHlwZShleGMpLl9fbmFtZV9fICsgJzogJyArIHN0cihleGMpWzoyMDBdfQoKCmRlZiBzY29yZShyZWNvcmRzLCBzY29yZXIsIHNjb3JlZF9hdCk6CiAgICAiIiJTY29yZSBvbmUgcG9sbCB3aXRoIHdoaWNoZXZlciBtb2RlbCBsb2FkZWQuIE5vIHdyaXRlcywgc2NoZWR1bGluZyBvciBhbGVydHMgc2VudC4iIiIKICAgIGlmIHNjb3JlclsnbW9kZSddID09ICdsb2dpc3RpY19mYWxsYmFjayc6CiAgICAgICAgcmV0dXJuIHNjb3JlX3NuYXBzaG90KHJlY29yZHMsIHNjb3JlclsnYmFzZWxpbmUnXSwgc2NvcmVyWydjb2hvcnQnXSwgc2NvcmVkX2F0KQogICAgbW9kZWxzLCBhY3MgPSBzY29yZXJbJ21vZGVscyddLCBzY29yZXJbJ2FjcyddCgogICAgZGVmIHN0YXRpb25fcHJvYmFiaWxpdHkob3V0Y29tZSwgc3RhdGlvbl9pZCwgc3RhdGlvbl9mZWF0dXJlcyk6CiAgICAgICAgaWYgc3RhdGlvbl9pZCBub3QgaW4gYWNzOgogICAgICAgICAgICByYWlzZSBTdGF0aW9uRmVhdHVyZU1pc3Npbmcoc3RhdGlvbl9pZCkKICAgICAgICByb3cgPSBsaXN0KHN0YXRpb25fZmVhdHVyZXMpICsgYWNzW3N0YXRpb25faWRdCiAgICAgICAgcmV0dXJuIG1vZGVsc1tvdXRjb21lXS5wcmVkaWN0X3Byb2JhKHBkLkRhdGFGcmFtZShbcm93XSwgY29sdW1ucz1DQU5ESURBVEVfRkVBVFVSRVMpKVswLCAxXQoKICAgIHJldHVybiBzY29yZV9zbmFwc2hvdChyZWNvcmRzLCBzY29yZXJbJ2Jhc2VsaW5lJ10sIHNjb3JlclsnY29ob3J0J10sIHNjb3JlZF9hdCwKICAgICAgICAgICAgICAgICAgICAgICAgICBzdGF0aW9uX3Byb2JhYmlsaXR5X2ZuPXN0YXRpb25fcHJvYmFiaWxpdHksIG1vZGVsX3ZlcnNpb249c2NvcmVyWyd2ZXJzaW9uJ10sCiAgICAgICAgICAgICAgICAgICAgICAgICAgcG9zaXRpdmVfb25seT1UcnVlKQo=', '471d291b72db9109bbe1e8d6e20d74b16021385618c13cc1852bf36e8afd3812'),
    'model_parameters.json': ('ewogICJ2ZXJzaW9uIjogImRldmVsb3BtZW50LTIwMjYtMTEtMDkiLAogICJza2xlYXJuX3ZlcnNpb24iOiAiMS43LjIiLAogICJjb2hvcnRfc2hhMjU2X3NvcnRlZF9uZXdsaW5lIjogIjNkNWJmNWNkMDU5NDk4MWZiNjBiOTk1YTA4ZjZjMzA1YjI5MjcxMjJhZTEyOTkwMjFlNDEzYTFiMzU0MTRmZjMiLAogICJjb2hvcnRfbm90ZSI6ICJyZWNvbnN0cnVjdGVkIGZyb20gY3VycmVudCBtZXRhZGF0YTsgb3JpZ2luYWwgSURzIG5vdCBzYXZlZCIsCiAgInRyYWluIjogIjIwMjYtMDgtMjQgdGhyb3VnaCAyMDI2LTA5LTI3IEFtZXJpY2EvTmV3X1lvcmsiLAogICJob3Jpem9uX21pbnV0ZXMiOiA2MCwKICAibW9kZWxzIjogewogICAgImJpa2UiOiB7CiAgICAgICJmZWF0dXJlX29yZGVyIjogWwogICAgICAgICJudW1fYmlrZXNfYXZhaWxhYmxlIiwKICAgICAgICAibnVtX2RvY2tzX2F2YWlsYWJsZSIsCiAgICAgICAgImNhcGFjaXR5IiwKICAgICAgICAiYmlrZV9mcmFjdGlvbiIsCiAgICAgICAgImRvY2tfZnJhY3Rpb24iLAogICAgICAgICJob3VyX3NpbiIsCiAgICAgICAgImhvdXJfY29zIgogICAgICBdLAogICAgICAibWVhbiI6IFsKICAgICAgICAxNC43OTgxNTk4MDY2MDI5MzYsCiAgICAgICAgMTIuOTAwMDc0MzAyMDY4NjAzLAogICAgICAgIDMwLjU5NjMxNzQ5NjMzNzgxOCwKICAgICAgICAwLjQ4NzUyMTczNDA1OTczMDksCiAgICAgICAgMC40MDkxODk3MzUyNjY1OTMxNCwKICAgICAgICAtMC4wMDQ3MDA5MDA5MzY2NjQ5MDEsCiAgICAgICAgLTAuMDA3ODMwMDE3NTg4ODA2NTI0CiAgICAgIF0sCiAgICAgICJzY2FsZSI6IFsKICAgICAgICAxMi43Mjg5NDQ2NDc4MDMyOTIsCiAgICAgICAgMTMuNDQxNTU1MDcxODU4MTE3LAogICAgICAgIDE2LjA2ODQ5NDM5NTg4NjgyLAogICAgICAgIDAuMjkzMjQ5NjU2MDE5NDk1NDMsCiAgICAgICAgMC4zMDAzNTY3NDYzMzEwMjM1LAogICAgICAgIDAuNzA3NDQ1NzQ1NzY0MTYzNCwKICAgICAgICAwLjcwNjcwODY0NTE2Nzk1NwogICAgICBdLAogICAgICAiY29lZiI6IFsKICAgICAgICAxLjM3ODE5ODM5NTQ1MjE5ODgsCiAgICAgICAgLTAuMzgzMDgxMjM4Nzc0NTgxNCwKICAgICAgICAwLjE1Njc4Njg3MjU4MTY5NjUsCiAgICAgICAgLTcuMDY0ODcyNDY2NzQzOTA4LAogICAgICAgIDAuMTQ5NzUzNTI1ODQ2MTgwMDQsCiAgICAgICAgMC4wNjQwMDYxMjQ2MDkxNzA5LAogICAgICAgIDAuMDA3MjY1OTM5ODczNjczMDc0CiAgICAgIF0sCiAgICAgICJpbnRlcmNlcHQiOiAtMTAuMTkzODA0Njg5MDExOTI1LAogICAgICAiY2xhc3NlcyI6IFsKICAgICAgICAwLAogICAgICAgIDEKICAgICAgXQogICAgfSwKICAgICJkb2NrIjogewogICAgICAiZmVhdHVyZV9vcmRlciI6IFsKICAgICAgICAibnVtX2Jpa2VzX2F2YWlsYWJsZSIsCiAgICAgICAgIm51bV9kb2Nrc19hdmFpbGFibGUiLAogICAgICAgICJjYXBhY2l0eSIsCiAgICAgICAgImJpa2VfZnJhY3Rpb24iLAogICAgICAgICJkb2NrX2ZyYWN0aW9uIiwKICAgICAgICAiaG91cl9zaW4iLAogICAgICAgICJob3VyX2NvcyIKICAgICAgXSwKICAgICAgIm1lYW4iOiBbCiAgICAgICAgMTQuNzk4MTU5ODA2NjAyOTM2LAogICAgICAgIDEyLjkwMDA3NDMwMjA2ODYwMywKICAgICAgICAzMC41OTYzMTc0OTYzMzc4MTgsCiAgICAgICAgMC40ODc1MjE3MzQwNTk3MzA5LAogICAgICAgIDAuNDA5MTg5NzM1MjY2NTkzMTQsCiAgICAgICAgLTAuMDA0NzAwOTAwOTM2NjY0OTAxLAogICAgICAgIC0wLjAwNzgzMDAxNzU4ODgwNjUyNAogICAgICBdLAogICAgICAic2NhbGUiOiBbCiAgICAgICAgMTIuNzI4OTQ0NjQ3ODAzMjkyLAogICAgICAgIDEzLjQ0MTU1NTA3MTg1ODExNywKICAgICAgICAxNi4wNjg0OTQzOTU4ODY4MiwKICAgICAgICAwLjI5MzI0OTY1NjAxOTQ5NTQzLAogICAgICAgIDAuMzAwMzU2NzQ2MzMxMDIzNSwKICAgICAgICAwLjcwNzQ0NTc0NTc2NDE2MzQsCiAgICAgICAgMC43MDY3MDg2NDUxNjc5NTcKICAgICAgXSwKICAgICAgImNvZWYiOiBbCiAgICAgICAgMC4yNTIwNDI1NzAzMzAwMDU5LAogICAgICAgIDMuNjg1NDMyMDg2NDQ2NzkwNywKICAgICAgICAtMC43NzA1OTIzMTAyNTc5NDM0LAogICAgICAgIC0wLjMxODY2MTA5NzE2MzQwNTU3LAogICAgICAgIC04LjY5NTcxNTI4NTA1OTIxMywKICAgICAgICAtMC4wNjI4MzA0NTgyMDYzOTk2NCwKICAgICAgICAwLjE5MjEyNjk3NDQ3ODA3NDE3CiAgICAgIF0sCiAgICAgICJpbnRlcmNlcHQiOiAtNy45NDkxMjE1NzI3MDQ2MDMsCiAgICAgICJjbGFzc2VzIjogWwogICAgICAgIDAsCiAgICAgICAgMQogICAgICBdCiAgICB9CiAgfQp9Cg==', '194bc9593a29d796eb192dcef7c065d6c4c362465268925609fe30cf15fada25'),
}
pkg = Path(tempfile.mkdtemp(prefix='citibike_v14_'))
for name, (b64, sha) in PKG_FILES.items():
    data = base64.b64decode(b64)
    assert hashlib.sha256(data).hexdigest() == sha, 'embedded file changed: ' + name
    (pkg / name).write_bytes(data)
(pkg / 'cohort.csv').write_text('selection_order,station_id\n' + '\n'.join('%d,%s' % (i + 1, s) for i, s in enumerate(sorted(C))) + '\n')
sys.path.insert(0, str(pkg))
from candidate_scorer_acs import load_candidate, CANDIDATE_FEATURES, ACS_FEATURES
from scoring import probability, load_artifact
CAND_DIR = home / 'Citi Bike - ACS Controls and Neighborhood Audit - ea8df268'
ACS_FILE = home / 'Citi Bike - ACS and July Trip Development - e1eb6e26' / 'acs_station_features.parquet'
# Confirm the model files and the ACS table match the freeze record; this stops if a rebuilt model differs
FROZEN = {'bike_acs_candidate_train_before_Sep28.joblib': 'aa74b79e7c67276cc8d91895eefb56ec659c620182a6f021f41509eb5febaea4', 'dock_acs_candidate_train_before_Sep28.joblib': 'd5ec6ce138da03d111ca912f9523bda5659939a6585400a772ec3c7c4c875dd8', 'manifest.json': 'f130a24ae73e151826d184703f21a153508328a06bc5c7c019ab94ff0d77a196'}
for name, sha in FROZEN.items():
    assert hashlib.sha256((CAND_DIR / name).read_bytes()).hexdigest() == sha, 'frozen file changed: ' + name
assert hashlib.sha256(ACS_FILE.read_bytes()).hexdigest() == '3332babcb605f43c85f60b979bcda82c66e1c5c1199167118f1a333dbc9b84f9'
# Load the histogram gradient boosting models and the logistic regression model
cand = load_candidate(CAND_DIR, 'train_before_Sep28', pkg / 'cohort.csv', pkg / 'model_parameters.json', ACS_FILE)
logit = load_artifact(pkg / 'model_parameters.json')
acs = pd.read_parquet(ACS_FILE); acs['station_id'] = acs.station_id.astype(str)
for k in ACS_FEATURES: acs[k] = pd.to_numeric(acs[k], errors='coerce')
station_acs = acs.set_index('station_id')[ACS_FEATURES]
SUMMARY['methods'] = {'acs_candidate': cand['version'], 'logistic': logit['version'], 'low_count': 'five lowest positive current counts, ties by station_id'}
print('MODELS_LOADED', json.dumps(SUMMARY['methods']), flush=True)

# COMMAND ----------

# Cell 5: score once, select alerts, count true positives
# Write the marker first, so any rerun stops in cell 1 before reading test data
with open(out_dir / 'SCORING_STARTED.txt', 'x') as f:
    f.write('Test-week scoring started ' + pd.Timestamp.now('UTC').isoformat() + ' under freeze record ' + FREEZE_RECORD_SHA256 + '. Do not rerun.\n')
OUTCOMES = [('bike', 'num_bikes_available'), ('dock', 'num_docks_available')]
pools, picks, rows = {}, {}, []
## Helper: the five highest values per poll (ties broken by station ID)
def top5(pool, col, ascending):
    return pool.sort_values(['fetched_at', col, 'station_id'], ascending=[True, ascending, True]).groupby('fetched_at', sort=False).head(5)
for o, count in OUTCOMES:
    lab = o + '_label'
    pool = examples[examples[o + '_ok'] & (examples[count] > 0)].copy()
    X = pool[features].copy()
    for c in ACS_FEATURES: X[c] = pool.station_id.map(station_acs[c]).to_numpy()
    # Risk Scores from histogram gradient boosting and from logistic regression
    pool['p_acs'] = cand['models'][o].predict_proba(X[CANDIDATE_FEATURES])[:, 1]
    pool['p_logistic'] = [probability(logit['models'][o], x) for x in pool[features].to_numpy().tolist()]
    pools[o] = pool
    picks[o, 'acs_candidate'] = top5(pool, 'p_acs', False)
    picks[o, 'logistic'] = top5(pool, 'p_logistic', False)
    picks[o, 'low_count'] = top5(pool, count, True)
    for mth in ['acs_candidate', 'logistic', 'low_count']:
        # Count true positives (the caught field) and unknown outcomes for each method
        pk = picks[o, mth]; known = int(pk[lab].notna().sum()); caught = int(pk[lab].sum())
        rows.append({'outcome': o, 'method': mth, 'candidates': len(pool), 'polls': int(pool.fetched_at.nunique()), 'alerts': len(pk), 'caught': caught, 'known_negative': known - caught, 'unknown': len(pk) - known, 'precision_known': round(caught / known, 4) if known else None, 'pool_known_shortages': int(pool[lab].sum())})
res = pd.DataFrame(rows)
SUMMARY['results'] = rows
print(res.to_string(index=False), flush=True)

# COMMAND ----------

# Cell 6: paired station-day resampling (1,000 draws, fixed seed) and neighborhood audit
## Paired station-day intervals: histogram gradient boosting against each baseline
rng = np.random.default_rng(20261110); B = 1000; intervals = []
for o, count in OUTCOMES:
    lab = o + '_label'; u = pools[o][['station_id', 'fetched_at']].copy(); u['day'] = u.fetched_at.dt.tz_convert('America/New_York').dt.date
    idx = u[['station_id', 'day']].drop_duplicates().set_index(['station_id', 'day']).index
    def sd(frame):
        x = frame[['station_id', 'fetched_at', lab]].copy(); x['day'] = x.fetched_at.dt.tz_convert('America/New_York').dt.date; x['c'] = x[lab].fillna(0)
        return x.groupby(['station_id', 'day']).c.sum().reindex(idx, fill_value=0)
    for a, b in [('acs_candidate', 'logistic'), ('acs_candidate', 'low_count')]:
        d = (sd(picks[o, a]) - sd(picks[o, b])).to_numpy(); draws = d[rng.integers(0, len(d), size=(B, len(d)))].sum(axis=1)
        intervals.append({'outcome': o, 'method': a, 'versus': b, 'difference': int(d.sum()), 'lo95': float(np.quantile(draws, .025)), 'hi95': float(np.quantile(draws, .975)), 'station_days': len(d)})
SUMMARY['intervals'] = intervals

def tertiles(col):
    g = acs[['station_id', col]].copy(); g['group'] = 'missing'; k = g[col].notna()
    g.loc[k, 'group'] = pd.qcut(g.loc[k, col].rank(method='first'), 3, labels=['lower', 'middle', 'higher']).astype(str)
    return g.set_index('station_id').group
GROUPINGS = {'poverty_share': tertiles('poverty_pct_500m'), 'no_vehicle_share': tertiles('households_no_vehicle_pct_500m')}
st_rows = []
for o, count in OUTCOMES:
    lab = o + '_label'
    el = pools[o].groupby('station_id')[lab].agg(eligible='size', eligible_known='count', positives='sum')
    for mth in ['acs_candidate', 'logistic', 'low_count']:
        al = picks[o, mth].groupby('station_id')[lab].agg(alerts='size', alert_known='count', caught='sum')
        st = pd.DataFrame(index=pd.Index(sorted(C), name='station_id')).join(el).join(al).fillna(0); st['outcome'] = o; st['method'] = mth
        st_rows.append(st.reset_index())
stations = pd.concat(st_rows, ignore_index=True)
for gname, gmap in GROUPINGS.items(): stations[gname] = stations.station_id.map(gmap)
audit, cluster = {}, []
## Station-area group audit with station-cluster resampling
rng = np.random.default_rng(20261112); BC = 3000
for gname in GROUPINGS:
    t = stations.groupby(['outcome', 'method', gname])[['eligible', 'eligible_known', 'positives', 'alerts', 'alert_known', 'caught']].sum().reset_index()
    tot = t.groupby(['outcome', 'method'])[['alerts', 'positives']].transform('sum')
    t['capture'] = t.caught / t.positives.replace(0, np.nan); t['share_of_alerts'] = t.alerts / tot.alerts; t['share_of_known_shortages'] = t.positives / tot.positives
    t['eligible_unknown_rate'] = 1 - t.eligible_known / t.eligible.replace(0, np.nan); t['alert_unknown_rate'] = 1 - t.alert_known / t.alerts.replace(0, np.nan)
    audit[gname] = t.rename(columns={gname: 'group'})
    for (o, mth), sub in stations.groupby(['outcome', 'method']):
        hi = sub[sub[gname] == 'higher'][['positives', 'caught']].to_numpy(); lo = sub[sub[gname] == 'lower'][['positives', 'caught']].to_numpy()
        point = hi[:, 1].sum() / hi[:, 0].sum() - lo[:, 1].sum() / lo[:, 0].sum()
        hs = hi[rng.integers(0, len(hi), size=(BC, len(hi)))].sum(axis=1); ls = lo[rng.integers(0, len(lo), size=(BC, len(lo)))].sum(axis=1)
        dd = hs[:, 1] / hs[:, 0] - ls[:, 1] / ls[:, 0]
        cluster.append({'grouping': gname, 'outcome': o, 'method': mth, 'higher_minus_lower_capture': round(float(point), 4), 'lo95': round(float(np.nanquantile(dd, .025)), 4), 'hi95': round(float(np.nanquantile(dd, .975)), 4)})
SUMMARY['audit'] = {g: audit[g].round(4).to_dict('records') for g in audit}
SUMMARY['audit_cluster_intervals'] = cluster
SUMMARY['notes'] = 'Single pre-registered evaluation of the reserved week; results reported regardless of direction. The week was partly exposed earlier through aggregate label counts. Availability times are simulated from collection times. Counts are snapshots, not distinct incidents. Poverty share is both a model input and an audit grouping. Groups describe neighborhood estimates, not riders.'
print('INTERVALS', json.dumps(intervals), flush=True)

# COMMAND ----------

# Cell 7: save
## Save the tables and the summary to the private result folder
res.to_parquet(out_dir / 'test_results.parquet'); stations.to_parquet(out_dir / 'test_audit_station_level.parquet')
for g in audit: audit[g].to_parquet(out_dir / ('test_audit_groups_%s.parquet' % g))
SUMMARY['finished_utc'] = pd.Timestamp.now('UTC').isoformat()
(out_dir / 'test_summary.json').write_text(json.dumps(SUMMARY, indent=2, default=str))
print('SAVED', out_dir.name, sorted(p.name for p in out_dir.iterdir()), flush=True)
