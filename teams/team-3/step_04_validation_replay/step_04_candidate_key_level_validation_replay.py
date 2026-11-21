# Databricks notebook source
# MAGIC %md
# MAGIC # Citi Bike: key-level validation replay of the histogram gradient boosting model (2026-11-10)
# MAGIC Replays the Sep 28 to Oct 4 validation week, poll by poll, through the prototype v14 scorer and compares it with a batch prediction from the same candidate files.
# MAGIC Safe to run top to bottom with Run all. Reads only Aug 24 to Oct 4 data. Never reads the reserved Oct 5 to 11 week. No S3 writes. Writes only a new private folder.
# MAGIC Pass criteria: identical scored keys, identical selected alert keys, identical labels on selected alerts, and maximum Risk Score difference below 1e-12.

# COMMAND ----------

# Cell 1: setup and guards
# Team 3 commented copy (step 04). Comments were added on November 16, 2026; the code is unchanged.
# Terms used in these comments (they match the v06 reports and the v06.1 deck):
#   Poll: one fetch of the public Citi Bike station feed.
#   Risk Score: a model's predicted probability of a shortage; the five highest Risk Scores per poll become alerts.
#   True positive: an alert (a flagged station observation) with a confirmed shortage at the target time.
#   Recall = true positives / all confirmed shortages among eligible stations.
#   Precision = true positives / alerts with known outcomes.
#   Methods: histogram gradient boosting (scikit-learn HistGradientBoostingClassifier), logistic regression,
#   and the low-count rule (the stations with the fewest bikes or open docks right now).
#   Code names such as `caught`, `probability` and `acs_candidate` are kept so saved results still load.
# Purpose: prove that the pilot scorer, fed one poll at a time, gives exactly the same scores and
# alerts as a batch prediction from the same approved histogram gradient boosting model files on the Sep 28 to Oct 4 validation week.
## Packages
import io, json, hashlib, uuid, re, base64, gzip, time, sys, tempfile, importlib
from collections import Counter
import boto3, pandas as pd, numpy as np, pyarrow.parquet as pq
import sklearn, joblib
from pathlib import Path
from botocore.config import Config
## S3 connection (keys are read at run time from the Databricks secret scope, never stored here)
scope = 'citibike-team3-secrets'; bucket = 'cuny-sps-citibike-archive'
client = boto3.client('s3', aws_access_key_id=dbutils.secrets.get(scope, 'aws_access_key_id'), aws_secret_access_key=dbutils.secrets.get(scope, 'aws_secret_access_key'), region_name='us-west-2', config=Config(connect_timeout=10, read_timeout=30, retries={'max_attempts': 2}))
## Date bounds: training starts Aug 24, validation starts Sep 28, development data ends before Oct 5 (New York midnight in UTC)
START = pd.Timestamp('2026-08-24T04:00:00Z'); VALID = pd.Timestamp('2026-09-28T04:00:00Z'); END = pd.Timestamp('2026-10-05T04:00:00Z')
RESERVED_TEST_START = pd.Timestamp('2026-10-05T04:00:00Z')
# Confirm the development data can never reach the reserved test week
assert END <= RESERVED_TEST_START
## Output folder: a new, uniquely named folder in the current user's workspace
user = spark.sql('select current_user() as u').first()['u']
home = Path('/Workspace/Users') / user
RUN_ID = uuid.uuid4().hex[:8]
out_dir = home / ('Citi Bike - Candidate A Key-Level Replay - ' + RUN_ID)
out_dir.mkdir(parents=False, exist_ok=False)
## Run summary, filled in by each cell and saved at the end
SUMMARY = {'run_id': RUN_ID, 'output_folder': out_dir.name, 'sklearn_version': sklearn.__version__, 'reserved_test_read': False, 's3_writes': 0, 'started_utc': pd.Timestamp.now('UTC').isoformat()}
# Display the setup summary
print('SETUP', json.dumps(SUMMARY), flush=True)

# COMMAND ----------

# Cell 2: rebuild development data by running cells 2 to 4 of the modeling notebook, after checking their exact text
## Rebuild the development examples by running cells 2 to 4 of the step 03 notebook (cohort, labels, ACS table)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat
# Fingerprints of the exact text of step 03 cells 2, 3 and 4. If anyone edits those cells, this replay stops.
EXPECTED_CELLS = {2: 'eea64538ff96e84aab789c64d93b65bbd5e727dbd702509880c431524dc5d36e', 3: '43dcd5bd9ba29ed85f60acc989cc266ea79cc64688ee1994f8197f23c8d7dfb2', 4: 'e11c4d73e12908ae6db57ad70ebceb803b8bb006c928dee239355a5e0a7e7e52'}
# Export the step 03 notebook source from the workspace
src = base64.b64decode(WorkspaceClient().workspace.export(str(Path('/Users') / user / 'Citi Bike — ACS Controls, Intervals and Neighborhood Audit 2026-11-10'), format=ExportFormat.SOURCE).content).decode('utf-8')
cells = src.split('# COMMAND ' + '-' * 10)
# Confirm each cell is unchanged before running it
for i, h in EXPECTED_CELLS.items():
    assert hashlib.sha256(cells[i].strip().encode()).hexdigest() == h, 'modeling notebook cell %d changed' % i
# Run the three cells here, so this notebook uses the same examples, cohort (C) and ACS table
for i in (2, 3, 4):
    exec(cells[i].strip(), globals())
# Confirm step 03's population counts reproduced
assert SUMMARY['population_reproduction']['match']
print('DATA_READY', len(examples), 'ACS sha256', ACS_SHA, flush=True)

# COMMAND ----------

# Cell 3: write the prototype v14 scorer files (embedded, hash-checked) and load the histogram gradient boosting model
## Embedded prototype v14 files (base64 text and SHA-256 for each): scorer, normalization, ACS scorer and logistic parameters
PKG_FILES = {
    'scoring.py': ('IiIiRGV2ZWxvcG1lbnQtb25seSwgaW4tbWVtb3J5IDYwLW1pbnV0ZSBDaXRpIEJpa2Ugc2NvcmVyOyBubyBuZXR3b3JrIG9yIHdyaXRlcy4iIiIKaW1wb3J0IGNzdgppbXBvcnQgaGFzaGxpYgppbXBvcnQganNvbgppbXBvcnQgbWF0aApmcm9tIGRhdGV0aW1lIGltcG9ydCBkYXRldGltZSwgdGltZWRlbHRhLCB0aW1lem9uZQpmcm9tIHpvbmVpbmZvIGltcG9ydCBab25lSW5mbwpmcm9tIGNvbGxlY3Rpb25zIGltcG9ydCBDb3VudGVyCmZyb20gbm9ybWFsaXphdGlvbiBpbXBvcnQgZmllbGRzCgpGRUFUVVJFUyA9IFsnbnVtX2Jpa2VzX2F2YWlsYWJsZScsICdudW1fZG9ja3NfYXZhaWxhYmxlJywgJ2NhcGFjaXR5JywgJ2Jpa2VfZnJhY3Rpb24nLAogICAgICAgICAgICAnZG9ja19mcmFjdGlvbicsICdob3VyX3NpbicsICdob3VyX2NvcyddCk5ZID0gWm9uZUluZm8oJ0FtZXJpY2EvTmV3X1lvcmsnKQoKCmRlZiB0aW1lc3RhbXAodmFsdWUpOgogICAgaWYgbm90IGlzaW5zdGFuY2UodmFsdWUsIHN0cik6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcigndGltZXN0YW1wcyBtdXN0IGJlIElTTyA4NjAxIHN0cmluZ3Mgd2l0aCB0aW1lem9uZScpCiAgICByZXN1bHQgPSBkYXRldGltZS5mcm9taXNvZm9ybWF0KHZhbHVlLnJlcGxhY2UoJ1onLCAnKzAwOjAwJykpCiAgICBpZiByZXN1bHQudHppbmZvIGlzIE5vbmUgb3IgcmVzdWx0LnV0Y29mZnNldCgpIGlzIE5vbmU6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcigndGltZXN0YW1wIGxhY2tzIHRpbWV6b25lJykKICAgIHJldHVybiByZXN1bHQuYXN0aW1lem9uZSh0aW1lem9uZS51dGMpCgoKZGVmIGxvYWRfY29ob3J0KHBhdGgsIGV4cGVjdGVkX2RpZ2VzdCk6CiAgICB3aXRoIG9wZW4ocGF0aCwgbmV3bGluZT0nJywgZW5jb2Rpbmc9J3V0Zi04JykgYXMgZjoKICAgICAgICBpZHMgPSBbcm93WydzdGF0aW9uX2lkJ10gZm9yIHJvdyBpbiBjc3YuRGljdFJlYWRlcihmKV0KICAgIGlmIGxlbihpZHMpICE9IDEwMCBvciBsZW4oc2V0KGlkcykpICE9IDEwMCBvciBhbnkobm90IHggZm9yIHggaW4gaWRzKToKICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdjb2hvcnQgbXVzdCBjb250YWluIDEwMCBkaXN0aW5jdCBzdGF0aW9uIElEcycpCiAgICBkaWdlc3QgPSBoYXNobGliLnNoYTI1NignXG4nLmpvaW4oc29ydGVkKGlkcykpLmVuY29kZSgpKS5oZXhkaWdlc3QoKQogICAgaWYgZGlnZXN0ICE9IGV4cGVjdGVkX2RpZ2VzdDoKICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdjb2hvcnQgaGFzaCBkb2VzIG5vdCBtYXRjaCBtb2RlbCBhcnRpZmFjdCcpCiAgICByZXR1cm4gc2V0KGlkcykKCgpkZWYgbG9hZF9hcnRpZmFjdChwYXRoKToKICAgIHdpdGggb3BlbihwYXRoLCBlbmNvZGluZz0ndXRmLTgnKSBhcyBmOgogICAgICAgIGEgPSBqc29uLmxvYWQoZikKICAgIGlmIGFbJ2hvcml6b25fbWludXRlcyddICE9IDYwIG9yIHNldChhWydtb2RlbHMnXSkgIT0geydiaWtlJywnZG9jayd9OgogICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ3VuZXhwZWN0ZWQgbW9kZWwgYXJ0aWZhY3QnKQogICAgZm9yIG0gaW4gYVsnbW9kZWxzJ10udmFsdWVzKCk6CiAgICAgICAgaWYgbVsnZmVhdHVyZV9vcmRlciddICE9IEZFQVRVUkVTIG9yIG1bJ2NsYXNzZXMnXSAhPSBbMCwgMV06CiAgICAgICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ3VuZXhwZWN0ZWQgZmVhdHVyZSBvcmRlciBvciBjbGFzcyBlbmNvZGluZycpCiAgICAgICAgaWYgYW55KGxlbihtW2tdKSAhPSBsZW4oRkVBVFVSRVMpIGZvciBrIGluICgnbWVhbicsJ3NjYWxlJywnY29lZicpKToKICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignaW52YWxpZCBtb2RlbCBkaW1lbnNpb25zJykKICAgICAgICBpZiBub3QgYWxsKG1hdGguaXNmaW5pdGUoZmxvYXQoeCkpIGZvciBrIGluICgnbWVhbicsJ3NjYWxlJywnY29lZicpIGZvciB4IGluIG1ba10pOgogICAgICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdub24tZmluaXRlIG1vZGVsIHBhcmFtZXRlcicpCiAgICAgICAgaWYgbm90IGFsbChmbG9hdCh4KSA+IDAgZm9yIHggaW4gbVsnc2NhbGUnXSkgb3Igbm90IG1hdGguaXNmaW5pdGUoZmxvYXQobVsnaW50ZXJjZXB0J10pKToKICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignaW52YWxpZCBtb2RlbCBwYXJhbWV0ZXInKQogICAgcmV0dXJuIGEKCgpkZWYgcHJvYmFiaWxpdHkobW9kZWwsIGZlYXR1cmVzKToKICAgIHogPSBmbG9hdChtb2RlbFsnaW50ZXJjZXB0J10pICsgc3VtKGZsb2F0KGMpICogKGZsb2F0KHgpLWZsb2F0KG11KSkvZmxvYXQoc2QpCiAgICAgICAgZm9yIHgsbXUsc2QsYyBpbiB6aXAoZmVhdHVyZXMsIG1vZGVsWydtZWFuJ10sIG1vZGVsWydzY2FsZSddLCBtb2RlbFsnY29lZiddKSkKICAgIHJldHVybiAxLygxK21hdGguZXhwKC16KSkgaWYgeiA+PSAwIGVsc2UgbWF0aC5leHAoeikvKDErbWF0aC5leHAoeikpCgoKZGVmIHNjb3JlX3NuYXBzaG90KHJlY29yZHMsIG1vZGVsX2FydGlmYWN0LCBjb2hvcnQsIHNjb3JlZF9hdCwgKiwgcHJvYmFiaWxpdHlfZm49Tm9uZSwgbW9kZWxfdmVyc2lvbj1Ob25lLCBwb3NpdGl2ZV9vbmx5PUZhbHNlLCBzdGF0aW9uX3Byb2JhYmlsaXR5X2ZuPU5vbmUpOgogICAgIiIiU2NvcmUgb25lIHBvbGwuIFJlcXVpcmVkIGZpZWxkcyBkb2N1bWVudGVkIGluIFJFQURNRS4gUmV0dXJucyByZWNvcmRzIGFuZCBhbGVydHMuCgogICAgQ2FsbGVyIG11c3Qgc3VwcGx5IHBvaW50LWluLXRpbWUgbWV0YWRhdGEgYW5kIGNvbGxlY3Rpb24gY29tcGxldGlvbiB0aW1lLiBBIG1hbGZvcm1lZAogICAgc25hcHNob3QgZmFpbHMgY2xvc2VkLiBDdXJyZW50bHkgZW1wdHkgc3RhdGlvbnMgYXJlIHNjb3JlZCwgYnV0IGV4Y2x1ZGVkIGZyb20gYWxlcnRzLgogICAgIiIiCiAgICBub3cgPSB0aW1lc3RhbXAoc2NvcmVkX2F0KQogICAgc2Vlbiwgc2NvcmVkLCBjYW5kaWRhdGVzID0gc2V0KCksIFtdLCB7J2Jpa2UnOiBbXSwgJ2RvY2snOiBbXX0KICAgIHJlamVjdGVkID0gQ291bnRlcigpCiAgICBwb2xsX2tleSA9IE5vbmUKICAgIGZvciByIGluIHJlY29yZHM6CiAgICAgICAgc2lkID0gclsnc3RhdGlvbl9pZCddCiAgICAgICAgaWYgc2lkIGluIHNlZW46CiAgICAgICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ2R1cGxpY2F0ZSBzdGF0aW9uIElEIGluIHNuYXBzaG90JykKICAgICAgICBzZWVuLmFkZChzaWQpCiAgICAgICAgZmV0Y2hlZCA9IHRpbWVzdGFtcChyWydmZXRjaGVkX2F0J10pCiAgICAgICAgYXZhaWxhYmxlID0gdGltZXN0YW1wKHJbJ2F2YWlsYWJsZV9hdCddKQogICAgICAgIGlmIHBvbGxfa2V5IGlzIE5vbmU6CiAgICAgICAgICAgIHBvbGxfa2V5ID0gKGZldGNoZWQsIGF2YWlsYWJsZSkKICAgICAgICBlbGlmIHBvbGxfa2V5ICE9IChmZXRjaGVkLCBhdmFpbGFibGUpOgogICAgICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdhbGwgcm93cyBtdXN0IGJlbG9uZyB0byBvbmUgcG9sbCcpCiAgICAgICAgaWYgc2lkIG5vdCBpbiBjb2hvcnQ6CiAgICAgICAgICAgIHJlamVjdGVkWydvdXRzaWRlX2NvaG9ydCddICs9IDEKICAgICAgICAgICAgY29udGludWUKICAgICAgICB0cnk6CiAgICAgICAgICAgIHJlcG9ydGVkID0gdGltZXN0YW1wKHJbJ3N0YXRpb25fcmVwb3J0ZWRfYXQnXSkKICAgICAgICAgICAgbWV0YWRhdGFfYXQgPSB0aW1lc3RhbXAoclsnbWV0YWRhdGFfYXZhaWxhYmxlX2F0J10pCiAgICAgICAgZXhjZXB0IChLZXlFcnJvciwgVmFsdWVFcnJvciwgVHlwZUVycm9yLCBPdmVyZmxvd0Vycm9yKToKICAgICAgICAgICAgcmVqZWN0ZWRbJ2ludmFsaWRfc3RhdGlvbl9vcl9tZXRhZGF0YV90aW1lc3RhbXAnXSArPSAxCiAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgIyBUcmFpbmluZyB0aW1lc3RhbXBzIHVzZSB0aGUgcG9sbC1zdGFydCB0aW1lLiBBIGxhdGVyIGF2YWlsYWJpbGl0eSB0aW1lIGlzCiAgICAgICAgIyBuZWNlc3NhcnkgZm9yIGxpdmUgdXNlIGFuZCBpcyBzZXBhcmF0ZWx5IGNoZWNrZWQgYWdhaW5zdCB0aGUgc2NvcmluZyBjbG9jay4KICAgICAgICBpZiBub3QgKG1ldGFkYXRhX2F0IDw9IGZldGNoZWQgPD0gYXZhaWxhYmxlIDw9IG5vdyk6CiAgICAgICAgICAgIHJlamVjdGVkWydpbnZhbGlkX2F2YWlsYWJpbGl0eV9vcmRlciddICs9IDEKICAgICAgICAgICAgY29udGludWUKICAgICAgICBpZiAoZmV0Y2hlZC1yZXBvcnRlZCkudG90YWxfc2Vjb25kcygpIDwgMDoKICAgICAgICAgICAgcmVqZWN0ZWRbJ3JlcG9ydF9hZnRlcl9wb2xsX3N0YXJ0X2Nsb2NrX3NrZXcnXSArPSAxCiAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgaWYgbm90ICgwIDw9IChmZXRjaGVkLXJlcG9ydGVkKS50b3RhbF9zZWNvbmRzKCkgPD0gMzAwIGFuZAogICAgICAgICAgICAgICAgMCA8PSAobm93LXJlcG9ydGVkKS50b3RhbF9zZWNvbmRzKCkgPD0gMzAwKToKICAgICAgICAgICAgcmVqZWN0ZWRbJ3N0YWxlX3JlcG9ydCddICs9IDEKICAgICAgICAgICAgY29udGludWUKICAgICAgICB0cnk6CiAgICAgICAgICAgIGJpa2VzLCBkb2NrcywgY2FwYWNpdHksIGluc3RhbGxlZCwgcmVudGluZywgcmV0dXJuaW5nID0gZmllbGRzKHIpCiAgICAgICAgZXhjZXB0IChLZXlFcnJvciwgVmFsdWVFcnJvciwgVHlwZUVycm9yLCBPdmVyZmxvd0Vycm9yKToKICAgICAgICAgICAgcmVqZWN0ZWRbJ2ludmFsaWRfbnVtZXJpY19vcl9mbGFnJ10gKz0gMQogICAgICAgICAgICBjb250aW51ZQogICAgICAgIGlmIGNhcGFjaXR5IDw9IDAgb3IgYmlrZXMgPCAwIG9yIGRvY2tzIDwgMDoKICAgICAgICAgICAgcmVqZWN0ZWRbJ2ludmFsaWRfY291bnRzX29yX2NhcGFjaXR5J10gKz0gMQogICAgICAgICAgICBjb250aW51ZQogICAgICAgIGlmIG5vdCBpbnN0YWxsZWQ6CiAgICAgICAgICAgIHJlamVjdGVkWydub3RfaW5zdGFsbGVkJ10gKz0gMQogICAgICAgICAgICBjb250aW51ZQogICAgICAgIGxvY2FsID0gZmV0Y2hlZC5hc3RpbWV6b25lKE5ZKQogICAgICAgIHBoYXNlID0gKGxvY2FsLmhvdXIgKyBsb2NhbC5taW51dGUvNjAgKyBsb2NhbC5zZWNvbmQvMzYwMCkqMiptYXRoLnBpLzI0CiAgICAgICAgeCA9IFtiaWtlcyxkb2NrcyxjYXBhY2l0eSxiaWtlcy9jYXBhY2l0eSxkb2Nrcy9jYXBhY2l0eSxtYXRoLnNpbihwaGFzZSksbWF0aC5jb3MocGhhc2UpXQogICAgICAgIGZvciBvdXRjb21lLCBlbmFibGVkLCBjb3VudCBpbiBbKCdiaWtlJyxyZW50aW5nLGJpa2VzKSwoJ2RvY2snLHJldHVybmluZyxkb2NrcyldOgogICAgICAgICAgICBpZiBub3QgZW5hYmxlZDoKICAgICAgICAgICAgICAgIHJlamVjdGVkW291dGNvbWUgKyAnX3NlcnZpY2VfZGlzYWJsZWQnXSArPSAxCiAgICAgICAgICAgICAgICBjb250aW51ZQogICAgICAgICAgICBpZiBwb3NpdGl2ZV9vbmx5IGFuZCBjb3VudCA9PSAwOgogICAgICAgICAgICAgICAgcmVqZWN0ZWRbb3V0Y29tZSArICdfYWxyZWFkeV9lbXB0eSddICs9IDEKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAgIGlmIHN0YXRpb25fcHJvYmFiaWxpdHlfZm4gaXMgbm90IE5vbmU6CiAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgcmlzayA9IGZsb2F0KHN0YXRpb25fcHJvYmFiaWxpdHlfZm4ob3V0Y29tZSxzaWQseCkpCiAgICAgICAgICAgICAgICBleGNlcHQgU3RhdGlvbkZlYXR1cmVNaXNzaW5nOgogICAgICAgICAgICAgICAgICAgIHJlamVjdGVkW291dGNvbWUgKyAnX3N0YXRpb25fZmVhdHVyZXNfbWlzc2luZyddICs9IDEKICAgICAgICAgICAgICAgICAgICBjb250aW51ZQogICAgICAgICAgICBlbHNlOgogICAgICAgICAgICAgICAgcmlzayA9IHByb2JhYmlsaXR5KG1vZGVsX2FydGlmYWN0Wydtb2RlbHMnXVtvdXRjb21lXSx4KSBpZiBwcm9iYWJpbGl0eV9mbiBpcyBOb25lIGVsc2UgZmxvYXQocHJvYmFiaWxpdHlfZm4ob3V0Y29tZSx4KSkKICAgICAgICAgICAgaWYgbm90IG1hdGguaXNmaW5pdGUocmlzaykgb3Igbm90IDAgPD0gcmlzayA8PSAxOgogICAgICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignbW9kZWwgcmV0dXJuZWQgYW4gaW52YWxpZCBwcm9iYWJpbGl0eScpCiAgICAgICAgICAgIGl0ZW0gPSB7J3N0YXRpb25faWQnOnNpZCwnb3V0Y29tZSc6b3V0Y29tZSwnZmV0Y2hlZF9hdCc6ZmV0Y2hlZC5pc29mb3JtYXQoKSwKICAgICAgICAgICAgICAgICAgICAnYXZhaWxhYmxlX2F0JzphdmFpbGFibGUuaXNvZm9ybWF0KCksJ3Njb3JlZF9hdCc6bm93Lmlzb2Zvcm1hdCgpLAogICAgICAgICAgICAgICAgICAgICdzdGF0aW9uX3JlcG9ydGVkX2F0JzpyZXBvcnRlZC5pc29mb3JtYXQoKSwKICAgICAgICAgICAgICAgICAgICAndGFyZ2V0X2F0JzooZmV0Y2hlZCt0aW1lZGVsdGEobWludXRlcz02MCkpLmlzb2Zvcm1hdCgpLAogICAgICAgICAgICAgICAgICAgICdtb2RlbF92ZXJzaW9uJzptb2RlbF92ZXJzaW9uIG9yIG1vZGVsX2FydGlmYWN0Wyd2ZXJzaW9uJ10sCiAgICAgICAgICAgICAgICAgICAgJ3Byb2JhYmlsaXR5JzpyaXNrLAogICAgICAgICAgICAgICAgICAgICdjdXJyZW50X2NvdW50Jzpjb3VudH0KICAgICAgICAgICAgc2NvcmVkLmFwcGVuZChpdGVtKQogICAgICAgICAgICBpZiBjb3VudCA+IDA6CiAgICAgICAgICAgICAgICBjYW5kaWRhdGVzW291dGNvbWVdLmFwcGVuZChpdGVtKQogICAgIyBEZW1vbnN0cmF0aW9uIHBvbGljeSBvbmx5OiB1cCB0byBmaXZlIHBlciBvdXRjb21lLCBwZXIgcG9sbDsgdGllcyBieSBzdGF0aW9uIElELgogICAgYWxlcnRzID0ge291dGNvbWU6IHNvcnRlZChyb3dzLCBrZXk9bGFtYmRhIHY6KC12Wydwcm9iYWJpbGl0eSddLHZbJ3N0YXRpb25faWQnXSkpWzo1XQogICAgICAgICAgICAgIGZvciBvdXRjb21lLHJvd3MgaW4gY2FuZGlkYXRlcy5pdGVtcygpfQogICAgZGlhZ25vc3RpY3MgPSBkaWN0KHJlamVjdGVkKQogICAgaWYgbm90IHNjb3JlZDoKICAgICAgICByYWlzZSBaZXJvU2NvcmVFcnJvcihkaWFnbm9zdGljcykKICAgIHJldHVybiBzY29yZWQsIGFsZXJ0cywgZGlhZ25vc3RpY3MKCgpjbGFzcyBTdGF0aW9uRmVhdHVyZU1pc3NpbmcoS2V5RXJyb3IpOgogICAgIiIiUmFpc2VkIGJ5IGEgc3RhdGlvbi1hd2FyZSBwcm9iYWJpbGl0eSBmdW5jdGlvbiB3aGVuIGEgc3RhdGlvbiBoYXMgbm8gc3RhdGljIGZlYXR1cmUgcm93LiIiIgoKCmNsYXNzIFplcm9TY29yZUVycm9yKFZhbHVlRXJyb3IpOgogICAgZGVmIF9faW5pdF9fKHNlbGYsIHJlYXNvbnMpOgogICAgICAgIHNlbGYucmVhc29ucyA9IHJlYXNvbnMKICAgICAgICBzdXBlcigpLl9faW5pdF9fKCd6ZXJvIHNjb3JlczogJyArIHN0cihyZWFzb25zKSkK', 'f61d202a6689ce58dd35df0461b19665d5fe858b9b801499b8ec058b38592707'),
    'normalization.py': ('IiIiU3RyaWN0LCBsb3NzbGVzcyBub3JtYWxpemF0aW9uIGZvciBzY2FsYXIgZGF0YSBmcm9tIEpTT04sIHBhbmRhcyBvciBQYXJxdWV0LiIiIgppbXBvcnQgbWF0aAppbXBvcnQgbnVtYmVycwoKZGVmIGludGVnZXIodmFsdWUpOgogICAgaWYgaXNpbnN0YW5jZSh2YWx1ZSwgYm9vbCkgb3IgKHR5cGUodmFsdWUpLl9fbW9kdWxlX18uc3RhcnRzd2l0aCgnbnVtcHknKSBhbmQgdHlwZSh2YWx1ZSkuX19uYW1lX18gaW4gKCdib29sJywgJ2Jvb2xfJykpOgogICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ2Jvb2xlYW4gaXMgbm90IGEgY291bnQnKQogICAgaWYgbm90IGlzaW5zdGFuY2UodmFsdWUsIG51bWJlcnMuUmVhbCk6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignZXhwZWN0ZWQgYSBudW1lcmljIGludGVnZXInKQogICAgaWYgbm90IG1hdGguaXNmaW5pdGUodmFsdWUpIG9yIGludCh2YWx1ZSkgIT0gdmFsdWU6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignZXhwZWN0ZWQgYSBmaW5pdGUgaW50ZWdyYWwgdmFsdWUnKQogICAgcmV0dXJuIGludCh2YWx1ZSkKCmRlZiBmbGFnKHZhbHVlKToKICAgIGlmIGlzaW5zdGFuY2UodmFsdWUsIGJvb2wpIG9yICh0eXBlKHZhbHVlKS5fX21vZHVsZV9fLnN0YXJ0c3dpdGgoJ251bXB5JykgYW5kIHR5cGUodmFsdWUpLl9fbmFtZV9fIGluICgnYm9vbCcsICdib29sXycpKToKICAgICAgICByZXR1cm4gYm9vbCh2YWx1ZSkKICAgIHYgPSBpbnRlZ2VyKHZhbHVlKQogICAgaWYgdiBub3QgaW4gKDAsIDEpOgogICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ2ZsYWcgbXVzdCBiZSBib29sZWFuIG9yIDAvMScpCiAgICByZXR1cm4gYm9vbCh2KQoKZGVmIGZpZWxkcyhyb3cpOgogICAgcmV0dXJuIChpbnRlZ2VyKHJvd1snbnVtX2Jpa2VzX2F2YWlsYWJsZSddKSwgaW50ZWdlcihyb3dbJ251bV9kb2Nrc19hdmFpbGFibGUnXSksCiAgICAgICAgICAgIGludGVnZXIocm93WydjYXBhY2l0eSddKSwgZmxhZyhyb3dbJ2lzX2luc3RhbGxlZCddKSwKICAgICAgICAgICAgZmxhZyhyb3dbJ2lzX3JlbnRpbmcnXSksIGZsYWcocm93Wydpc19yZXR1cm5pbmcnXSkpCg==', 'f257fbbe6363c91de1727a228651287c1177da17053954e9fd5cd303a747d8e3'),
    'candidate_scorer_acs.py': ('IiIiUHJvdG90eXBlIHYxNDogQUNTIGNhbmRpZGF0ZSBzY29yZXIgd2l0aCBhIGxvZ2lzdGljIGZhbGxiYWNrLiBEZXZlbG9wbWVudCBvbmx5LgoKVGhlIGNhbmRpZGF0ZSBpcyB0aGUgcG9zaXRpdmUtY291bnQgYm9vc3RlZCB0cmVlIHVzaW5nIHRoZSBzZXZlbiBzdGF0aW9uIGZlYXR1cmVzIHBsdXMgc2l4CnN0YXRpYyBBQ1MgKEFtZXJpY2FuIENvbW11bml0eSBTdXJ2ZXkpIG1lYXN1cmVzIGpvaW5lZCBieSBzdGF0aW9uIElELiBUaGlzIG1vZHVsZSBwZXJmb3JtcwpubyBuZXR3b3JrIGNhbGxzLCBubyB3cml0ZXMgYW5kIG5vIHNjaGVkdWxpbmcuCgpPbmx5IGxvYWQgdHJ1c3RlZCBhcnRpZmFjdHMuIGpvYmxpYiBkZXNlcmlhbGl6YXRpb24gY2FuIGV4ZWN1dGUgY29kZSwgYW5kIGEgbWF0Y2hpbmcgaGFzaCBpbiBhbgphZGphY2VudCBtYW5pZmVzdCBjaGVja3MgaW50ZWdyaXR5LCBub3Qgb3JpZ2luLgoiIiIKaW1wb3J0IGhhc2hsaWIKaW1wb3J0IGlvCmltcG9ydCBqc29uCmltcG9ydCBtYXRoCmZyb20gcGF0aGxpYiBpbXBvcnQgUGF0aAoKaW1wb3J0IGpvYmxpYgppbXBvcnQgcGFuZGFzIGFzIHBkCmltcG9ydCBza2xlYXJuCgpmcm9tIHNjb3JpbmcgaW1wb3J0IEZFQVRVUkVTLCBTdGF0aW9uRmVhdHVyZU1pc3NpbmcsIGxvYWRfYXJ0aWZhY3QsIGxvYWRfY29ob3J0LCBzY29yZV9zbmFwc2hvdAoKQUNTX0ZFQVRVUkVTID0gWydlc3RpbWF0ZWRfcG9wdWxhdGlvbl81MDBtJywgJ2JhY2hlbG9yc19wbHVzX3BjdF81MDBtJywgJ2hvdXNlaG9sZHNfbm9fdmVoaWNsZV9wY3RfNTAwbScsCiAgICAgICAgICAgICAgICAncG92ZXJ0eV9wY3RfNTAwbScsICdiaWN5Y2xlX2NvbW11dGVfcGN0XzUwMG0nLCAnc2Nob29sX2Vucm9sbG1lbnRfcGN0XzUwMG0nXQpDQU5ESURBVEVfRkVBVFVSRVMgPSBGRUFUVVJFUyArIEFDU19GRUFUVVJFUwpPVVRDT01FUyA9ICgnYmlrZScsICdkb2NrJykKCgpkZWYgX3NoYTI1NihkYXRhKToKICAgIHJldHVybiBoYXNobGliLnNoYTI1NihkYXRhKS5oZXhkaWdlc3QoKQoKCmRlZiBsb2FkX2Fjc190YWJsZShwYXRoLCBleHBlY3RlZF9zaGEyNTYsIGNvaG9ydCk6CiAgICAiIiJSZXR1cm4ge3N0YXRpb25faWQ6IFtzaXggQUNTIHZhbHVlc119IGFmdGVyIGhhc2gsIHNjaGVtYSBhbmQgY292ZXJhZ2UgY2hlY2tzLgoKICAgIE1pc3NpbmcgQUNTIHZhbHVlcyBzdGF5IE5hTiwgbWF0Y2hpbmcgdHJhaW5pbmcsIHdoZXJlIHRoZSBib29zdGVkIHRyZWUgaGFuZGxlZCB0aGVtIG5hdGl2ZWx5LgogICAgVGhlIHRhYmxlIG11c3QgY292ZXIgZXhhY3RseSB0aGUgY29ob3J0LCBzbyBhIGNvaG9ydCBzdGF0aW9uIGNhbm5vdCBiZSBhYnNlbnQgYXQgc2NvcmluZyB0aW1lCiAgICB1bmxlc3MgdGhlIGluLW1lbW9yeSB0YWJsZSBpcyBhbHRlcmVkOyBzY29yaW5nIHN0aWxsIGd1YXJkcyB0aGF0IGNhc2UuCiAgICAiIiIKICAgIGRhdGEgPSBQYXRoKHBhdGgpLnJlYWRfYnl0ZXMoKQogICAgaWYgX3NoYTI1NihkYXRhKSAhPSBleHBlY3RlZF9zaGEyNTY6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignQUNTIHRhYmxlIGNoZWNrc3VtIG1pc21hdGNoJykKICAgIGlmIHN0cihwYXRoKS5lbmRzd2l0aCgnLnBhcnF1ZXQnKToKICAgICAgICB0YWJsZSA9IHBkLnJlYWRfcGFycXVldChpby5CeXRlc0lPKGRhdGEpKQogICAgZWxzZToKICAgICAgICB0YWJsZSA9IHBkLnJlYWRfY3N2KGlvLkJ5dGVzSU8oZGF0YSksIGR0eXBlPXsnc3RhdGlvbl9pZCc6IHN0cn0pCiAgICBtaXNzaW5nID0gW2MgZm9yIGMgaW4gWydzdGF0aW9uX2lkJ10gKyBBQ1NfRkVBVFVSRVMgaWYgYyBub3QgaW4gdGFibGUuY29sdW1uc10KICAgIGlmIG1pc3Npbmc6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignQUNTIHRhYmxlIGxhY2tzIGNvbHVtbnM6ICcgKyAnLCAnLmpvaW4obWlzc2luZykpCiAgICB0YWJsZSA9IHRhYmxlW1snc3RhdGlvbl9pZCddICsgQUNTX0ZFQVRVUkVTXS5jb3B5KCkKICAgIHRhYmxlWydzdGF0aW9uX2lkJ10gPSB0YWJsZS5zdGF0aW9uX2lkLmFzdHlwZShzdHIpCiAgICBpZiB0YWJsZS5zdGF0aW9uX2lkLmR1cGxpY2F0ZWQoKS5hbnkoKToKICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdBQ1MgdGFibGUgaGFzIGR1cGxpY2F0ZSBzdGF0aW9uIElEcycpCiAgICBpZiBzZXQodGFibGUuc3RhdGlvbl9pZCkgIT0gc2V0KGNvaG9ydCk6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignQUNTIHRhYmxlIHN0YXRpb25zIGRvIG5vdCBtYXRjaCB0aGUgY29ob3J0JykKICAgIGZvciBjIGluIEFDU19GRUFUVVJFUzoKICAgICAgICB0YWJsZVtjXSA9IHBkLnRvX251bWVyaWModGFibGVbY10sIGVycm9ycz0nY29lcmNlJykKICAgIHZhbHVlcyA9IHRhYmxlW0FDU19GRUFUVVJFU10udG9fbnVtcHkoZHR5cGU9ZmxvYXQpCiAgICBpZiBhbnkobWF0aC5pc2luZih2KSBmb3IgdiBpbiB2YWx1ZXMucmF2ZWwoKSk6CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignQUNTIHRhYmxlIGNvbnRhaW5zIGluZmluaXRlIHZhbHVlcycpCiAgICByZXR1cm4ge3NpZDogW2Zsb2F0KHYpIGZvciB2IGluIHJvd10gZm9yIHNpZCwgcm93IGluIHppcCh0YWJsZS5zdGF0aW9uX2lkLCB2YWx1ZXMpfQoKCmRlZiBsb2FkX2NhbmRpZGF0ZShmb2xkZXIsIHdpbmRvdywgY29ob3J0X2ZpbGUsIGJhc2VsaW5lX2FydGlmYWN0X2ZpbGUsIGFjc19maWxlKToKICAgICIiIkxvYWQgYW5kIHZlcmlmeSB0aGUgZnJvemVuLWNhbmRpZGF0ZSBmaWxlcy4gUmFpc2VzIFZhbHVlRXJyb3Igb24gYW55IG1pc21hdGNoLiIiIgogICAgZm9sZGVyID0gUGF0aChmb2xkZXIpCiAgICBtYW5pZmVzdCA9IGpzb24ubG9hZHMoKGZvbGRlciAvICdtYW5pZmVzdC5qc29uJykucmVhZF90ZXh0KGVuY29kaW5nPSd1dGYtOCcpKQogICAgaWYgbWFuaWZlc3QuZ2V0KCdza2xlYXJuX3ZlcnNpb24nKSAhPSBza2xlYXJuLl9fdmVyc2lvbl9fOgogICAgICAgIHJhaXNlIFZhbHVlRXJyb3IoJ3NjaWtpdC1sZWFybiB2ZXJzaW9uIG1pc21hdGNoOyByZWJ1aWxkIG9yIHZlcmlmeSBjb21wYXRpYmlsaXR5JykKICAgIGlmIG1hbmlmZXN0LmdldCgnZmVhdHVyZV9vcmRlcicpICE9IENBTkRJREFURV9GRUFUVVJFUzoKICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCd1bmV4cGVjdGVkIGNhbmRpZGF0ZSBmZWF0dXJlIG9yZGVyJykKICAgIGlmIG1hbmlmZXN0LmdldCgncGFyYW1zJywge30pLmdldCgnZWFybHlfc3RvcHBpbmcnKSBpcyBub3QgVHJ1ZToKICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCdtYW5pZmVzdCBkb2VzIG5vdCByZWNvcmQgZXhwbGljaXQgZWFybHlfc3RvcHBpbmc9VHJ1ZScpCiAgICBiYXNlbGluZSA9IGxvYWRfYXJ0aWZhY3QoYmFzZWxpbmVfYXJ0aWZhY3RfZmlsZSkKICAgIGlmIG1hbmlmZXN0LmdldCgnY29ob3J0X3NoYTI1NicpICE9IGJhc2VsaW5lWydjb2hvcnRfc2hhMjU2X3NvcnRlZF9uZXdsaW5lJ106CiAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignY29ob3J0IG1hbmlmZXN0IG1pc21hdGNoJykKICAgIGNvaG9ydCA9IGxvYWRfY29ob3J0KGNvaG9ydF9maWxlLCBtYW5pZmVzdFsnY29ob3J0X3NoYTI1NiddKQogICAgYWNzID0gbG9hZF9hY3NfdGFibGUoYWNzX2ZpbGUsIG1hbmlmZXN0WydhY3NfdGFibGUnXVsnc2hhMjU2J10sIGNvaG9ydCkKICAgIG1vZGVscyA9IHt9CiAgICBmb3Igb3V0Y29tZSBpbiBPVVRDT01FUzoKICAgICAgICBuYW1lID0gJyVzX2Fjc19jYW5kaWRhdGVfJXMuam9ibGliJyAlIChvdXRjb21lLCB3aW5kb3cpCiAgICAgICAgbWV0YSA9IG1hbmlmZXN0Wydtb2RlbHMnXVtuYW1lXQogICAgICAgIGlmIG1ldGEuZ2V0KCdmZWF0dXJlcycpICE9IENBTkRJREFURV9GRUFUVVJFUzoKICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcigndW5leHBlY3RlZCBmZWF0dXJlIG9yZGVyIGZvciAnICsgbmFtZSkKICAgICAgICBwYXRoID0gZm9sZGVyIC8gbmFtZQogICAgICAgIGlmIF9zaGEyNTYocGF0aC5yZWFkX2J5dGVzKCkpICE9IG1ldGEuZ2V0KCdzaGEyNTYnKToKICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignbW9kZWwgY2hlY2tzdW0gbWlzbWF0Y2ggZm9yICcgKyBuYW1lKQogICAgICAgIG1vZGVsID0gam9ibGliLmxvYWQocGF0aCkKICAgICAgICBpZiBsaXN0KG1vZGVsLmNsYXNzZXNfKSAhPSBbMCwgMV0gb3IgbGlzdChtb2RlbC5mZWF0dXJlX25hbWVzX2luXykgIT0gQ0FORElEQVRFX0ZFQVRVUkVTOgogICAgICAgICAgICByYWlzZSBWYWx1ZUVycm9yKCd1bmV4cGVjdGVkIG1vZGVsIGNsYXNzIG9yIGZpdHRlZCBmZWF0dXJlcyBmb3IgJyArIG5hbWUpCiAgICAgICAgaWYgbW9kZWwuZ2V0X3BhcmFtcygpLmdldCgnZWFybHlfc3RvcHBpbmcnKSBpcyBub3QgVHJ1ZToKICAgICAgICAgICAgcmFpc2UgVmFsdWVFcnJvcignbW9kZWwgZWFybHlfc3RvcHBpbmcgaXMgbm90IGV4cGxpY2l0bHkgVHJ1ZSBmb3IgJyArIG5hbWUpCiAgICAgICAgbW9kZWxzW291dGNvbWVdID0gbW9kZWwKICAgIHZlcnNpb24gPSAnYWNzX2Jvb3N0ZWRfcG9zaXRpdmVfJXNfJXMnICUgKG1hbmlmZXN0LmdldCgncnVuX2lkJywgJ3Vua25vd24nKSwgd2luZG93KQogICAgcmV0dXJuIHsnbW9kZSc6ICdhY3NfY2FuZGlkYXRlJywgJ3ZlcnNpb24nOiB2ZXJzaW9uLCAnYmFzZWxpbmUnOiBiYXNlbGluZSwgJ2NvaG9ydCc6IGNvaG9ydCwKICAgICAgICAgICAgJ21vZGVscyc6IG1vZGVscywgJ2Fjcyc6IGFjcywgJ2ZhbGxiYWNrX3JlYXNvbic6IE5vbmV9CgoKZGVmIGxvYWRfc2NvcmVyKGZvbGRlciwgd2luZG93LCBjb2hvcnRfZmlsZSwgYmFzZWxpbmVfYXJ0aWZhY3RfZmlsZSwgYWNzX2ZpbGUpOgogICAgIiIiTG9hZCB0aGUgQUNTIGNhbmRpZGF0ZSwgb3IgZmFsbCBiYWNrIHRvIHRoZSBvcmlnaW5hbCBsb2dpc3RpYyBtb2RlbCBpZiBhbnkgYXJ0aWZhY3QgZmFpbHMuCgogICAgVGhlIGxvZ2lzdGljIGJhc2VsaW5lIGl0c2VsZiBtdXN0IGxvYWQ7IGlmIGl0IGNhbm5vdCwgdGhpcyByYWlzZXMgYW5kIHRoZSBydW4gZmFpbHMgY2xvc2VkLgogICAgIiIiCiAgICB0cnk6CiAgICAgICAgcmV0dXJuIGxvYWRfY2FuZGlkYXRlKGZvbGRlciwgd2luZG93LCBjb2hvcnRfZmlsZSwgYmFzZWxpbmVfYXJ0aWZhY3RfZmlsZSwgYWNzX2ZpbGUpCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGV4YzogICMgYW55IGNhbmRpZGF0ZS1hcnRpZmFjdCBmYWlsdXJlIHNlbGVjdHMgdGhlIGZhbGxiYWNrCiAgICAgICAgYmFzZWxpbmUgPSBsb2FkX2FydGlmYWN0KGJhc2VsaW5lX2FydGlmYWN0X2ZpbGUpCiAgICAgICAgY29ob3J0ID0gbG9hZF9jb2hvcnQoY29ob3J0X2ZpbGUsIGJhc2VsaW5lWydjb2hvcnRfc2hhMjU2X3NvcnRlZF9uZXdsaW5lJ10pCiAgICAgICAgcmV0dXJuIHsnbW9kZSc6ICdsb2dpc3RpY19mYWxsYmFjaycsICd2ZXJzaW9uJzogYmFzZWxpbmVbJ3ZlcnNpb24nXSwgJ2Jhc2VsaW5lJzogYmFzZWxpbmUsCiAgICAgICAgICAgICAgICAnY29ob3J0JzogY29ob3J0LCAnbW9kZWxzJzogTm9uZSwgJ2Fjcyc6IE5vbmUsCiAgICAgICAgICAgICAgICAnZmFsbGJhY2tfcmVhc29uJzogdHlwZShleGMpLl9fbmFtZV9fICsgJzogJyArIHN0cihleGMpWzoyMDBdfQoKCmRlZiBzY29yZShyZWNvcmRzLCBzY29yZXIsIHNjb3JlZF9hdCk6CiAgICAiIiJTY29yZSBvbmUgcG9sbCB3aXRoIHdoaWNoZXZlciBtb2RlbCBsb2FkZWQuIE5vIHdyaXRlcywgc2NoZWR1bGluZyBvciBhbGVydHMgc2VudC4iIiIKICAgIGlmIHNjb3JlclsnbW9kZSddID09ICdsb2dpc3RpY19mYWxsYmFjayc6CiAgICAgICAgcmV0dXJuIHNjb3JlX3NuYXBzaG90KHJlY29yZHMsIHNjb3JlclsnYmFzZWxpbmUnXSwgc2NvcmVyWydjb2hvcnQnXSwgc2NvcmVkX2F0KQogICAgbW9kZWxzLCBhY3MgPSBzY29yZXJbJ21vZGVscyddLCBzY29yZXJbJ2FjcyddCgogICAgZGVmIHN0YXRpb25fcHJvYmFiaWxpdHkob3V0Y29tZSwgc3RhdGlvbl9pZCwgc3RhdGlvbl9mZWF0dXJlcyk6CiAgICAgICAgaWYgc3RhdGlvbl9pZCBub3QgaW4gYWNzOgogICAgICAgICAgICByYWlzZSBTdGF0aW9uRmVhdHVyZU1pc3Npbmcoc3RhdGlvbl9pZCkKICAgICAgICByb3cgPSBsaXN0KHN0YXRpb25fZmVhdHVyZXMpICsgYWNzW3N0YXRpb25faWRdCiAgICAgICAgcmV0dXJuIG1vZGVsc1tvdXRjb21lXS5wcmVkaWN0X3Byb2JhKHBkLkRhdGFGcmFtZShbcm93XSwgY29sdW1ucz1DQU5ESURBVEVfRkVBVFVSRVMpKVswLCAxXQoKICAgIHJldHVybiBzY29yZV9zbmFwc2hvdChyZWNvcmRzLCBzY29yZXJbJ2Jhc2VsaW5lJ10sIHNjb3JlclsnY29ob3J0J10sIHNjb3JlZF9hdCwKICAgICAgICAgICAgICAgICAgICAgICAgICBzdGF0aW9uX3Byb2JhYmlsaXR5X2ZuPXN0YXRpb25fcHJvYmFiaWxpdHksIG1vZGVsX3ZlcnNpb249c2NvcmVyWyd2ZXJzaW9uJ10sCiAgICAgICAgICAgICAgICAgICAgICAgICAgcG9zaXRpdmVfb25seT1UcnVlKQo=', '471d291b72db9109bbe1e8d6e20d74b16021385618c13cc1852bf36e8afd3812'),
    'model_parameters.json': ('ewogICJ2ZXJzaW9uIjogImRldmVsb3BtZW50LTIwMjYtMTEtMDkiLAogICJza2xlYXJuX3ZlcnNpb24iOiAiMS43LjIiLAogICJjb2hvcnRfc2hhMjU2X3NvcnRlZF9uZXdsaW5lIjogIjNkNWJmNWNkMDU5NDk4MWZiNjBiOTk1YTA4ZjZjMzA1YjI5MjcxMjJhZTEyOTkwMjFlNDEzYTFiMzU0MTRmZjMiLAogICJjb2hvcnRfbm90ZSI6ICJyZWNvbnN0cnVjdGVkIGZyb20gY3VycmVudCBtZXRhZGF0YTsgb3JpZ2luYWwgSURzIG5vdCBzYXZlZCIsCiAgInRyYWluIjogIjIwMjYtMDgtMjQgdGhyb3VnaCAyMDI2LTA5LTI3IEFtZXJpY2EvTmV3X1lvcmsiLAogICJob3Jpem9uX21pbnV0ZXMiOiA2MCwKICAibW9kZWxzIjogewogICAgImJpa2UiOiB7CiAgICAgICJmZWF0dXJlX29yZGVyIjogWwogICAgICAgICJudW1fYmlrZXNfYXZhaWxhYmxlIiwKICAgICAgICAibnVtX2RvY2tzX2F2YWlsYWJsZSIsCiAgICAgICAgImNhcGFjaXR5IiwKICAgICAgICAiYmlrZV9mcmFjdGlvbiIsCiAgICAgICAgImRvY2tfZnJhY3Rpb24iLAogICAgICAgICJob3VyX3NpbiIsCiAgICAgICAgImhvdXJfY29zIgogICAgICBdLAogICAgICAibWVhbiI6IFsKICAgICAgICAxNC43OTgxNTk4MDY2MDI5MzYsCiAgICAgICAgMTIuOTAwMDc0MzAyMDY4NjAzLAogICAgICAgIDMwLjU5NjMxNzQ5NjMzNzgxOCwKICAgICAgICAwLjQ4NzUyMTczNDA1OTczMDksCiAgICAgICAgMC40MDkxODk3MzUyNjY1OTMxNCwKICAgICAgICAtMC4wMDQ3MDA5MDA5MzY2NjQ5MDEsCiAgICAgICAgLTAuMDA3ODMwMDE3NTg4ODA2NTI0CiAgICAgIF0sCiAgICAgICJzY2FsZSI6IFsKICAgICAgICAxMi43Mjg5NDQ2NDc4MDMyOTIsCiAgICAgICAgMTMuNDQxNTU1MDcxODU4MTE3LAogICAgICAgIDE2LjA2ODQ5NDM5NTg4NjgyLAogICAgICAgIDAuMjkzMjQ5NjU2MDE5NDk1NDMsCiAgICAgICAgMC4zMDAzNTY3NDYzMzEwMjM1LAogICAgICAgIDAuNzA3NDQ1NzQ1NzY0MTYzNCwKICAgICAgICAwLjcwNjcwODY0NTE2Nzk1NwogICAgICBdLAogICAgICAiY29lZiI6IFsKICAgICAgICAxLjM3ODE5ODM5NTQ1MjE5ODgsCiAgICAgICAgLTAuMzgzMDgxMjM4Nzc0NTgxNCwKICAgICAgICAwLjE1Njc4Njg3MjU4MTY5NjUsCiAgICAgICAgLTcuMDY0ODcyNDY2NzQzOTA4LAogICAgICAgIDAuMTQ5NzUzNTI1ODQ2MTgwMDQsCiAgICAgICAgMC4wNjQwMDYxMjQ2MDkxNzA5LAogICAgICAgIDAuMDA3MjY1OTM5ODczNjczMDc0CiAgICAgIF0sCiAgICAgICJpbnRlcmNlcHQiOiAtMTAuMTkzODA0Njg5MDExOTI1LAogICAgICAiY2xhc3NlcyI6IFsKICAgICAgICAwLAogICAgICAgIDEKICAgICAgXQogICAgfSwKICAgICJkb2NrIjogewogICAgICAiZmVhdHVyZV9vcmRlciI6IFsKICAgICAgICAibnVtX2Jpa2VzX2F2YWlsYWJsZSIsCiAgICAgICAgIm51bV9kb2Nrc19hdmFpbGFibGUiLAogICAgICAgICJjYXBhY2l0eSIsCiAgICAgICAgImJpa2VfZnJhY3Rpb24iLAogICAgICAgICJkb2NrX2ZyYWN0aW9uIiwKICAgICAgICAiaG91cl9zaW4iLAogICAgICAgICJob3VyX2NvcyIKICAgICAgXSwKICAgICAgIm1lYW4iOiBbCiAgICAgICAgMTQuNzk4MTU5ODA2NjAyOTM2LAogICAgICAgIDEyLjkwMDA3NDMwMjA2ODYwMywKICAgICAgICAzMC41OTYzMTc0OTYzMzc4MTgsCiAgICAgICAgMC40ODc1MjE3MzQwNTk3MzA5LAogICAgICAgIDAuNDA5MTg5NzM1MjY2NTkzMTQsCiAgICAgICAgLTAuMDA0NzAwOTAwOTM2NjY0OTAxLAogICAgICAgIC0wLjAwNzgzMDAxNzU4ODgwNjUyNAogICAgICBdLAogICAgICAic2NhbGUiOiBbCiAgICAgICAgMTIuNzI4OTQ0NjQ3ODAzMjkyLAogICAgICAgIDEzLjQ0MTU1NTA3MTg1ODExNywKICAgICAgICAxNi4wNjg0OTQzOTU4ODY4MiwKICAgICAgICAwLjI5MzI0OTY1NjAxOTQ5NTQzLAogICAgICAgIDAuMzAwMzU2NzQ2MzMxMDIzNSwKICAgICAgICAwLjcwNzQ0NTc0NTc2NDE2MzQsCiAgICAgICAgMC43MDY3MDg2NDUxNjc5NTcKICAgICAgXSwKICAgICAgImNvZWYiOiBbCiAgICAgICAgMC4yNTIwNDI1NzAzMzAwMDU5LAogICAgICAgIDMuNjg1NDMyMDg2NDQ2NzkwNywKICAgICAgICAtMC43NzA1OTIzMTAyNTc5NDM0LAogICAgICAgIC0wLjMxODY2MTA5NzE2MzQwNTU3LAogICAgICAgIC04LjY5NTcxNTI4NTA1OTIxMywKICAgICAgICAtMC4wNjI4MzA0NTgyMDYzOTk2NCwKICAgICAgICAwLjE5MjEyNjk3NDQ3ODA3NDE3CiAgICAgIF0sCiAgICAgICJpbnRlcmNlcHQiOiAtNy45NDkxMjE1NzI3MDQ2MDMsCiAgICAgICJjbGFzc2VzIjogWwogICAgICAgIDAsCiAgICAgICAgMQogICAgICBdCiAgICB9CiAgfQp9Cg==', '194bc9593a29d796eb192dcef7c065d6c4c362465268925609fe30cf15fada25'),
}
## Write the embedded files to a temporary folder, checking each fingerprint first
pkg = Path(tempfile.mkdtemp(prefix='citibike_v14_'))
for name, (b64, sha) in PKG_FILES.items():
    data = base64.b64decode(b64)
    assert hashlib.sha256(data).hexdigest() == sha, 'embedded file changed: ' + name
    (pkg / name).write_bytes(data)
# Write the 100-station cohort file the scorer expects
(pkg / 'cohort.csv').write_text('selection_order,station_id\n' + '\n'.join('%d,%s' % (i + 1, s) for i, s in enumerate(sorted(C))) + '\n')
# Load the scorer modules from the temporary folder
sys.path.insert(0, str(pkg))
import scoring, candidate_scorer_acs
from candidate_scorer_acs import load_candidate, load_scorer, score, CANDIDATE_FEATURES
from scoring import ZeroScoreError
## Approved histogram gradient boosting model files (step 03 output folder) and the ACS station table
CAND_DIR = home / 'Citi Bike - ACS Controls and Neighborhood Audit - ea8df268'
ACS_FILE = home / 'Citi Bike - ACS and July Trip Development - e1eb6e26' / 'acs_station_features.parquet'
WINDOW = 'train_before_Sep28'
# Load the candidate directly, then through the loader with fallback, and confirm no fallback happened
scorer = load_candidate(CAND_DIR, WINDOW, pkg / 'cohort.csv', pkg / 'model_parameters.json', ACS_FILE)
fallback_check = load_scorer(CAND_DIR, WINDOW, pkg / 'cohort.csv', pkg / 'model_parameters.json', ACS_FILE)
assert fallback_check['mode'] == 'acs_candidate', fallback_check['fallback_reason']
# Record which model files were used (fingerprints from the candidate manifest)
manifest = json.loads((CAND_DIR / 'manifest.json').read_text())
SUMMARY['candidate'] = {'window': WINDOW, 'version': scorer['version'], 'manifest_sha256': hashlib.sha256((CAND_DIR / 'manifest.json').read_bytes()).hexdigest(), 'models': {k: v['sha256'] for k, v in manifest['models'].items() if WINDOW in k}, 'acs_sha256': manifest['acs_table']['sha256'], 'feature_order': CANDIDATE_FEATURES, 'early_stopping': manifest['params']['early_stopping']}
# Display the loaded candidate
print('CANDIDATE_LOADED', json.dumps(SUMMARY['candidate']), flush=True)

# COMMAND ----------

# Cell 4: batch predictions for the validation week (reference)
## Outcome gap: 60 minutes plus a 150-second matching window (3,750 seconds)
GAP = pd.Timedelta(seconds=3750)
OUTCOMES = [('bike', 'num_bikes_available'), ('dock', 'num_docks_available')]
## Validation polls only, ending one gap before Oct 5 so no outcome reaches the test week
val = examples[(examples.fetched_at >= VALID) & (examples.fetched_at < END - GAP)].copy()
# Confirm no label reaches the reserved test week
assert len(val) and val.target_at.max() + pd.Timedelta(seconds=150) < RESERVED_TEST_START
# Timestamps in microseconds, the join key used to compare batch and replay
val['fetched_us'] = (val.fetched_at - pd.Timestamp('1970-01-01', tz='UTC')) // pd.Timedelta(microseconds=1)
nanos = int((val.fetched_at.dt.nanosecond != 0).sum())
## Batch reference: score every eligible validation row at once with the same models
station_acs = acs.set_index('station_id')[acs_features]
batch, batch_alerts = {}, {}
for o, count in OUTCOMES:
    lab = o + '_label'
    # Eligible pool: the outcome is in service and the station is not already empty
    pool = val[val[o + '_ok'] & (val[count] > 0)].copy()
    # Station features plus the six ACS measures for each row
    X = pool[features].copy()
    for c in acs_features: X[c] = pool.station_id.map(station_acs[c]).to_numpy()
    pool['p_batch'] = scorer['models'][o].predict_proba(X[CANDIDATE_FEATURES])[:, 1]
    batch[o] = pool[['station_id', 'fetched_us', lab, 'p_batch']]
    # Batch alerts: the five stations with the highest Risk Scores per poll (ties broken by station ID)
    batch_alerts[o] = pool.sort_values(['fetched_at', 'p_batch', 'station_id'], ascending=[True, False, True]).groupby('fetched_at', sort=False).head(5)[['station_id', 'fetched_us', lab]]
# Display batch sizes
print('BATCH_READY', {o: len(batch[o]) for o in batch}, 'polls', val.fetched_at.nunique(), 'nanosecond_timestamps', nanos, flush=True)

# COMMAND ----------

# Cell 5: poll-by-poll replay through the v14 scorer
## Helper: timestamps as ISO 8601 strings, the format the scorer expects
def iso(t):
    return pd.Timestamp(t).to_pydatetime().isoformat()

## Replay: send each validation poll through the scorer, as the live pilot would
scored_rows, alert_rows, reasons_total, zero_polls = [], [], Counter(), 0
t0 = time.time()
for fetched, g in val.groupby('fetched_at', sort=True):
    f_iso = iso(fetched)
    # Build the poll's station records in the live feed's field layout
    recs = [{'station_id': r.station_id, 'fetched_at': f_iso, 'available_at': f_iso, 'station_reported_at': iso(r.station_reported_at), 'metadata_available_at': iso(r.metadata_time),
             'num_bikes_available': int(r.num_bikes_available), 'num_docks_available': int(r.num_docks_available), 'capacity': int(r.capacity),
             'is_installed': bool(r.is_installed), 'is_renting': bool(r.is_renting), 'is_returning': bool(r.is_returning)} for r in g.itertuples()]
    # Score the poll; a poll with no scorable stations is counted, not treated as an error
    try:
        out, alerts, reasons = score(recs, scorer, f_iso)
    except ZeroScoreError as e:
        zero_polls += 1; reasons_total.update(e.reasons); continue
    reasons_total.update(reasons)
    # Save each score and each alert with its rank
    us = int(pd.Timestamp(fetched).value // 1000)
    scored_rows += [(r['station_id'], r['outcome'], us, r['probability']) for r in out]
    for o in alerts:
        alert_rows += [(a['station_id'], o, us, rank) for rank, a in enumerate(alerts[o])]
elapsed = time.time() - t0
# Collect the replay scores and alerts as tables
sc = pd.DataFrame(scored_rows, columns=['station_id', 'outcome', 'fetched_us', 'p_scorer'])
al = pd.DataFrame(alert_rows, columns=['station_id', 'outcome', 'fetched_us', 'rank'])
# Display replay totals
print('REPLAY_DONE', len(sc), 'scores', len(al), 'alerts', round(elapsed, 1), 'seconds', flush=True)

# COMMAND ----------

# Cell 6: compare keys, Risk Scores, selected alerts and labels; save the summary
## Expected true positives from the step 03 run (top-five alerts, validation week)
EXPECTED_CAUGHT = {'bike': 1814, 'dock': 2656}
checks = {}
for o, count in OUTCOMES:
    lab = o + '_label'
    # Join batch and replay scores on station and poll time
    m = batch[o].merge(sc[sc.outcome == o], on=['station_id', 'fetched_us'], how='outer', indicator=True)
    both = m[m._merge == 'both']
    # Selected alerts from each path, in the same order
    sa = al[al.outcome == o][['station_id', 'fetched_us']].sort_values(['fetched_us', 'station_id']).reset_index(drop=True)
    ba = batch_alerts[o][['station_id', 'fetched_us']].sort_values(['fetched_us', 'station_id']).reset_index(drop=True)
    # Labels for the replay's alerts
    sa_lab = sa.merge(batch[o][['station_id', 'fetched_us', lab]], on=['station_id', 'fetched_us'], how='left')
    ## Compare: missing keys, largest Risk Score gap, identical alerts, same true positives
    checks[o] = {'batch_scores': len(batch[o]), 'scorer_scores': int((sc.outcome == o).sum()), 'keys_only_in_batch': int((m._merge == 'left_only').sum()), 'keys_only_in_scorer': int((m._merge == 'right_only').sum()),
                 'max_abs_prob_diff': float(np.max(np.abs(both.p_batch - both.p_scorer))) if len(both) else None,
                 'alerts_scorer': len(sa), 'alerts_batch': len(ba), 'alert_keys_identical': bool(sa.equals(ba)),
                 'caught_scorer_alerts': int(sa_lab[lab].sum()), 'caught_batch_alerts': int(batch_alerts[o][lab].sum()), 'unknown_scorer_alerts': int(sa_lab[lab].isna().sum()), 'expected_caught_from_modeling_run': EXPECTED_CAUGHT[o]}
    c = checks[o]
    # Confirm the pass rule: no missing keys, Risk Score gap below 1e-12, identical alerts and true positives
    c['pass'] = bool(c['keys_only_in_batch'] == 0 and c['keys_only_in_scorer'] == 0 and c['max_abs_prob_diff'] is not None and c['max_abs_prob_diff'] < 1e-12 and c['alert_keys_identical'] and c['caught_scorer_alerts'] == c['caught_batch_alerts'] == c['expected_caught_from_modeling_run'])
## Save the summary to the new private folder and display it
SUMMARY['replay'] = {'week': 'Sep28-Oct04 (validation, previously used)', 'polls': int(val.fetched_at.nunique()), 'zero_score_polls': zero_polls, 'scorer_rejections': dict(reasons_total), 'scorer_seconds': round(elapsed, 1), 'nanosecond_timestamps': nanos, 'checks': checks, 'overall_pass': all(c['pass'] for c in checks.values()),
                     'boundary_note': 'Development data only. Snapshots whose outcome window would cross Oct 5 04:00 UTC are excluded. Confirms the scoring path, not test-week behavior. Availability times are simulated from collection times.'}
SUMMARY['finished_utc'] = pd.Timestamp.now('UTC').isoformat()
(out_dir / 'replay_summary.json').write_text(json.dumps(SUMMARY, indent=2, default=str))
print(json.dumps(SUMMARY['replay'], indent=1, default=str))
