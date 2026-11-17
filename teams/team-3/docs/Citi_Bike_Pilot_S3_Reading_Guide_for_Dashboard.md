# Reading the Team 3 Citi Bike pilot files from S3 in Databricks

A guide for the dashboard teammate. It assumes your Databricks account can already reach the class S3 (Simple Storage Service, Amazon's file storage) bucket. Everything here is read-only: please do not write to or delete anything under the Team 3 prefix.

There are three places to know about:

| Folder (under `s3://cuny-sps-citibike-archive/contributions/team-3/`) | What it is | Use it for |
|---|---|---|
| `pilot-test/` | One tiny archive from a November 10 trial (TEST DATA) | Checking you can read the bucket |
| `pilot-synthetic/v1/` | Three days of **SYNTHETIC** pilot files in the final layout | **Building the dashboard now** |
| `pilot/v1/` | Real pilot files, written at the end of each pilot window | Swapping in real data later |

`pilot-synthetic/v1/` and `pilot/v1/` have the same folders, file names and columns, so code written against the synthetic files will work on the real ones by changing one prefix. The synthetic files were written; verify access before use.

## 1. Make a notebook in Databricks

1. In the left sidebar, click **Workspace**, then open your own folder under **Users**.
2. Click **Create** (or the ⋮ menu), then **Notebook**. Name it something like `Citi Bike - Read Pilot Files`.
3. Make sure the language at the top is **Python**.
4. At the top right, click **Connect** and choose **Serverless** (or your usual compute).
5. Paste each code block below into its own cell. Run one cell at a time with **Shift + Enter**.

## 2. Connect to S3 (cell 1)

Use the option that matches how your account is connected. Option A is the setup Team 3 uses: AWS (Amazon Web Services) keys kept in a Databricks secret scope, which is a protected store for credentials. Change `SCOPE` and the key names to yours. Never print keys or paste them into a cell.

```python
import io, tarfile, hashlib, json
import boto3
import pandas as pd

BUCKET = 'cuny-sps-citibike-archive'
REGION = 'us-west-2'

# Option A: keys kept in a Databricks secret scope (change names to yours)
SCOPE = 'your-secret-scope-name'
s3 = boto3.client(
    's3',
    aws_access_key_id=dbutils.secrets.get(SCOPE, 'aws_access_key_id'),
    aws_secret_access_key=dbutils.secrets.get(SCOPE, 'aws_secret_access_key'),
    region_name=REGION,
)

# Option B: if your compute already has S3 access built in, use this line instead
# s3 = boto3.client('s3', region_name=REGION)
```

## 3. List what is there (cell 2)

```python
for prefix in ['contributions/team-3/pilot-test/', 'contributions/team-3/pilot-synthetic/', 'contributions/team-3/pilot/']:
    print(prefix)
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            print('  ', obj['Key'], obj['Size'], 'bytes')
```

If this fails with `AccessDenied`, your account cannot read the prefix yet. Share the error code with the project owner (never any keys).

## 4. Load the SYNTHETIC files for dashboard work (cell 3)

**SYNTHETIC, NOT FOR ANALYSIS, NOT MODEL OUTPUT.** Every row has `data_kind = SYNTHETIC`. Station IDs, names and coordinates are the real 100-station cohort; counts, probabilities and outcomes are simulated. Dates are November 2 to 4, 2026, two windows a day (am 06:30 to 10:30 and pm 15:30 to 20:30 New York time), one poll every 5 minutes.

```python
PREFIX = 'contributions/team-3/pilot-synthetic/v1/'   # later: 'contributions/team-3/pilot/v1/'

def read_all(sub, reader):
    frames = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=PREFIX + sub):
        for obj in page.get('Contents', []):
            body = s3.get_object(Bucket=BUCKET, Key=obj['Key'])['Body'].read()
            frames.append(reader(body).assign(source_key=obj['Key']))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

predictions = read_all('predictions/', lambda b: pd.read_csv(io.BytesIO(b)))
outcomes = read_all('outcomes/', lambda b: pd.read_csv(io.BytesIO(b)))
run_health = read_all('run_health/', lambda b: pd.DataFrame([json.loads(b)]))
print(s3.get_object(Bucket=BUCKET, Key=PREFIX + 'README_schema.md')['Body'].read().decode())
print(len(predictions), len(outcomes), len(run_health))
```

Expected sizes for the synthetic set: 6 prediction files and 6 outcome files (58,573 rows each in total), 6 run-health files, and one `README_schema.md`.

## 5. Join predictions to outcomes (cell 4)

```python
keys = ['run_id', 'station_id', 'outcome']
joined = predictions.merge(outcomes[keys + ['status', 'label', 'matched_observation_utc']], on=keys, how='left')
alerts = joined[joined.model_selected == 1]
display(alerts.groupby(['window_id', 'outcome', 'status', 'label'], dropna=False).size().reset_index(name='rows'))
```

## 6. Columns

**predictions_<window_id>.csv** (one row per poll, station and outcome, only for stations with a positive current count)

| Column | Meaning |
|---|---|
| data_kind | SYNTHETIC in the synthetic set; PILOT in real files |
| window_id | YYYYMMDD_am or YYYYMMDD_pm |
| run_id | One per poll |
| slot_utc, fetched_at_utc | Scheduled poll time and actual fetch time (UTC, ending in Z) |
| target_at_utc | fetched time plus 60 minutes, the moment being predicted |
| station_id, station_name, lat, lon | Station identity and location |
| outcome | bike = will the station have zero bikes; dock = zero open docks |
| model_version | Which model scored the poll |
| current_count, capacity | Bikes (for bike rows) or open docks (for dock rows) now, and station size |
| probability | The Risk Score: the model's predicted probability of a shortage. For ranking only, not a calibrated chance. Show it as "Risk Score" |
| risk_rank | 1 = highest risk within that poll and outcome |
| risk_tier | high = top 5, medium = 6 to 15, low = the rest |
| model_selected | 1 if one of the model's 5 alerts |
| low_count_selected | 1 if one of the 5 lowest current counts (the simple comparison rule) |

**outcomes_<window_id>.csv** (one row per prediction, published after the window's one-hour tail; join on run_id, station_id, outcome)

| Column | Meaning |
|---|---|
| status | known or unknown (pending rows are never published) |
| label | 1 = the shortage happened, 0 = it did not, blank if unknown |
| matched_observation_utc | Time of the observation used to decide |
| evaluated_at_utc | When the outcome was finalized |
| target_at_utc, model_selected, low_count_selected | Repeated from predictions for convenience |

**run_health_<window_id>.json** (one per window): polls expected, succeeded and failed; missing and late slots; scored polls; prediction and outcome counts; unknown share; and, in real files, maximum checkpoint size and publish time.

## 7. The pilot-test archive (optional connectivity check)

```python
KEY = 'contributions/team-3/pilot-test/2026-11-10/TEST_DATA_NOT_FOR_ANALYSIS_pilot_handoff_sample_v1.tar.gz'
data = s3.get_object(Bucket=BUCKET, Key=KEY)['Body'].read()
print('fingerprint matches:', hashlib.sha256(data).hexdigest() == '5212cd288eae8dafd45bdf5795f8b4243a538fd39c4e1271076844bf42e964d4')
with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as t:
    print([m.name for m in t.getmembers()])
```

This archive uses older, provisional column names from the November 10 trial. Build the dashboard on `pilot-synthetic/v1/`, not on this archive.

## 8. Real pilot files

Same layout under `contributions/team-3/pilot/v1/`. Files appear after each pilot window's one-hour tail, so roughly twice a day while the pilot runs. Files are never overwritten; a correction appears as a new file ending in `_v2`.

## 9. Dashboard reminders

- **Risk Scores rank stations; they are not exact chances.** Show the Risk Score as a rank, a tier or a percentage, and label it "Risk Score".
- **Only stations with at least one bike (or open dock) are eligible.** An empty station is a status, not a new prediction.
- **Counts are 5-minute snapshots, not separate events.** One 30-minute shortage can appear six times.
- **Live results are monitoring, not evaluation.** The model was evaluated once on a test week partly exposed through earlier aggregate label counts.
- **Neighborhood panel:** a station-to-group file (thirds of the cohort by poverty share and by share of households without a vehicle, from 500-meter ACS (American Community Survey) estimates) supports a "capture by neighborhood group" view. Groups describe neighborhoods, not riders. Request the approved group mapping from the project owner.

## 10. Display labels (added November 17, 2026)
Use these labels on the dashboard so it matches the v06 reports and the v06.1 presentation. The column names in the files do not change.

| Show this | Computed from |
|---|---|
| Risk Score | `probability` |
| Poll | one fetch of the public Citi Bike station feed (one `run_id` per poll; `fetched_at_utc`) |
| True positives | alert rows (`model_selected = 1`, or `low_count_selected = 1` for the rule) whose outcome has `label = 1` |
| Recall | true positives ÷ all rows with `label = 1` for that outcome (every eligible station, flagged or not) |
| Precision | true positives ÷ alert rows with `status = known` |
| Mean Risk Score | the average of `probability` over the rows shown |
| Histogram gradient boosting | rows with `model_selected = 1` |
| Low-count rule | rows with `low_count_selected = 1` (fewest bikes or open docks available right now) |

Example from the November 13, 3:30 p.m. poll: Echo Pl & Grand Concourse had a 30.21% Risk Score and was one true positive. On the bike list, 2 of the 3 actual shortages were flagged (66.7% Recall), and 2 of the 5 selections were confirmed (40% Precision).
