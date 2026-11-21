# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Citi Bike: ACS Controls, Intervals and Neighborhood Audit (2026-11-10)
# MAGIC Exploratory development work for the Team 3 MVP. Safe to run top to bottom with Run all.
# MAGIC - Reads only the class S3 station-status partitions dated Aug 24 to Oct 4, 2026. It never reads the reserved Oct 5 to 11 test week.
# MAGIC - Makes no S3 writes and no shared-catalog writes. It writes only a new, uniquely named folder in your private workspace.
# MAGIC - Candidate: positive-count boosted tree with 7 station features plus 6 ACS measures. Trips and weather are not used.

# COMMAND ----------

# Cell 1: setup and guards
# Team 3 commented copy (step 03). Comments were added on November 16, 2026; the code is unchanged.
# Terms used in these comments (they match the v06 reports and the v06.1 deck):
#   Poll: one fetch of the public Citi Bike station feed.
#   Risk Score: a model's predicted probability of a shortage; the five highest Risk Scores per poll become alerts.
#   True positive: an alert (a flagged station observation) with a confirmed shortage at the target time.
#   Recall = true positives / all confirmed shortages among eligible stations.
#   Precision = true positives / alerts with known outcomes.
#   Methods: histogram gradient boosting (scikit-learn HistGradientBoostingClassifier), logistic regression,
#   and the low-count rule (the stations with the fewest bikes or open docks right now).
#   Code names such as `caught`, `probability` and `acs_candidate` are kept so saved results still load.
# Purpose: add six ACS (American Community Survey) measures to the seven station features, test whether they
# help with controls and intervals, audit results by station-area group, and save the candidate models.
# The bike and dock models saved in cell 9 (trained before Sep 28) became the approved histogram gradient boosting models (used for the test week and the pilot).
#
# Cells 2, 3 and 4 have no added comments on purpose: step 04 checks their exact text before it runs them.
# What they do:
#   Cell 2 rebuilds the 100-station cohort and reads Aug 24 to Oct 4 station status from the class S3 exports.
#   Cell 3 joins point-in-time station info, builds the 60-minute bike and dock labels and the seven station
#   features, and confirms the row counts match the earlier report.
#   Cell 4 loads the ACS station table, records its fingerprint, lists stations with missing ACS values, and
#   compares it with the copy embedded in the step 02 notebook.
## Packages
import io, json, hashlib, uuid, re, base64, gzip, time
import boto3, pandas as pd, numpy as np, pyarrow.parquet as pq
import sklearn, joblib
from pathlib import Path
from botocore.config import Config
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score
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
out_dir = home / ('Citi Bike - ACS Controls and Neighborhood Audit - ' + RUN_ID)
out_dir.mkdir(parents=False, exist_ok=False)
## Run summary, filled in by each cell and saved at the end
SUMMARY = {'run_id': RUN_ID, 'output_folder': out_dir.name, 'sklearn_version': sklearn.__version__, 'reserved_test_read': False, 's3_writes': 0, 'started_utc': pd.Timestamp.now('UTC').isoformat()}
# Display the setup summary
print('SETUP', json.dumps(SUMMARY), flush=True)

# COMMAND ----------

# Cell 2: reconstruct the fixed cohort and read only dated development partitions (same logic as the principal notebook cell 1)
from pyspark.sql import functions as F, Window
spark.conf.set('spark.sql.session.timeZone', 'UTC')
info = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket, Key='exports/station_info.parquet')['Body'].read()), columns=['station_id', 'fetched_at', 'capacity']).to_pandas()
info['fetched_at'] = pd.to_datetime(info.fetched_at, utc=True)
pre = info.loc[info.fetched_at <= START].sort_values('fetched_at').drop_duplicates('station_id', keep='last')
pre = pre.loc[pre.capacity > 0, ['station_id']]
ids_df = spark.createDataFrame(pre.astype({'station_id': 'str'}))
C = {r.station_id for r in ids_df.orderBy(F.xxhash64('station_id'), 'station_id').limit(100).collect()}
COHORT_SHA = '3d5bf5cd0594981fb60b995a08f6c305b2927122ae1299021e413a1b35414ff3'
assert len(C) == 100 and hashlib.sha256('\n'.join(sorted(C)).encode()).hexdigest() == COHORT_SHA
info = info.loc[info.station_id.isin(C)].rename(columns={'fetched_at': 'metadata_time'}).sort_values('metadata_time')
columns = ['station_id', 'fetched_at', 'station_reported_at', 'num_bikes_available', 'num_docks_available', 'is_installed', 'is_renting', 'is_returning']
days = pd.date_range('2026-08-24', '2026-10-04', freq='D')
assert days.max() < pd.Timestamp('2026-10-05')
parts = []
for day in days:
    prefix = 'exports/station_status/nyc_date=' + day.strftime('%Y-%m-%d') + '/'
    items = client.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', [])
    keys = [o['Key'] for o in items if o['Key'].endswith('.parquet')]
    if len(keys) != 1: raise ValueError('Unexpected number of files for ' + day.strftime('%Y-%m-%d'))
    frame = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket, Key=keys[0])['Body'].read()), columns=columns).to_pandas()
    parts.append(frame[frame.station_id.isin(C)])
raw = pd.concat(parts, ignore_index=True)
for col in ['fetched_at', 'station_reported_at']: raw[col] = pd.to_datetime(raw[col], utc=True)
raw = raw.loc[(raw.fetched_at >= START) & (raw.fetched_at < END)].sort_values('fetched_at')
assert not raw.duplicated(['station_id', 'fetched_at']).any() and raw.fetched_at.max() < RESERVED_TEST_START
print('READ_COMPLETE', len(raw), 'days', len(days), 'last_day', str(days.max().date()), flush=True)

# COMMAND ----------

# Cell 3: point-in-time metadata, labels and station features (same logic as the principal notebook cell 2)
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
end_train = VALID - pd.Timedelta(seconds=3750); end_val = END - pd.Timedelta(seconds=3750)
within = ((df.fetched_at < end_train) | ((df.fetched_at >= VALID) & (df.fetched_at < end_val)))
examples = df.loc[within & df.both].copy()
examples['target_at'] = examples.fetched_at + pd.Timedelta(minutes=60)
future = df[['station_id', 'fetched_at', 'bike_ok', 'dock_ok', 'num_bikes_available', 'num_docks_available']].rename(columns={'fetched_at': 'future_at', 'bike_ok': 'future_bike_ok', 'dock_ok': 'future_dock_ok', 'num_bikes_available': 'future_bikes', 'num_docks_available': 'future_docks'})
examples = pd.merge_asof(examples.sort_values('target_at'), future.sort_values('future_at'), by='station_id', left_on='target_at', right_on='future_at', direction='nearest', tolerance=pd.Timedelta(seconds=150))
examples['split'] = np.where(examples.fetched_at < VALID, 'train', 'validation')
for o in ['bike', 'dock']:
    examples[o + '_label'] = np.where(examples[o + '_ok'] & examples['future_' + o + '_ok'].fillna(False), (examples['future_' + o + 's'] == 0).astype(float), np.nan)
examples['bike_fraction'] = examples.num_bikes_available / examples.capacity
examples['dock_fraction'] = examples.num_docks_available / examples.capacity
local = examples.fetched_at.dt.tz_convert('America/New_York')
phase = (local.dt.hour + local.dt.minute / 60 + local.dt.second / 3600) * 2 * np.pi / 24
examples['hour_sin'] = np.sin(phase); examples['hour_cos'] = np.cos(phase)
assert not examples.duplicated(['station_id', 'fetched_at']).any()
assert examples.target_at.max() + pd.Timedelta(seconds=150) < RESERVED_TEST_START
features = ['num_bikes_available', 'num_docks_available', 'capacity', 'bike_fraction', 'dock_fraction', 'hour_sin', 'hour_cos']
assert np.isfinite(examples[features].to_numpy()).all()
repro = {}
for o, count in [('bike', 'num_bikes_available'), ('dock', 'num_docks_available')]:
    tr = examples[(examples.split == 'train') & examples[o + '_label'].notna()]
    val = examples[examples.split == 'validation']
    repro[o] = {'train_labeled': len(tr), 'val_labeled': int(val[o + '_label'].notna().sum()), 'val_candidates': int((val[o + '_ok'] & (val[{'bike': 'num_bikes_available', 'dock': 'num_docks_available'}[o]] > 0)).sum())}
expected = {'bike': {'train_labeled': 944792, 'val_labeled': 192544, 'val_candidates': 185592}, 'dock': {'train_labeled': 944792, 'val_labeled': 192544, 'val_candidates': 174735}}
SUMMARY['population_reproduction'] = {'observed': repro, 'expected_from_report': expected, 'match': repro == expected}
print('POPULATION', json.dumps(SUMMARY['population_reproduction']), flush=True)

# COMMAND ----------

# Cell 4: ACS provenance and missingness (no rebuild of the ACS table)
acs_features = ['estimated_population_500m', 'bachelors_plus_pct_500m', 'households_no_vehicle_pct_500m', 'poverty_pct_500m', 'bicycle_commute_pct_500m', 'school_enrollment_pct_500m']
acs_path = home / 'Citi Bike - ACS and July Trip Development - e1eb6e26' / 'acs_station_features.parquet'
acs_bytes = acs_path.read_bytes(); ACS_SHA = hashlib.sha256(acs_bytes).hexdigest()
acs = pd.read_parquet(io.BytesIO(acs_bytes))
acs_columns = acs.columns.tolist()
for k in acs_features: acs[k] = pd.to_numeric(acs[k], errors='coerce')
acs['station_id'] = acs.station_id.astype(str)
assert len(acs) == 100 and not acs.station_id.duplicated().any() and set(acs.station_id) == set(C)
meta = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket, Key='exports/station_info.parquet')['Body'].read()), columns=['station_id', 'fetched_at', 'name', 'short_name']).to_pandas()
meta['fetched_at'] = pd.to_datetime(meta.fetched_at, utc=True)
meta = meta[meta.station_id.isin(C) & (meta.fetched_at <= START)].sort_values('fetched_at').drop_duplicates('station_id', keep='last')
miss = acs[acs[acs_features].isna().any(axis=1)].merge(meta[['station_id', 'name', 'short_name']], on='station_id', how='left')
missing_records = [{'station_id': r.station_id, 'name': r.name, 'short_name': r.short_name, 'missing_fields': [k for k in acs_features if pd.isna(getattr(r, k))]} for r in miss.itertuples()]
# Compare the parquet with the ACS table embedded in the principal notebook cell 17 (the table used for the ACS models in cell 18)
embedded_check = {'status': 'not_checked'}
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ExportFormat
    w = WorkspaceClient()
    src = base64.b64decode(w.workspace.export(str(Path('/Users') / user / 'Citi Bike — Positive-Count Model Comparison'), format=ExportFormat.SOURCE).content).decode('utf-8')
    blob = re.search(r"blob='([A-Za-z0-9+/=]+)'", src).group(1)
    raw_json = gzip.decompress(base64.b64decode(blob))
    emb = pd.DataFrame(json.loads(raw_json))
    for k in acs_features: emb[k] = pd.to_numeric(emb[k], errors='coerce')
    emb['station_id'] = emb.station_id.astype(str)
    cmp = acs.set_index('station_id')[acs_features].sort_index().compare(emb.set_index('station_id')[acs_features].sort_index()) if set(emb.station_id) == set(acs.station_id) else None
    embedded_check = {'status': 'checked', 'embedded_json_sha256': hashlib.sha256(raw_json).hexdigest(), 'expected_embedded_sha256': '18f55c17cbf88dfe07dc4b7f3152aca23c002ba5afb011307f52491dae14da83', 'same_station_set': cmp is not None, 'differing_cells': None if cmp is None else int(cmp.notna().sum().sum() // 2), 'embedded_columns': emb.columns.tolist()}
except Exception as e:
    embedded_check = {'status': 'error', 'error': type(e).__name__ + ': ' + str(e)[:200]}
nan_train_rows = int(examples.station_id.isin(miss.station_id).sum())
SUMMARY['acs'] = {'path': str(acs_path.relative_to(home)), 'sha256': ACS_SHA, 'bytes': len(acs_bytes), 'columns': acs_columns, 'features_in_model_order': acs_features, 'stations': len(acs), 'missing_by_field': acs[acs_features].isna().sum().to_dict(), 'stations_with_missing': missing_records, 'example_rows_at_missing_station': nan_train_rows, 'embedded_table_check': embedded_check, 'value_summary': acs[acs_features].describe().round(4).to_dict()}
print('ACS_PROVENANCE', json.dumps(SUMMARY['acs'], default=str), flush=True)

# COMMAND ----------

# Cell 5: refit station-only, ACS candidate, permuted-ACS controls and prior shortage-rate control on identical rows
## Three development weeks; each is scored by models trained only on earlier data
WEEKS = [('Sep14-20', pd.Timestamp('2026-09-14T04:00Z'), pd.Timestamp('2026-09-21T04:00Z')), ('Sep21-27', pd.Timestamp('2026-09-21T04:00Z'), pd.Timestamp('2026-09-28T04:00Z')), ('Sep28-Oct04', VALID, END)]
OUTCOMES = [('bike', 'num_bikes_available'), ('dock', 'num_docks_available')]
# Outcome gap: 60 minutes plus a 150-second matching window (3,750 seconds)
GAP = pd.Timedelta(seconds=3750)
## Boosted-tree settings (the same as step 02), with a fixed random seed
HGB = dict(max_iter=100, max_leaf_nodes=15, min_samples_leaf=200, learning_rate=0.1, random_state=42)
# Early stopping is set explicitly, so the saved models record it
EARLY_STOPPING = True
## Permutation controls: shuffle the ACS rows across stations with three seeds
PERM_SEEDS = [101, 202, 303]
# ACS values by station
station_acs = acs.set_index('station_id')[acs_features]
order = sorted(C)
perm_tables = {}
# Each shuffled table keeps the values but breaks the link to the right station; count stations left in place
for s in PERM_SEEDS:
    p = np.random.default_rng(s).permutation(len(order))
    perm_tables[s] = (pd.DataFrame(station_acs.loc[order].to_numpy()[p], index=pd.Index(order, name='station_id'), columns=acs_features), int((p == np.arange(len(order))).sum()))
## Attach the ACS values to every example, then confirm the rows and their order are unchanged
ex = examples.merge(acs[['station_id'] + acs_features], on='station_id', how='left', validate='many_to_one')
assert len(ex) == len(examples) and ex[['station_id', 'fetched_at']].equals(examples[['station_id', 'fetched_at']].reset_index(drop=True))

## Helper: station features plus any extra station-level columns
def design(frame, extra):
    X = frame[features].copy()
    if extra is not None:
        for c in extra.columns: X[c] = frame.station_id.map(extra[c]).to_numpy()
    return X

## Helper: the five stations with the highest Risk Scores per poll (ties broken by station ID)
def top5(pool, lab, risk):
    p = pool[['station_id', 'fetched_at', lab]].copy(); p['risk'] = risk
    return p.sort_values(['fetched_at', 'risk', 'station_id'], ascending=[True, False, True]).groupby('fetched_at', sort=False).head(5)

## Fit every variant on identical training rows and score identical candidate pools
results, picks_store, pools, models, es_checks = [], {}, {}, {}, []
for week, start, end in WEEKS:
    for o, count in OUTCOMES:
        lab = o + '_label'
        # Training rows end one gap before the scored week; the scored pool leaves out stations already empty
        train = ex[(ex.fetched_at < start - GAP) & ex[lab].notna() & (ex[count] > 0)]
        pool = ex[(ex.fetched_at >= start) & (ex.fetched_at < end - GAP) & ex[o + '_ok'] & (ex[count] > 0)].copy()
        # Confirm no label crosses the week boundary
        assert len(pool) and pool.target_at.max() + pd.Timedelta(seconds=150) < end and train.fetched_at.max() < start - GAP
        pools[week, o] = pool
        y = train[lab].astype(int)
        # Prior shortage rate per station, from training rows only (a control that uses no ACS)
        prior = train.groupby('station_id')[lab].mean().rename('prior_shortage_rate').to_frame()
        # Variants: station features only, real ACS, three shuffled-ACS controls, and the prior-rate control
        variants = [('station_only', None), ('acs', station_acs)] + [('perm_acs_seed%d' % s, perm_tables[s][0]) for s in PERM_SEEDS] + [('prior_rate', prior)]
        known = pool[lab].notna().to_numpy()
        for name, extra in variants:
            Xtr, Xpool = design(train, extra), design(pool, extra)
            # Fit, score, pick the top five per poll, and count true positives and average precision
            m = HistGradientBoostingClassifier(early_stopping=EARLY_STOPPING, **HGB).fit(Xtr, y)
            risk = m.predict_proba(Xpool)[:, 1]
            pk = top5(pool, lab, risk); picks_store[week, o, name] = pk; models[week, o, name] = m
            nk = int(pk[lab].notna().sum()); caught = int(pk[lab].sum())
            ap = float(average_precision_score(pool.loc[known, lab].astype(int), risk[known]))
            results.append((week, o, name, len(train), len(pool), len(pk), caught, nk - caught, len(pk) - nk, int(m.n_iter_), round(ap, 6)))
            print('FIT', results[-1], flush=True)
            # For the ACS model only, also fit with the library's default early-stopping setting and compare
            if name == 'acs':
                m_auto = HistGradientBoostingClassifier(**HGB).fit(Xtr, y)
                r_auto = m_auto.predict_proba(Xpool)[:, 1]
                pk_auto = top5(pool, lab, r_auto)
                es_checks.append({'week': week, 'outcome': o, 'explicit_early_stopping': EARLY_STOPPING, 'n_iter_explicit': int(m.n_iter_), 'n_iter_auto': int(m_auto.n_iter_), 'auto_resolves_on': bool(len(Xtr) > 10000), 'max_abs_prob_diff': float(np.max(np.abs(risk - r_auto))), 'same_selected_keys': bool(pk[['station_id', 'fetched_at']].reset_index(drop=True).equals(pk_auto[['station_id', 'fetched_at']].reset_index(drop=True)))})
                print('EARLY_STOPPING_CHECK', es_checks[-1], flush=True)
        # Low-count rule: the five stations with the fewest bikes (or docks) per poll
        pk = pool.sort_values(['fetched_at', count, 'station_id']).groupby('fetched_at', sort=False).head(5)[['station_id', 'fetched_at', lab]]
        picks_store[week, o, 'low_count'] = pk
        nk = int(pk[lab].notna().sum()); caught = int(pk[lab].sum())
        results.append((week, o, 'low_count', 0, len(pool), len(pk), caught, nk - caught, len(pk) - nk, None, None))
## Results table, compared with the counts in earlier reports
res = pd.DataFrame(results, columns=['week', 'outcome', 'model', 'train_rows', 'candidates', 'alerts', 'caught', 'known_negative', 'unknown', 'n_iter', 'eligible_average_precision'])
expected_caught = {('Sep14-20', 'bike', 'station_only'): 2165, ('Sep14-20', 'dock', 'station_only'): 2819, ('Sep21-27', 'bike', 'station_only'): 1520, ('Sep21-27', 'dock', 'station_only'): 2376, ('Sep28-Oct04', 'bike', 'station_only'): 1715, ('Sep28-Oct04', 'dock', 'station_only'): 2362,
                   ('Sep14-20', 'bike', 'acs'): 2197, ('Sep14-20', 'dock', 'acs'): 3164, ('Sep21-27', 'bike', 'acs'): 1592, ('Sep21-27', 'dock', 'acs'): 2505, ('Sep28-Oct04', 'bike', 'acs'): 1814, ('Sep28-Oct04', 'dock', 'acs'): 2656,
                   ('Sep14-20', 'bike', 'low_count'): 1830, ('Sep14-20', 'dock', 'low_count'): 2341, ('Sep21-27', 'bike', 'low_count'): 1347, ('Sep21-27', 'dock', 'low_count'): 2033, ('Sep28-Oct04', 'bike', 'low_count'): 1575, ('Sep28-Oct04', 'dock', 'low_count'): 2179}
res['expected_caught_from_reports'] = [expected_caught.get((r.week, r.outcome, r.model)) for r in res.itertuples()]
SUMMARY['results'] = res.to_dict('records')
SUMMARY['reproduction_mismatches'] = res[res.expected_caught_from_reports.notna() & (res.caught != res.expected_caught_from_reports)][['week', 'outcome', 'model', 'caught', 'expected_caught_from_reports']].to_dict('records')
# Record how many stations each shuffle left in place, and the early-stopping checks
SUMMARY['permutation_fixed_points'] = {s: perm_tables[s][1] for s in PERM_SEEDS}
SUMMARY['early_stopping_checks'] = es_checks
# Display any mismatch with the earlier reports (expected: none)
print('REPRODUCTION_MISMATCHES', SUMMARY['reproduction_mismatches'], flush=True)

# COMMAND ----------

# Cell 6: saved-model early-stopping evidence (read-only loads of your own saved research files, verified by hash first)
## Read-only checks of earlier saved models: confirm the fingerprint first, then load and compare with today's refit
saved = {}
try:
    # Station-only boosted models saved by step 02
    m1 = json.loads((home / 'citibike_experimental_models_c4e388f77d614002a3512d0478d40037' / 'manifest.json').read_text())
    for o in ['bike', 'dock']:
        p = home / 'citibike_experimental_models_c4e388f77d614002a3512d0478d40037' / (o + '_boosted_positive.joblib')
        h = hashlib.sha256(p.read_bytes()).hexdigest(); ok = h == m1['models'][p.name]['sha256']
        rec = {'file': p.name, 'sha256_matches_manifest': ok}
        if ok:
            sm = joblib.load(p); rec.update({'early_stopping_param': str(sm.get_params()['early_stopping']), 'n_iter_': int(sm.n_iter_), 'refit_same_window_max_abs_diff': float(np.max(np.abs(sm.predict_proba(design(pools['Sep28-Oct04', o], None))[:, 1] - models['Sep28-Oct04', o, 'station_only'].predict_proba(design(pools['Sep28-Oct04', o], None))[:, 1])))})
        saved['station_only_' + o] = rec
    # ACS models saved by step 02 (their manifest has no fingerprint, so the file hash is recorded instead)
    for o in ['bike', 'dock']:
        p = home / 'Citi Bike - ACS and July Trip Development - e1eb6e26' / (o + '_acs.joblib')
        rec = {'file': p.name, 'sha256': hashlib.sha256(p.read_bytes()).hexdigest(), 'manifest_has_hash': False}
        sm = joblib.load(p)
        Xp = design(pools['Sep28-Oct04', o], station_acs)
        rec.update({'early_stopping_param': str(sm.get_params()['early_stopping']), 'n_iter_': int(sm.n_iter_), 'feature_names_in_': list(sm.feature_names_in_), 'refit_same_window_max_abs_diff': float(np.max(np.abs(sm.predict_proba(Xp[list(sm.feature_names_in_)])[:, 1] - models['Sep28-Oct04', o, 'acs'].predict_proba(Xp)[:, 1])))})
        saved['acs_' + o] = rec
except Exception as e:
    saved['error'] = type(e).__name__ + ': ' + str(e)[:300]
# Display the checks
SUMMARY['saved_model_checks'] = saved
print('SAVED_MODEL_CHECKS', json.dumps(saved), flush=True)

# COMMAND ----------

# Cell 7: paired station-day resampling intervals (per week and pooled over the three weeks)
## Comparisons: ACS, each shuffled control and the prior-rate control against station-only; ACS against the low-count rule
COMPARE = [('acs', 'station_only')] + [('perm_acs_seed%d' % s, 'station_only') for s in PERM_SEEDS] + [('prior_rate', 'station_only'), ('acs', 'low_count')]
# Fixed seed and 1,000 resamples
rng = np.random.default_rng(20261110); B = 1000

## Helper: true positives per station per New York day
def station_day(frame, lab, idx):
    x = frame[['station_id', 'fetched_at', lab]].copy(); x['day'] = x.fetched_at.dt.tz_convert('America/New_York').dt.date; x['confirmed'] = x[lab].fillna(0)
    return x.groupby(['station_id', 'day']).confirmed.sum().reindex(idx, fill_value=0)

## Each week and outcome: resample station-days with replacement; the 2.5 and 97.5 percentiles give the interval
intervals, pooled_delta = [], {}
for week, start, end in WEEKS:
    for o, count in OUTCOMES:
        lab = o + '_label'; u = pools[week, o][['station_id', 'fetched_at']].copy(); u['day'] = u.fetched_at.dt.tz_convert('America/New_York').dt.date
        idx = u[['station_id', 'day']].drop_duplicates().set_index(['station_id', 'day']).index
        for a, b in COMPARE:
            d = (station_day(picks_store[week, o, a], lab, idx) - station_day(picks_store[week, o, b], lab, idx)).to_numpy()
            pooled_delta.setdefault((o, a, b), []).append(d)
            draws = d[rng.integers(0, len(d), size=(B, len(d)))].sum(axis=1)
            intervals.append((week, o, a, b, int(d.sum()), float(np.quantile(draws, .025)), float(np.quantile(draws, .975)), len(d)))
## Pooled over the three weeks
for (o, a, b), parts_ in pooled_delta.items():
    d = np.concatenate(parts_); draws = d[rng.integers(0, len(d), size=(B, len(d)))].sum(axis=1)
    intervals.append(('pooled', o, a, b, int(d.sum()), float(np.quantile(draws, .025)), float(np.quantile(draws, .975)), len(d)))
# Interval table
ints = pd.DataFrame(intervals, columns=['week', 'outcome', 'model', 'versus', 'difference', 'lo95', 'hi95', 'station_days'])
SUMMARY['intervals'] = ints.round(1).to_dict('records')
print('INTERVALS_DONE', len(ints), flush=True)

# COMMAND ----------

# Cell 8: neighborhood audit for ACS candidate, station-only boosted and low-count rule
## Helper: split the 100 stations into lower, middle and higher thirds of one ACS measure (missing kept separate)
def tertiles(col):
    g = acs[['station_id', col]].copy(); g['group'] = 'missing'; k = g[col].notna()
    g.loc[k, 'group'] = pd.qcut(g.loc[k, col].rank(method='first'), 3, labels=['lower', 'middle', 'higher']).astype(str)
    return g.set_index('station_id').group
## Two groupings: poverty share and households without a vehicle
GROUPINGS = {'poverty_share': tertiles('poverty_pct_500m'), 'no_vehicle_share': tertiles('households_no_vehicle_pct_500m')}
# Models audited: ACS candidate, station-only boosted, low-count rule
AUDIT_MODELS = ['acs', 'station_only', 'low_count']
## Per-station totals for each week, outcome and model: eligible snapshots, known shortages, alerts, true positives
station_rows = []
for week, start, end in WEEKS:
    for o, count in OUTCOMES:
        lab = o + '_label'; pool = pools[week, o]
        el = pool.groupby('station_id')[lab].agg(eligible='size', eligible_known='count', positives='sum')
        for mname in AUDIT_MODELS:
            pk = picks_store[week, o, mname]
            al = pk.groupby('station_id')[lab].agg(alerts='size', alert_known='count', caught='sum')
            st = pd.DataFrame(index=pd.Index(sorted(C), name='station_id')).join(el).join(al).fillna(0)
            st['week'] = week; st['outcome'] = o; st['model'] = mname
            station_rows.append(st.reset_index())
stations = pd.concat(station_rows, ignore_index=True)
# Attach each station's group
for gname, gmap in GROUPINGS.items(): stations[gname] = stations.station_id.map(gmap)

## Helper: group totals, capture rate, share of alerts, share of known shortages, and unknown-label rates
def group_table(frame, gname):
    t = frame.groupby(['outcome', 'model', gname])[['eligible', 'eligible_known', 'positives', 'alerts', 'alert_known', 'caught']].sum().reset_index()
    tot = t.groupby(['outcome', 'model'])[['alerts', 'positives']].transform('sum')
    t['capture'] = t.caught / t.positives.replace(0, np.nan)
    t['share_of_alerts'] = t.alerts / tot.alerts
    t['share_of_known_shortages'] = t.positives / tot.positives
    t['eligible_unknown_rate'] = 1 - t.eligible_known / t.eligible.replace(0, np.nan)
    t['alert_unknown_rate'] = 1 - t.alert_known / t.alerts.replace(0, np.nan)
    t['stations'] = frame.groupby(['outcome', 'model', gname]).station_id.nunique().to_numpy()
    return t.rename(columns={gname: 'group'})

## Group tables for each week and pooled
audit = {}
for gname in GROUPINGS:
    weekly = pd.concat([group_table(stations[stations.week == w], gname).assign(week=w) for w, _, _ in WEEKS], ignore_index=True)
    pooled = group_table(stations, gname).assign(week='pooled')
    audit[gname] = pd.concat([weekly, pooled], ignore_index=True)

# Pooled three-week station-cluster resampling of higher minus lower capture, stratified by group
# 3,000 resamples of stations within each group, with a fixed seed
rng = np.random.default_rng(20261112); BC = 3000; cluster = []
for gname in GROUPINGS:
    tot = stations.groupby(['outcome', 'model', 'station_id', gname])[['positives', 'caught']].sum().reset_index()
    for (o, mname), sub in tot.groupby(['outcome', 'model']):
        hi = sub[sub[gname] == 'higher'][['positives', 'caught']].to_numpy(); lo = sub[sub[gname] == 'lower'][['positives', 'caught']].to_numpy()
        point = hi[:, 1].sum() / hi[:, 0].sum() - lo[:, 1].sum() / lo[:, 0].sum()
        hs = hi[rng.integers(0, len(hi), size=(BC, len(hi)))].sum(axis=1); ls = lo[rng.integers(0, len(lo), size=(BC, len(lo)))].sum(axis=1)
        d = hs[:, 1] / hs[:, 0] - ls[:, 1] / ls[:, 0]
        # How concentrated the higher group is: known shortages at its top three stations
        top3 = sub[sub[gname] == 'higher'].sort_values('positives', ascending=False).head(3)
        cluster.append((gname, o, mname, round(float(point), 4), round(float(np.nanquantile(d, .025)), 4), round(float(np.nanquantile(d, .975)), 4), len(hi), len(lo), int(top3.positives.sum()), int(hi[:, 0].sum())))
clus = pd.DataFrame(cluster, columns=['grouping', 'outcome', 'model', 'higher_minus_lower_capture', 'lo95', 'hi95', 'higher_stations', 'lower_stations', 'higher_top3_positives', 'higher_total_positives'])
## Record the pooled tables, the intervals and the reading note
SUMMARY['audit_pooled'] = {g: audit[g][audit[g].week == 'pooled'].round(4).to_dict('records') for g in GROUPINGS}
SUMMARY['audit_cluster_intervals'] = clus.to_dict('records')
SUMMARY['audit_note'] = 'Poverty share is both a model input and the audit grouping variable. Groups describe 500m neighborhood estimates, not riders. Differences are signals for review, not evidence of cause.'
print('AUDIT_DONE', len(stations), flush=True)

# COMMAND ----------

# Cell 9: save candidate artifacts and tables to the new private folder (no S3)
## Final feature order: seven station features, then six ACS measures
FINAL_FEATURES = features + acs_features
cand = {}
for o, count in OUTCOMES:
    lab = o + '_label'
    # Two training windows: before Sep 28 (validated, later frozen) and through Oct 4 (saved, not frozen)
    for window, cutoff in [('train_before_Sep28', VALID - GAP), ('train_through_Oct04', END - GAP)]:
        if window == 'train_before_Sep28':
            m = models['Sep28-Oct04', o, 'acs']
        else:
            # Refit on development data through Oct 4, confirming no label reaches the test week
            tr = ex[(ex.fetched_at < cutoff) & ex[lab].notna() & (ex[count] > 0)]
            assert tr.target_at.max() + pd.Timedelta(seconds=150) < RESERVED_TEST_START
            m = HistGradientBoostingClassifier(early_stopping=EARLY_STOPPING, **HGB).fit(design(tr, station_acs), tr[lab].astype(int))
        # Confirm the feature order, then save the model and record its fingerprint
        assert list(m.feature_names_in_) == FINAL_FEATURES
        name = '%s_acs_candidate_%s.joblib' % (o, window)
        joblib.dump(m, out_dir / name)
        cand[name] = {'outcome': o, 'training_window': window, 'training_cutoff_exclusive_utc': cutoff.isoformat(), 'sha256': hashlib.sha256((out_dir / name).read_bytes()).hexdigest(), 'bytes': (out_dir / name).stat().st_size, 'n_iter_': int(m.n_iter_), 'features': FINAL_FEATURES}
## Manifest for the candidate files (the freeze record later fixed these fingerprints)
manifest = {'purpose': 'Exploratory development; NOT frozen. Freeze requires explicit approval from Robert.', 'run_id': RUN_ID, 'sklearn_version': sklearn.__version__, 'model_class': 'HistGradientBoostingClassifier', 'params': dict(HGB, early_stopping=EARLY_STOPPING), 'early_stopping_note': 'Set explicitly to True. scikit-learn default "auto" enables early stopping above 10,000 samples, so earlier fits used early stopping too; see early_stopping_checks.',
            'feature_order': FINAL_FEATURES, 'acs_table': {'path': SUMMARY['acs']['path'], 'sha256': ACS_SHA}, 'missing_value_policy': 'ACS NaN values are passed to the model unchanged (native missing-value handling). No imputation in training or scoring.', 'positive_count_rule': 'Score an outcome only when its current count is greater than zero.', 'alert_rule': 'Top five by probability per outcome per update, ties by station_id ascending.',
            'cohort_sha256': COHORT_SHA, 'reserved_test_scored': False, 'models': cand}
(out_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2))
## Save the tables and the run summary to the new private folder
res.to_parquet(out_dir / 'development_results.parquet'); ints.to_parquet(out_dir / 'station_day_intervals.parquet'); clus.to_parquet(out_dir / 'audit_cluster_intervals.parquet')
stations.to_parquet(out_dir / 'audit_station_level.parquet')
for g in GROUPINGS: audit[g].to_parquet(out_dir / ('audit_groups_%s.parquet' % g))
SUMMARY['candidate_manifest'] = manifest
SUMMARY['finished_utc'] = pd.Timestamp.now('UTC').isoformat()
(out_dir / 'summary.json').write_text(json.dumps(SUMMARY, indent=2, default=str))
# Display what was saved
print('SAVED', out_dir.name, sorted(p.name for p in out_dir.iterdir()), flush=True)

# COMMAND ----------

# Cell 10: compact summary for review
## Display a compact summary
print(json.dumps({k: SUMMARY[k] for k in ['run_id', 'output_folder', 'sklearn_version', 'population_reproduction', 'reproduction_mismatches', 'early_stopping_checks', 'saved_model_checks', 'permutation_fixed_points']}, default=str, indent=1))