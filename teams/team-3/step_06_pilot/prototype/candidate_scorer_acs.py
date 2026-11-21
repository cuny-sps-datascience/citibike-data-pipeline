"""Prototype v14: ACS candidate scorer with a logistic fallback. Development only.

The candidate is the positive-count boosted tree using the seven station features plus six
static ACS (American Community Survey) measures joined by station ID. This module performs
no network calls, no writes and no scheduling.

Only load trusted artifacts. joblib deserialization can execute code, and a matching hash in an
adjacent manifest checks integrity, not origin.
"""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Purpose: load the model the pilot actually uses (boosted trees with ACS
# neighborhood features). If any candidate file fails a check, fall back to the
# frozen logistic baseline and record why. The fallback keeps a window going when
# only the candidate fails; it can still fail if its own files fail to load, and the
# runner has other failure exits.

## Packages
import hashlib  # File fingerprints
import io  # Read table bytes that were already fingerprinted
import json  # Read the model manifest
import math  # Detect infinite values
from pathlib import Path  # Build file paths

import joblib  # Load saved scikit-learn models
import pandas as pd  # Read the ACS table and build feature rows
import sklearn  # Check the installed version against training

from scoring import FEATURES, StationFeatureMissing, load_artifact, load_cohort, score_snapshot  # Shared scoring path

## Constants
# Six ACS 2020-2024 estimates for the area within 500 meters of each station.
ACS_FEATURES = ['estimated_population_500m', 'bachelors_plus_pct_500m', 'households_no_vehicle_pct_500m',  # Population, education, no-vehicle households
                'poverty_pct_500m', 'bicycle_commute_pct_500m', 'school_enrollment_pct_500m']  # Poverty, bike commuting, school enrollment
CANDIDATE_FEATURES = FEATURES + ACS_FEATURES  # 7 live features followed by 6 ACS features (13 total)
OUTCOMES = ('bike', 'dock')  # One model per shortage type


## Calculate a SHA-256 fingerprint of bytes
def _sha256(data):
    """Return the SHA-256 hex digest of a bytes object."""
    return hashlib.sha256(data).hexdigest()


## Load the ACS table and confirm it matches the frozen one
def load_acs_table(path, expected_sha256, cohort):
    """Return {station_id: [six ACS values]} after hash, schema and coverage checks.

    Missing ACS values stay NaN, matching training, where the boosted tree handled them natively.
    The table must cover exactly the cohort, so a cohort station cannot be absent at scoring time
    unless the in-memory table is altered; scoring still guards that case.
    """
    data = Path(path).read_bytes()  # Read once, then fingerprint and parse the same bytes
    # Confirm the table is byte-for-byte the frozen ACS table.
    if _sha256(data) != expected_sha256:
        raise ValueError('ACS table checksum mismatch')
    # Read Parquet or CSV; station IDs stay text so leading zeros or letters are kept.
    if str(path).endswith('.parquet'):
        table = pd.read_parquet(io.BytesIO(data))
    else:
        table = pd.read_csv(io.BytesIO(data), dtype={'station_id': str})
    # Confirm every needed column is present.
    missing = [c for c in ['station_id'] + ACS_FEATURES if c not in table.columns]
    if missing:
        raise ValueError('ACS table lacks columns: ' + ', '.join(missing))
    table = table[['station_id'] + ACS_FEATURES].copy()  # Keep only the needed columns, in order
    table['station_id'] = table.station_id.astype(str)
    # Confirm one row per station.
    if table.station_id.duplicated().any():
        raise ValueError('ACS table has duplicate station IDs')
    # Confirm the table covers exactly the 100 cohort stations.
    if set(table.station_id) != set(cohort):
        raise ValueError('ACS table stations do not match the cohort')
    # Convert values to numbers; anything unreadable becomes NaN, as in training.
    for c in ACS_FEATURES:
        table[c] = pd.to_numeric(table[c], errors='coerce')
    values = table[ACS_FEATURES].to_numpy(dtype=float)
    # Confirm there are no infinite values (NaN is allowed; infinity is not).
    if any(math.isinf(v) for v in values.ravel()):
        raise ValueError('ACS table contains infinite values')
    return {sid: [float(v) for v in row] for sid, row in zip(table.station_id, values)}  # station_id -> 6 values


## Load and verify the ACS candidate models for one model version
# 'window' here is the model training-period suffix in the file names (the pilot passes
# 'train_before_Sep28'), not the morning or evening pilot window.
def load_candidate(folder, window, cohort_file, baseline_artifact_file, acs_file):
    """Load and verify the frozen-candidate files. Raises ValueError on any mismatch."""
    folder = Path(folder)
    manifest = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))  # Fingerprints and settings recorded at training
    # Confirm the installed scikit-learn matches training, so the saved models behave the same.
    if manifest.get('sklearn_version') != sklearn.__version__:
        raise ValueError('scikit-learn version mismatch; rebuild or verify compatibility')
    # Confirm the 13 features are in the trained order.
    if manifest.get('feature_order') != CANDIDATE_FEATURES:
        raise ValueError('unexpected candidate feature order')
    # Confirm training recorded early stopping, as approved for the histogram gradient boosting models.
    if manifest.get('params', {}).get('early_stopping') is not True:
        raise ValueError('manifest does not record explicit early_stopping=True')
    baseline = load_artifact(baseline_artifact_file)  # The logistic baseline is still needed for its cohort fingerprint
    # Confirm the candidate and the baseline use the same cohort.
    if manifest.get('cohort_sha256') != baseline['cohort_sha256_sorted_newline']:
        raise ValueError('cohort manifest mismatch')
    cohort = load_cohort(cohort_file, manifest['cohort_sha256'])
    acs = load_acs_table(acs_file, manifest['acs_table']['sha256'], cohort)
    models = {}
    for outcome in OUTCOMES:
        name = '%s_acs_candidate_%s.joblib' % (outcome, window)  # For example bike_acs_candidate_train_before_Sep28.joblib
        meta = manifest['models'][name]
        # Confirm the manifest lists the same feature order for this model.
        if meta.get('features') != CANDIDATE_FEATURES:
            raise ValueError('unexpected feature order for ' + name)
        path = folder / name
        # Confirm the model file is the frozen one before loading it.
        if _sha256(path.read_bytes()) != meta.get('sha256'):
            raise ValueError('model checksum mismatch for ' + name)
        model = joblib.load(path)
        # Confirm 0/1 classes and the same fitted features.
        if list(model.classes_) != [0, 1] or list(model.feature_names_in_) != CANDIDATE_FEATURES:
            raise ValueError('unexpected model class or fitted features for ' + name)
        # Confirm the loaded model itself has early stopping on.
        if model.get_params().get('early_stopping') is not True:
            raise ValueError('model early_stopping is not explicitly True for ' + name)
        models[outcome] = model
    version = 'acs_boosted_positive_%s_%s' % (manifest.get('run_id', 'unknown'), window)  # Label written to every prediction row
    # Return everything the scorer needs, marked as the ACS candidate.
    return {'mode': 'acs_candidate', 'version': version, 'baseline': baseline, 'cohort': cohort,
            'models': models, 'acs': acs, 'fallback_reason': None}


## Load the ACS candidate, or the logistic baseline if the candidate fails
def load_scorer(folder, window, cohort_file, baseline_artifact_file, acs_file):
    """Load the ACS candidate, or fall back to the logistic regression model if any artifact fails.

    The logistic baseline itself must load; if it cannot, this raises and the run fails closed.
    """
    try:
        return load_candidate(folder, window, cohort_file, baseline_artifact_file, acs_file)
    except Exception as exc:  # any candidate-artifact failure selects the fallback
        baseline = load_artifact(baseline_artifact_file)
        cohort = load_cohort(cohort_file, baseline['cohort_sha256_sorted_newline'])
        # Record the reason (first 200 characters) so the window report shows why the fallback was used.
        return {'mode': 'logistic_fallback', 'version': baseline['version'], 'baseline': baseline,
                'cohort': cohort, 'models': None, 'acs': None,
                'fallback_reason': type(exc).__name__ + ': ' + str(exc)[:200]}


## Score one poll with whichever model loaded
def score(records, scorer, scored_at):
    """Score one poll with whichever model loaded. No writes, scheduling or alerts sent."""
    # Fallback: score with the logistic baseline.
    if scorer['mode'] == 'logistic_fallback':
        return score_snapshot(records, scorer['baseline'], scorer['cohort'], scored_at)
    models, acs = scorer['models'], scorer['acs']

    # Return one station's Risk Score: its 7 live features followed by its 6 ACS values.
    def station_probability(outcome, station_id, station_features):
        if station_id not in acs:
            raise StationFeatureMissing(station_id)
        row = list(station_features) + acs[station_id]
        return models[outcome].predict_proba(pd.DataFrame([row], columns=CANDIDATE_FEATURES))[0, 1]

    # Use the shared scoring path; positive_only matches how the candidate was trained.
    return score_snapshot(records, scorer['baseline'], scorer['cohort'], scored_at,
                          station_probability_fn=station_probability, model_version=scorer['version'],
                          positive_only=True)
