"""Experimental positive-count boosted scorer; separate from the pilot loop.

Only load trusted local artifacts. joblib deserialization can execute code; a
matching hash in an accompanying manifest does not authenticate its source.
"""
# Reading copy with comments added. The code is unchanged from the original module (this file is not embedded in the deployed notebook).
#
# Purpose: load the two boosted models (bike shortage, dock shortage) without
# the ACS neighborhood features, check that they match the frozen cohort and
# feature list, and score a poll. The pilot itself uses candidate_scorer_acs.py.

## Packages
import hashlib,json  # Model fingerprints and the manifest file
from pathlib import Path  # Build file paths
import joblib,pandas as pd,sklearn  # Load saved models, build feature tables, check library version
from scoring import FEATURES,load_artifact,load_cohort,score_snapshot  # Shared scoring helpers

## Constants
MODEL_VERSION='experimental_boosted_positive_2026-11-10'  # Label written into every prediction row


## Load and verify the candidate models
def load_candidate(folder, cohort_file, baseline_artifact_file):
    """Return (baseline, cohort, models) after every fingerprint and feature check passes."""
    folder=Path(folder)
    # Load the manifest that lists each model's fingerprint and features.
    with (folder/'manifest.json').open(encoding='utf-8') as f: manifest=json.load(f)
    # Confirm the installed scikit-learn matches the version that trained the models.
    if manifest.get('sklearn_version')!=sklearn.__version__:
        raise ValueError('scikit-learn version mismatch; rebuild or verify compatibility')
    baseline=load_artifact(baseline_artifact_file)  # Load the frozen logistic baseline
    # Confirm the models were trained on the same 100-station cohort as the baseline.
    if manifest.get('cohort_sha256')!=baseline['cohort_sha256_sorted_newline']:
        raise ValueError('cohort manifest mismatch')
    cohort=load_cohort(cohort_file,manifest['cohort_sha256'])  # Load the cohort and check its fingerprint
    models={}
    # Load one model per outcome: bike shortage and dock shortage.
    for outcome in ('bike','dock'):
        name=outcome+'_boosted_positive.joblib'
        meta=manifest['models'][name]
        # Confirm the manifest lists features in the expected order.
        if meta.get('features')!=FEATURES:raise ValueError('unexpected feature order')
        path=folder/name
        # Confirm the model file is byte-for-byte the frozen one before loading it.
        if hashlib.sha256(path.read_bytes()).hexdigest()!=meta.get('sha256'):
            raise ValueError('model checksum mismatch')
        model=joblib.load(path)
        # Confirm the loaded model predicts classes 0/1 and was fitted on the same features.
        if list(model.classes_)!=[0,1] or list(model.feature_names_in_)!=FEATURES:
            raise ValueError('unexpected model class or fitted features')
        models[outcome]=model
    return baseline,cohort,models  # Return everything scoring needs


## Score one poll with the candidate models
def score_candidate(records, baseline, cohort, models, scored_at):
    """Score only positive-count candidates; no writes, scheduling or alerts sent."""
    # Return the Risk Score (model probability of a shortage) for one station's feature values.
    def predict(outcome,values):
        return models[outcome].predict_proba(pd.DataFrame([values],columns=FEATURES))[0,1]
    # Reuse the shared scoring path; positive_only=True skips stations already at zero.
    return score_snapshot(records,baseline,cohort,scored_at,probability_fn=predict,
                          model_version=MODEL_VERSION,positive_only=True)
