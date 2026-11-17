"""Prototype v16: score one poll in a single batch per outcome.

Wraps the frozen v14 loader (`candidate_scorer_acs.load_scorer`) without changing it. The
first pass runs the unchanged eligibility checks and collects feature rows; one
predict_proba call per outcome follows; the second pass rebuilds the same output with those
probabilities. A logistic fallback scorer is passed through unchanged.
"""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Why two passes: calling the model once per station was too slow for a poll
# every few minutes. Pass 1 collects every station's features, one model call
# per outcome scores them all, and pass 2 fills in the results. The output is
# the same as scoring stations one at a time.

## Packages
from scoring import StationFeatureMissing, score_snapshot  # Shared scoring path and its missing-feature error

## Constants: the 13 features in the exact order the ACS candidate models were fitted on
CANDIDATE_FEATURES = ['num_bikes_available', 'num_docks_available', 'capacity', 'bike_fraction',  # Live station counts
                      'dock_fraction', 'hour_sin', 'hour_cos', 'estimated_population_500m',  # Fill shares, time of day, population
                      'bachelors_plus_pct_500m', 'households_no_vehicle_pct_500m', 'poverty_pct_500m',  # ACS neighborhood shares
                      'bicycle_commute_pct_500m', 'school_enrollment_pct_500m']  # ACS shares within 500 m of the station


## Build the scoring function for a loaded scorer
def make_score_fn(scorer):
    """Return (score_fn(rows, scored_at), model_version, mode) for a loaded scorer dict."""
    base, cohort, version = scorer['baseline'], scorer['cohort'], scorer['version']
    # If the ACS models could not be loaded, use the logistic baseline as is.
    if scorer['mode'] == 'logistic_fallback':
        return (lambda rows, at: score_snapshot(rows, base, cohort, at)), version, 'logistic_fallback'
    import pandas as pd  # Needed only for the ACS candidate path
    models, acs = scorer['models'], scorer['acs']  # Fitted models and per-station ACS values
    # Confirm both models still expect the 13 features in this order.
    for o in ('bike', 'dock'):
        if list(models[o].feature_names_in_) != CANDIDATE_FEATURES:
            raise ValueError('candidate feature order changed')

    ## Score one poll in two passes
    def score_fn(rows, scored_at):
        requests = {}  # (outcome, station_id) -> 13 feature values

        # Pass 1 helper: record each station's features instead of predicting.
        def collect(outcome, station_id, x):
            if station_id not in acs:
                raise StationFeatureMissing(station_id)  # No ACS values: this station cannot be scored
            requests[(outcome, station_id)] = list(x) + acs[station_id]  # Live features + ACS features
            return 0.5  # Placeholder probability; pass 1 output is thrown away

        # Pass 1: run the normal eligibility checks and collect feature rows.
        score_snapshot(rows, base, cohort, scored_at, station_probability_fn=collect,
                       model_version=version, positive_only=True)
        probs = {}
        # Score all collected rows with one model call per outcome.
        for o in ('bike', 'dock'):
            keys = [k for k in requests if k[0] == o]
            if keys:
                p = models[o].predict_proba(pd.DataFrame([requests[k] for k in keys], columns=CANDIDATE_FEATURES))[:, 1]  # Probability of shortage
                probs.update(zip(keys, (float(v) for v in p)))  # Store each station's probability

        # Pass 2 helper: return the precomputed Risk Score.
        def lookup(outcome, station_id, x):
            if (outcome, station_id) not in probs:
                raise StationFeatureMissing(station_id)
            return probs[(outcome, station_id)]

        # Pass 2: rebuild the output with the real Risk Scores.
        return score_snapshot(rows, base, cohort, scored_at, station_probability_fn=lookup,
                              model_version=version, positive_only=True)
    return score_fn, version, 'acs_candidate'  # Return the function, its version label, and the mode
