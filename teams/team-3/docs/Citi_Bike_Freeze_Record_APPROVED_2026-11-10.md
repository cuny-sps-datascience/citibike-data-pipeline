# Citi Bike: freeze record (APPROVED)

Prepared November 10, 2026, before any test-week scoring. **Approved by Robert on November 10, 2026** ("I approve the freeze record"). The October 5 to 11 week is to be scored once, with this record unchanged. The draft is preserved separately.

## Training window (decided November 10, 2026)

Robert chose **A. Trained before Sep 28** (cutoff 2026-09-28 02:57:30 UTC): the object validated on Sep 28 to Oct 4, trained on the same period as the original logistic model. Version B (trained through Oct 4) is kept on file but is not the candidate. This settles the window only; the record as a whole still needs explicit approval.

## Candidate

- Model: `HistGradientBoostingClassifier`, one per outcome (bike, dock).
- Settings: `max_iter=100, max_leaf_nodes=15, min_samples_leaf=200, learning_rate=0.1, random_state=42, early_stopping=True` (explicit; reproduces the earlier default exactly). All fits ran 100 rounds.
- scikit-learn 1.7.2.
- Training rows: known label and current count above zero for that outcome.
- Feature order (13): `num_bikes_available, num_docks_available, capacity, bike_fraction, dock_fraction, hour_sin, hour_cos, estimated_population_500m, bachelors_plus_pct_500m, households_no_vehicle_pct_500m, poverty_pct_500m, bicycle_commute_pct_500m, school_enrollment_pct_500m`. No trip or weather features.
- Missing ACS values: passed as NaN, no imputation, in training and scoring.
- Cohort: 100 stations, SHA-256 `3d5bf5cd0594981fb60b995a08f6c305b2927122ae1299021e413a1b35414ff3`.

## Files (private Databricks folder "Citi Bike - ACS Controls and Neighborhood Audit - ea8df268")

| File | SHA-256 |
|---|---|
| bike_acs_candidate_train_before_Sep28.joblib (A) | aa74b79e7c67276cc8d91895eefb56ec659c620182a6f021f41509eb5febaea4 |
| dock_acs_candidate_train_before_Sep28.joblib (A) | d5ec6ce138da03d111ca912f9523bda5659939a6585400a772ec3c7c4c875dd8 |
| bike_acs_candidate_train_through_Oct04.joblib (B) | 65363b8ea9e532b46f4823c9e0d685ca12271caf10b6f902d82a349053fd3cf5 |
| dock_acs_candidate_train_through_Oct04.joblib (B) | d388ec3e22ac97b96ca09edd3d4243d5e4a49cbf90e0e67df6e7b75674f58df5 |
| manifest.json | f130a24ae73e151826d184703f21a153508328a06bc5c7c019ab94ff0d77a196 |
| ACS table: e1eb6e26/acs_station_features.parquet | 3332babcb605f43c85f60b979bcda82c66e1c5c1199167118f1a333dbc9b84f9 |

Scorer: prototype v14 `candidate_scorer_acs.py` (ACS join by station ID, hash checks, fixed feature order, station absent from table is not scored, logistic fallback if files fail to load). Before freezing, the key-level validation replay below should pass.

## Alert rule

Up to five alerts per outcome per update. Rank by predicted probability, ties broken by station ID ascending. Only stations whose current count for that outcome is above zero and whose service flag is on are candidates. Five per outcome is a pilot rule, not a network-wide budget.

## Planned single test comparison (Oct 5 04:00 UTC to Oct 12 04:00 UTC)

- Methods: ACS candidate; original logistic model (`model_parameters.json`, version development-2026-11-09); low-count rule (five lowest positive current counts, ties by station ID).
- Same candidate pools for all three. Same outcome definition as development: nearest station observation within 150 seconds of 60 minutes ahead; 300-second report freshness; snapshots whose outcome window would cross the test-week end are excluded (3,750-second boundary).
- Primary metric: confirmed shortage snapshots among selected alerts, per outcome. Unknown outcomes are not counted as catches and are reported separately. Secondary: precision among known-label alerts.
- Uncertainty: paired station-day resampling, 1,000 draws, fixed seed, for ACS candidate minus logistic and ACS candidate minus low-count. Neighborhood audit repeated with the same groups and station-cluster resampling.
- Reporting: the result is reported whatever it shows. No tuning or model change after viewing it.
- Caveat already known: the test week was partly exposed earlier through aggregate label counts.

## Pre-freeze checks

1. **Key-level validation replay: PASSED** (Databricks run 038b7619, November 11 03:45 to 04:33 UTC; summary in `05_Replay_Outputs_038b7619/replay_summary.json`, SHA-256 a3482c4c...). Candidate A was loaded through the v14 scorer with all hash checks and scored poll by poll for 2,004 polls in Sep 28 to Oct 4.
   - Bike: 185,592 scores, no missing or extra keys, maximum probability difference 0.0, 10,020 alerts with identical keys, 1,814 confirmed catches (24 unknown), equal to the modeling run.
   - Dock: 174,735 scores, no missing or extra keys, maximum probability difference 0.0, 10,020 alerts with identical keys, 2,656 confirmed catches (8 unknown), equal to the modeling run.
   - The fallback path did not trigger (`load_scorer` returned the candidate).
   - Boundary limitation: development data only, so this confirms the scoring path, not test-week behavior. Availability times are simulated from collection times.
   - Speed note: one-row-at-a-time scoring took 2,827 seconds for 2,004 polls, about 1.4 seconds per poll. That is within a 5-minute polling cycle but should be batched per poll before the pilot.
2. **Explicit approval of this record by Robert: GIVEN November 10, 2026.**
