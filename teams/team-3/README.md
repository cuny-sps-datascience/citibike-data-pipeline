# Citi Bike Team 3: station shortage ranking pilot

CUNY DATA 622, Team 3. Final package, November 17, 2026.

For a fixed cohort of 100 Citi Bike stations, this project predicts which stations will have zero bikes, and which will have zero open docks, about 60 minutes ahead. An approved histogram gradient boosting model (scikit-learn's `HistGradientBoostingClassifier`) gives each station a Risk Score, and the pilot ranks stations by it. A scheduled Databricks job runs it live twice a day and publishes its predictions and the real outcomes to the team's area in S3 (Amazon Simple Storage Service) for the dashboard.

The folders follow the order the work was done, from raw feed data to the live pilot. Read them top to bottom. **To rerun the whole project, follow [`RUNBOOK.md`](RUNBOOK.md).** It covers what you need, the exact import names, the run order, the expected check values, and the `setup/` helpers.

## Terms
These terms match the v06 reports and the v06.1 presentation.
- **Poll:** one fetch of the public Citi Bike station feed. The pilot polls every 5 minutes.
- **Risk Score:** the model's predicted probability of a shortage about 60 minutes later. The pilot ranks stations by Risk Score, and the five highest per outcome become alerts. **Mean Risk Score** is the average of those probabilities.
- **True positive:** a flagged station observation with a confirmed shortage at the target time.
- **Recall:** true positives ÷ all confirmed shortages among eligible stations. **Precision:** true positives ÷ flagged observations with known outcomes.
- **Methods:** histogram gradient boosting (the approved model), logistic regression (the earlier model), and the low-count rule (the stations with the fewest bikes or open docks available right now).
- **Example (November 13, 3:30 p.m. poll):** Echo Pl & Grand Concourse had a 30.21% Risk Score, was flagged, and was confirmed empty at 4:30. That made it one true positive. Across the whole bike list, two of the three actual shortages were flagged (66.7% Recall), and two of the five selections were confirmed (40% Precision).

Field names in the code and saved files keep their original names, so saved results and the dashboard still load. For example, `caught` holds true positives, `probability` holds the Risk Score, and `acs_candidate` is the histogram gradient boosting method key.

Every notebook and module has comments added for reading. The comments sit on their own lines, and the code is unchanged; this was checked by comparing each file's Python syntax tree with the original. One exception: step 03's cells 2, 3 and 4 have no added comments, because step 04 checks the exact text of those cells before it runs them. A note at the top of step 03 explains what those cells do.

## The steps at a glance
| Step | Folder | What it does | Output used by the next step |
|---|---|---|---|
| 01 | `step_01_upstream_pipeline/` | Shared class pipeline: raw feed files to bronze, silver and gold tables | Station status and station info data |
| 02 | `step_02_model_comparison/` | Compares model types and tests trip and weather features | Choice of histogram gradient boosting on stations with a positive count |
| 03 | `step_03_acs_controls_audit/` | Adds ACS (American Community Survey) measures, runs controls and a neighborhood audit, trains the histogram gradient boosting model | The approved bike and dock models |
| 04 | `step_04_validation_replay/` | Replays the validation week poll by poll through the pilot scorer | Proof that live scoring equals batch scoring |
| 04b | `step_04b_test_week_scoring/` | One-time scoring of the reserved test week (October 5 to 11) under the freeze record | The test-week result |
| 05 | `step_05_development_sensitivity/` | Development checks (M1): five methods, one setting at a time, learning curve | Development results for the write-up |
| 06 | `step_06_pilot/` | The live pilot job and the code it runs | Predictions and outcomes in S3 |
| | `setup/` | Helpers for a rerun: copy a new run folder to the fixed name later steps read, compare model files with the freeze record, rebuild the station-to-group file for the pilot notebook builder | |
| | `docs/` | Freeze record, test-week result, ACS results, operations note, dashboard guide, equity wording, changes from the proposal | |

The model was approved and frozen after step 04 (see the freeze record), then scored once on the test week (October 5 to 11, 2026). Steps 05 and 06 do not change it.

## Step 01: upstream pipeline (shared class code)
These notebooks belong to the shared class pipeline. The runnable originals are at the repository root. The files here are Team 3 reading copies with step comments added. On November 17, 2026 their code was checked against the root notebooks on `main`, and all 7 match. (The root notebooks have not changed since October 10.)

| Order | File here | Original at the repository root | What it builds |
|---|---|---|---|
| 01a | not copied | [`0-citibike_station_info_downloader`](../../0-citibike_station_info_downloader.ipynb), [`0-citibike_station_status_downloader`](../../0-citibike_station_status_downloader.ipynb) | Raw feed archive |
| 01b | `1-citibike-station-info-json-processor.py` | [`1-citibike-station-info-json-processor`](../../1-citibike-station-info-json-processor.ipynb) | Bronze station info table |
| 01c | `1-citibike_json_status_processor.py` | [`1-citibike_json_status_processor`](../../1-citibike_json_status_processor.ipynb) | Bronze station status table |
| 01d | `2-citibike-station-info-silver.py` | [`2-citibike-station-info-silver`](../../2-citibike-station-info-silver.ipynb) | Silver station info table |
| 01e | `2-citibike-station-status-silver.py` | [`2-citibike-station-status-silver`](../../2-citibike-station-status-silver.ipynb) | Silver station status table |
| 01f | `shared-station-feature-pipeline.py` | [`shared-station-feature-pipeline`](../../shared-station-feature-pipeline.ipynb) | Shared feature functions |
| 01f | `3-citibike-stockout-prediction-gold.py` | [`3-citibike-stockout-prediction-gold`](../../3-citibike-stockout-prediction-gold.ipynb) | Gold feature and target tables |
| config | `pipeline_config.py` | [`pipeline_config.py`](../../pipeline_config.py) | Schemas and folder paths for each run mode |

File names are kept as in the original, because the notebooks load `pipeline_config` and `shared-station-feature-pipeline` by name. Team 3 did not rerun these notebooks; they read and write the shared class catalog.

How step 01 connects to step 02: the modeling notebooks read the class S3 export files (`exports/station_info.parquet` and `exports/station_status/nyc_date=*/`), which the root notebook [`5-weekly-training-export`](../../5-weekly-training-export.ipynb) produces from the silver tables. They do not read the gold tables. The gold notebook uses a 30-minute horizon; the Team 3 models use 60 minutes.

## Step 02: model comparison
`step_02_positive_count_model_comparison.py`. This notebook rebuilds the 100-station cohort, reads only dated training and validation partitions, builds point-in-time features and compares model types. It also tested trip and weather features, which were not kept. Every selection used the same validation candidates. Result: a positive-count boosted tree.

## Step 03: ACS controls, intervals and neighborhood audit
`step_03_acs_controls_intervals_neighborhood_audit.py`. This notebook adds six ACS measures to seven station features and trains the histogram gradient boosting model on data before September 28. It runs shuffled-ACS controls and paired station-day resampling intervals, plus an audit by station-area group. It reads only August 24 to October 4 and writes only a new private folder. The bike and dock models it saved became the frozen models (see `docs/Citi_Bike_Freeze_Record_APPROVED_2026-11-10.md` and `docs/Citi_Bike_ACS_Controls_and_Audit_Results_2026-11-10.md`).

## Step 04: validation replay
`step_04_candidate_key_level_validation_replay.py`. This notebook replays the September 28 to October 4 validation week, poll by poll, through the pilot scorer. It then compares the result with a batch prediction from the same model files. It passes when the scored keys, the selected alerts and their labels are identical, and the largest probability difference is below 1e-12.

**Freeze and test week.** After step 04 the model was approved and frozen, then scored once on October 5 to 11. With up to five alerts per outcome per update, histogram gradient boosting had 1,924 bike and 2,603 dock true positives (Recall 58.9% and 50.9%; Precision 19.3% and 26.1%). Logistic regression had 1,640 and 2,234, and the low-count rule 1,614 and 2,144. The week is labeled "partly exposed through earlier aggregate label counts" and is closed to new scoring. The notebook is `step_04b_test_week_scoring/step_04b_one_time_test_week_scoring.py`. See `docs/Citi_Bike_Reserved_Test_Week_Result_2026-11-10.md` and its correction note.

## Step 05: development sensitivity (M1)
`step_05_m1_ordered_development_sensitivity.py`. These are development checks only, and they cannot change the approved model. The notebook fits separate in-memory models and never reads the test week.
- **Part 1:** five methods on September 28 to October 4: the approved histogram gradient boosting model, logistic regression, the low-count rule, persistence and a station hour-of-week rate.
- **Part 2:** training versus validation for the boosted tree, one setting at a time, plus a learning curve with 2 to 5 training weeks.
- **Part 3:** logistic folds.

A repeat run matched exactly except some lasso (L1) rows in Part 3. That is likely because that solver shuffles rows without a fixed seed, but this was not directly tested.

## Step 06: live pilot
| File | Use |
|---|---|
| `step_06_pilot_window_job_v18_IMPORT_INTO_DATABRICKS.py` | The exact notebook the scheduled job runs. It embeds the pilot code as a fingerprinted package. Import this one. |
| `step_06_pilot_window_job_v18_COMMENTED_READING_COPY.py` | The same notebook with comments, for reading. The code is identical. |
| `prototype/` | The pilot code (commented), its tests, `cohort_reconstructed.csv` (the 100 stations) and `model_parameters.json` |

**What the job does:**
- It polls the public station feed every 5 minutes inside two daily windows: 06:30 to 10:30 and 15:30 to 20:30 New York time.
- It gives every station a Risk Score with the approved histogram gradient boosting model and flags the five highest per outcome per update. It also records the five stations a simple low-count rule would flag.
- About 60 minutes later, it checks what actually happened.
- After each window and its one-hour outcome tail, it publishes predictions, outcomes and a run-health file under `pilot/v1/`.
- The pilot sends no alerts to anyone.

**Prototype modules, in the order a window runs:**
1. `pilot_job_entry.py` checks the model and ACS fingerprints. It stops with exit code 4 if any differ.
2. `window_runner.py` and `window_runner_cli.py` drive one window.
3. `pilot_loop.py` polls the feed.
4. `private_artifacts.py` saves each raw feed response with a fingerprint.
5. `normalization.py` cleans each poll.
6. `candidate_scorer_acs.py`, `batch_scorer.py` and `scoring.py` score the stations.
7. `outcomes.py` labels outcomes after 60 minutes.
8. `pilot_state.py` keeps the state and checkpoints.
9. `s3_publisher.py` publishes the window files.

Support modules:
- `dashboard_view.py`: an offline monitoring view of a window
- `check_outcomes.py`: a self-check of the outcome matching
- `replay_guard.py`: keeps historical replays off the test week
- `candidate_scorer.py`: an earlier experimental scorer
- `live_demo.py`: a one-poll public feed demo
- `make_job_notebook.py` with `job_notebook_template.txt`: the notebook builder

The deployed notebook embeds the uncommented versions of the pilot modules. The commented copies here have the same code, checked by comparing Python syntax trees. Import the deployed notebook as is. It is the exact file the job runs. You only need to rebuild it if the model fingerprints change (see `RUNBOOK.md` section 4). To rebuild it, first recreate the station-to-group file with `setup/rebuild_station_groups.py`, then run `make_job_notebook.py`. The rebuilt notebook was checked: it has the same code and identical data files as the deployed one. Its fingerprint differs because of the added comments.

**How the job is configured**
- One task running the v18 notebook on serverless compute.
- Schedule: cron `0 25 6,15 * * ?`, time zone America/New_York (runs start at 06:25 and 15:25).
- Parameters: `publish_mode = on` (`off` and `dry_run` also exist), `retention_keep_last = 3`, `takeover_owner` blank (crash recovery is manual).
- Timeout 6 hours 30 minutes; one concurrent run; no retries; email on failure.
- Credentials are read at run time with `dbutils.secrets.get` from a Databricks secret scope. No key is stored in this repository.
- The operations note in `docs/` is for v17. v18 adds a kill-safe restart, which sets orphaned checkpoint files aside and never deletes them. It also adds the S3 publisher: conditional writes, read-back checks, nothing outside `pilot/v1/`, and no overwrites (a corrected file gets a `_v2` name).

**Run the tests**
```
cd step_06_pilot/prototype
python3 -m pytest -q -p no:cacheprovider
```
Result on this package, November 16, 2026: 88 passed. The tests cover the pilot code only. They do not cover the notebooks, external data or the Databricks environment.

## Approved model files and ACS table
Both live in a private Databricks workspace and are not in this repository. Their fingerprints are fixed by the approved freeze record (`docs/Citi_Bike_Freeze_Record_APPROVED_2026-11-10.md`, SHA-256 `0118cb65eac2d22dcc5a86306f78a1e56fc90dd5a21129821ba452b84ec9c324`).

| File | Where | SHA-256 |
|---|---|---|
| Bike model (histogram gradient boosting) | `Citi Bike - ACS Controls and Neighborhood Audit - ea8df268/bike_acs_candidate_train_before_Sep28.joblib` | `aa74b79e7c67276cc8d91895eefb56ec659c620182a6f021f41509eb5febaea4` |
| Dock model (histogram gradient boosting) | same folder, `dock_acs_candidate_train_before_Sep28.joblib` | `d5ec6ce138da03d111ca912f9523bda5659939a6585400a772ec3c7c4c875dd8` |
| Model manifest | same folder, `manifest.json` | `f130a24ae73e151826d184703f21a153508328a06bc5c7c019ab94ff0d77a196` |
| ACS station table | `Citi Bike - ACS and July Trip Development - e1eb6e26/acs_station_features.parquet` | `3332babcb605f43c85f60b979bcda82c66e1c5c1199167118f1a333dbc9b84f9` |

The cohort's fingerprint (SHA-256 of the sorted, newline-joined station IDs) is `3d5bf5cd0594981fb60b995a08f6c305b2927122ae1299021e413a1b35414ff3`. The public ACS package (10 files) is published separately under the team's S3 prefix.

## Results and limits
- There was one test week, and the same 100 stations were used for training and testing. Results do not show network-wide or long-term performance.
- Counts are 5-minute snapshots. They are not separate incidents, and they are not prevented shortages.
- Risk Scores are for ranking only; they are not calibrated probabilities.
- ACS measures act mainly as a station profile. Shuffled ACS values gave the same bike gain and about half to three quarters of the dock gain. They do not show that neighborhood traits predict or cause shortages.
- Station-area group results describe areas, not riders. Poverty share is both a model input and the grouping variable. See `docs/M2_Equity_Wording_2026-11-15.md`.
- **Live pilot results are operations monitoring, not model evaluation.**

## Not included, and why
- The step 01a downloaders and other root notebooks: they are already at the repository root.
- The review-only check notebooks: they verified earlier runs and are not needed to rerun the project.
- Project plan and working records: internal planning documents.
- Trained model files: kept in the private workspace and identified by fingerprint above.
- Raw feed archives, checkpoints, state databases and S3 data files: these are data, not code.
- The station-to-group file (poverty and no-vehicle thirds) as a separate file: kept out by team decision. `setup/rebuild_station_groups.py` rebuilds it from the ACS table when the pilot notebook needs rebuilding. The deployed pilot notebook already contains a copy inside its embedded package.
- Credentials of any kind.
