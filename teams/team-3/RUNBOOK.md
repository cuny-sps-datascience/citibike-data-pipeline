# Runbook: running the Team 3 project from start to finish

This runbook explains how to rerun every step in Databricks, from the station data to the live pilot. It assumes you have the class environment and data listed in section 1. Every step was run this way in November 2026. The notebooks are unchanged from those runs except for added comments.

Terms follow the README:
- **Poll:** one fetch of the public Citi Bike station feed.
- **Risk Score:** the model's predicted probability.
- **True positives:** flagged observations with a confirmed shortage. Recall and Precision are computed from them.

## 1. What you need
| Need | Details |
|---|---|
| Databricks workspace | Serverless notebooks and jobs. The runs used serverless environment version 6 with **scikit-learn 1.7.2**. Use the same version: the model files were saved with it, and if the version differs, the pilot falls back to logistic regression |
| Secret scope | Named `citibike-team3-secrets`, holding `aws_access_key_id` and `aws_secret_access_key` for a key that can read the class bucket `cuny-sps-citibike-archive` (region `us-west-2`). No key is stored in this repository |
| Station data in S3 | `exports/station_info.parquet` and `exports/station_status/nyc_date=YYYY-MM-DD/`, with exactly one parquet file per day from 2026-08-24 through 2026-10-11. The class pipeline (step 01) produces these |
| Internet access | Step 02 reads the public Citi Bike trip archives and a public weather forecast archive. The pilot reads the public Citi Bike station feed |
| Shared class catalog | Only for step 01, which reads and writes the shared pipeline tables |
| S3 write access | Only to publish pilot files to `contributions/team-3/pilot/v1/`. For a rerun, leave publishing off |
| Python 3 with pytest, pandas, scikit-learn 1.7.2, joblib and pyarrow | For the local tests and the helper script |

## 2. Import the notebooks
In Databricks, use Workspace > Import > File and choose each `.py` file. Two notebooks must have exact names in your home folder, because later steps find them by name. The names contain an em dash (—).

| Repository file | Import as | Why the name matters |
|---|---|---|
| `step_02_model_comparison/step_02_positive_count_model_comparison.py` | `/Users/<you>/Citi Bike — Positive-Count Model Comparison` | Step 03 cell 4 compares its ACS table with the copy embedded in this notebook (an optional check) |
| `step_03_acs_controls_audit/step_03_acs_controls_intervals_neighborhood_audit.py` | `/Users/<you>/Citi Bike — ACS Controls, Intervals and Neighborhood Audit 2026-11-10` | Step 04 runs this notebook's cells 2 to 4 after checking their exact text. The repository copy passes that check |
| Everything else (steps 04, 04b, 05, 06 and `setup/`) | Any name | |

## 3. Run order
Each step writes to a new, uniquely named folder in your home folder (`/Workspace/Users/<you>/`). No step except the pilot writes to S3, and the pilot writes only when publishing is on.

**Fixed folder names.** Later steps read three folders by the run IDs of the original runs. After the step that makes each folder, run `setup/setup_link_run_folder` to copy your new folder to the fixed name. It never overwrites a folder. You can also rename the folder by hand in the workspace.

| Copy (`link` choice) | Made by | Fixed name |
|---|---|---|
| `experimental_models` | step 02 cell 11 | `citibike_experimental_models_c4e388f77d614002a3512d0478d40037` |
| `acs_table` | step 02 cell 28 | `Citi Bike - ACS and July Trip Development - e1eb6e26` |
| `approved_models` | step 03 cell 9 | `Citi Bike - ACS Controls and Neighborhood Audit - ea8df268` |

### Step 01: class pipeline (usually skip)
Run this step only if the S3 station exports in section 1 are missing. It runs in the shared class environment:
1. Run the root notebooks `0-` to `3-`. The reading copies in `step_01_upstream_pipeline/` explain them.
2. Then run [`5-weekly-training-export`](../../5-weekly-training-export.ipynb) to write the S3 exports the modeling steps read.

### Step 02: model comparison
Run the cells one at a time, in order, in one session. Do not use Run all, because cell 14 needs a file this package does not include. If the session restarts, start again from cell 0.

| Cells | Notes |
|---|---|
| 0 to 13 | Data, labels, model comparison and calibration. After **cell 11**, run `setup_link_run_folder` with `experimental_models` |
| 14 | **Skip.** It replays one saved poll from an early prototype trial (`citibike_manual_trial_...`). That file is not part of this package, and the cell is a check only |
| 15 | Optional. One live poll of the public feed |
| 16 to 27 | ACS, trip and weather tests. Cell 22 downloads the July trip archive (several hundred MB). After **cell 28**, run `setup_link_run_folder` with `acs_table` |
| 28 to 55 | Saving, reload checks and the group audit. Cells 31 and 33 read the two folders you copied |

Expected:
- Cell 2 prints `MATCH_COUNTS` for bike and dock.
- Cell 3 prints one `MODEL_RESULT` row per method for the Sep 28 to Oct 4 week.

Step 03 then checks this step's population counts against the earlier report.

### Step 03: ACS controls and audit
Run all. Check two outputs:
- `POPULATION` shows `match: true`.
- `REPRODUCTION_MISMATCHES` is an empty list.

Then run `setup_link_run_folder` with `approved_models`.

### Step 04: validation replay
Run all. The final summary should show `overall_pass: true`. Under `checks`, `bike` and `dock` should each show `pass: true`, with 1,814 bike and 2,656 dock true positives (`caught_scorer_alerts`).

### Check the fingerprints
Run `setup/setup_check_fingerprints`.
- **`ALL_MATCH`:** your files are the approved files. Continue.
- **Anything else:** see section 4 before running steps 04b or 06.

### Step 04b: one-time test week
Run all, once. It refuses a second run in the same workspace, and it stops unless the fingerprints match.

Expected, matching `docs/Citi_Bike_Reserved_Test_Week_Result_2026-11-10.md`:

| Method | Bike true positives | Dock true positives |
|---|---|---|
| Histogram gradient boosting | 1,924 | 2,603 |
| Logistic regression | 1,640 | 2,234 |
| Low-count rule | 1,614 | 2,144 |

This week's result is already known, so a rerun reproduces it rather than testing anything new.

### Step 05: development sensitivity (M1)
Run the cells in order. Expected values:
- `PART1_FROZEN_MATCH` is true.
- Histogram gradient boosting has 1,814 bike and 2,656 dock true positives.
- Logistic regression has 1,612 and 2,296.
- The low-count rule has 1,575 and 2,179.

A few lasso (L1) rows in Part 3 may differ slightly between runs. This is likely because that solver runs without a fixed seed (not directly tested).

### Step 06: live pilot
1. Run the local tests:
   ```
   cd step_06_pilot/prototype
   python3 -m pytest -q -p no:cacheprovider
   ```
   Expected: 88 passed.
2. Import `step_06_pilot/step_06_pilot_window_job_v18_IMPORT_INTO_DATABRICKS.py`. The commented copy beside it is for reading only.
3. Create a job with one notebook task pointing at it, on serverless compute:
   - Schedule: cron `0 25 6,15 * * ?` in `America/New_York`.
   - Limits: timeout 23,400 seconds (6 hours 30 minutes), one concurrent run, no retries.
4. Set the parameters:
   - `publish_mode = off` for a rerun. `dry_run` builds the S3 files without writing them. Use `on` only with write approval for the team prefix.
   - `retention_keep_last = 3`.
   - `takeover_owner` blank.
5. Before polling, the job checks the four fingerprints and stops with exit code 4 if any differ.
6. Results go to `/Workspace/Users/<you>/Citi Bike - Pilot State/`, in `window_reports/`, `publish_log/` and `raw/`.

## 4. If the fingerprints differ
Different bytes mean your files are not the approved model, even if their predictions agree. Steps 04b and 06 stop on purpose. You have two choices:
- **Use the approved files.** Copy the original approved model folder and ACS table from the team workspace into the two fixed folders, then check again.
- **Treat your files as a new model.** This makes it a new study, not a reproduction:
  1. Write a new freeze record.
  2. Put your fingerprints in `FROZEN` in `step_06_pilot/prototype/pilot_job_entry.py`.
  3. Rebuild the station-to-group file into `prototype/`:
     ```
     python3 setup/rebuild_station_groups.py <acs_station_features.parquet> step_06_pilot/prototype/station_groups_poverty_and_no_vehicle_thirds.csv
     ```
     This reproduced the deployed file exactly.
  4. Run `python3 make_job_notebook.py` in `step_06_pilot/prototype/`. It writes `step_06_pilot/Citi_Bike_Pilot_Window_Job_v18_IMPORT_INTO_DATABRICKS.py`.
  5. Import that notebook.
  6. Delete the group file afterwards; it is kept out of this repository.

  Do not score October 5 to 11 again with a new model, because that week is no longer a clean test.

## 5. What was checked for this runbook (November 17, 2026)
**Checked:**
- The notebooks' code is unchanged from the runs that produced the saved results; only comments were added.
- Step 03 cells 2 to 4 pass step 04's text check.
- The step 01 copies match `main`.
- The station-to-group rebuild is byte-identical to the deployed copy.
- A pilot notebook rebuilt from this repository's code contains the same code and identical data files as the deployed one; only comments differ, so its fingerprint differs.
- The 88 tests pass.

**Not checked:**
- Nobody has run this sequence from scratch in a new workspace.
- It is unknown whether a fresh run of step 03 writes byte-identical model files. That depends on matching library versions, and section 4 covers the case where it doesn't.
- The setup notebooks were written for this runbook and have not been run in Databricks.
- Public sources may change: the trip archives, the weather archive and the station feed.
