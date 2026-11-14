# Citi Bike Station Availability — Runbook

Predicts whether each of 100 MVP stations will be **empty** or **full 15 minutes ahead**, scores live
GBFS snapshots every 15 minutes, and serves results on a Databricks AI/BI dashboard.
Built for Databricks **Free Edition** (serverless only, one 2X-Small SQL warehouse, ≤5 concurrent job tasks).

## Files

| File | Purpose | How it runs |
|---|---|---|
| `config.py` | Every name, path, window, threshold | Imported by all notebooks |
| `citibike_lib.py` | Shared logic: GBFS parsing, as-of state, features, labels, station selection, metrics | Imported; same code for training and live scoring |
| `00_setup.py` | Schema/volume, validate professor's silver tables, cadence audit | Once |
| `01_live_collector.py` | Fetch GBFS → `live_station_status` (MERGE, idempotent) | Job `citibike_collect`, every 5 min |
| `02_labels_features.py` | Station selection, `features_labels_15m`, data-quality gates | Once (after 00) |
| `03_train_evaluate.py` | Split, baselines, LR + GBM, metrics, calibration, MLflow + UC registry | Once (after 02) |
| `04_score_live.py` | Score newest grid time → `predictions` | Job `citibike_live` task 1, every 15 min |
| `05_monitor.py` | Freshness/volume checks, realized outcomes → `live_outcomes`, `monitoring_log` | Job `citibike_live` task 2 |
| `06_analysis.py` | Report analysis: EDA, transition metrics, results by stratum, skill scores, permutation importance, ablation → `analysis_*` tables + PNGs | Once, after 03 (read-only; ~15–25 min) |
| `07_weather.py` | Publishes `nyc_weather_hourly` (Open-Meteo, hourly temperature, wind, precipitation) + example join | Once (rerun to extend dates) |
| `dashboard_queries.sql` | 8 dashboard datasets | AI/BI dashboard |

## Setup (in this order)

1. **Import the folder.** Workspace → your folder → Import → upload all `.py` files and `config.py`/`citibike_lib.py`
   (or clone your Git repo via Git folder). All files must sit in **one folder**; notebooks import the two modules from their own directory.
2. **Edit `config.py`:** set `SOURCE_STATUS_TABLE` / `SOURCE_INFO_TABLE` to where the professor's notebook wrote the silver tables.
3. **Run `01_live_collector` once by hand.** This is the critical test: Free Edition only allows outbound calls to an
   unpublished set of trusted domains. If both GBFS hosts fail, the error lists each attempt. See *If the live feed is blocked* below.
4. **Create two jobs.** The collector must run every **5 minutes** to match the historical snapshot cadence
   (the model's state at time t is the latest snapshot within 10 min before t; 15-minute collection leaves every state stale).
   - Job `citibike_collect`: one task `collect` → `01_live_collector`, serverless; schedule Interval, every 5 minutes.
   - Job `citibike_live`: tasks below, every 15 minutes. It does NOT include the collector (two concurrent MERGEs into one table can conflict).
   - Task `score` → `04_score_live` (exits cleanly until a model is registered; fails loudly if no station has a fresh state)
   - Task `monitor` → `05_monitor`, depends on `score`, **Run if: All done** (so monitoring runs even when scoring fails)
   - Schedule: cron, `0 2/15 * * * ?`, timezone UTC (fires at :02/:17/:32/:47, just after the feed's typical update)
   - Notifications: email on failure; max concurrent runs = 1
5. **Run `00_setup`.** Check the printed schema against `STATUS_COLMAP`; fix names in `config.py` if it fails. Read the cadence audit:
   median gap must be below `ASOF_TOLERANCE_MIN` (10 min).
6. **Run `02_labels_features`.** All four data-quality assertions must pass. Requires `live_station_status` to exist so
   selection only keeps stations in the live feed.
7. **Run `03_train_evaluate`.** Registers `workspace.citibike.citibike_empty_15m@champion` and `…_full_15m@champion`.
   From the next job run on, `score` writes predictions.
8. **Build the dashboard** (Dashboards → Create): add each query in `dashboard_queries.sql` as a dataset, then the widget named in its header.
   Set dashboard refresh to 15 minutes.

## Day plan to Nov 13

| Day | Do | Done when |
|---|---|---|
| Mon 11/9 | Steps 1–4 | Job run history shows green `collect` runs |
| Tue 11/10 | Steps 5–6; check overnight run history for Free Edition throttling | `features_labels_15m` passes checks |
| Wed 11/11 | Step 7; run PR #61 to publish `nyc_weather_hourly` in this schema | Both models registered; comparison table shows result vs persistence |
| Thu 11/12 | Step 8; read `live_outcomes` after ≥24 h of scoring; write report sections | Dashboard live; live Brier logged |
| Fri 11/13 | Reproducibility check: re-run 00→03 from scratch, confirm same metrics; demo | Submit |

## If the live feed is blocked on Free Edition

The proposal (P §7) allows **labeled historical playback** if live integration is not ready. Fallback, no code change in the model:
replay the last day of `std_station_status_hist` through `04_score_live` by pointing its source at the historical table, and
state the limitation in the report. Ask the professor whether paid compute is acceptable; on a paid workspace the code runs unchanged.

## Free Edition limits that matter here

- Serverless jobs: max 5 concurrent tasks per account; fair-usage quotas can shut compute off for the rest of the day.
  The job uses 3 sequential tasks with max concurrency 1. If runs start failing with quota errors, change the schedule to every 30 min
  (`0 2/30 * * * ?`) and expect about half the grid times to be scorable.
- Continuous streaming is not supported on serverless jobs; scheduled batch is the supported pattern and is what this uses.
- No Model Serving dependency: scoring is batch inside the job.

## Cross-reference with teammates

`03_train_evaluate` writes `test_predictions` and exports
`/Volumes/workspace/citibike/raw/exports/test_predictions_<user>.parquet` with columns
`station_id, prediction_time_utc, empty_label, full_label, p_empty, p_full, p_persistence_empty, p_persistence_full, model_empty, model_full, author`.
Comparisons are only valid if teammates use the same station list, label rules, grid, and split dates. Share
`mvp_stations` (CSV) and the `split_config` row with them when you're ready.
