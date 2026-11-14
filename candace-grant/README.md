# Team 1 · Candace Grant — Citi Bike station availability, 15 minutes ahead

Team 1 agreed that each member builds the full project independently on Databricks Free Edition, then cross-references results. This folder is my build.

**Question.** Will a station be **empty** (no bikes) or **full** (no docks) 15 minutes from now?
**Baseline.** Persistence: the station's state in 15 minutes equals its state now.
**Scope.** 100 stations (50 with the highest event rates, 50 seeded-random), scored live every 15 minutes.

Live app (reads the pipeline's tables): https://dockcast-ljmdgrb6va-uk.a.run.app · source: [Aicoaching2025/citibike-forecast](https://github.com/Aicoaching2025/citibike-forecast) (`web/`)

## Pipeline

| Notebook | What it does | Runs |
|---|---|---|
| `00_setup.py` | Validates the course silver tables (`silver_station_status`, `silver_station_info`), drops quality-flagged rows, audits snapshot cadence | Once |
| `01_live_collector.py` | Fetches the GBFS feed → `live_station_status` (idempotent MERGE) | Job `citibike_collect`, every 5 min |
| `02_labels_features.py` | Station selection, 15-min UTC grid, as-of state, features, labels at t+15, data-quality gates | Once |
| `03_train_evaluate.py` | Chronological split, baselines, logistic regression and gradient boosting, calibration, MLflow + Unity Catalog registry | Once |
| `04_score_live.py` | Scores the newest grid time with the `@champion` models → `predictions` | Job `citibike_live`, every 15 min |
| `05_monitor.py` | Joins realized outcomes → `live_outcomes`; freshness, volume, accuracy checks → `monitoring_log` | Job `citibike_live`, after scoring |
| `06_analysis.py` | Transition metrics, results by stratum, permutation importance, ablation | Once |
| `07_weather.py` | Hourly NYC weather (Open-Meteo, CC BY 4.0) → `nyc_weather_hourly` | Once |
| `config.py`, `citibike_lib.py` | All settings; shared logic used identically in training and live scoring | Imported |
| `dashboard_queries.sql` | AI/BI dashboard datasets | Dashboard |

Setup steps and job configuration: [`RUNBOOK.md`](RUNBOOK.md). Design decisions and limitations: [`ASSUMPTIONS.md`](ASSUMPTIONS.md).

## Method

- **Data.** Course silver tables, 2026-08-21 → 2026-10-26: 47.1 M snapshots, 2,531 stations, median 5-minute cadence.
- **State at time t.** Latest snapshot in (t − 10 min, t]. Label: state at t + 15 min.
- **Exclusions.** Station not operating, or no snapshot at t or t + 15 (0.8 % of 626,500 rows).
- **Split.** Chronological with a 60-minute embargo: train < Oct 12, validation Oct 12–19, test Oct 19–26. Thresholds tuned on validation (max F1); the test week was scored once.
- **Features.** Current state, momentum, time of day / week, station × hour habits, data freshness.
- **Leakage fix.** Station × hour habits are out-of-fold (7-day blocks) on training rows. The first version encoded each row's own label, which `06_analysis` caught (empty PR-AUC dropped 0.870 → 0.779 when habits were added); fixed in v2.

## Results (test week, v2 champions: gradient boosting)

| Target | Persistence PR-AUC | Model PR-AUC | Brier skill vs persistence |
|---|---|---|---|
| Empty | 0.692 | **0.881** | 13.2 % |
| Full | 0.897 | **0.974** | 15.2 % |

- **Transitions** (where persistence scores zero by definition): the model warned of 82 of 493 empty onsets (17 %) at 44 % precision.
- **Live (Nov 12–13, n = 9,114 forecasts with known outcomes):** Brier 0.0089 empty / 0.0198 full; mean predicted rate within 0.007 of observed.
- **Operations:** collector 288 runs / 24 h, 0 failures; all automated checks passing.

## Limitations

- 100 stations, not the full network; event rates rise across the split weeks.
- Live comparisons span different days, so v1 vs v2 live numbers are not a controlled comparison.
- Weather is published and joinable but is not yet a model input.

## Reproduce

1. Import every file in this folder into **one** Databricks workspace folder (notebooks import `config.py` and `citibike_lib.py` from their own directory).
2. Point `SOURCE_STATUS_TABLE` / `SOURCE_INFO_TABLE` in `config.py` at the course silver tables.
3. Follow [`RUNBOOK.md`](RUNBOOK.md): run `01` once, create the two jobs, then run `00` → `02` → `03` (→ `06`, `07`).

No credentials are stored in this folder. The app reads Databricks with a token held in Google Secret Manager.
