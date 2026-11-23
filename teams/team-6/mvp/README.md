# Team 6 MVP — 6-station hourly departure forecast

For the post-demo next-hour model, see [Databricks training, scoring, and source plan](databricks/README.md).

Scope agreed 11/6: few stations, deep history, departures as the target, three models compared
(calendar only → + weather → + weather + subway). This folder holds the data prep and **model 1**.

## Stations (6)
| station_id | name | why |
|---|---|---|
| 5137.11 | Fulton St & William St | from the 32-station pilot (polled status in test window); passes strict reliability |
| 5105.01 | Liberty St & Broadway | pilot; busiest of the 32 |
| 5001.08 | Little West St & 1 Pl | pilot; residential Battery Park City |
| 6140.05 | W 21 St & 6 Ave | busiest clean station in Manhattan (~22 dep/hr) |
| 5905.12 | Broadway & E 14 St | Union Sq, ~17 dep/hr |
| 7260.09 | Amsterdam Ave & W 73 St | UWS, ~12 dep/hr, higher zero share |

**How the stations were chosen.** This is a retrospectively selected case study: the first reliability screen
(`data/station_reliability_named.csv`) used the whole March 2025 – August 2026 panel, which includes the validation
and test periods. To check that the choice does not depend on those periods, the screen was rerun using only
training-period data (March – December 2025, `data/station_reliability_train_period.csv`): all 6 stations pass the
strict screen on training data alone (253 of 689 stations do; every station that passes on the full period also
passes on the training period). In our own 24-month trip data all 6 are active in every month of 2024 and 2025.

Reliability screening of all 689 panel stations is in `data/station_reliability_named.csv`
(`pass_basic`: existed Mar 2025–Aug 2026 with no gap > 7 days — 502 stations;
`pass_strict`: + active every month, ≥ 5 dep/hr, trip in ≥ 90% of daytime hours, never dark > 2 days — 170).

## Data
`scripts/fetch_trips.py` downloads the public monthly trip files Jan 2024 → Aug 2026, keeps trips
touching the 6 stations, and writes `data/trips_6stations_hourly.parquet`: station × UTC hour, `departures`,
`returns` (140,262 rows). Hours with no trips are real zeros. Trip timestamps are NYC local: trips at an
ambiguous (fall-back) or nonexistent (spring-forward) local time can't be placed in a UTC hour, so the repeated
fall-back hour is stored as **unknown (null), not zero** — 24 station-hours — and excluded from training and
scoring (same rule as `historical_station_panel.py`). All 6 stations are active every month, longest dead
stretch 75 h. Rebuild the hourly table from already-downloaded trips with
`python3 scripts/fetch_trips.py --aggregate-only <trips_6stations_raw.parquet>`.

## Splits (by target local date)
train 2024-01-01 → 2025-12-31 (two winters) · validation 2026-01-01 → 2026-02-28 · test 2026-03-01 → 2026-08-31 (6-month simulation).
Validation is used for early stopping and for any feature experiments (including the notebook's). Test is only used to
report the final model of each stage. Rows, splits and features are defined once in `scripts/mvp_data.py`, which both
`scripts/train_mvp.py` and the notebook import, so they score identical rows.
Horizons 1–3 = target hour at origin + (h−1), same convention as the 689-station panel.

## Model 1 — calendar only (`scripts/train_mvp.py`, runs from any directory)
LightGBM, Poisson objective, features: station, horizon, target hour / weekday / month / day-of-year / weekend / US holiday.
Baseline: station × hour × weekday mean from train.

| MAE / RMSE (dep/hr) | LightGBM | baseline |
|---|---|---|
| validation | 3.17 / 5.08 | 5.98 / 8.87 |
| test | **3.84 / 6.18** | 4.36 / 6.96 |

This full-history run (train from January 2024) is kept as a **separate benchmark**. It is not the model 1 used in
the model 1 vs 2 comparison below; that one is retrained on the common window.

Beats baseline at all 6 stations (4–17%). Horizons 1–3 score identically — a calendar model has no information about
"now", so this is expected and will change once weather and recent-count lags enter. Per-station / per-horizon
metrics in `model1/report.json`; every test prediction in `model1/predictions.parquet`.
"Recent-count lags" are not planned: the team ruled them out because trip counts are not available live.

## Play with it
Two notebooks, both run in Colab with nothing installed (or locally: `pip install -r requirements.txt && jupyter lab`).

**Model 1** — [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/jamilton08/citibike-data-pipeline/blob/jonathan/mvp-6-stations-model1/teams/team-6/mvp/play_model1.ipynb)
`play_model1.ipynb`: pick a station/week and see predicted vs actual, error by hour of day, the worst-missed days,
ask the model about any date/hour, and retrain with features switched off.

**Model 2** — [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/jamilton08/citibike-data-pipeline/blob/jonathan/mvp-6-stations-model1/teams/team-6/mvp/play_model2.ipynb)
`play_model2.ipynb`: model 1 vs model 2 for any station/week with the weather forecast underneath, rainy vs dry hours,
whether model 2 fixed the days model 1 missed worst, a what-if (same hour, different weather), and retraining with
weather features switched off.

## Model 2 — calendar + forecast weather (`scripts/train_compare.py`)
Model 1 is **retrained** (not re-scored) on the common window, and models 1 and 2 are trained and scored on
**identical rows**: same stations, same horizons, same splits. Both use the same fixed LightGBM settings, the same
early-stopping rule on validation and the same 5 seeds; neither was tuned. The only difference is the weather columns.

**Common window** (weather archive starts 2024-08-10, subway archive ends 2026-08-10):
train 2024-08-11 → 2025-12-31 · validation 2026-01-01 → 2026-02-28 · test 2026-03-01 → 2026-08-10.
A row is kept only if its label is known and its weather forecast is usable; rows without usable weather are
dropped from **both** models.

| rows (station × origin × horizon) | in common window | used | dropped | reason dropped |
|---|---|---|---|---|
| train | 219,402 | 217,242 | 2,160 | weather archive flags the forecast as having missing values |
| validation | 25,488 | 25,488 | 0 | |
| test | 70,398 | 70,074 | 324 | weather archive flags the forecast as having missing values |

No row is unmatched (every row finds a weather forecast) and no row is dropped for the cutoff. 0.8% dropped in total.

**Weather inputs** (`scripts/fetch_weather.py` → `data/weather_6stations.parquet`, `data/station_map.csv`): forecast
temperature, precipitation, rain, snowfall and wind speed for the target hour, copied from the team-6 release for the
3 weather grid cells our 6 stations fall in. Stations are bridged from trip-file short ids to the release's GBFS uuids by name (6 of 6).

**Horizon alignment.** The weather archive numbers horizons as target = origin + h; the MVP uses origin + (h − 1).
We join the forecast issued one hour before our origin with the same h, which lands on the same target hour, and
`mvp_data.add_weather()` asserts the two target timestamps are equal on every row. We never pick a run by target hour
alone: each row uses the run the weather archive selected for the origin one hour before ours, so a later run cannot
be chosen. Every selected run was available at least 60 minutes (median 4 hours) before the forecast origin, so the
5-minute issue cutoff holds on all 312,804 rows.

| MAE / RMSE (dep/hr) | baseline | model 1 calendar | model 2 + weather | model 2 vs 1 (MAE) |
|---|---|---|---|---|
| validation (Jan–Feb 2026) | 6.30 / 9.29 | 3.21 / 5.19 | **2.35 / 3.81** | −27% |
| test (Mar 1–Aug 10 2026) | 4.29 / 6.77 | 3.86 / 6.16 | **3.40 / 5.37** | −12% |
| test, wet hours (forecast precip > 0.005 in, 11% of hours) | 6.33 / 9.58 | 6.24 / 9.30 | **4.39 / 7.25** | −30% |
| test, dry hours | 4.03 / 6.32 | 3.56 / 5.63 | **3.27 / 5.08** | −8% |

- Model 2 beats model 1 at all 6 stations on both validation and test (test: −7% to −15%).
- Each model was trained with 5 seeds; validation MAE varies by about ±0.01 across seeds, so a 0.85 gap is not seed noise.
- On wet hours model 1 is no better than the baseline (6.24 vs 6.33); weather is what fixes them.
- The validation gain is large because Jan–Feb 2026 was colder and snowier than the one winter in the training
  window (mean forecast temperature 26 °F vs 30 °F; demand about 30% lower). A calendar model can only repeat last
  winter; temperature lets the model adjust.
- Horizons 1–3 still score the same: the weather forecast for a target hour barely changes with one or two hours
  less lead time.
- Shortening training from 24 to 16.5 months cost model 1 almost nothing (test MAE 3.84 → 3.86 on the same rows).
- Limitation (from the weather release): early archive entries are reconstructed forecasts with an assumed
  availability delay, so this is a retrospective forecasting experiment, not proof of live performance.

Full per-station / per-horizon / wet-dry metrics and seed summaries: `model2/report.json`. Predictions for both
models on the same rows: `model2/predictions.parquet`.

**Checks** (`python3 scripts/check_model2.py`, results in `model2/checks.json`; all pass):

| check | result |
|---|---|
| same rows for both models | 95,562 scored rows, identical keys, no duplicates |
| weather is for the departure hour | 0 mismatches in 312,804 rows |
| weather available before the cutoff | 0 rows after the cutoff; earliest margin 60 minutes |
| unmatched rows | none; 2,484 dropped as flagged missing (table above) |
| placebo: weather from the wrong week | validation gain falls from 0.86 to 0.04 MAE, so the gain is real weather signal |
| month-boundary dates (train from Sep 1 2024, test to Aug 1 2026) | same conclusion: validation 3.20 → 2.36, test 3.84 → 3.37 |

Feature and model decisions use validation only. The test period is not untouched: it was used to report model 1 and
in early feature experiments, and here to report models 1 and 2. If the team wants a clean final holdout, August
11–31 2026 has trips and weather but no subway coverage.

**Where it ran.** Locally only so far (Python 3.10–3.13, LightGBM 4.7); the saved outputs reproduce byte-for-byte from
a fresh clone. Not yet run in Databricks.

## Next — model 3 (subway)
Only **2 of the 6 stations** are in the release's subway bridge (Broadway & E 14 St and Fulton St & William St, both
within ~30 m of a subway complex). The other four have no subway features. Options to decide as a team:
1. run model 3 on the 2 mapped stations only;
2. swap in stations that are in the bridge (331 docks are mapped) and rerun models 1–2 on the new cohort;
3. widen the bridge so every station maps to its nearest complex.

Everything runs unchanged in a Databricks notebook once the `data/` files are uploaded to a volume.
