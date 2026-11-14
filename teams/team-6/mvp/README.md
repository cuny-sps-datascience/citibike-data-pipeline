# Team 6 MVP — 6-station hourly departure forecast

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

| MAE (dep/hr) | LightGBM | baseline |
|---|---|---|
| validation | 3.17 | 5.98 |
| test | **3.84** | 4.36 |

Beats baseline at all 6 stations (4–17%). Horizons 1–3 score identically — a calendar model has no information about
"now", so this is expected and will change once weather and recent-count lags enter. Per-station / per-horizon
metrics in `model1/report.json`; every test prediction in `model1/predictions.parquet`.

## Play with it
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/jamilton08/citibike-data-pipeline/blob/jonathan/mvp-6-stations-model1/teams/team-6/mvp/play_model1.ipynb)

`play_model1.ipynb` — pick a station/week and see predicted vs actual, error by hour of day, the worst-missed days,
ask the model about any date/hour, and retrain with features switched off. Runs in Colab with nothing installed,
or locally with `pip install -r requirements.txt && jupyter lab`.

## Model 2 — calendar + forecast weather (`scripts/train_compare.py`)
Model 1 and model 2 are trained and scored on **identical rows**, so the only difference is the weather columns.

**Common window** (weather archive starts 2024-08-10, subway archive ends 2026-08-10):
train 2024-08-11 → 2025-12-31 · validation 2026-01-01 → 2026-02-28 · test 2026-03-01 → 2026-08-10.
A row is kept only if its label is known and its weather forecast is usable (99.2% of rows); rows without usable
weather are dropped from **both** models.

**Weather inputs** (`scripts/fetch_weather.py` → `data/weather_6stations.parquet`, `data/station_map.csv`): forecast
temperature, precipitation, rain, snowfall and wind speed for the target hour, copied from the team-6 release for the
3 weather grid cells our 6 stations fall in. Stations are bridged from trip-file short ids to the release's GBFS uuids by name (6 of 6).

**Horizon alignment.** The weather archive numbers horizons as target = origin + h; the MVP uses origin + (h − 1).
We join the forecast issued one hour before our origin with the same h, which lands on the same target hour, and
`mvp_data.add_weather()` asserts the two target timestamps are equal on every row. Every selected weather run was
available at least 60 minutes before the forecast origin, so the 5-minute issue cutoff holds.

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

## Next — model 3 (subway)
Only **2 of the 6 stations** are in the release's subway bridge (Broadway & E 14 St and Fulton St & William St, both
within ~30 m of a subway complex). The other four have no subway features. Options to decide as a team:
1. run model 3 on the 2 mapped stations only;
2. swap in stations that are in the bridge (331 docks are mapped) and rerun models 1–2 on the new cohort;
3. widen the bridge so every station maps to its nearest complex.

Everything runs unchanged in a Databricks notebook once the `data/` files are uploaded to a volume.
