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
`returns`, zeros filled (140,262 rows). Trip timestamps are NYC local; the two ambiguous/nonexistent DST hours
per year are dropped rather than guessed. All 6 stations are active every month, longest dead stretch 75 h.

## Splits (by target local date)
train 2024-01-01 → 2025-12-31 (two winters) · validation 2026-01-01 → 2026-02-28 · test 2026-03-01 → 2026-08-31 (6-month simulation, touched once).
Horizons 1–3 = target hour at origin + (h−1), same convention as the 689-station panel.

## Model 1 — calendar only (`scripts/train_mvp.py`)
LightGBM, Poisson objective, features: station, horizon, target hour / weekday / month / day-of-year / weekend / US holiday.
Baseline: station × hour × weekday mean from train.

| MAE (dep/hr) | LightGBM | baseline |
|---|---|---|
| validation | 3.16 | 5.98 |
| test | **3.84** | 4.36 |

Beats baseline at all 6 stations (4–17%). Horizons 1–3 score identically — a calendar model has no information about
"now", so this is expected and will change once weather and recent-count lags enter. Per-station / per-horizon
metrics in `model1/report.json`; every test prediction in `model1/predictions.parquet`.

## Play with it
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/jamilton08/citibike-data-pipeline/blob/jonathan/mvp-6-stations-model1/teams/team-6/mvp/play_model1.ipynb)

`play_model1.ipynb` — pick a station/week and see predicted vs actual, error by hour of day, the worst-missed days,
ask the model about any date/hour, and retrain with features switched off. Runs in Colab with nothing installed,
or locally with `pip install -r requirements.txt && jupyter lab`.

## Next
Model 2 (+ weather) and model 3 (+ weather + subway) from the team-6 weather/subway release, same splits, same table.
Runs unchanged in a Databricks notebook once the hourly parquet is uploaded to a volume.
