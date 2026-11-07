# Team 4 Code Directory

This folder is owned by team 4 and their code files should live here.

## Predicting empty and full Citi Bike stations

Status as of November 7, 2026.

Question: for a station that is not stocked out right now, will it run out of bikes (or run out of open docks) within the next 30 minutes? The original proposal also listed 15 and 60 minute horizons, which are not done yet.

## Files in this folder

* `model_experiments.ipynb`: the instructor's `4-train-sklearn-FE` notebook plus our added cells (held out evaluation, gradient boosting, weather tests). Outputs are cleared. It runs in a Databricks Free Edition workspace and needs the tables described below.

## Data and period

* Source: the weekly silver export of the class GBFS station status and station information logs, from the public bucket `cuny-sps-citibike-archive`. The notebook `1-download-silver-FE` loads it into `workspace.default`.
* Coverage: 73 daily files, August 21 to November 1, 2026. That is 52,254,834 station status rows, or about 2,485 stations reporting per 5 minute poll.
* Station information (138,282 rows) is current only through October 12, so stations added later may be missing capacity and location.
* Inactive stations report 0 bikes and 0 docks. Of all status rows, 96.27% (50,306,744) are from stations that were installed, renting, and returning. 2.91% (1,522,993) were none of those, and 0.81% (425,097) were installed but not renting or returning. The gold notebook sets the stockout flag to null for these rows, so they never count as empty or full events.

## What we ran in Databricks

The repo was added as a Git folder on the `main` branch, and we used the notebooks in `free_edition_models`.

1. `1-download-silver-FE` in production mode, which builds `silver_station_status` and `silver_station_info`.
2. `3-gold-features-FE`, first in dev mode to check the rates and then in production mode to save `gold_station_predictors` (38 columns) and `gold_station_targets` (6 columns), each with 52,254,834 rows.
3. `4-train-sklearn-FE` in dev mode, which trains a logistic regression for bikes and one for docks with time ordered cross validation and a 40 minute gap. It needs `%pip install scikit-lego==0.9.9 skops==0.16.0` and a Python restart first.

## Labels and base rates

A target is 1 if the station is observed stocked out within 30 minutes, 0 if it never is, and null if it is already stocked out, ineligible, or has a coverage gap.

* Bike onsets: 1.87% of 48,117,322 labeled polls.
* Dock onsets: 3.45% of 45,066,194 labeled polls.
* Always predicting no would be 98.1% accurate for bikes and 96.5% for docks, so we use precision, recall, and precision recall AUC.
* For comparison, the share of active rows that are currently empty is 4.14% and currently full is 10.22%.

## Evaluation setup

* A 10% random sample of labeled polls: 4,811,263 bike rows and 4,505,071 dock rows.
* Time ordered split: train before October 24, 23:20, then a 40 minute gap, then test from October 25 to November 1.
* Bike test week: 537,655 polls with 11,996 onsets (2.23%), against 1.82% in training. Dock test base rate: 3.85%.
* About 6 consecutive polls are positive per event, so the bike test week holds roughly 2,000 distinct events.
* Instructor baseline cross validation: bike log loss about 0.05 and dock 0.081, both with C = 0.3, against 0.093 and 0.150 for always predicting the base rate.

## Results on the held out week

PR AUC (higher is better):

| Model | Bikes | Docks |
| --- | --- | --- |
| Base rate | 0.0223 | 0.0385 |
| Logistic regression | 0.3615 | 0.4494 |
| Gradient boosting | 0.3911 | 0.4834 |
| Gradient boosting with raw weather | 0.3544 | not run |
| Gradient boosting with rain flag | 0.3854 | 0.4853 |

Precision and recall when flagging the polls with the highest predicted risk:

| Model | Top 1% | Top 5% |
| --- | --- | --- |
| Bikes, logistic | 0.483, 0.217 | 0.295, 0.661 |
| Bikes, gradient boosting | 0.512, 0.229 | 0.314, 0.704 |
| Docks, logistic | 0.603, 0.156 | 0.438, 0.568 |
| Docks, gradient boosting | 0.651, 0.169 | 0.455, 0.591 |

Gradient boosting beat logistic regression by about 8% in PR AUC for both outcomes. The gradient boosting model uses the same features plus the grid cell split into x and y numbers.

## Weather

We added `workspace.default.weather_hourly_nyc` (1,800 hourly rows, August 20 to November 2, 2026) from the Open Meteo historical archive. It is one citywide series: the requested point near Central Park snapped to a grid cell about 5.3 km away. Times are in UTC so they join cleanly to `fetched_at`. Units are degrees Fahrenheit, inches, and miles per hour.

Result: weather did not help in this window.

* Raw temperature, precipitation, and wind lowered bike PR AUC from 0.3911 to 0.3544.
* A rain yes or no flag left it essentially unchanged (bikes 0.3854, docks 0.4834 to 0.4853).
* Likely reason: every poll in an hour shares the same weather, so the training period has only about 1,550 independent weather observations, and unrestricted trees can memorize specific hours.

## Limits

* We compared several variants on the same test week, so gaps are indicative. A new weekly export would give a fresh week to confirm.
* Ten weeks from late summer into fall cannot show seasonal patterns.
* Metrics are per poll, so they understate how many distinct events would be caught.
* Training used observed weather, while a live system would only have forecasts.
* The weather table exists only in one Free Edition workspace and is not yet in the shared class catalog.
* Only the 30 minute horizon has been built.

## Next steps

* Register the models and run the prediction notebooks for an end to end demo.
* Build 15 and 60 minute targets.
* Add station history features, such as typical availability at each hour of the week.
* Evaluate at the event level.
* Ask the instructor how to publish the weather table to the shared catalog.

## How to rebuild

1. Create a free Databricks workspace, add the class repo as a Git folder, and run `CREATE VOLUME IF NOT EXISTS workspace.default.citibike` in the SQL editor.
2. Run `free_edition_models/1-download-silver-FE` with `run_mode` set to production.
3. Run `free_edition_models/3-gold-features-FE` with `run_mode` set to production.
4. Optional weather table. Pull the data locally, save it as `nyc_weather_hourly.csv`, upload it to the `citibike` volume, and create the table:

```python
import requests
import pandas as pd

params = {
    "latitude": 40.78, "longitude": -73.97,
    "start_date": "2026-08-20", "end_date": "2026-11-02",
    "hourly": "temperature_2m,precipitation,rain,snowfall,wind_speed_10m",
    "timezone": "UTC",
    "temperature_unit": "fahrenheit",
    "precipitation_unit": "inch",
    "wind_speed_unit": "mph",
}
resp = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params, timeout=60)
resp.raise_for_status()
weather = pd.DataFrame(resp.json()["hourly"])
weather = weather.rename(columns={
    "time": "hour_utc", "temperature_2m": "temperature_f",
    "precipitation": "precipitation_in", "rain": "rain_in",
    "snowfall": "snowfall_in", "wind_speed_10m": "wind_speed_mph",
})
weather.to_csv("nyc_weather_hourly.csv", index=False)
```

```sql
CREATE OR REPLACE TABLE workspace.default.weather_hourly_nyc AS
SELECT CAST(hour_utc AS TIMESTAMP) AS hour_utc, temperature_f, precipitation_in,
       rain_in, snowfall_in, wind_speed_mph
FROM read_files('/Volumes/workspace/default/citibike/nyc_weather_hourly.csv',
                format => 'csv', header => true)
```

5. Open `model_experiments.ipynb` in the Databricks workspace and run it from the top.

The `data` files are generated locally or in Databricks and are not committed to git.
