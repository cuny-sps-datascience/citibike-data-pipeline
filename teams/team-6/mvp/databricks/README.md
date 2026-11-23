# Team 6 next-hour demand in Databricks

The model estimates **recorded departures** for the next New York clock hour at six pilot stations. Given an issue time of 1:05 p.m., it predicts trips beginning from 2:00 through 2:59 p.m. Train and score in Databricks, using the original Colab comparison as a reproducible starting point. Reported departures measure completed trips. Inventory and unmet demand require separate evidence.

## GitHub integration plan

| Work | Disposition |
|---|---|
| #72, Jonathan's six-station Colab MVP | The PR now targets `team-6`, and its base history pulls in shared and other-team changes. The new `team-6-databricks-next-hour` branch starts at `team-6` and carries just Jonathan's six MVP commits with authorship and original commit references, followed by the Databricks port. Use the new PR for integration and #72 for its review history. |
| #64, foundation and source audit | Retain as a reference for data contracts, station IDs and source coverage. Pull narrowly scoped reusable pieces into a later PR if required. |
| #65, Manhattan panel and feature preview | Retain as reference for expanding coverage and on-time joins. |
| #66, stockout onset and frozen confirmation | Retain as a separate historical experiment. |

Archived branch heads are identifiable by the `archive/team-6/` tags. New Team 6 implementation PRs target `team-6`.

## Run the end-to-end demo

1. Open the feature branch in a Databricks Git folder. Open `mvp/databricks/train_station_demand.py` with Python serverless notebook compute. The notebook installs `requirements-databricks.txt`. The Git folder includes the compact, derived Parquet inputs that backed the Colab study. Alternatively, set `data_dir` to a Unity Catalog volume directory containing `trips_6stations_hourly.parquet`, `station_map.csv`, and `weather_6stations.parquet`.
2. Use the default `artifact_root` if you have write access to the Team 6 exports volume, or set it to another existing writable volume directory. Run the training notebook. It displays the weekday and weekend curves, overall scores, and scores by station and local hour. Keep the printed `MODEL_DIR`. The output includes versioned boosters, station curves, metrics and held-out predictions.
3. Open `mvp/databricks/score_station_demand.py` and set `model_dir` to that path. Set a trip station ID or leave it blank for all six. Select `both`, `calendar`, or `weather`. The notebook fetches and archives a weather snapshot for weather-enabled scoring when no snapshot is supplied. It displays the forecast, the model used, the corresponding historical rate, and how the prediction compares with that rate. It also writes the scored rows and input timestamps to the volume.
4. If compute cannot reach Open-Meteo, run `python mvp/scripts/live_weather.py /path/to/staging` from the Team 6 directory, upload the generated forecast Parquet to the volume, and provide that file as `weather_snapshot`. When timely weather is unavailable for a station, scoring uses the calendar model and records the fallback in `model_used`. [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)

The acceptance check is one Databricks training run and one next-hour score with weather. Also check the calendar fallback with a stale snapshot, and retain the resulting model directory, predictions and run links for the demo. Schedule hourly scoring after verifying egress, source freshness and workspace compute availability.

## Demand curves, weighting and loss

- The training-only curve gives typical departures by station, local weekday, and local hour. It uses known trip counts, including genuine zeros, and retains the number of observed hours supporting each cell.
- For comparisons across stations, the **demand index** is 100 at a station's overall hourly average. The **share of a typical day** compares each hour with the full curve for the same station and weekday. For a live prediction, **relative to typical** is expected departures divided by the historical rate for that station and hour. A zero historical denominator leaves the ratio undefined.
- The **weighted comparison** gives each prediction an evaluation weight proportional to `1 / max(typical_departures, 2)`. The floor prevents quiet hours from getting arbitrarily large weights. This gives low-volume hours and stations more influence than the unweighted score. Report both scores alongside expected departures in actual bikes per hour. The weights use the training curve and are applied during evaluation.
- **Poisson log loss** is the negative log probability of the observed count. The model provides the expected count and the Poisson distribution supplies the probability of that count. Rates have a 0.1 lower bound. MAE is secondary. Comparing calendar, weather, and both on identical rows isolates the information in each input set.
- In the original Colab convention, horizon 2 is the target one hour after the origin. We retain that horizon and the original date splits. The retrospective March–August test period was already used in earlier exploration. Freeze the model and weighting, then evaluate on fresh forward data with matched forecasts before making a live-performance claim.

## Validation

Databricks serverless trained all three model variants and saved the boosters, station curves and retrospective predictions in Unity Catalog. The live scoring run used a current ECMWF forecast for all six stations. [Databricks training run](https://dbc-d00cbd4c-c74c.cloud.databricks.com/?o=7474644096185196#job/107505380933494/run/617109931608033) · [Databricks live scoring run](https://dbc-d00cbd4c-c74c.cloud.databricks.com/?o=7474644096185196#job/813444845826644/run/719524888056)

On 23,358 matched next-hour test rows, calendar plus weather had the lowest scores. The two log-loss columns measure the same per-row loss with different weights.

| Model | Poisson log loss | Weighted Poisson log loss | MAE (departures/hour) |
|---|---:|---:|---:|
| Station, hour and weekday average | 3.225 | 2.179 | 4.28 |
| Calendar | 2.979 | 2.028 | 3.85 |
| Weather | 5.962 | 6.682 | 7.73 |
| Calendar and weather | 2.722 | 1.915 | 3.40 |

The curves show distinct weekday peaks. These are training-period averages across Monday through Friday, in New York time.

| Station | Peak hour begins | Typical departures |
|---|---:|---:|
| Little West St & 1 Pl | 8 a.m. | 14.4 |
| Liberty St & Broadway | 5 p.m. | 30.4 |
| Fulton St & William St | 5 p.m. | 14.7 |
| Broadway & E 14 St | 6 p.m. | 40.5 |
| W 21 St & 6 Ave | 6 p.m. | 57.1 |
| Amsterdam Ave & W 73 St | 8 a.m. | 23.9 |

At 1:09 p.m. New York time on Nov 23, 2026, Databricks scored all six stations for the 2 p.m. hour. Broadway & E 14 St was forecast at 28.3 departures compared with its usual 21.8 for that hour. The forecasts were retrieved before the scoring timestamp. A separate local stale-snapshot check selected the calendar fallback at all six stations.

## Source decisions

| Source | Use and coverage |
|---|---|
| Citi Bike trip files | Train departure counts and station profiles on completed trips. Monthly publication delays make these unsuitable for a live recent-departures feature. October 2026 was the latest published month as of Nov 23. A future September or October holdout needs a matching as-issued weather archive and a check that the outcomes were kept outside development. [Citi Bike trip data](https://citibikenyc.com/system-data) |
| Citi Bike GBFS | Use `station_status` for live inventory and operating flags and `station_information` for IDs, locations, and capacity. Verified all six pilot stations in both feeds. Preserve fetch, feed and per-station timestamps. Inventory changes combine rides, rebalancing, and equipment status changes. Evaluate GBFS predictors later on their own common window with matching trip labels. [Official feed discovery](https://gbfs.citibikenyc.com/gbfs/gbfs.json) · [Course station-status archive](https://cuny-sps-citibike-archive.s3.amazonaws.com/?list-type=2&prefix=exports/station_status/) |
| Open-Meteo ECMWF IFS | Use the existing Team 6 model archive for training and explicitly request ECMWF IFS at the same three grid locations for live scoring. Record receipt time and exact target timestamp. Use a forecast received within three hours of the issue time and by that issue time. Reconstructed archive runs used an assumed publication lag, so the original backtest remains retrospective. Open-Meteo stamps rain, precipitation, and snowfall with the hour ending at that timestamp. The port preserves the Colab join convention of using the values stamped at the start of the departure hour. [Forecast API](https://open-meteo.com/en/docs#hourly_parameter_definition) · [Historical single runs](https://open-meteo.com/en/docs/single-runs-api) |
| NWS and NOAA | Public alternative. NWS offers live hourly or gridded forecasts, and NCEI archives NDFD. Match the grid, issuance time, target interval, and weather fields before training and evaluating a separate model. [NWS API](https://www.weather.gov/documentation/services-web-api) · [NDFD archive](https://www.ncei.noaa.gov/products/weather-climate-models/national-digital-forecast-database) |
| AccuWeather | Keyed hourly forecasts. Public historical-current-conditions access covers observations. The enterprise product advertises as-issued historical forecasts since 2020 via Databricks Delta Sharing. Confirm access, hourly archive coverage and issue cadence before a comparison. [Hourly forecasts](https://developer.accuweather.com/core-weather/location-key-hourly) · [Enterprise archive](https://business.accuweather.com/solutions/retail/) |
| Visual Crossing | Commercial alternative with historical forecasts by issue run and target time from 2020. Its historical forecast API requires a subscription add-on. [Historical Forecast API](https://www.visualcrossing.com/resources/documentation/weather-api/historical-forecast-api/) |

For the first working model, the trips supply historical usage patterns and Open-Meteo supplies timely weather. For a later GBFS model, first verify archival coverage alongside the released trip labels. Use only station information retrieved before the issue cutoff, and keep stale or absent snapshots unknown.
