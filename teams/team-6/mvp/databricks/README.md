# Team 6 next-hour demand in Databricks

The model estimates **recorded departures** for the next New York clock hour at six pilot stations. Given an issue time of 1:05 p.m., it predicts trips beginning from 2:00 through 2:59 p.m. Train and score in Databricks, using the original Colab comparison as a reproducible starting point. Reported departures measure completed trips. Inventory and unmet demand require separate evidence.

## Adapting the professor's workflow

The professor's model estimates whether a new stockout will occur in the next 30 minutes. Team 6 estimates an hourly count of recorded departures. We adapted his validation, modeling, and deployment methods to the timing of Citi Bike's monthly count labels and the different daily curves of the six pilot stations. [Professor's training notebook](https://github.com/cuny-sps-datascience/citibike-data-pipeline/blob/a778401df8e1c33ed7b09020b78fa0d6555c155a/free_edition_models/4-train-sklearn-FE.ipynb) · [Professor's live monitoring notebook](https://github.com/cuny-sps-datascience/citibike-data-pipeline/blob/a778401df8e1c33ed7b09020b78fa0d6555c155a/free_edition_models/4-publish-log-loss-chart.ipynb)

1. **Fit using only labels available at the time.** We reused the professor's expanding-window approach. Each outer fold fits five minutes before the first nominal issue time for that New York target month. A row can enter fitting only after its departure hour ends and its month has a release timestamp strictly earlier than fitting. The latest complete mature month is used for tuning. The inner training fit observes its own earlier cutoff. After tuning, both models refit on the same eligible data available to the outer fold. Each fold also rebuilds its historical average and station-hour weights using those fitting rows.
2. **Compare LightGBM with a regularized count model.** The professor's 24-hour and 12-hour sine and cosine terms give a smooth daily cycle and permit two daily peaks. The Poisson challenger gives each station its own cycle, with separate adjustments on weekends. It also uses day of week, holidays, annual terms, and the same forecast weather as LightGBM. The L2 penalty is selected on inner validation using Poisson log loss. The comparison uses the same held-out station-hours for both models. This tests whether the boosted trees improve on a simpler count model with distinct station curves.
3. **Separate training, live scoring, and evaluation.** MLflow records settings, fold losses, the tuning decisions, and pooled metrics. A registry training run saves the Poisson bundle as `challenger`. The already verified LightGBM bundle can initialize `champion` when that alias is first created. Later challenger training leaves the champion alias under explicit review. Scoring resolves an alias once, loads that exact model version, and saves it on each prediction row along with the issue, target and forecast receipt times. The monitoring notebook joins frozen forecasts to validated counts once release evidence is available. It separates model versions and reports coverage, Poisson log loss and MAE by station and local hour. [Databricks model registry and aliases](https://docs.databricks.com/aws/en/machine-learning/manage-model-lifecycle/)

### Release-aware results

The March through Aug 10, 2026 comparison contains 23,358 matched rows in six monthly folds. August is a partial **evaluation** fold. Lower is better for every metric.

| Model | Poisson log loss | Weighted Poisson log loss | MAE (departures/hour) |
|---|---:|---:|---:|
| Historical station, weekday and hour average | 3.354 | 2.280 | 4.48 |
| Regularized Poisson challenger | 3.219 | 2.341 | 4.23 |
| LightGBM with calendar and weather | 2.722 | 1.942 | 3.40 |

LightGBM leads on both versions of log loss. The Poisson model improves the unweighted score over historical averages, while its weighted score is higher. That distinction matters because the weighted score gives quieter station-hours more influence.

The compact historical dataset currently supports **the nominal target-month release assumption** used in Team 6's earlier work. The backtest defaults to month end plus 45 days at 00:00 UTC. This is a retrospective timing assumption. The compact file lacks per-trip-file provenance, and some historical forecast availability times are also reconstructed. A verified receipt must give the timestamp when all files and validated counts supporting that target month became available. The tested March–August window was already used in project development. Keep a fresh period for any final performance claim.

### Run the release-aware workflow

1. Run `train_release_aware.py` with the same `data_dir` as the original training notebook. Use `run_mode=dev` to log a development comparison, or `run_mode=register` to update the challenger alias in `workspace.citibike_3h.team6_next_hour_departures`. Set `champion_bootstrap_dir` to a verified LightGBM model directory when initializing the champion for the first time. Subsequent scoring runs can choose either alias.
2. Run `score_station_demand.py` with `model_dir` blank to score by registry alias. The default is `champion`. Set `artifact_root` to the scoring output directory in a writable volume. An explicit `model_dir` continues to support direct replay of the original volume bundles. Archive the forecast rows with the exact version and the saved station-hour weight.
3. Run `monitor_station_demand.py` with the same `artifact_root`. To score released outcomes, pass a validated hourly Parquet file with `station_id`, `hour_utc`, and `departures`, and a release CSV with unique `target_month` values (YYYY-MM) and `label_available_at_utc` values with time zones. An absent receipt leaves the row pending. An available receipt with no validated count leaves the row awaiting a count. Validated zeros enter the metrics. Monitoring reports pending and evaluated row counts alongside the loss, so gaps in outcome coverage are visible.

The same release-receipt CSV can be supplied to historical training to use known availability wherever the receipts cover the study period. Once supplied, missing months are treated as unavailable. Monitoring always requires verified receipts to assign a score.

Databricks reproduced the table above and logged the fold results to MLflow on Nov 23, 2026. The registry contains LightGBM `champion` version 2 and Poisson `challenger` version 1. Both versions scored six stations for the 2 p.m. New York hour using Open-Meteo forecasts captured before issue. The monitoring refresh retained all 12 forecast rows as pending because their target hours had yet to finish. Its evaluated-row count was zero. The forecast and monitoring outputs are under `/Volumes/workspace/citibike_3h/exports/team6_next_hour/professor-adaptations-20261123`. [Training and registration run](https://dbc-d00cbd4c-c74c.cloud.databricks.com/?o=7474644096185196#job/879427206491328/run/322811839115423) · [Live scoring and monitoring run](https://dbc-d00cbd4c-c74c.cloud.databricks.com/?o=7474644096185196#job/714909205400302/run/762173014669954)

## GitHub integration plan

| Work | Disposition |
|---|---|
| #72, Jonathan's six-station Colab MVP | The PR now targets `team-6`, and its base history pulls in shared and other-team changes. The new `team-6-databricks-next-hour` branch starts at `team-6` and carries just Jonathan's six MVP commits with authorship and original commit references, followed by the Databricks port. Use the new PR for integration and #72 for its review history. |
| #64, foundation and source audit | Retain as a reference for data contracts, station IDs and source coverage. Pull narrowly scoped reusable pieces into a later PR if required. |
| #65, Manhattan panel and feature preview | Retain as reference for expanding coverage and on-time joins. |
| #66, stockout onset and frozen confirmation | Retain as a separate historical experiment. |

Archived branch heads are identifiable by the `archive/team-6/` tags. New Team 6 implementation PRs target `team-6`.

## Reproduce the original volume-based demo

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
