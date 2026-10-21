# Predicting Citi Bike Station Availability

**DATA 622 Final Project Proposal** _In Progress, not Final_· Due October 11, 2026
Team: Johnathan Cruz,Randy Howk, Farhod Ibragimov, Michael Mayne, Jerald Melukkaran

Citi Bike riders lose trips when a station has no bike to rent or no dock to return to. Operations staff rebalance bikes, but they react to problems that have already happened. We will build a production system that predicts, every 15 minutes, which stations are likely to be empty or full 30 minutes later. We will run it live for four weeks and measure its accuracy against what actually happens.

Bike type also matters. A station can have bikes available while running out of the classic or electric bikes its riders tend to use. We will include `rideable_type` in historical demand profiles and evaluate which available mix is associated with the greatest completed-ridership rate for each station, weekday and hour. The operational goal is to stock enough bikes of each type to serve that demand.

We will also examine how the mix changes during a specific day at each station in 30-minute intervals. Each station/date has 48 local-time slots with separate classic/electric counts, unknown/other coverage, shares and percentage-point changes from the previous slot and the day's overall mix. Slots with no departures have zero counts and undefined shares. Counts and small-sample flags distinguish observed changes from unstable percentages; a share difference alone is not evidence of a statistically significant shift. Historical model profiles should preserve this half-h00000
These are two **supervised binary classification** tasks. A station can be at risk of both, so we model them separately rather than as one multi-class label. A station that the feed reports as not renting or not returning is a different outcome from running out of bikes or docks. Those snapshots are labeled "service disabled" and excluded from the shortage labels. Missing or stale snapshots produce no label, so we never assume a station was working when we cannot see it.

**Who uses the outputs:**
- **Citi Bike operations:** a ranked list of stations likely to fail in the next half hour, so rebalancing crews can go there first.
- **Riders:** an app-style view showing whether a nearby station is likely to have a classic bike, electric bike or dock when they arrive, where type-level data are available.
- **NYC DOT and the public:** reliability measured by neighborhood. Combined with household car ownership, this shows whether service failures fall hardest on areas that depend most on alternatives to driving.

## 2. Data sources

| Source | Use | Status |
|---|---|---|
| Class station status (GBFS, every 5 minutes), with vehicle-type definitions | Current bikes and docks, rentable counts by bike type, disabled equipment, renting and returning flags, and report time. These give model inputs, 30-minute-ahead labels and bike availability exposure. | Provided in the class Databricks workspace; audit coverage, retained type counts and the mapping from feed vehicle types to classic/electric before enabling type-specific outputs. |
| Class station information (GBFS, daily) | Capacity, coordinates and station names, effective-dated. | Provided in the class workspace. |
| Weather (NOAA via BigQuery) | Temperature, precipitation and wind, using only values available at forecast time. | Provided through the class BigQuery access. |
| Citi Bike trip history | Historical demand profiles per station, hour, weekday and `rideable_type` (departures, arrivals, net flow, completed-trip shares and sample counts). | **Already prepared locally:** 323.8 million trips from June 2013 to August 2026. Every source file was inventoried, de-duplicated, normalized to Parquet and reconciled row for row, with 0 rows lost. Modern trips retain bike type; missing types, including legacy records, remain unknown. |
| **Added: Census ACS 5-year table B08201** (households by vehicles available) with TIGER census tracts | Neighborhood transportation need: share of households with no car, with margins of error. Each station is assigned to a tract. | Our required external dataset, to be published as a documented shared table plus a station-to-tract crosswalk. |

The ACS table describes **neighborhoods, not riders**. We will use it to group stations and compare performance, and to give context on the dashboard, not as an input that changes a station's predictions. Trip history can only enter as features computed from rides that were published before each forecast time. Citi Bike releases trip files monthly, after the fact.

## 3. Initial approach and models

**Features (all known at forecast time *t*):**
- Current bikes and docks, and the fraction of capacity each represents.
- Rentable classic and electric counts, their shares of available bikes, and recent changes by type, where reported.
- Changes over the previous 15, 30 and 60 minutes.
- Disabled bikes and docks, and the operating flags.
- Hour, weekday and holidays.
- Weather.
- Typical net flow for this station and hour from past trips.
- Historical departures, arrivals and net flow by bike type, completed-trip shares, sample counts and the difference between current inventory mix and historical trip mix. All historical profiles use the same publication cutoff rules.

**Models, in increasing complexity:**
1. **Persistence baseline:** predict that the station's current state continues. This is the bar any model has to beat.
2. **Logistic regression:** interpretable, well calibrated, and fast to retrain.
3. **Gradient-boosted trees (LightGBM or XGBoost):** to capture interactions such as "low bikes, morning rush, rising temperature."

We will also train each model with and without the trip-history features. That tells us whether the 323 million historical trips actually improve predictions beyond what the live snapshots already show.

**Bike-mix analysis:** First, compute historical classic/electric completed-trip shares per station, weekday and hour as a demand benchmark, with sample counts and unknown-type coverage. Next, join trips to fresh, operating station-status intervals over their overlapping dates. Measure completed departures per observed operating station-hour and, for each type, completed departures per available bike-hour (`sum(rentable bikes of that type × interval hours)`). Restrict numerators to the same covered intervals as their denominators; zero exposure makes a per-bike rate undefined. Both-type availability, type-specific stockout time and feed gaps remain explicit.

We will compare candidate classic/electric mixes at the same total inventory, within effective-dated dock capacity and feasible fleet supply, accounting for station, time, weather, pricing periods where known, and recent demand. The target is the highest expected **total completed departures per station-hour**, with bike-hour rates as supporting measures. Candidate mixes need adequate historical support; sparse cases fall back to a documented pooled benchmark with an uncertainty flag. Completed-trip shares reflect supply as well as demand, so they cannot establish an optimal stocking ratio by themselves. Observational comparisons remain estimates of association; a controlled rebalancing trial is needed to establish increased ridership caused by a mix change.

## 4. Evaluation

- **Chronological splits only.** We train on earlier weeks and test on later ones, using rolling-origin backtests. A random split would put neighboring 5-minute snapshots in both training and test, which would inflate the scores.
- **Matured labels only.** A training example is used only once its *t* + 30 minute outcome has been observed within a set tolerance.
- **Metrics:**
  - precision-recall AUC (empty and full stations are relatively rare events)
  - Brier score and calibration curves
  - precision and recall among the top-*k* alerts each interval, matching what a crew could realistically respond to
  - the same shortage metrics separately for classic and electric bikes where labels are supported
  - for mix analysis: held-out departure-count error, completed departures per operating station-hour, departures per available bike-hour by type, stockout time by type, sample support and interval coverage
- **Breakdowns:** busy versus quiet stations, borough, and ACS tracts with a high versus low share of car-free households. This checks that accuracy does not drop in the neighborhoods the project aims to serve.
- **Breakdowns:** bike type, busy versus quiet stations, borough, and ACS tracts with a high versus low share of car-free households. This checks that accuracy does not drop in the neighborhoods the project aims to serve.
- **Production evaluation:** every stored forecast is later matched to the observed outcome. The final report will cover four weeks of live accuracy, coverage and calibration, not just backtest numbers.

## 5. Training: batch

**Batch training with frequent inference.** Models retrain weekly on a schedule. Each candidate is compared with the current model and the persistence baseline on the most recent week, and it is promoted only if it passes a documented check. Otherwise the previous model stays in service. Scoring runs every 15 minutes on the latest snapshots. Each run writes its predictions, keyed by station, forecast time, horizon and model version, **before** the outcome occurs, so the production evaluation can be reproduced. Online learning is unnecessary: availability patterns change slowly enough for weekly updates, and batch retraining is easier to audit and roll back.

## 6. Computational needs

No neural networks, LLMs, GPUs or TPUs. The workload is CPU-based tabular modeling:
- About 2,300 stations × 288 snapshots per day for feature building.
- About 220,000 station-forecasts per day for scoring.

Spark on the class Databricks workspace handles feature preparation. Model training fits on a single small cluster. Trip-history profiles are computed once, outside the class cluster, and uploaded as compact aggregates. The full trip archive is about 9.8 GB of Parquet, so we will not reprocess raw trips in the workspace.

## 7. Minimum viable product (November 2 to 7)

An end-to-end loop running on a schedule in Databricks:
1. **Feature job** that checks the snapshots are fresh and builds station features.
2. **Prediction job** every 15 minutes writing empty and full probabilities to a Delta table.
3. **Evaluation job** that matches earlier forecasts to observed outcomes and records daily metrics by model version.
4. **Dashboard** showing current status, predicted risk, data freshness, and the ACS no-car share for each station. Add classic/electric inventory, historical trip mix and shortage risk when supported, with coverage and uncertainty visible.

The MVP uses the persistence baseline and logistic regression. Gradient boosting, trip-history features and the neighborhood performance report are added before production starts on November 16. Dock-cost and capacity scenarios are a decision-support extension only if time allows. Forecasts alone cannot show how many extra trips new docks would create.

Type-aware historical profiles are part of the trip-history work. Type-specific live predictions and inventory-mix recommendations depend on the class feed retaining historical counts by type. If those counts are absent, we will deliver observed trip mix and its limitations, and record the missing input rather than claim a ridership-maximizing mix.

## References

1. Citi Bike System Data and General Bikeshare Feed Specification (GBFS). https://citibikenyc.com/system-data
2. GBFS field reference (`station_status`, `station_information`). https://github.com/MobilityData/gbfs/blob/master/gbfs.md
3. U.S. Census Bureau, American Community Survey 5-Year Estimates, Table B08201: Household Size by Vehicles Available. https://data.census.gov/table?q=B08201
4. U.S. Census Bureau, ACS API. https://www.census.gov/data/developers/data-sets/acs-5year.html
5. U.S. Census Bureau, TIGER/Line Shapefiles: Census Tracts. https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html
6. NOAA Integrated Surface Database (Global Hourly), as available in Google BigQuery public datasets. https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database
7. Mitchell, M. et al. (2019). Model Cards for Model Reporting. *FAT\* '19.* https://arxiv.org/abs/1810.03993
8. Gebru, T. et al. (2021). Datasheets for Datasets. *Communications of the ACM* 64(12). https://arxiv.org/abs/1803.09010
