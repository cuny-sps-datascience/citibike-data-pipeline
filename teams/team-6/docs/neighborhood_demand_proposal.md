# Predicting Citi Bike Ridership with Neighborhood Profiles

**DATA 622 Final Project Proposal · Draft**
**Team:** Randy Howk, Farhod Ibragimov, Michael Mayne

## 1 Problem and intended use

We propose to identify where Citi Bike rides most often begin, describe the surrounding residential and employment context, and predict future ridership at existing stations. Our central question is whether neighborhood characteristics improve departure forecasts beyond historical ridership and calendar patterns. This connects the team's customer-base idea to a measurable machine learning task.

The primary task is **supervised count regression**: predict the number of recorded departures from each operating NYC station on the following calendar day. We will summarize station forecasts by census tract and examine weekday morning (7–10 a.m.) and evening (4–7 p.m.) patterns as possible commuting signals. These windows are analytical choices; membership and timing do not establish a trip's purpose.

Citi Bike could use the outputs to identify consistently busy areas, plan community outreach, and investigate service needs. NYC planners could compare ridership with transportation access, while New Yorkers could explore local usage patterns. Neighborhood profiles describe potential service populations. A trip's origin does not reveal a rider's home, workplace, income, or rental status, and completed rides do not measure demand suppressed by unavailable bikes. Expansion opportunities will therefore be hypotheses for further study.

## 2 Data sources and shared contribution

**Citi Bike trip history.** Published files contain station locations, trip timestamps, bike type, and member/casual status [1]. The existing local validation report reconciles **323,817,126 published trips from June 2013 through August 2026**, with 512 duplicate records removed [6]. We will initially model January 2025 through August 2026, using earlier available releases to construct historical features. This avoids combining legacy station identifiers with modern IDs.

**Required external source: Census ACS five-year estimates.** We will use tract-level population (B01003), median household income (B19013), renter-occupied housing (B25003), and household vehicle availability (B08201), retaining estimates and margins of error [2]. We propose the 2019–2023 vintage for backtesting, subject to verifying release dates and the team's existing Census extract. These are multiyear neighborhood estimates, not annual rider measurements. Matching TIGER/Line tract boundaries will support geographic joins and population density [3].

**Class station information and status.** We will audit available histories for station coordinates, capacity, operating dates, and service interruptions. Citi Bike publishes these feeds through GBFS [1]. Effective-dated coordinates will map stations to tracts; current locations or capacities will not be assigned to historical records without verification. Unmatched stations remain flagged.

**Optional workplace data.** Census LEHD Origin-Destination Employment Statistics (LODES) Workplace Area Characteristics provide jobs at census-block resolution [4]. If time permits, we will aggregate a compatible, previously released vintage to tracts to compare residential and workplace context. Workplace counts do not identify actual Citi Bike commuters.

Our shared-catalog contribution will be a documented Delta table, **`nyc_tract_neighborhood_profile`**, with one row per tract and ACS vintage. It will include GEOID, population, land area, population density, income, renter share, no-vehicle household share, underlying counts, uncertainty fields, source URLs, release/retrieval dates, and geography vintage. A separate station-to-tract crosswalk will contain station ID, identifier scheme, effective dates, and match status. Documentation will define units, denominators, missing-value handling, and joins. These tables remain proposed; publication is part of the MVP.

## 3 Initial approach and models

We will aggregate eligible departures to station-day counts, keeping arrivals separate. We will include zero-departure days only when station operation and source coverage are established; missing files or uncertain closures will not become zero demand. Tract summaries will report station counts and operating exposure so that neighborhoods with more infrastructure are distinguishable from those with higher usage per station.

Features will include weekday, month, holidays, station identity, and historical departure averages by station and weekday. Historical member/casual and classic/electric shares will describe usage where supported, with unknown types retained. Neighborhood predictors will include population density, renter share, median income, and no-vehicle share. Capacity will be included only where historical values are verified. Optional employment features will follow the same geographic and release checks.

We will compare three approaches:

1. **Historical baseline:** the station's weekday average over the latest eight weeks of trips available at forecast time, with a pooled fallback for sparse stations.
2. **Regularized Poisson regression:** an interpretable model for nonnegative departure counts.
3. **Gradient-boosted trees with a count objective:** a model that can capture nonlinear relationships and interactions.

We will compare otherwise identical models with and without neighborhood features. This directly tests the value of the added dataset. If time permits, clustering standardized usage patterns will support descriptive station groups, such as morning-departure or evening-departure patterns. Cluster names will describe observed behavior; they will not assign rider identities or verified commute purposes.

## 4 Evaluation

We will train initially on January–August 2025, tune on September–December 2025, and reserve January–August 2026 for rolling monthly evaluation. Each forecast will use only labels and features available at its origin. Because trip files arrive after the trips occur, a yesterday-count feature is unavailable unless another validated source supplies it. Release dates, or explicitly documented conservative availability assumptions, will govern all historical profiles. Census and employment vintages must also have been released before the corresponding forecasts.

Primary metrics will be **mean absolute error**, which expresses error in rides per station-day, and **Poisson deviance**, which evaluates count predictions. Root mean squared error will show sensitivity to large misses. We will report improvement over the baseline, errors by borough and station activity level, and errors across renter-share and no-vehicle-share groups. Neighborhood-feature comparisons will use the same splits and preprocessing fitted only on training data.

For operational ranking, we will measure overlap between the predicted and observed top 20 busiest stations each day. For neighborhood analysis, we will compare tract totals and departures per operating station-day. A separate test holding out entire tracts will assess geographic transfer where sample size permits. It will not establish demand at locations without stations. Forecast quality, geographic match coverage, and Census uncertainty will remain visible alongside the profiles.

## 5 Batch training and production monitoring

Training will be **batch**, triggered monthly after new trip releases pass validation. Scoring will run daily to produce next-day forecasts from the latest available profiles and calendar information. Weekly retraining can be considered if the class provides suitable fresher labels. Online learning is unnecessary for the MVP.

Scheduled Databricks jobs will validate inputs, build features, and store forecasts with station ID, forecast origin, target date, model version, and source cutoff. Candidate models must improve validation performance over the baseline before promotion. An evaluation job will join saved forecasts to observed counts when the corresponding trip release becomes available. Monitoring will track missed runs, stale profiles, geographic coverage, and error by model version. Monthly trip publication delays live outcome scoring; the dashboard will distinguish pending outcomes from evaluated predictions.

## 6 Computational needs

No large neural networks, LLMs, GPUs, or TPUs are planned. The project will use CPU-based tabular models. DuckDB and Parquet already support local archive processing; compact station-day features and tract profiles can be published to Databricks for scheduled jobs and model tracking. A recent two-year station-day dataset should contain roughly one to two million rows, depending on verified station coverage. A standard CPU environment should be sufficient, with memory and training time measured during the pilot. We will avoid loading the full trip archive into pandas or repeatedly uploading raw historical trips.

## 7 Minimum viable product

The midterm MVP will demonstrate the complete workflow for **one borough and a complete recent year**, then expand to the remaining NYC stations. It will deliver:

1. Validated station-day departures, a geographic crosswalk, and the documented Census profile table in the shared catalog.
2. A historical baseline and Poisson model, with a chronological holdout report and a comparison of neighborhood features.
3. A scheduled scoring job that stores next-day forecasts before outcomes occur, plus an evaluation job tested on historical releases.
4. A dashboard or notebook showing observed and predicted departures, busiest starting areas, morning/evening patterns, and neighborhood profiles, with source cutoffs and uncertainty flags.

Feasibility is established when the pipeline completes from published inputs to stored predictions and evaluation, shared tables can be queried by another team, and model performance is compared with the baseline. The validated local archive provides a starting point; aggregation, Census integration, modeling, and deployment remain to be built. Later iterations can add boosted trees, workplace context, and descriptive clusters. The core deliverable remains reproducible ridership forecasting supported by neighborhood profiles.

## References

1. Citi Bike. [System Data and Trip Histories](https://citibikenyc.com/system-data).
2. U.S. Census Bureau. [American Community Survey Five-Year Data](https://www.census.gov/data/developers/data-sets/acs-5year.html). Proposed 2023 detailed tables: [B01003 Population](https://api.census.gov/data/2023/acs/acs5/groups/B01003.html), [B19013 Median Household Income](https://api.census.gov/data/2023/acs/acs5/groups/B19013.html), [B25003 Tenure](https://api.census.gov/data/2023/acs/acs5/groups/B25003.html), and [B08201 Household Vehicle Availability](https://api.census.gov/data/2023/acs/acs5/groups/B08201.html).
3. U.S. Census Bureau. [TIGER/Line Shapefiles](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html).
4. U.S. Census Bureau. [LEHD Data Products and LODES](https://lehd.ces.census.gov/data/).
5. Citi Bike project team. *Normalized Trips Data Dictionary*. Local project file: `docs/data_dictionary.md`.
6. Citi Bike project team. *Archive Validation, June 2013–August 2026*. Local project file: `docs/validation/validate_2013-06_2026-08.md`. Published rows: 323,817,126; removed duplicates: 512.
