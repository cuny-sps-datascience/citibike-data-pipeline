# Public-only three-hour benchmark

Randy is executing this work in the existing Databricks Free Edition account, without class tables. The runnable benchmark predicts **total Manhattan departures for three separate future hourly intervals**. This is a reduced spatial scope from the supplied station-level proposal. Historical station predictions and the subway-disruption ablation remain separate, unresolved proposal requirements.

See the [model card and test plot](manhattan_benchmark_model_card.md), [weather datasheet](weather_reference_datasheet.md) and [three-slide/five-minute demo notes](public_mvp_demo.md).

The [PowerPoint](presentations/data622_mvp.pptx) and [PDF](presentations/data622_mvp.pdf) are prepared. A [dated station/subway reference](station_subway_mapping.md) is available in CSV, Parquet and cloud views. [Daily health monitoring](daily_forecast_health.md) is enabled at 13:15 UTC, with the first manual cloud report OK. The dashboard displays both health and mapping coverage. These additions preserve the original historical station/alert gates.

The [release-aware evaluation and public evidence job](release_aware_evidence_pipeline.md) adds monthly fitting/selection cutoffs, a daily trip-release queue, imports of prepared validated outcomes and normalized station/MTA observations at 13:00 UTC. September's newly observed ZIP remains queued for local ingestion; current live outcomes remain unavailable. Advisory features and station configurations are dated observations with publication and coverage gates, not historical operation or estimated disruption effects.

## Data and scope decision

The public [CityBikes archive](https://data.citybik.es/) supplies 22 monthly NYC Parquet files, November 2024–August 2026: 1,268,214,271 downloaded bytes and 145,523,643 change records. Raw files, retrieval metadata, SHA-256 checksums and listing modification times are retained on the processed RAID. Original trip archives are unchanged.

The publisher's [collector](https://github.com/citybikes/analytics/blob/master/collect.py) deduplicates unchanged bike/dock counts. Thus the archive is not a regular full-station snapshot log; a long silence does not prove continuous operation. A conservative audit joins each historical UUID to a unique same-month trip station within 25 metres, rejects another candidate within 50 metres and many-to-one mappings, and checks each observed coordinate against the archived monthly median. Renting endpoints must be active, reports no more than ten minutes old, and consecutive observations no more than ten minutes apart. Only whole hours inside chains of these brackets qualify. Five- and fifteen-minute gap sensitivities are retained.

That policy yields **1,123,844 conditional Manhattan station-hours, only 1,655 with zero departures (0.147%)**. This strong dependence on inventory activity prevents interpreting the subset as an unbiased operating-station evaluation. Its `operation_verified` and `main_evaluation_eligible` fields remain false. The tables are useful identity/coverage audits, and are not model-training inputs. Historical `last_updated` changes from epoch seconds to ISO text in February 2026; both formats are parsed, with UTC interpretation recorded as an inference. Repeated local fall-back hours are excluded.

The geographic benchmark instead scans the 161 ledger-selected trip files covering August 2024–August 2026, applies the downloaded Manhattan boundary to **each trip start coordinate**, and counts eligible departures. It contains 60,343,317 assigned departures across 18,264 clock hours. All eligible departures reconcile: 96,650,125 across all boroughs, less 7,654 with ambiguous/nonexistent start-time flags. No eligible start coordinates are missing or invalid in this selection. The four repeated fall-back clock hours have null borough labels and are excluded from models and historical exposure; their assigned-count audit remains available. Aggregate zero counts require complete trip input coverage, rather than assumptions about station operation.

## Forecast contract

For nominal origin `T`, targets are `[T,T+1h)`, `[T+1h,T+2h)` and `[T+2h,T+3h)`. Backtest inputs freeze at `T−5 minutes`. Training origins cover March 2025–March 2026, validation April–May 2026, and test June–August 2026. Origins crossing a split boundary or touching an unassignable target hour are removed.

Historical rates use complete geographic hours, including genuine zeros, from already released months. Numerators retain each trip's supplying source release. Availability is **assumed** to be nominal month end plus 45 days at 00:00 UTC; it is not a verified release calendar. The station-cohort interpolation is never used in these denominators. No recent trip-count lags, destination, membership or future bike-type information enters a predictor.

The weather cache contains **2,191 of 2,200 requested ECMWF IFS runs**, every six hours from February 28, 2025 through August 31, 2026. Nine requests return unavailable runs; additional null weather values remain missing. Each response, request, retrieval time and checksum is retained. The common requested point is 40.75, −73.98, serving as a Manhattan proxy; returned grid coordinates are preserved. This does not implement station-specific weather cells. Temperature is °C, precipitation mm, and wind m/s. Temperature and wind are instantaneous at target start; precipitation stamped at target end covers the target hour.

The newest run initialized before the cutoff minus seven hours supplies weather. The seven-hour delay is an **assumption**, and early Single Runs archives include hindcasts. Missing weather is excluded from the common-key model comparison; prospective scoring uses the selected calendar/profile fallback and records the reason. No reanalysis or observed weather is silently substituted. Attribute [Open-Meteo](https://open-meteo.com/en/docs/single-runs-api), ECMWF and CC BY 4.0. CityBikes data remains a separate ODbL-1.0 database with its own attribution; the borough model does not derive its labels or profiles from that archive.

Compare an hour-of-day pooled profile, hour-of-week profile, calendar/profile Poisson regression, and weather-enhanced Poisson regression. The fixed regularization choices are 0.1 and 1.0, with at most 300 iterations. Fit preprocessing only on training examples. Select the winner and the calendar fallback by validation MAE before test evaluation; exclude unconverged fits from selection. Report MAE, RMSE, Poisson deviance, mean bias, horizon and wet/dry slices. Counts are borough totals per hour, not per-station errors. The study is retrospective development: May validation labels were not available at the earliest June origins under the release assumption.

## Operations and reproduction

Run from `/Users/randyhowk/Sites/CUNY/citibike-data-pipeline/teams/team-6` on branch `team-6` with the locked `uv` environment. Credentials stay in the OAuth profile outside Git.

```sh
uv run python scripts/collect_public_history.py citybikes
uv run python scripts/collect_public_history.py weather
uv run python scripts/build_public_mvp.py labels --sources /absolute/source/package
uv run python scripts/build_public_mvp.py features
uv run python scripts/deploy_public_mvp.py train --package /absolute/feature/package
uv run python scripts/deploy_public_mvp.py status
uv run python scripts/deploy_public_mvp.py activate
uv run python scripts/deploy_public_mvp.py dashboard
uv run python scripts/deploy_public_mvp.py verify
uv run python scripts/deploy_public_mvp.py sensitivity
uv run python scripts/manage_three_hour_workspace.py status
uv run python scripts/run_tests.py
```

Acquisition resumes by checksum. New label/feature/audit builds are immutable, with separate current pointers. The label builder reads only the saved selected-file ledger, never canonical data globs. Archive reads are sequential, with DuckDB limited to 3 GB and four threads. Do not overlap full archive scans or run multiple canonical-manifest writers.

The test runner disables the host Miniforge/readline import that otherwise crashes pytest startup on this Mac, while preserving multiprocessing's main guard. It runs the same test suite; it does not disable or skip assertions.

Training runs in notebook `04_train_borough_benchmark`, logs comparisons in MLflow, checks model reload parity, and saves an immutable model directory under `/Volumes/workspace/citibike_3h/models/borough/`. Activation verifies a successful run and publishes `borough_active.json`. It updates the existing hourly job to collect inputs and score sequentially, retaining its `:55 UTC` schedule, bounded timeouts and one concurrent run. The scorer's nominal origin comes from the job trigger time, so a delayed job cannot relabel a missed hour as a timely forecast. A missing weather collection can still use the calendar fallback.

`borough_forecast_output` inserts only a previously unseen scope/origin/horizon/model-version key. Retries preserve the original predictions and provenance. `borough_publication_log` records completion after persistence; `borough_forecasts` derives on-time status from that completion time, not merely calculation start. The dashboard shows predictions, selected model, input cutoffs, fallback reasons, publication timing and retrospective comparisons. Newly issued forecasts have no accuracy score until actual trip labels are published and imported.

The pause command in `manage_three_hour_workspace.py` pauses the shared collection/scoring schedule. Redeploying only the collector with that older script replaces job tasks, so use `deploy_public_mvp.py activate` to restore both tasks afterwards. Daily health and public evidence have separate pause controls. No automatic model retraining or raw source-month ingestion is enabled; prepared validated geographic outcome exports are imported by the evidence job.

Jobs and exported notebook metadata pin serverless environment 5; model tasks install scikit-learn 1.7.2, matching the stored model. A one-time run using the earlier default environment failed to load NumPy-backed objects; the pinned environment passed full saved-prediction parity (maximum difference 2.37e-11) and stale-weather retry checks. The late retry attempted the calendar fallback but preserved the original three weather forecasts and their original publication times.

`deploy_public_mvp.py actuals --package /absolute/borough/label/build` imports validated version-2 labels and refreshes `borough_actual_hourly` and `borough_prospective_evaluation`. The initial import contains 18,264 historical clock hours and evaluates **zero** live forecasts because October outcomes are not available. Repeat after future released trip months have been normalized and a new complete label export built. Main evaluation requires publication before the **nominal origin**, preserving the intended horizon lead times for all three targets; a late origin cannot qualify just because horizon 2 or 3 has not started. The dashboard also shows per-target publication status, separately from this stricter evaluation gate.

`deploy_public_mvp.py weather --package /absolute/weather/handoff` tests the portable import in the private workspace. This was executed successfully for 105,168 rows, with checksum/key/interval checks and a table comment. The 730,880-byte ZIP is under `modeling_exports/three_hour/handoffs/weather_reference_20261016T005315Z-35753b1b.zip`. It contains data, manifest, datasheet and a destination-parameterized import notebook. Its private test does not mark class sharing complete.

## Remaining class deliverables

The documented weather table and importable Parquet are ready to transfer. The [assignment](https://georgehagstrom.github.io/DATA622Fall2026/assignments/project.html) requires development/deployment in its class workspace and an external table in its shared catalog. A private Free Edition table does not meet that requirement. Transfer requires the actual class destination and access; it is the remaining environment dependency, not a reason to stop public-data development.

To restore the original station-level scope, obtain a coverage-supported historical operation contract, resolve historical location changes, and evaluate zero exposure without activity-dependent selection. Historical alert timezone and continuous lookback coverage remain unresolved, so the disruption ablation is not implemented. The shared-catalog transfer, station model and alert comparison must not be described as completed by this aggregate benchmark.

For November–December production evaluation, monitor public monthly trip releases and import outcomes when they actually appear. A 45-day assumed delay may extend beyond the final presentation, whereas actual public releases can arrive earlier. Record first-seen release evidence and arrange the evaluation window with the instructor; never substitute bike-inventory changes for true departures. Until outcomes mature, report freshness, successful forecast issuance and late/missed runs as operational metrics, separately from retrospective predictive accuracy.
