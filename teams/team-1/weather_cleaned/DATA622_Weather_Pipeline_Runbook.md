# DATA622 Citi Bike weather pipeline

Working implementation documented October 26, 2026.

## Purpose and current status

This pipeline collects NOAA GHCNh recorded weather for stations near Citi Bike locations, stores original observations in Databricks, derives hourly features, and links weather stations to bike stations. It supports the team's proposed 15-minute bike/dock availability project.

Confirmed through notebook outputs: historical Citi Bike data downloaded; nine NOAA station files ingested; 2,526 bike locations provisionally mapped to six weather stations; update job and freshness check successfully run; all 100 observations in the demonstration join matched. The sample is one collection time and does not establish full-dataset join coverage.

Not yet confirmed: an active daily schedule, notification delivery, access from teammates' accounts, or publication into the professor's shared catalog. These require checks in the workspace. A successful manual job run does not verify the schedule.

## Notebook responsibilities and run order

1. **Professor's `free_edition_models/1-download-silver-FE`**: run with its `run_mode` set to `production` to refresh the exported bike metadata and historical status. This is separate from the NOAA job; the NOAA job does not refresh Citi Bike exports.
2. **`01_citibike_noaa_weather`**: initial inventory download, coordinate preparation, distance matching, file availability checks, initial weather downloads, and registry creation. Retain as the setup/exploration record. Do not schedule it: initial table-creation cells may intentionally fail if rerun.
3. **`02_update_noaa_weather`**: maintained updater. Reads the saved registry, refreshes selected station-year files, validates them, selectively replaces corresponding database records, defines the observation view, and runs the freshness check. Schedule this notebook only after `03` has created the freshness view.
4. **`03_prepare_noaa_weather`**: defines hourly and precipitation views, provisional bike-to-weather mapping, freshness view, catalog comments, and demonstration queries. View definitions update automatically when underlying observations change; they do not require daily reruns.

The provisional mapping is a stored table, not a dynamic view. New bike stations, station relocations, or changes in eligible weather stations require a deliberate mapping refresh. Do not rerun the mapping-selection cell during a frozen model evaluation.

## Data objects

All objects below currently live in `workspace.default`.

| Object | Type/purpose |
|---|---|
| `silver_station_info` | Professor's exported bike metadata, including IDs and coordinates |
| `silver_station_status` | Professor's exported historical bike/dock observations |
| `noaa_station_registry` | Nine selected weather station IDs and download-enabled flags |
| `citibike_noaa_candidates` | Saved geographic candidates within the selected search area and 50 km radius |
| `noaa_weather_raw_2026` | Initial weather snapshot; retained as the updater's bootstrap source |
| `noaa_weather_raw` | Main multi-year raw observation table used by the updater |
| `noaa_weather_observations` | Readable observation view, retaining selected quality/source fields |
| `nyc_weather_hourly` | Initial hourly temperature, wind, and precipitation-report view |
| `noaa_precipitation_report_rules` | Maintained report-level precipitation interpretation |
| `nyc_weather_hourly_with_precipitation` | Preferred hourly analysis view, adding interpreted precipitation and maintenance flag |
| `citibike_noaa_mapping` | Provisional nearest eligible weather station per bike station |
| `noaa_weather_freshness` | Observation age and refresh age, with mapped bike-station counts |

Earlier objects `noaa_precipitation_interpreted` and temporary `noaa_report_check` were exploratory. They are not inputs to the maintained hourly precipitation view. No objects need deletion to operate the pipeline.

Original downloads live under `/Volumes/workspace/default/citibike/noaa/`. The updater retains each run's files in `download_runs/<run_id>/`. No retention cleanup is currently implemented; storage use will grow.

## Station selection

The NOAA directory contained 38,870 entries. The initial geographic search returned 87, then the NY/NJ and 50 km distance rules yielded 29 candidates. Eighteen had accessible 2026 files at the checked endpoints. Selecting the nearest three available candidates per bike location resulted in nine distinct downloads.

Three downloaded stations—The Battery, Kings Point, and Robins Reef—ended in March 2026 and were excluded from the August–October mapping. They remain enabled as downloadable backups, with no currently mapped bike stations.

| Weather station | NOAA ID | Mapped bike stations |
|---|---|---:|
| Downtown Manhattan/Wall Street | USI0000KJRB | 1,195 |
| LaGuardia | USW00014732 | 742 |
| Newark Liberty | USW00014734 | 5 |
| Central Park | USW00094728 | 569 |
| Teterboro | USW00094741 | 8 |
| JFK | USW00094789 | 7 |

Distances are great-circle distances. The farthest assignment is approximately 14.53 km. Proximity does not guarantee identical local weather.

The status archive contains 2,531 distinct IDs, versus 2,526 usable metadata locations. The difference has not been reconciled by a full ID comparison. Historical metadata validity also remains unverified.

## Transformation rules

- NOAA timestamps are parsed in UTC. Preserve UTC for joins; use America/New_York explicitly for local calendar features.
- Temperature and wind use the latest non-null report within each UTC clock hour. Measurements may originate at different times within that hour; retain their observation timestamps.
- Entirely absent hours do not have rows. A missing join must not be interpreted as a dry or calm hour.
- Source quality codes remain preserved. The implementation does not yet apply comprehensive source-specific quality screening.
- Precipitation is a reported accumulation, not a calculated clock-hour total. Do not sum overlapping reports or treat it as an instantaneous rain rate.
- `numeric_reported` preserves an explicit amount. `trace_reported` preserves trace status even when the numeric amount is zero.
- `inferred_zero_asos` applies only to the implemented six-station message rules: matching station/time, source 413, remarks and AO2 markers, no precipitation group, no conflicting measurement flag, and no NIL/PNO/PWINO exclusion. Other cases remain `unknown`.
- `$` is a general maintenance indicator, retained separately rather than used as an automatic precipitation rejection. This does not certify the observation as good.
- The latest eligible precipitation report is selected per hour. Complete matched reports with unavailable precipitation can supersede earlier reported amounts.
- Same-time report conflicts use the current deterministic struct ordering. That ordering is not a scientific quality ranking. Keep `maintenance_flag` last in the struct so adding it does not change the existing selection priority.

Use `precipitation_interpreted` together with `precipitation_interpretation`, `interpreted_precip_is_trace`, and `precipitation_maintenance_flag`. Older precipitation fields remain for comparison and can disagree with the revised interpretation.

## Update job and monitoring

Job name: **Daily NOAA weather update**. Task notebook: `02_update_noaa_weather`.

Intended configuration: daily at 08:00 America/New_York, available serverless compute, maximum concurrent runs 1, email notifications on failure. Verify these settings in the job UI; schedule activation and notification delivery have not been independently demonstrated.

The updater refreshes the current and previous year, bounded at 2026. Older stored years remain intact, but their later NOAA corrections are not automatically fetched. Newly selected stations needing older history require an explicit backfill.

Each file is downloaded to a unique run folder and checked for Parquet markers, readable contents, expected station/year, valid timestamps, and nonempty data. Reduced row counts or date coverage stop replacement for review. Successful station-year updates persist even if another update fails; a run is not atomic across all stations.

Selective station-year replacement avoids duplicate accumulation from rerunning downloads. It does not resolve duplicate or conflicting source reports. Annual refreshes currently redownload full files, including unchanged ones.

Freshness checks apply only to mapped stations. The initial thresholds are 96 hours since the latest observation and 30 hours since the latest table refresh. These are operational thresholds, not limits suitable for live forecasting. The check detects problems only when the job executes; it does not detect a disabled schedule by itself.

### Failure handling

- Network, HTTP, or file-validation failure: inspect the first failed station-year in job output, then retry after resolving the cause. Do not replace failed downloads with empty data.
- Reduced archive size or coverage: compare the old and new files before relaxing validation; the change may be a legitimate correction or an incomplete upstream file.
- Freshness failure: inspect `noaa_weather_freshness` to distinguish recent downloads of old observations from an old database refresh.
- Year-rollover 404: inspect NOAA availability. A new-year file may not yet exist; do not manufacture observations.
- Missing view/table: follow setup order. The updater's final check depends on the freshness view created in `03`.

## Historical joins and prediction limitations

The demonstration joins each bike observation to its assigned station and the preceding completed UTC weather hour. It is labeled for historical analysis only.

Observed NOAA archive delay was approximately 60 hours during testing. An observation timestamp does not prove that the report was available to the prediction pipeline then. `snapshot_loaded_at_utc` is refreshed on each replacement; it is not a persistent first-seen time or an original NOAA publication timestamp.

Before model training or production scoring, implement and document an availability strategy: collect version/first-seen information going forward, evaluate explicitly assumed historical delays with sensitivity analysis, or obtain a suitably timely weather source. A model excluding weather is a useful baseline. Do not claim a fixed historical delay guarantees absence of leakage.

The mapping was selected using coverage across the project period. Revisit selection using development/training data before final model evaluation. Do not use disabled bike stations' zero counts as ordinary shortage labels without operational eligibility checks.

## Remaining acceptance checks

1. Confirm daily schedule, one-run concurrency, failure notification destination, and next scheduled execution.
2. Verify teammate access and agree with the professor on shared-catalog publication. Personal-workspace storage does not satisfy shared access automatically.
3. Reconcile status IDs with metadata IDs; test joins across all intended stations and representative wet, dry, missing, and maintenance periods.
4. Validate source-specific quality codes, measurement units, same-time conflicts, and precipitation rules beyond the inspected examples.
5. Check missing hours explicitly against a complete expected time grid.
6. Implement availability-aware joins before prediction use.
7. Add a raw-file retention policy after deciding reproducibility requirements.

## Sources

- NOAA GHCNh overview: https://www.ncei.noaa.gov/products/global-historical-climatology-network-hourly
- NOAA format and variable documentation: https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh_DOCUMENTATION.pdf
- NOAA ASOS reporting guide: https://www.weather.gov/media/asos/aum-toc.pdf
- Aviation Weather Center report and sensor flags: https://aviationweather.gov/help/data/
- Databricks selective replacement: https://docs.databricks.com/aws/en/delta/selective-overwrite
- Databricks notebook schedules: https://docs.databricks.com/aws/en/notebooks/schedule-notebook-jobs

Record the retrieval dates and a version of this documentation with the team's final dataset submission.
