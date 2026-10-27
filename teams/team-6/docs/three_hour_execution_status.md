# Three hour Citi Bike execution status

**Updated:** October 26, 2026. **Operator:** Randy Howk owns every workstream for now. **Workspace:** `workspace.citibike_3h` in the authenticated Free Edition account.

## October 26 historical station panel

The [separate historical Manhattan panel](historical_station_panel.md) now covers March 1, 2025–August 10, 2026 for 689 retrospectively mapped stations, with 26,178,555 station-origin-horizon rows. It reuses the existing hourly trip aggregates, audits Farhod's newer five-table release, and aligns all three weather targets and prior-origin subway publications under a five-minute issue cutoff. The user authorized retrospective geography and unknown unsupported zero counts; full historical station evaluation remains gated. The original 32-station spatial-weather alignment is superseded due to a one-hour target mismatch. The new versioned package is uploaded to the shared `inputs` volume: all 39 objects (46,921,271 bytes) passed independent download/size/SHA-256 verification, and effective `account users` read access is confirmed. Existing labels/receipts, dataset pointers, models, November confirmation and schedules are preserved. See the [protocol](historical_station_panel_protocol.json), [validation receipt](validation/historical_station_panel_2026-10-26.json) and [publication receipt](validation/historical_station_panel_publication_2026-10-26.json).

## October 21 station-hour integration

The [canonical station-hour integration](station_hour_integration.md) now combines the validated 32-station operating-hour cohort with dated UUID/short-name identity, next-hour departures and returns, current bike/dock state, Farhod's spatial weather handoff, release-aware subway advisory fields and the frozen chronological split tags. The immutable local build contains 78,049 station-hours; features and outcomes are separate checksummed artifacts.

The join completed, but common-fold coverage does not support a new combined model. Spatial weather has 13,471 eligible training and 8,230 validation rows but no September test rows, and its October 18 static station mapping fails strict historical as-of availability. Valid disruption fields have 14,224 September test rows but none in April/June training or validation. No model, catalog table or schedule was activated.

Public-only execution now includes real **Manhattan-total** model training, held-out evaluation, and live forecast issuance in Databricks Free Edition. The combined collection/scoring job runs hourly at **minute 55 UTC**. This is a reduced spatial scope: the original station-level model and historical alert ablation remain gated. See the [public-only scope decision/runbook](public_only_mvp.md) and [model card](manhattan_benchmark_model_card.md). The earlier six-example synthetic capability model remains separately labeled.

## October 16 September refresh and midterm preparation

The [September refresh](september_refresh.md) adds 4,982,855 validated source rows with zero quarantine rows, duplicate ride IDs or removals. The full observed package now has 169 selected input files and 21,167,148 station-hour rows through September. Manhattan labels contain 18,984 hours and 63,393,151 assigned departures, including 720 September hours with 3,049,503 departures. Source spillovers revise 17 August geographic hours by 331 departures; original benchmark metrics remain attached to their original data snapshot.

The extended weather contribution has 2,311 successful runs of 2,320 requested and 110,928 rows, including all 120 September initializations. Its immutable handoff is preserved, while the catalog exposes one stable current table with `dataset_version`. The active model and original feature package remain frozen. The daily importer has approved outcomes through September; live October accuracy still has no matching outcomes. The refreshed daily health report is OK for 16 due origins at 17:32 UTC.

The [whitepaper draft](technical_whitepaper_draft.md), [PDF](presentations/data622_whitepaper_draft.pdf), [updated slides](presentations/data622_mvp.pptx), [timed demo script](midterm_demo_script.md), [departure datasheet](borough_departure_datasheet.md) and [station/advisory datasheet](prospective_station_alert_datasheet.md) are prepared. The [refresh receipt](validation/september_refresh_2026-10-16.json) records cloud and artifact validation. Randy's spoken rehearsal, class transfer, original station/alert scope and sustained production evaluation remain.

## Initial October 15 execution

| Component | Verified result |
|---|---|
| Selected local inputs | 161 ledger-selected Parquet files covering August 2024–August 2026; source archive unchanged |
| Hourly observations | 20,251,420 station-hour rows, containing 96,642,471 departures |
| Count reconciliation | 96,650,125 eligible departures = assigned counts + 7,654 departures with ambiguous/nonexistent start-hour flags |
| Release cohorts | 20,258,144 station-hour/release rows preserve source availability joins for future profiles |
| Geography observations | 54,667 station/month rows; retrospective coordinate means and borough matches remain explicitly unverified |
| Full local package | 75 Parquet files, 47,686,487 bytes; generated in 197.22 seconds before final package verification |
| External sources | 68,324 historical subway-alert revisions, 496 subway station references, 2,519 Citi Bike station references/statuses, a live alert snapshot, and 192 weather rows from four runs |
| Cloud capability | Delta imports, volume access, model save/reload with zero prediction difference, MLflow artifact logging, and all three outbound feed requests passed |
| Cloud runtime | Successful capability run: 37.9 seconds elapsed; hourly import: 64.7 seconds; first live collection: 28.5 seconds, including 17.0 seconds inside the collection notebook |
| Public station archive | 22 CityBikes monthly files, November 2024–August 2026; 145,523,643 change records and 1.27 GB downloaded |
| Station audit | 1,123,844 conditional Manhattan station-hours, only 1,655 zero hours; activity-dependent selection prevents main station evaluation |
| Manhattan aggregate labels | 60,343,317 assigned departures, 18,264 clock hours; four ambiguous fall-back hours have null labels and are excluded |
| Weather history | 2,191 of 2,200 requested runs; 105,168 normalized valid-time rows; explicit missing runs and null values |
| Real cloud model | Weather Poisson alpha 0.1 selected on validation; calendar Poisson alpha 0.1 fallback; all four fits converged |
| Historical accuracy | Selected test MAE 761.00 vs hour-of-week profile 1,385.91 and calendar fallback 815.81; 6,564 common covered forecast rows |
| First live issuance | Origin October 16 01:00 UTC; all three predictions persisted at 00:59:19 UTC, before target start |
| Verification | All 204 local tests pass; cloud checks cover keys, input cutoffs, reload parity, fallback accuracy and station eligibility flags |

The historical alert snapshot contains 37 dates without records across the requested window. Archive timestamps stop on August 11, and the recurring month-end omissions persist. Missing records are not converted to normal service. The provider's PDF dictionary confirms publication semantics but does not resolve the timestamp timezone. Alert variables are excluded from the aggregate benchmark. Weather uses one common reference point, an assumed seven-hour delay and disclosed early-archive hindcasts; trip release dates use an unverified 45-day rule. These are retrospective development results, not live production accuracy.

## Databricks resources

### October 16 operational check

At approximately 10:03 New York time, all **13 scheduled collection/scoring runs** since scoring deployment succeeded and published before their nominal origins. The audit found no missed elapsed schedule slots, no fallback use, and no source failures across 65 requests. This covers the observed post-deployment interval, not a full 24 hours. The [saved operational evidence](validation/operational_health_2026-10-16.json) includes each run, task outputs and actual attempt publication times, so a timely original prediction cannot hide a late retry. The full local suite now passes **217 tests**.

Repeat with `uv run python scripts/audit_three_hour_operations.py --output docs/validation/operational_health_YYYY-MM-DD.json`. The audit is read-only in Databricks and writes a local JSON report. Live predictive accuracy still waits for published trip outcomes.

### October 16 slides and monitoring deployment

The [three-slide PowerPoint](presentations/data622_mvp.pptx) and [PDF](presentations/data622_mvp.pdf) are complete, with timed speaker notes and a dashboard link. The [station/subway reference](station_subway_mapping.md) was built from fresh public sources and imported successfully: 2,518 bike stations; 778 within 250m and 1,687 within 500m of an MTA reference point. It preserves separate stop, complex and daytime-route tables plus unmatched coverage; historical validity and operation remain unverified.

The [daily health job](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/899230762229374) passed its first manual run and is enabled at 13:15 UTC, starting October 17. Its initial report at 13:57 UTC returned OK for 12 due origins, with no incomplete/late forecasts, fallback rows, failed retrievals or stale sources. The refreshed dashboard displays health and mapping coverage. [Deployment evidence](validation/mvp_extensions_2026-10-16.json) records successful mapping, monitor and dashboard runs; **217 local tests pass**. Daily health measures canonical forecast availability and retrieval freshness; the separate SDK audit measures scheduler attempts. Zero matured live outcomes means no live predictive-accuracy estimate.

### October 16 release aware evaluation and evidence

The [release aware evidence pipeline](release_aware_evidence_pipeline.md) is deployed separately at 13:00 UTC. Monthly June–August folds gate training, validation selection and refitting by assumed label releases, including supplying-source dependencies. Pooled MAE is 767.77 versus weekly profile 1,386.49 on 6,552 common forecast rows; these previously inspected months remain retrospective evidence. The deployed model pointer is unchanged.

The initial release watcher queued September 2026's NYC ZIP; the subsequent [September refresh](september_refresh.md) completed ingestion and outcome approval. No bulk archive is automatically ingested. Prepared outcome packages pass strict calendar/count/checksum/DST/provenance checks before daily import. Live evaluation still contains zero forecasts. The first normalized prospective batch included 47,858 station observations across 2,519 distinct IDs, 9,078 alert observations, 43,888 selectors and 95,716 station/radius advisory feature rows. Seven station configuration changes are observed; configuration includes capacity and identity/name as well as coordinates. These observations do not establish historical continuous operation or disruption effects. Replay added zero inputs/rows, and cloud checks found no duplicate keys, temporal violations, bus/subway collisions or unsupported operation promotion. The initial full local suite passed **232 tests**; the September refresh passes 238. [Saved verification](validation/release_aware_evidence_2026-10-16.json) records the runs and schedule.

- [Capability pilot](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/53323071853168): successful run `385037608032599`; source imports, synthetic model reload, MLflow, and egress checks.
- [Hourly observation import](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/317506266996784): successful run `443992273350358`.
- [Collection and forecast job](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/1072725421467387): first successful combined run `562057928115153`; active UTC schedule `0 55 * * * ?`.
- [Real benchmark training job](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/578040656965614): successful run `504254985199629`; immutable model version `20261016T005315Z-35753b1b-9ac3c3ff`.
- [Forecast dashboard](https://dbc-d00cbd4c-c74c.cloud.databricks.com/editor/notebooks/2763628353985993): live forecasts, retrospective comparisons, test plot and source coverage; initial dashboard run `31816774972497` succeeded.
- [Public station audit](https://dbc-d00cbd4c-c74c.cloud.databricks.com/editor/notebooks/2763628353985991): imported crosswalks and conditional exposure, with main eligibility false; run `199202042598361` succeeded.
- [Data readiness notebook](https://dbc-d00cbd4c-c74c.cloud.databricks.com/editor/notebooks/1720373823574244): source counts and coverage queries; its review run `543334315990577` completed successfully.
- [Hourly observations notebook](https://dbc-d00cbd4c-c74c.cloud.databricks.com/editor/notebooks/1720373823574245): monthly Manhattan summaries and eligibility flags.

Source and hourly inputs are uploaded under `/Volumes/workspace/citibike_3h/inputs/`. Imported Delta tables have immutable build suffixes. The original `manhattan_hourly_observed` view uses provisional station/month geography and remains ineligible for main station evaluation. The new aggregate benchmark instead classifies each trip start coordinate, with separate `borough_features`, `borough_profiles`, `borough_weather_runs`, `borough_model_metrics`, `borough_forecast_output`, `borough_publication_log` and `borough_forecasts` objects. The older `nyc_weather_forecast_run` view still represents the four-run capability package; use `borough_weather_runs` for the full reference history.

The collector stores compressed raw snapshots and a per-run manifest in `inputs/live`, appends `gbfs_status_observations`, and records `collection_log`. Its downstream scorer checks model/package hashes, uses released profiles and fresh weather or a calendar fallback, and inserts only unseen forecast keys. `borough_publication_log` records persistence completion; the forecast view derives on-time status from that timestamp. Prospective predictive accuracy waits for matching published trip outcomes. Hourly GBFS observations still do not establish continuous station operation.

All jobs use the signed-in user, serverless compute and bounded timeouts. Hourly forecasting, daily health and daily evidence each have one concurrent run. Training and archive ingestion remain manual; prepared validated outcome imports run daily. Public benchmark deployments pin environment version 5 and scikit-learn 1.7.2 to avoid differing one-time/job defaults. Evidence processing pins GTFS bindings 3.0.0. The documented short runtimes are measured results, not a guaranteed Free Edition allowance. Review actual runs and missed collections over 24–48 hours. Pause all three recurring jobs when the project ends or while investigating persistent failures.

## Local commands and artifacts

Run commands from the canonical project root with the locked environment. The official CLI binary is local at `.local/bin/databricks`; OAuth credentials use the `DATA622` profile outside Git. No passwords or tokens are committed.

```sh
uv sync --locked
uv run citibike doctor
uv run python scripts/manage_three_hour_workspace.py status
uv run python scripts/manage_three_hour_workspace.py pause-collector
```

The pause command pauses the shared collection/scoring schedule and preserves data. Use `deploy_public_mvp.py activate` to deploy both tasks. The older `deploy-collector` operation replaces the task list with collection only; reactivate both tasks afterwards. `deploy-hourly --package /absolute/build/path` imports verified observations, and `deploy_three_hour_pilot.py` manages the older synthetic capability check. Uploads with different existing content are rejected.

Current local hourly package: `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/builds/20261016T172712Z-eb309064`.

Local external source package: `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/sources/20261015T234334Z-d29974e2`.

`CURRENT_OBSERVED` identifies the latest validated observation build by name under `builds/`. Separate pointers under `borough_labels`, `borough_features` and `public_exposure` identify the public-only builds; the feature pointer stays frozen on the active model's original inputs. Canonical normalized trip audit fields remain intact. The [earlier evidence](validation/three_hour_execution_2026-10-15.json) records initial exports; [public-only evidence](validation/public_only_mvp_2026-10-15.json) records real training, forecasts and verification.

## Remaining dependencies

1. Extend the [executed public station pilot](station_history_pilot.md): 32 stations trained with bounded fresh polling and dated-configuration assumptions; 31 September stations evaluated (weather MAE 3.48 versus calendar 3.63). Exact alert ablation needs full-field April–June snapshots or publisher re-compaction: complete public fields start August 7, too late for the fixed release-aware training/validation fold. Original 2025 coverage and unconditional continuous-operation claims remain unresolved. Class data is an optional source route, not the only route; the older CityBikes activity-biased archive still cannot clear these gates.
2. Extend the common weather proxy to station-specific cells if station modeling resumes, and obtain better evidence for source publication latency. The public historical cache no longer depends on locating the capstone cache.
3. Resolve alert timezone, coverage/lookback rules and historical mapping before the disruption ablation. The new prospective proximity reference does not establish historical geography or alert effects. Raw archives and prospective snapshots remain separate.
4. Monitor upcoming scheduled runs and eventually import released production outcomes. Prepare the midterm demonstration around the documented aggregate scope and remaining proposal gaps.
5. Publish the documented external contribution into the authorized **class** catalog. The private `workspace.citibike_3h` tables alone do not satisfy class sharing.

Randy remains the operator. Public-only work proceeds independently; class-specific transfer and the original station/alert scope remain incomplete.
