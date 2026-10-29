# Release aware evaluation and prospective station evidence

Randy operates the [daily public evidence job](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/537646732304684). It runs at **13:00 UTC**, or 09:00 New York during daylight saving time and 08:00 during standard time. Tasks run in order: observe public trip releases, import prepared validated outcomes and refresh evaluation, then normalize collected station information and subway advisories. Each downstream task runs even if its upstream task fails; the overall job still reports failures. The existing hourly forecast model remains frozen.

The manual deployment passed with all three tasks. The first scheduled run is October 17. The [forecast dashboard](https://dbc-d00cbd4c-c74c.cloud.databricks.com/editor/notebooks/2763628353985993) now includes the rolling comparisons, release queue, outcome status and prospective reference summaries. [Saved execution evidence](validation/release_aware_evidence_2026-10-16.json) records runs, schedules, temporal checks and counts.

## Monthly evaluation with release gates

June, July and August 2026 are separate forecast folds. A fold fits five minutes before the first monthly origin in New York. A target label becomes available only after both its own target-month release and every supplying source-cohort release. Zero geographic counts also require their full target-month release. The rule remains the documented **month end plus 45 days at 00:00 UTC assumption**, rather than verified historical publication times.

For each fold, validation uses the latest target month whose entire reference label set has matured. Training targets precede that validation month. The existing profile/calendar/weather candidates and fixed alpha choices remain unchanged. Selection uses validation MAE, is saved before fold test evaluation, and is followed by a refit of the selected model and fallback on all matured weather-covered rows. All refits converged. Origins touching the next fold month are purged across all three horizons.

| Forecast month | Validation month | Selected model | Forecast rows | Selected MAE | Weekly profile MAE |
|---|---|---|---:|---:|---:|
| June 2026 | March 2026 | Weather Poisson alpha 1.0 | 2,100 | 734.30 | 1,592.10 |
| July 2026 | April 2026 | Weather Poisson alpha 0.1 | 2,226 | 839.87 | 1,260.88 |
| August 2026 | April 2026 | Weather Poisson alpha 0.1 | 2,226 | 727.24 | 1,318.11 |
| Pooled | — | Monthly selection above | **6,552** | **767.77** | **1,386.49** |

Errors are departures per Manhattan hour. July's and August's latest fully mature validation month is April because later labels have unreleased supplying-source dependencies at their respective fit cutoffs. Some later individual labels can still enter refitting after their dependencies mature. Calendar Poisson performed better than the selected weather model in June: MAE 649.80 versus 734.30. Monthly variation is retained rather than selecting a different winner after seeing that fold's outcomes.

These are additional retrospective analyses on previously inspected months, not a newly untouched test set. Weather availability is still assumed, early archived forecasts include hindcasts, and overlapping horizons are not independent observations. This analysis strengthens fitting/selection chronology without establishing exact historical provider availability. It does not replace the deployed model or its [original model card](manhattan_benchmark_model_card.md). The original test has 6,564 rows; the rolling comparison has 6,552 because it also purges intermediate monthly boundaries. Their pooled errors should not be compared as if the keys were identical.

Cloud views: `workspace.citibike_3h.borough_rolling_predictions` and `borough_rolling_metrics`. Full predictions and per-fold selection reports are saved under `/Volumes/workspace/citibike_3h/exports/rolling_backtests/`. Compact inputs came from existing features and label-release cohorts; original trips were not rescanned.

```sh
uv run python scripts/package_rolling_backtest.py
uv run python scripts/deploy_public_mvp.py rolling --package /absolute/rolling/package
```

## Public release observations and outcome imports

The watcher fetches the complete [Citi Bike public object listing](https://s3.amazonaws.com/tripdata/) with bounded pagination, saves its XML and checksum, and records monthly NYC/Jersey City ZIP observations. Version identity includes the object key, ETag, bytes and provider modification timestamp. An ETag is retained as metadata, not asserted to be a SHA-256 checksum. The object's first-seen time is our observation; provider modification time can describe a revision and is not original publication proof. Older yearly archives and unrecognized object names are outside the monthly candidate parser.

The first poll at **October 16, 11:22:36 New York time** found 164 monthly objects and queued `202609-citibike-tripdata.zip` (972,151,835 bytes). Its provider modification date is October 5. September was initially a candidate relative to August outcomes. The subsequent [September refresh](september_refresh.md) ingested and validated it, then imported outcomes through September. Those outcomes cannot evaluate October live forecasts. Repeated polls preserve the first-seen value and increase sightings. The watcher neither downloads the bulk ZIP nor writes the canonical archive.

| Object | Purpose |
|---|---|
| `trip_release_observations` | Every successful complete listing's monthly object observations |
| `trip_release_object_versions` | First/last observed times and sightings per object version |
| `trip_release_candidates` | Objects in the latest successful listing, with NYC ingestion readiness |
| `borough_evaluation_daily` | Every daily outcome-refresh result, including a valid zero evaluated count |

All names are in `workspace.citibike_3h`. Candidate status remains `awaiting_local_ingestion_and_label_validation` until separately prepared outcomes cover that period. A listing alone never approves an archive, infers complete labels or marks the candidate ingested. Failed listing requests fail the job; candidate views retain the last successful observation and its timestamp.

New archives require local acquisition, selection, normalization and validation under the existing single-writer rules before geographic labels are rebuilt. Preserve original sources read-only. The pipeline deliberately does not automate those archive mutations or treat a listing alone as a validated import. Stage a **complete prepared** version-2 geographic label export:

```sh
uv run python scripts/deploy_evidence_pipeline.py stage-outcomes --package /absolute/validated/borough/label/build
```

The staging gate checks the outcome SHA-256, counts, exact hourly calendar, unique hours, Manhattan scope, nonnegative integer departures, null unassignable DST labels, assigned-count reconciliation and selected-input provenance. It uploads the outcome file and manifest, then publishes `ready.json` last. The daily importer repeats these checks, verifies the ready receipt and refuses shorter coverage than the current actuals. It selects the prepared package with the latest coverage. This automatic import begins only after local data preparation has passed that gate.

The initial staged export contains 18,264 historical hours through August; the validated September refresh extends the imported actuals to 18,984 hours. `borough_prospective_evaluation` requires genuine matching outcomes and publication strictly before the nominal origin for every horizon. It currently has **zero evaluated live forecasts**. The daily report stores this pending state rather than inventing accuracy or substituting inventory movements for departures. Original benchmark inputs stay immutable; the approved actuals view advances only to validated prepared outcomes. There is no automatic retraining.

Raw listing/report paths are under `exports/release_watch/<poll_id>/`. Outcome reports are under `exports/outcome_evaluation/<run_id>/`; latest pointers are `trip_release_watch_latest.json` and `borough_evaluation_status.json` in the exports volume.

## Prospective subway advisory features

The hourly collector's compressed raw MTA snapshots are verified against the saved uncompressed SHA-256 before parsing. Each record retains collection ID, raw observation time, provider feed timestamp, entity ID, revision hash, display intervals and English headline. Revision history preserves first/last sightings. First-seen evidence is distinct from creation/publication timestamps; provider publication verification remains false.

The official [MTA alert dictionary](https://www.mta.info/document/90881) identifies `MTASBWY` as subway and `MTA NYCT`/`MTABC` as bus agencies **in the alert feed**. The [regular static subway GTFS](https://www.mta.info/developers) uses `MTA NYCT` as its agency ID. That explicit crosswalk prevents a bus route number from matching a subway route. The regular GTFS ZIP and dictionary are retained by checksum with retrieval/cache provenance. The dictionary's direct fetch returned HTTP 403; the package preserves the earlier downloaded official PDF and records that cache use, while the current text was readable through the web reader.

GTFS routes, trips, stops and stop times supply actual scheduled route/stop/direction associations. The three shuttle IDs remain `GS`, `FS` and `H`; express IDs such as `6X` and `7X` remain distinct. Parent/directional stops resolve through the GTFS hierarchy. Fields inside one selector are combined with **AND**; separate selectors are combined with **OR**, as the [GTFS reference](https://gtfs.org/documentation/realtime/reference/#message-entityselector) specifies. Trip-specific restrictions remain unresolved rather than being broadened. Stops with no scheduled match are retained in the resolution audit. Regular GTFS associations describe reference service at some scheduled times, not actual service at a target hour or every temporary reroute.

The [250m/500m mapping](station_subway_mapping.md) supplies nearby reference points. Counts deduplicate an advisory across multiple selectors and nearby subway points. Snapshot rows contain matched currently active advisory counts, known display-period overlap with the next three hours, and alerts without explicit display periods. MTA active periods describe display; these are **advisory proximity features**, not verified disruption durations, causal effects or model inputs in the deployed Manhattan model.

| View | Purpose |
|---|---|
| `mta_alert_observations` | Published normalized entity/revision sightings, including non-subway records for audit |
| `mta_alert_selectors` | Original identifiers, restrictions and resolution outcomes |
| `mta_alert_revision_history` | First/last observations of each revision |
| `station_subway_alert_features_available` | Per collection, bike station and radius; counts, quality flags and derived availability |

The first batch normalized **9,078 alert observations, 43,888 selectors and 95,716 station/radius feature rows** from 19 MTA snapshots. All observed subway selectors resolved or were explicitly recorded as having no scheduled match; none had unknown/unsupported restrictions. A no-match record is not proof of unaffected service.

Counts become null when the feed is stale/missing, the matching GBFS information is stale/missing, the bike station lacks a mapping, its current short-name ID differs, or its coordinates are more than 25 meters from the mapped point. The source freshness limits are 15 minutes for MTA provider timestamps and 90 minutes for GBFS information, with a five-minute clock-skew allowance. New/unmatched bike stations remain visible. `normal_service_verified` and historical-operation flags remain false. Zero matched advisories means zero resolved matches in that observed snapshot; it never means normal service or zero departures.

Derived availability is the greatest of observation time, reference availability and completed batch persistence. Backfilled raw observations retain their earlier source timestamps but do not pretend the derived table existed then. A forecast consumer must enforce both publication availability and the age of the underlying observation:

```sql
SELECT *
FROM workspace.citibike_3h.station_subway_alert_features_available
WHERE available_from_utc < :input_cutoff_utc
  AND observed_at_utc <= :input_cutoff_utc
  AND observed_at_utc >= :input_cutoff_utc - INTERVAL 90 MINUTES
  AND feed_fresh AND gbfs_info_fresh AND mapping_compatible
  AND unresolved_subway_selectors = 0;
```

Select the latest eligible snapshot per bike station, radius and processing version. This hourly, sampled feed can miss short advisories and does not resolve the historical alert archive's timezone or coverage gaps.

## Observed station configuration history

The same job normalizes every collected GBFS station-information snapshot into `station_information_observations`. Rows retain string UUIDs and short-name IDs as separate schemes, names, coordinates, capacity, provider/observation timestamps and a configuration hash. They can be joined to the existing status observations by collection ID and station UUID, retaining both retrieval times. Neither an installed/renting flag at a sampled instant nor an absence from a snapshot establishes continuous operation.

The first batch contains **47,858 station observations across 19 snapshots and 2,519 distinct IDs**, and detects **seven configuration changes**. A separate audit of the saved observations confirms that all seven were capacity updates; there were no observed coordinate, short-name ID or name changes in this interval. `station_geography_changes` retains the previous name, ID and coordinates for inspection. `station_geography_observed_intervals` separates consecutive observed configurations, including a return to an earlier configuration. Its first/last observations are not asserted effective start/end dates.

`station_observation_gaps` flags more than 90 minutes between a station's observed snapshots; `prospective_source_gaps` applies the same threshold between successful source collections. Initial data contains no such interior gaps. A missing station or a collection gap is unknown coverage, not an inferred closure or an invented zero-departure interval. The source-coverage table also retains failed requests and their errors.

Publication uses append-only raw derived tables plus `prospective_publications`. Public views expose completed batches and deduplicate logical keys within each processing version. Published coverage receipts drive incremental processing. A failed partially written batch stays outside the public views; replay can publish a complete batch without changing already published logical rows. Each daily run processes up to 72 pending source inputs and reports any backlog. The first run processed 38 inputs with zero backlog. An immediate replay processed zero inputs and left all published row counts unchanged. Routine runs preserve existing view definitions; outcome pointers change only when a different prepared package is selected, avoiding unnecessary dependency invalidation during dashboard reads.

## Commands and operating controls

```sh
uv run python scripts/package_prospective_reference.py
uv run python scripts/deploy_evidence_pipeline.py deploy --include-prospective --package /absolute/GTFS/reference/build
uv run python scripts/deploy_evidence_pipeline.py status
uv run python scripts/deploy_evidence_pipeline.py schedule
uv run python scripts/verify_evidence_pipeline.py
uv run python scripts/run_tests.py
```

The reference package is under the processed RAID's `modeling_exports/three_hour/prospective_reference/20261016T153134Z-5401b8c2`. Deployment copies it and the content-hashed helper into the private input volume. Source trip archives and the canonical normalization manifest are not changed. Once the prospective task exists, redeployment requires the explicit prospective flag and reference package so it cannot silently remove that task. `schedule` enforces a successful manual run and enables daily 13:00 UTC; it does not launch another run.

Jobs use serverless environment 5. The evidence environment pins `gtfs-realtime-bindings==3.0.0`; rolling training uses the existing scikit-learn 1.7.2 environment. The daily job has one concurrent run, an 1,800-second overall timeout, task timeouts and no automatic retries. No email or external messages are sent. Pause this job independently of the hourly forecasting and daily health jobs when recurring project work stops. Class-workspace deployment, shared-catalog publication, historical station exposure and an alert-effect model remain incomplete.
