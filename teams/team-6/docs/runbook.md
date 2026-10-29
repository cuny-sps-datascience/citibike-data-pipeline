# Runbook: local Citi Bike pipeline

All commands run from `~/Sites/CUNY/citibike-data-pipeline/teams/team-6` on the `team-6` branch. The RAID must be mounted.

For the active three-hour proposal, read [execution status](three_hour_execution_status.md) and the [public-only forecasting runbook](public_only_mvp.md). Use `uv run python scripts/manage_three_hour_workspace.py status` to inspect jobs and `pause-collector` to pause the combined hourly collection/scoring job while preserving data. Manhattan-total forecasts are trained and deployed; the original station-level scope remains gated. Use `deploy_public_mvp.py activate` for both tasks: redeploying only the older collector replaces the task list.

The separate [daily health job](daily_forecast_health.md) runs at 13:15 UTC and must be paused independently when stopping recurring work. The dashboard shows its latest status and the [prospective station/subway mapping](station_subway_mapping.md). [Presentation slides and demo notes](public_mvp_demo.md) are ready for the midterm.

The [daily public evidence job](release_aware_evidence_pipeline.md) runs at 13:00 UTC: public trip listing, import/evaluation of staged validated outcomes, and incremental normalization of hourly station/MTA snapshots. It has its own pause control. A newly listed ZIP remains a candidate until local ingestion and full label validation; stage the resulting export with `deploy_evidence_pipeline.py stage-outcomes --package /absolute/validated/label/build`. Monthly release-aware backtests are manual and do not activate a new forecast model.

The separate [availability dashboard](availability_mvp.md) is notebook `22_availability_dashboard` in `DATA622_ThreeHour`. Use **Run all** to refresh fresh GBFS counts and score its frozen inventory model; choose a station and horizon for interval charts. This is manual and does not retrain or change schedules. Its model is `/Volumes/workspace/citibike_3h/exports/availability_pilots/20261016T205658Z-53093e33`; source snapshots/publication receipts live in `exports/availability_live/<refresh-version>`. Check the saved `report.json` when evaluating publication timing. Unknown/offline/stale readings stay distinct from zero inventory. Commands and evidence are in the availability report; do not use the borough activation command for this model.

See [the modeling export plan](modeling_export_plan.md) for the neighborhood forecasting dataset. The initial observed-day `model-export` command is implemented; `aggregate` and `publish-plan` remain placeholders.

The [ensemble result](ensemble_results.md) and [incorporation plan](ensemble_incorporation_plan.md) record the completed development comparisons. Versioned station and availability packages/results are under the processed RAID's `modeling_exports/three_hour/*_ensemble_packages` and `*_ensemble_results` directories. Reproduce package derivation with `build_ensemble_package.py`, run/audit locally with `run_ensemble_experiment.py` and `audit_ensemble_experiment.py`, and submit/fetch with `deploy_ensemble_experiment.py` and `fetch_ensemble_result.py`. These commands do not activate models, overwrite stable tables or change schedules. Keep departure counts, inventory counts, risk-model versions and horizon routing distinct.

## Export a compact modeling handoff

```sh
uv run citibike model-export --dry-run
uv run citibike model-export --start 2025-01-01 --end-exclusive 2026-09-01
uv run citibike model-export --validate /absolute/path/to/build
```

Outputs live under the processed RAID root's `modeling_exports/builds/BUILD_ID`; `modeling_exports/CURRENT` identifies the most recently validated build. The corresponding portable ZIP is in `modeling_exports/handoffs/BUILD_ID.zip`. Failed or interrupted staging directories never become `CURRENT`; previous builds are preserved. Start a fresh build after interruption; partition-level resume is not yet implemented.

Extract the ZIP locally, upload the directory to a Unity Catalog volume, then import `databricks_import.py` as a notebook. Set its `export_root` and `target_schema` widgets. It creates new Delta tables and fails if names already exist; use a new schema per build or deliberately choose different table names. Upload/publication is a separate action and requires actual workspace access.

The first export includes all observed stations, not a verified borough. Read `data_dictionary.md` and `validation.json`: historical operation/zero-day coverage, verified source publication dates, Census profiles and an effective-dated tract crosswalk are still missing. Departure means and Census predictors stay null. All `main_evaluation_eligible` flags remain false. The default 45-day release lag is an explicit reconstruction assumption, not evidence. Do not score only observed positive station-days and claim unconditional station demand accuracy.

- Source (read-only): `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject`
- Outputs: `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed` (written `$P` below)

Only one `citibike` command can run at a time: the manifest is a DuckDB file with a single writer.

## Setup

    uv sync
    uv run pytest -q
    uv run citibike doctor

## Inventory (resumable; rerun any step after an interruption)

    uv run citibike inventory discover
    uv run citibike inventory profile --all --workers 1
    curl -fsS https://s3.amazonaws.com/tripdata/ -o "$P/reports/tripdata_listing.xml"   # optional, read-only
    uv run citibike inventory report
    uv run citibike inventory select --release YYYY-MM [--release ...] [--dry-run]
    uv run citibike inventory select --release YYYY-MM --set "YYYY-MM=<set_id>"         # only for a reviewed conflict

Use `--workers 1` on this RAID. Four workers made every worker wait on disk seeks (about 4 MB/s each);
one sequential worker profiled the remaining 48 GB in 26 minutes.

`--release` accepts `YYYY` (all twelve months) or `YYYY-MM`.

## Normalize and validate

    uv run citibike normalize --release YYYY-MM|YYYY [--dry-run]
    uv run citibike validate --release YYYY-MM|YYYY

New monthly releases go into the separate configured raw-download archive, preserving original sources read-only. The executed [September refresh](september_refresh.md) documents the full path from download to validated outcomes and a separate weather contribution. `inventory discover --additional-only` scans all configured additional roots and preserves primary archive inventory; it detects absent external inputs without rebuilding old nested ZIP caches. Every configured scanned root must be mounted. Discovery failure leaves existing missing-source status unchanged.

```sh
uv run python scripts/download_trip_release.py --month 2026-09
uv run citibike inventory discover --additional-only
uv run citibike inventory profile --release 2026-09 --workers 1
uv run citibike inventory select --release 2026-09
uv run citibike normalize --release 2026-09
uv run citibike validate --release 2026-08 --release 2026-09
```

Run these sequentially, with one manifest writer. Refresh the complete desired hourly export and Manhattan labels after validation; a new release can add earlier-month spillovers. Stage only the validated version-2 label package using `deploy_evidence_pipeline.py stage-outcomes`. The daily importer then updates released actuals. This changes neither active model artifacts nor original benchmark metrics.

To extend weather, request the **entire accumulated window**, allowing the collector to reuse cached responses. A September-only request to the same root would publish a manifest containing only September. Existing acquisition manifests are archived by hash. Normalize a separate weather contribution to preserve frozen feature/model packages:

```sh
uv run python scripts/collect_public_history.py weather --first 2025-02-28 --last-exclusive 2026-10-01
uv run python scripts/normalize_weather_history.py
uv run python scripts/package_weather_contribution.py --package /absolute/weather_reference_builds/BUILD --destination /absolute/handoffs
uv run python scripts/deploy_public_mvp.py weather --package /absolute/handoffs/weather_reference_BUILD
```

The last command replaces the stable private Delta table and records the immutable handoff name in `dataset_version`. The versioned volume package and original RAID handoff remain unchanged. Follow the [catalog retention policy](catalog_retention_policy.md), update the datasheet before packaging, and retain original handoffs. Class publication and other-team readability remain separate delivery steps. Rebuild presentation artifacts with `build_mvp_slides.py` and `render_whitepaper.py`; the [timed script](midterm_demo_script.md) describes a technical dry run and Randy's spoken practice.

`normalize` refuses a release whose selected sources are not exactly one complete source set.
It processes releases in chronological order (then by source key), because a modern source removes a
trip that the release of the trip's start month already published identically (see Deduplication in
`docs/data_dictionary.md`): normalize 2026-04 and 2026-05 in one run, or 2026-04 first. Removed rows go
to `$P/removed/` and are listed in the ledger as dataset `removed`; each source's output line shows
`removed=N conflicts=N` (conflicts are differing copies, kept). If the earlier release fails in a
combined run, fix it and rerun both releases: the later one compared against whatever the earlier one
had published, so it must be normalized again after the earlier one succeeds. Likewise, after
re-selecting or re-normalizing a release, re-normalize the following release too.
Reruns are safe: each run publishes new files, commits the ledger, then moves superseded files to
`retired/<run_id>/`. An interrupted run is rolled back to the last committed ledger at the start of
the next run. When a release's selection changed, the sources no longer selected are unpublished
only after every newly selected source of that release published; if one fails, the release keeps
its previous publication (and `validate` reports it as stale until a rerun succeeds).

Run one `citibike` command at a time. DuckDB holds an exclusive lock on the manifest file, so a
second process that opens it fails with `duckdb.IOException` (could not set lock on file); there is
no other concurrency control.

`validate` passes only when source rows equal published plus quarantined plus removed rows (the
removed rows in the ledger), in total and for every source, every removed row's kept copy is still
published by the release named in its reason (`orphaned removals` must be 0; the report lists sample
ride_ids), the selection is one complete set, every trip file has the same schema,
and there are no unlisted files under `trips/`, `quarantine/` or `removed/` and no ledger entries for
sources that are no longer selected. Its report also lists each source's profiled, published,
quarantined and removed rows, and requires the quarantined rows to equal the profile's malformed plus
invalid-start rows for every source. `source_runs.removed_rows` is NULL for runs recorded before
schema_version 2; read it as 0.

Measured on 2025 (53 sources, 45,771,102 rows): normalize 7.5 min, peak RSS 1.8 GB, no DuckDB spill.
Validating the full year takes a few minutes. The whole archive (159 releases, 333 sources, 323.8 M rows) normalized in 55 min at 2.2 GB peak RSS.

## When something fails

- `failed` in profile: read `status_reason` in `$P/reports/inventory_report.md`; fix with a test; bump
  `parser_version` in `config/local.yaml`; rerun profile. A failed source's profile fields are cleared,
  so it cannot be selected until it profiles again.
- `conflict` in select: compare the copies record by record (see the 2018-04 entry in
  `docs/implementation_status.md`), record the decision there, then use `--set`.
- `incomplete_parts` in select: a part is missing or the part numbers have a gap; investigate before
  using `--set`.
- `quarantine share` in normalize: a failed source's staging dir is moved to
  `$P/retired/<run_id>/staging/`; inspect `quarantine.parquet` there. Fix the parser or record a reason
  before raising `max_quarantine_fraction`.
- `header differs` or `profiling saw`: the source changed since inventory; rerun discover and profile.
- `selection` problems in normalize or validate: rerun `inventory select` for that release.

## Housekeeping (manual, user decision)

`retired/`, `staging/<run_id>/`, `staging/nested/` (about 20 GB) and `tmp/` are never deleted by the
pipeline. Check `du -sh` on each and remove old ones by hand when you are satisfied with a run.

## Querying

Pin the partition types: DuckDB infers `trip_year`/`trip_month` differently depending on which
partitions a query touches.

    uv run python -c "import duckdb; print(duckdb.sql(\"SELECT trip_year, trip_month, count(*) FROM read_parquet('$P/trips/*/*/*.parquet', hive_partitioning=true, hive_types={'trip_year': INTEGER, 'trip_month': INTEGER}) GROUP BY ALL ORDER BY ALL\"))"

A glob over `trips/` is only trustworthy right after a passing `validate`; the ledger
(`published_files` in the manifest) is the authoritative list of published files.

## Half-hour station bike mix

    uv run citibike bike-mix --date 2025-04-15
    uv run citibike bike-mix --date 2025-04-15 --station 6331.01 --min-rides 20

The first command exports every station observed that day; the second selects W 31 St & 7 Ave by exact station ID. Each run prints a unique output directory under `$P/reports/bike_mix_halfhour/date=2025-04-15/`, holding `intervals.csv`, `intervals.parquet` and `station_summary.csv`. The manifest is opened read-only, and inputs come only from selected published files in the start-month partition.

Intervals include all 48 local half-hours per observed station, zero counts where no eligible departures occurred, classic/electric mix, unknown/other coverage, daily mix and percentage-point changes. Shares are null with no known-type rides. The default small-sample threshold is 20 known-type departures per interval; the summary range uses only intervals meeting that threshold. Compare counts alongside shares. An observed change does not prove a statistically significant change or identify an optimal inventory ratio.

## Known gaps

The independent October 16 [public station-history pilot](station_history_pilot.md) has its own acquisition, build, audit, cloud-submit and result-fetch commands. Inputs are pinned under `station_pilot_sources/20261016T195145Z-57b9a118`; package `station_pilot_packages/20261016T200304Z-7c2377e9`; successful cloud/local results `station_pilot_results/20261016T201856Z-d67ba148` (cloud uses `exports/station_pilots`). Run stages sequentially on the RAID. Its 32-station conditional operation policy does not promote the canonical observed export or change active borough forecasts. The exact alert ablation is gated by 122 lossy archive dates before August 7, not account authentication. Use the [saved receipt](validation/station_pilot_2026-10-16.json) for the historical execution names; future pilot imports use stable current tables with `dataset_version`. Do not activate an unvalidated station candidate through the borough deployment commands.

- `citibike validate` used to hold every trip row of a unit in memory (a parameterized DuckDB relation is
  executed eagerly), so the combined unit over about 90 releases was killed by macOS. The trips view is
  now lazy and validate uses one connection capped at min(memory_limit, 3GB) and 4 threads. The
  combined unit still scans every listed file, so one year per process stays the quicker habit:

      for y in 2014 2015 2016; do uv run citibike validate --release $y; done

  A year that includes months outside the archive (2013, 2026) reports those empty months as not reconciled; list the real months explicitly instead.
- Set IDs contain `|`. Report tables escape it as `\|` so the columns stay intact; when copying a set ID
  from the raw markdown into `--set`, drop the backslashes.
