# Exporting Modeling Data for the Citi Bike Neighborhood Project

**Status:** Initial observed-day exporter implemented, October 11, 2026; full Census/borough pilot remains pending.
**Working directory:** `/Users/randyhowk/Sites/CUNY/citibike-data-pipeline/teams/team-6` on branch `team-6`.

Build a compact, reproducible dataset for predicting next-day departures at existing stations and describing their neighborhoods. Keep the normalized archive as the audit source. Publish separate label, feature, geography and provenance tables rather than using all normalized trip columns as predictors. The first package should cover one borough, January 2025 through August 2026 targets, and the earlier released history needed for eight-week profiles.

### Executable export now available

`uv run citibike model-export --start 2025-01-01 --end-exclusive 2026-09-01` exports available **all-station observed-day** data to the RAID. It includes published departure labels, reliable elapsed-duration alternatives, separate start-cohort arrivals, calendar features, historical counts/shares, monthly retrospective coordinate observations, checksums, a data dictionary and a Databricks import notebook. `--dry-run` verifies ledger inputs without writing; `--validate BUILD_DIRECTORY` verifies an existing package. A portable ZIP is written under `modeling_exports/handoffs/`; extract it before uploading its directory to a Unity Catalog volume.

This is a provisional handoff, not completion of all seven stages below. No verified Census/crosswalk/operating-period inputs were located. Borough assignments and zero-trip days are not fabricated. Historical departure averages and Census predictors remain null; all main-evaluation eligibility flags are false. Release availability uses a configurable, explicitly **unverified** assumption of month-end plus 45 days, not local manifest ingestion times or proven original publication dates. Historical summaries cover 56 days ending at the latest month-end available under that assumption. Before modeling, resolve operation exposure and selection bias from observed departure days, add the geography/Census inputs, and validate publication assumptions.

## Inputs and source of truth

Read `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/manifest/manifest.duckdb` in read-only mode. Select `published_files` rows with `dataset = 'trips'`, joined to sources with `status = 'selected'`. Resolve their paths relative to the processed root. Do not glob the archive, read retired output files, or concatenate retained trips with duplicate-removal records.

The ledger checked on October 11 lists 1,145 published trip files containing 323,817,126 rows, plus three removal files containing 512 rows. Source releases run from June 2013 through August 2026. These are ledger observations; a modeling build must also verify that its selected files exist and reconcile its own scanned counts.

Use the manifest to preserve which release supplied each row. Trip partitions follow the start timestamp, so a March partition can contain trips supplied by an April release. Publication eligibility must follow the supplying release rather than assuming the partition month proves availability.

The illustrative Parquet excerpts in the independent [public sample repository](https://github.com/Vaene/CUNY622FinalProjectPublic) are not input for training or estimating threshold impact. Modern `source_duration_seconds` is intentionally null; calculated `duration_seconds` remains available when both timestamps parse. Legacy and modern station IDs are different schemes and must not be equated.

## Export layers and columns

| Layer | Grain | Role |
|---|---|---|
| Optional thin trips | One retained eligible endpoint record | Bounded development and duration-policy inspection |
| Station-day labels | Station, identifier scheme, local date and label version | Observed targets and threshold variants |
| Historical profiles | Station, weekday, forecast cutoff and profile version | Released historical averages and shares |
| Neighborhood and station geography | Tract/vintage and effective-dated station mapping | Census predictors, map labels and join quality |
| Forecast features | Station, forecast origin and next-day target date | Predictors actually available at the origin |
| Build manifest and audit tables | Build/source/policy identifiers | Reconciliation and reproducibility |

**Optional thin trip export.** Include `started_at_local`, `ended_at_local`, `start_station_id`, `end_station_id`, `duration_seconds`, `rideable_type`, `membership_group`, `eligible_departure`, `eligible_arrival`, and the quality flags needed for duration/coordinate review. Add identifier scheme and a compact release key for availability. Carry a trip key only if the consumer needs traceability. This export is optional and should be limited to the pilot; the normal cloud handoff uses aggregates.

Omit repeated station names, pipeline run IDs, source paths/rows, normalization versions, original membership labels, and modern all-null source-duration values from the prediction input. Preserve those fields in the archive and build audit. Keep station names and validated coordinates in geography tables. Before omitting source duration from any legacy review extract, retain it in the duration-audit sidecar so timestamp rounding and source-duration disagreements remain inspectable.

**Station-day labels.** Key by `station_id`, `id_scheme`, `service_date`, `label_version`. Keep `departures_published`, `departures_ge30`, `departures_ge60`, `departures_ge120`, separate arrivals, and baseline morning/evening departure counts. Include counts with missing duration, nonpositive duration, and DST-related flags. Include source coverage and station-operation evidence. The selected target is a documented label version; it must not silently replace the baseline counts.

The `ge` variants use a documented trustworthy-duration rule and include the exact boundary, such as 60 seconds. Unknown or unreliable durations stay in separate counts; they do not become zero-second trips. If morning/evening targets are later filtered, add explicitly versioned counts under that same rule. These descriptive counts belong to labels/reports, not contemporaneous predictors.

**Forecast features.** Key by station, identifier scheme, `forecast_origin_utc`, and NYC `target_date`. Include weekday, month/season and holiday indicators; released eight-week weekday departure averages and pooled fallbacks; historical member/casual and bike-type shares; and Census population density, renter share, no-vehicle household share and median household income. Carry `profile_as_of`, latest included release, geography vintage, missingness flags, and the model-feature version. Capacity is optional and requires effective-dated history.

Exclude target-day actual departures, arrival totals, durations, destinations, member/bike-type composition and quality outcomes from predictor columns. Those may be labels, reports, or historical aggregates once their releases are available. Do not use a full station-ID dummy matrix in the primary Poisson Census comparison; tract-level features that are constant within station are confounded with those indicators. Retain station ID as a join/scoring key.

**Audit manifest.** Store selected input paths/source keys, source and schema versions, row counts, input file fingerprints, code revision or source hash, package checksums, chosen date/borough filters, time definitions, duration policy, release-availability evidence, feature definitions, and validation results. This small sidecar keeps pipeline bookkeeping out of the model matrix without discarding lineage.

## Duration threshold decision

The normalization pipeline currently retains short trips and flags nonpositive durations; departure eligibility requires a valid start timestamp and station. It does not add a 60-second threshold. [Citi Bike's documentation](https://citibikenyc.com/system-data) describes provider-side exclusion of trips shorter than 60 seconds. Actual records must be checked before assuming every release follows that statement.

Run the implemented audit from the canonical root:

```sh
uv run python scripts/audit_model_inputs.py \
  --start 2025-01-01 --end-exclusive 2026-09-01 \
  --output docs/validation/model_input_audit_2026-10-11.json
```

The audit reports monthly published and eligible-departure counts; missing/nonpositive durations; positive durations under 30, 60 and 120 seconds; DST-crossing counts; and under-60 counts without DST ambiguity/nonexistent-hour flags. Its values are raw wall-clock-duration diagnostics, not an approved cleaned-label definition.

The [October 11 audit](validation/model_input_audit_2026-10-11.md) covered 75,931,061 rows and 75,893,264 eligible departures. It found four positive durations below 60 seconds, 482 nonpositive durations, and 2,150,766 positive durations below 120 seconds (2.8339% of eligible departures). One under-60 trip has a DST-related flag. The counts support reviewing duration reliability and threshold effects rather than assuming the provider left a large volume of under-60 false starts.

Before choosing a policy:

1. Inspect reliable elapsed duration. Modern timestamps have no timezone offset and cannot always identify repeated-hour instants uniquely. Route ambiguous/nonexistent timestamps to an uncertainty bucket; correct offset changes only where elapsed time can be established. Review legacy source duration separately from rounded timestamp differences.
2. Compare baseline and each cutoff by month, station, same/different destination, bike type, membership and commute window. Preserve missing destinations so they do not erase otherwise valid departures.
3. Report the proportion removed and changes to station/tract rankings. Inspect a stratified set of short trips; duration alone does not prove a false start.
4. Compare baseline forecast skill within each label definition on the same station-day cohort. Lower MAE after reducing target counts is not, by itself, evidence that the cutoff is better.
5. Select and version the rule with the team. Record whether it measures all recorded departures or departures meeting the chosen completed-ride definition. Build labels and historical features with consistent policies.

Until that review, retain published departure counts as the reference and export alternatives for comparison. Do not rewrite or renormalize the archive to apply the eventual modeling policy.

## Station geography and zero-trip days

Obtain an effective-dated station identity/location table and operating evidence from class data where available. Assign stations to matching-vintage census tracts. Store unmatched, relocated and uncertain identities explicitly; current station coordinates are not proof of a historical location.

Create a date grid only for periods with documented station operation and complete source coverage. A missing trip row can then be a zero departure count. With no operating history, first-to-last observed trips can support an explicitly uncertain activity interval, but do not prove uninterrupted operation. Keep such inferred rows distinguishable and exclude unsupported zeros from the main evaluation. Establish the operating-period input before claiming forecasts cover every active station-day.

Publish the Census profile with GEOID, vintage, estimates, margins of error, underlying denominators, derived shares, land-area definition and retrieval/release metadata. Special unavailable Census values become missing with flags. Check that crosswalk joins do not multiply station-day rows. Workplace data remains an extension.

## Publication cutoffs and chronological evaluation

For each origin, include only source releases available before that origin and trips whose relevant observation times precede it. The manifest's `published_at` records our local ingestion, not Citi Bike's original release date. It cannot establish historical forecast availability.

Build a separate release-availability table using preserved provider evidence. A current bucket object's last-modified time may represent a revision rather than first publication; document that distinction. Where exact historical publication dates are unavailable, use a conservative documented assumption and run a sensitivity check. Report reconstructed forecasts as backtests, not predictions originally saved live.

Start with the proposal's January–August 2025 training period, September–December 2025 validation, and January–August 2026 evaluation, adjusting each origin to matured labels. January forecasts cannot use December outcomes or tuned parameters selected from them before those outcomes are published. Freeze preprocessing and parameters using only available training/validation labels, or document a later first evaluable origin.

For each cutoff, average the latest eight weeks of eligible released history by station/weekday, with documented exposure denominators, sample counts and pooled fallback. Include supported zero-trip days in those denominators. Never divide only by days that had trips. Keep uncertainty and unknown bike/member categories explicit.

## Package publication and size controls

Write generated packages to:

```text
/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/
  modeling_exports/
    builds/<build_id>/
      labels/station_day_labels/*.parquet
      features/forecast_features/*.parquet
      dimensions/nyc_tract_neighborhood_profile.parquet
      dimensions/station_tract_crosswalk.parquet
      audit/source_release_availability.parquet
      audit/duration_policy_impact.parquet
      manifest.json
      data_dictionary.md
      validation.json
    CURRENT
```

Build into a unique staging directory, validate all outputs, then publish the build and atomically replace `CURRENT`. Preserve previous builds. Resume interrupted work from explicit completed partitions; do not append partial data to the current package. Output artifacts must carry the same build and feature versions across all four development accounts.

Use Parquet with a consistent compression setting, measured in the pilot. Partition larger label/feature datasets by month and borough where those dimensions are verified, targeting files on the order of tens of MB rather than one file per station/day. Read only needed columns. Keep training data separate from label/audit columns in the API that assembles the feature matrix.

Target a one-borough cloud handoff below 500 MB, then measure all-borough bytes, row counts and training-memory expansion before scaling. This is a project budget, not a Databricks storage quota. Upload compact files to Unity Catalog volumes and create managed Delta tables following the [Free Edition execution plan](databricks_free_execution_plan.md). Publish the documented Census table into the class shared catalog through authorized access, and verify another team can query it.

## Implementation sequence and acceptance checks

| Stage | Proposed implementation | Acceptance check |
|---|---|---|
| 1 | Read-only ledger selector and build manifest | Every input is selected/published; missing or stale inputs fail explicitly |
| 2 | Duration audit and trustworthy-duration policy | Threshold totals reconcile; missing, invalid and DST cases remain visible |
| 3 | Station-day label aggregation and operation grid | Departures/arrivals reconcile independently; supported zeros and unknown operation differ |
| 4 | Release-availability and historical profile builder | No source after the cutoff; eight-week denominators include supported zeros |
| 5 | Census profile and effective-dated crosswalk | Unique geography keys; no duplicated station-days; uncertainty retained |
| 6 | Feature assembly and package publication | Predictor allowlist excludes target/audit outcomes; output checksums and counts verify |
| 7 | Borough pilot and Databricks import | Baseline/Poisson score the same holdout keys; another team queries Census table |

The initial `citibike model-export` command supports explicit dates, publication-lag assumptions, dry-run and package validation. Verified borough filters, operating-period grids, Census integration and trained-model comparison remain future extensions. The current `observed-station-day-v1` package must not be represented as the completed proposal pilot.

Meaningful tests should cover a missing-duration departure, exact threshold boundaries, a missing destination, a DST-ambiguous trip, a source released after the forecast cutoff, a true covered zero-trip day, unknown operation, a relocated station, and a geography join that would duplicate labels. Verify deterministic reruns and that an interrupted build cannot become `CURRENT`. The real pilot should record runtime, peak memory, output bytes, row counts and reconciliation results before publishing.

Use read-only manifest connections, at most four DuckDB threads and a 3 GB memory cap for export development, with spill on the RAID. The current broader normalization configuration need not change. Full normalization and cloud deployment are separate actions from this documented export plan.
