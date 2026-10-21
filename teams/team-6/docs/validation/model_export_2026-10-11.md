# Modeling export — October 11, 2026

Build `20261011T144948Z-0aa84cfe` is published locally and selected by the RAID's `modeling_exports/CURRENT` pointer.
The ZIP is **46.16 MB**, below the project's 500 MB budget. This is a measured
package size, not an assertion that Databricks publishes a 500 MB storage quota.

## Files and size

- Build: `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/builds/20261011T144948Z-0aa84cfe`.
- Portable ZIP: `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/handoffs/20261011T144948Z-0aa84cfe.zip`.
- Targets: January 2025 through August 2026; additional history starts 2024-08-01.
- Scope: all observed stations; borough assignment is unverified.
- 1,290,282 label rows and the same number of feature rows.
- 45 ZSTD Parquet files, 46.01 MB combined.
- Largest Parquet file: 21.07 MB.
- Full feature table loaded into Arrow: 260.53 MB.
- Eight calendar/history candidate columns in Arrow: 58.06 MB.
- Export processing through file writing: 307.58 seconds,
  excluding subsequent input fingerprints and ZIP creation.

Arrow bytes describe table buffers, not a training process's peak memory. Preprocessing and
model fitting have not been measured. The export runs locally with a 3 GB DuckDB memory cap
and four threads; cloud uploads contain compact outputs rather than the 3,511.90 MB
of selected trip Parquet inputs or temporary spill files.

## Contents and verification

The package contains monthly departure labels and features, separate arrivals limited to the selected
trip-start cohort, retrospective coordinate observations, release assumptions, duration sensitivity,
release-level station-day history, input/output fingerprints, data dictionary, validation report and
`databricks_import.py`. Published departures remain the reference; no duration threshold is selected.
All count/key/duration/publication-cutoff checks pass, the CLI package validator passes, and ZIP CRC
and file counts pass. The full test suite passes: 184 tests. Monthly historical cohorts are computed once
and reused across forecast dates; a reference test verifies equivalence to explicit per-origin filtering.

Duration totals: 75,893,264 published departures; 75,888,433 at either the reliable 30- or 60-second
cutoff; 73,737,803 at 120 seconds; 4,828 departures with uncertain duration. The difference between
published and threshold counts includes uncertain durations, not only short rides. Those uncertainties
remain in the published baseline; duration alone does not establish a false start.

## Databricks handoff

Extract the ZIP locally and upload the build directory to a Unity Catalog volume, preserving its
subdirectories. Import `databricks_import.py`, then set `export_root` to the uploaded build directory
and `target_schema` to an existing writable catalog/schema. It creates new managed Delta tables;
existing names cause an error rather than replacement. Nothing has been uploaded to Databricks yet.
The package fits our cloud file-size budget in one account. See
[the Free Edition plan](../databricks_free_execution_plan.md) for platform limits and the class-catalog requirement.

## Remaining proposal requirements

This is a **provisional observed-day handoff**, not the completed seven-stage Census/borough pilot.
No zero-trip station-days are invented. All `main_evaluation_eligible` flags are false. Historical
rate averages and Census predictors are null. The month-end-plus-45-days publication schedule is an
explicit, unverified backtest assumption. Retrospective coordinate means are not historical station locations.

Required next inputs: verified Census profiles, an effective-dated station-to-tract crosswalk,
historical station operation/coverage, and historical release evidence. Until those are resolved,
positive observed-day selection and unknown exposure prevent a defensible unconditional demand evaluation.
No model has been trained and no class shared-Census table has been published.

Machine-readable measurements: [model_export_2026-10-11.json](model_export_2026-10-11.json).
