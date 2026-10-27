# Cleaned DATA622 NOAA notebooks

These are reviewed copies of the three exported notebooks. Your original Downloads files and live Databricks job have not been modified.

## What was corrected

- The exported **active six-station precipitation rule still contained `AND INSTR(message, '$') = 0`**, plus a duplicated NIL/PNO/PWINO exclusion. The cleaned version removes the blanket maintenance exclusion, keeps the specific unavailable-data exclusions once, and retains the maintenance output flag. This restores the behavior agreed in the conversation; rerunning the original export would otherwise have reverted it.
- The maintenance flag remains last in the hourly comparison struct, preserving the existing tie-break order.
- Notebook 01 no longer points the observation view back at the initial 2026 snapshot. Both setup and maintained transformations define it against `noaa_weather_raw`.
- Notebook 02 updates data only; it no longer rewrites transformation views on every scheduled run.
- Notebook 03 preserves an existing station mapping on Run All instead of silently rebuilding it using full-project-period coverage.
- Freshness validation starts from mapped stations and detects missing/disabled registry entries, missing raw observations, and an empty mapping. It no longer requires the freshness view to run.
- Added per-run JSON manifests with source URLs, HTTP metadata, update outcomes, and timestamps alongside the downloaded files. These are audit records, not original NOAA publication-time evidence.
- Added `interpreted_precip_unknown` to distinguish unknown interpreted precipitation from the older `precipitation_missing` column, which refers to the original rule.
- Added notebook 04 for read-only validation. Superseded experimental view-creation cells are not in the maintained workflow. Existing experimental database objects are not deleted.
- All exported execution outputs, execution counts, and Databricks-specific notebook metadata were removed from the cleaned copies.

## Import into your existing workspace

1. Keep the original notebooks as a rollback reference.
2. Create a separate folder such as `teams/<your-team>/weather_cleaned` in Databricks. Import the four `.ipynb` files there. Do not replace the scheduled task immediately.
3. **Skip 01 for your already-initialized workspace.** Run cleaned **03** to apply the corrected maintained view definitions. It preserves the existing mapping. These views use your existing `workspace.default` tables; the separate notebook folder does not isolate the database objects.
4. Run cleaned **04**. Review duplicate checks, missing-hour counts, unmapped IDs, and the sample join. Missing hours and unmapped IDs are reported for investigation rather than filled with invented observations.
5. Test cleaned **02** manually, ideally as a temporary unscheduled job using the same compute and run-as permissions as the existing job. Run one updater at a time and avoid overlap with the current scheduled job. A source older than the configured threshold can correctly fail the freshness check even when the code runs correctly.
6. Once validated, edit the existing **Daily NOAA weather update** job's notebook task to point to cleaned 02. Keep a single active schedule rather than creating a second daily updater.
7. Verify the task path, daily trigger, time zone, maximum concurrent runs of 1, and failure notification destination. Run the changed job once and inspect its results.

Importing a notebook does not redirect an existing job automatically. No schedule or permissions were changed by this cleanup.

## Fresh workspace setup

First use the professor's Free Edition download notebook to create the source bike tables and volume. Then run **01 → 03 → 02 → 04**. Setup 01 creates the bootstrap archive, main raw table, candidate mappings, registry, and observation view. Notebook 03 creates the provisional final mapping and remaining views. Only 02 is intended for daily scheduling.

01 preserves existing bootstrap tables on reruns but still performs network downloads. It is not a station-registry refresh workflow. Changes in bike coverage or weather-station eligibility need deliberate review/backfill.

## Important remaining limits

- This is an initial historical weather pipeline, not a validated real-time prediction feature service. The source was approximately 60 hours behind during the walkthrough.
- `snapshot_loaded_at_utc` is a replacement/load timestamp, not persistent first-seen time. Retained run files/manifests support future audits but do not reconstruct past availability automatically.
- Source-specific quality screening, same-time conflicting reports, and precipitation interval interpretation still need evaluation. The cleanup deliberately preserves the agreed selection behavior rather than silently changing the science.
- The six-station precipitation inference is bounded by station/message checks. Nonmatching formats remain unknown. An inferred zero is labeled as an inference; maintenance flags remain separate.
- Precipitation is a selected report accumulation, not a total for the clock hour. Unknown and trace are distinct from an explicit or inferred zero.
- Current and previous years are refreshed, starting at 2026. Older corrections and newly selected stations' historical backfills require explicit runs. New-year files may initially return 404 and cause the job to fail for review.
- Full raw files are retained for every run. No retention cleanup is installed.
- A freshness test inside a job cannot alert if the job never starts. An independent job-status monitor is not included.
- Team/shared-catalog access and actual schedule activation must be verified in Databricks.

## Validation performed locally

The notebook JSON and Python cells were checked for syntax; active-rule fixes, view targets, mapping preservation, dependency order, output removal, and updater guards were checked structurally. See `VALIDATION.json` for counts and checks.

**No Spark SQL or live Databricks execution was performed on these cleaned files.** The original versions were demonstrated in the conversation, but the cleaned copies need the manual/job validation above.

The original walkthrough runbook is included for project context. Where it describes a workflow changed by this cleanup (such as transformation DDL in 02), this README and the cleaned notebooks take precedence.
