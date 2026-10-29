# Manhattan departure labels and model features datasheet

Randy Howk · October 16, 2026. These project derivatives support the Manhattan-total three-hour benchmark. The latest prepared label build covers August 1, 2024 through September 30, 2026 in New York time. Its exact counts, input fingerprints and cloud import are recorded in the [September refresh](september_refresh.md) and its [validation receipt](validation/september_refresh_2026-10-16.json). The active model continues to use its original immutable feature package; a new outcome export does not change the training sample or model.

## Collection and composition

Inputs are selected normalized [Citi Bike monthly trip histories](https://citibikenyc.com/system-data), read through the canonical published-file ledger. The new observed package has 169 selected input files, 78 derived Parquet artifacts and 21,167,148 observed station-hour rows across 26 months. Its 21,174,182 station-hour/release rows preserve supplying-source provenance, and 57,045 station/month geography rows contain explicitly unverified retrospective means. Bulk data stays on the RAID under `modeling_exports/three_hour`.

The borough label builder classifies each eligible trip's actual start coordinates against the checksum-pinned Manhattan boundary from the external source package. The refreshed labels contain 18,984 hours and 63,393,151 assigned Manhattan departures, including 3,049,503 September departures across 720 hours. Four repeated fall-clock hours remain null. New source spillovers revise 17 August geographic hours by 331 departures. Missing/invalid coordinates are excluded and counted. Departure and arrival eligibility differ. Historical labels use completed served trips, without estimating unmet demand. The full borough calendar supports a zero aggregate geographic count, but does not establish operation for a station with no trips.

## Label tables

| Table/file | Key | Fields and meaning |
|---|---|---|
| `borough_hourly.parquet` | `hour_utc` | UTC start, nullable integer `departures`, integer `assigned_departures`, `label_assignable`, borough and coverage description |
| `borough_hourly_release.parquet` | Input-contribution rows; aggregate by `hour_utc`, `nominal_release` | Counts from each selected input contribution, retaining the monthly supplying release |

The release file can contain several contributions for an hour and release because different input files contribute. Consumers must sum contributions rather than assume that pair is unique. The geographic-hour table is unique by hour. Four repeated fall-clock UTC hours have null labels; ambiguous start rows are separately audited. Null is not zero. Three-target origins touching an unassignable label are excluded from fitting/evaluation.

The manifest records the complete calendar, selected input records, input audit, boundary hash, output hashes, row counts and label policy. The staging validator checks calendar completeness, nonnegative integer counts, unique hours, assigned-count reconciliation, DST nulls and provenance. `CURRENT_BOROUGH_LABELS` identifies the latest local prepared build. Cloud `borough_actual_hourly` points to the latest approved imported outcomes; prepared-package changes do not retrain the active model.

## Frozen benchmark features

The original feature package remains `20261016T005315Z-35753b1b`. `features.parquet` has one row per `origin_utc`, `horizon`, with the three separate hourly target bounds, split and outcome. Its weather-complete train/validation/test rows are 28,149 / 4,386 / 6,564. Target dates run March 2025 through August 2026; the September refresh does not assign a new training or test split. The [model card](manhattan_benchmark_model_card.md) documents candidate selection and fitted transforms.

`profile_baseline` and `pooled_baseline` describe released historical rates. `profile_release` and `profile_available_at_utc` retain source-month lineage under the unverified month-end-plus-45-day assumption. Calendar cycles, weekday and horizon are known before the origin. Forecast temperature, its square, precipitation, wind and wet indicator come from a single Manhattan proxy point. `issue_cutoff_utc` is five minutes before origin; `weather_initialization_utc` plus seven hours supplies the assumed weather availability. Missing weather selects a calendar fallback and is excluded from covered weather comparisons.

`departures` is an outcome, never a predictor. Future destination, duration, membership, bike type and unavailable recent counts are excluded. `profiles.parquet` records the rate numerator, covered-hour denominator and release cutoff. The original `weather_runs.parquet` stays frozen with the benchmark; a separate extended weather contribution supplies September history without changing that artifact. See the [weather datasheet](weather_reference_datasheet.md).

## Limitations and distribution

A newly released file may add spillover trips to an earlier start month. Refreshed labels can therefore revise historical hours; original model metrics remain attached to the original immutable data snapshot. Historical provider publication times are unverified, weather archives include hindcasts, and repeated forecast targets create dependent rows. Separate monthly backtests gate fitting by supplying-source availability and retain their own keys and results.

These tables are geographic departure counts, not person-level profiles. Preserve operator attribution and applicable source terms when transferring derivatives. The original station-level proposal still requires reliable operation and effective-dated geography; alert-effect estimation requires suitable historical evidence. Private Free Edition imports do not complete class shared-catalog publication.
