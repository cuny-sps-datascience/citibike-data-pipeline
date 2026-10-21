# Farhod spatial-weather handoff

Team 6 requested the portable weather package from [shared-pipeline PR 47](https://github.com/cuny-sps-datascience/citibike-data-pipeline/pull/47) on October 20, 2026. The request is recorded in [the PR comment](https://github.com/cuny-sps-datascience/citibike-data-pipeline/pull/47#issuecomment-6305310216) and [the historical request receipt](validation/weather_handoff_request_2026-10-20.json). Farhod supplied the package on October 21 in commit `accfcca`; Team 6's [follow-up audit comment](https://github.com/cuny-sps-datascience/citibike-data-pipeline/pull/47#issuecomment-6307459777) records the remaining handoff issues.

The request asks for the manifest, Parquet files, checksums, station mapping and import instructions or an accessible volume path for a versioned April 1–June 30, 2026 handoff. It explicitly retains forecast horizon, fallback and missing-weather fields, request-class mapping and the assumed seven-hour availability delay. Notebook code review and data acceptance remain separate.

The package is now received and structurally validated. Its ZIP SHA-256 is `b98e6120a46abaa1d1c8db18197fae44590e42dbae66233b3cae21c9e710f4cc`. Team 6 verified 2,780,568 unique origin/horizon/class rows, 16,551 uninterrupted hourly origins, 2,519 unique station mappings, horizons 1–6 and the implied 250,151,814 station-level rows. The local RAID receipt hashes every extracted member because the contributed transfer manifest does not contain exported-file hashes.

The [station-hour integration](station_hour_integration.md) joins this package to the dated GBFS UUID/short-name crosswalk and the frozen chronological partitions. It preserves the following acceptance checks:

1. Verify the outer ZIP and every extracted file against the independent local receipt; request equivalent producer-supplied hashes before treating a later copy as identical.
2. Confirm the reported 28 request classes, 2,519 station mappings, hourly origin coverage and unique origin/horizon/class keys.
3. Reject sensitivity rows whose selected run is not strictly available before the five-minute issue cutoff.
4. Preserve missing-weather and fallback indicators rather than replacing them with actual weather.
5. Match only the frozen April–June station examples and compare spatial weather against the existing Manhattan proxy on identical keys.
6. Keep current-coordinate retrospective mapping and the seven-hour delay labeled as assumptions.

The first integration contains 13,471 spatial-weather sensitivity rows in training and 8,230 in validation, but zero in the September test. Strict as-of spatial eligibility is zero because the station mapping uses an October 18 current snapshot. These fields can support a declared retrospective sensitivity; they cannot establish a deployable common-key train/validation/test comparison. The archive also cannot supply weather for the November stockout confirmation window without a separately versioned extension.
