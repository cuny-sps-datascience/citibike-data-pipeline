# Farhod spatial-weather handoff

Team 6 requested the portable weather package from [shared-pipeline PR 47](https://github.com/cuny-sps-datascience/citibike-data-pipeline/pull/47) on October 20, 2026. The request is recorded in [the PR comment](https://github.com/cuny-sps-datascience/citibike-data-pipeline/pull/47#issuecomment-6305310216) and [the historical request receipt](validation/weather_handoff_request_2026-10-20.json). Farhod supplied the package on October 21 in commit `accfcca`; Team 6's [follow-up audit comment](https://github.com/cuny-sps-datascience/citibike-data-pipeline/pull/47#issuecomment-6307459777) records the remaining handoff issues.

The request asks for the manifest, Parquet files, checksums, station mapping and import instructions or an accessible volume path for a versioned April 1–June 30, 2026 handoff. It explicitly retains forecast horizon, fallback and missing-weather fields, request-class mapping and the assumed seven-hour availability delay. Notebook code review and data acceptance remain separate.

The original package was received and structurally validated on October 21. Its subsequent Databricks publication is recorded in [PR 58’s handoff receipt](https://github.com/cuny-sps-datascience/citibike-data-pipeline/blob/team-6-hourly-integration/teams/team-6/docs/validation/databricks_team_handoff_2026-10-21.json). Its ZIP SHA-256 is `b98e6120a46abaa1d1c8db18197fae44590e42dbae66233b3cae21c9e710f4cc`. Team 6 verified 2,780,568 unique origin/horizon/class rows, 16,551 uninterrupted hourly origins, 2,519 unique station mappings, horizons 1–6 and the implied 250,151,814 station-level rows. The local RAID receipt hashes every extracted member because the contributed transfer manifest does not contain exported-file hashes.

The [station-hour integration](station_hour_integration.md) joins this package to the dated GBFS UUID/short-name crosswalk and the frozen chronological partitions. It preserves the following acceptance checks:

1. Verify the outer ZIP and every extracted file against the independent local receipt; request equivalent producer-supplied hashes before treating a later copy as identical.
2. Confirm the reported 28 request classes, 2,519 station mappings, hourly origin coverage and unique origin/horizon/class keys.
3. Reject sensitivity rows whose selected run is not strictly available before the five-minute issue cutoff.
4. Preserve missing-weather and fallback indicators rather than replacing them with actual weather.
5. Match only the frozen April–June station examples and compare spatial weather against the existing Manhattan proxy on identical keys.
6. Keep current-coordinate retrospective mapping and the seven-hour delay labeled as assumptions.

The first integration contains 13,471 spatial-weather sensitivity rows in training and 8,230 in validation, but zero in the September test. Strict as-of spatial eligibility is zero because the station mapping uses an October 18 current snapshot. These fields can support a declared retrospective sensitivity; they cannot establish a deployable common-key train/validation/test comparison. The archive also cannot supply weather for the November stockout confirmation window without a separately versioned extension.

## October 23 weather/subway release

**Status reviewed October 26, 2026:** Farhod published a newer [five-table release](https://github.com/cuny-sps-datascience/citibike-data-pipeline/releases/tag/team-6-weather-subway-2026-10-23). Its [dataset documentation](weather_subway_handoff.md) preserves the table names, schemas, import instructions, limitations and common March 1, 2025–August 10, 2026 New York modeling dates. This is a separate version from the October 21 integration above.

The release asset is `citibike_weather_subway_handoff_20261023T174914038037Z.zip` (72,443,437 bytes). GitHub reports SHA-256 `d750b7cc991d28c638a5bbfd44c4435d3de1666dec6ca92ad826ecffdb47ff41`, matching the producer's documented hash. This documentation update verifies the release metadata; it does not claim a new recipient-side download, member-checksum audit, import or modeling join.

The newer weather archive extends through the final August 2026 local origins, and the subway origin-feature archive ends August 10 local time. It addresses the earlier package's narrower weather coverage but supplies no full September or November forecast window. The October 18 retrospective station geography, assumed seven-hour weather delay, archive silence, unmapped subway docks and publication-time semantics remain explicit limitations. Retrospective measured disruption exposure stays separate from forecasting predictors. A later integration must verify source timestamps against its actual issue cutoff and retain only origins whose three target hours remain within the study and split boundaries.

The frozen November 1–30 stockout confirmation, cohort, model bundle, freshness rules and December 1 earliest evaluation remain unchanged. Neither handoff activates or retrains a model or changes a schedule.
