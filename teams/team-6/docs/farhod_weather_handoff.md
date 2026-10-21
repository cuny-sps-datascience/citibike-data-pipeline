# Farhod spatial-weather handoff

Team 6 requested the portable weather package from [shared-pipeline PR 47](https://github.com/cuny-sps-datascience/citibike-data-pipeline/pull/47) on October 20, 2026. The request is recorded in [the PR comment](https://github.com/cuny-sps-datascience/citibike-data-pipeline/pull/47#issuecomment-6305310216) and [the local receipt](validation/weather_handoff_request_2026-10-20.json).

The request asks for the manifest, Parquet files, checksums, station mapping and import instructions or an accessible volume path for a versioned April 1–June 30, 2026 handoff. It explicitly retains forecast horizon, fallback and missing-weather fields, request-class mapping and the assumed seven-hour availability delay. Notebook code review and data acceptance remain separate.

No portable artifact or access path had been supplied when this request was recorded. The spatial-weather sensitivity therefore remains pending an external handoff. Receipt of a package will not activate a model. Before evaluation, Team 6 will:

1. Verify every file against the supplied manifest and checksum.
2. Confirm the reported 28 request classes, 2,519 station mappings, hourly origin coverage and unique origin/horizon/class keys.
3. Reject any weather forecast whose initialization plus the declared delay is not strictly before the station forecast origin.
4. Preserve missing-weather and fallback indicators rather than replacing them with actual weather.
5. Match only the frozen April–June station examples and compare spatial weather against the existing Manhattan proxy on identical keys.
6. Keep current-coordinate retrospective mapping and the seven-hour delay labeled as assumptions.

The weather archive ends June 30 in the pull-request description, so it cannot supply weather for the November stockout confirmation window without a separately versioned extension.
