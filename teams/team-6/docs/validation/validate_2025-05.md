# Validation: 2025-05

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 5
- source rows: 4325553
- published rows: 4325553
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2025, 4, 29, 23, 2, 37, 671000), datetime.datetime(2025, 5, 31, 23, 58, 6, 793000))
- distinct start stations: 2245
- duration p50 / p99 / max / nonpositive: (549.327, 3707.91167999998, 90048.562, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2025 | 04 | 542 |
| 2025 | 05 | 4325011 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 12706 |
| end_station_missing | 12726 |
| start_coords_invalid | 1691 |
| start_station_missing | 1691 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 841628 |
| member | member | 3483925 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 1317209 |
| electric_bike | 3008344 |
