# Validation: 2025-07

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 5
- source rows: 4988053
- published rows: 4988053
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2025, 6, 29, 23, 16, 34, 475000), datetime.datetime(2025, 7, 31, 23, 57, 27, 342000))
- distinct start stations: 2157
- duration p50 / p99 / max / nonpositive: (570.902, 3963.0947999999953, 90028.101, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2025 | 06 | 780 |
| 2025 | 07 | 4987273 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 20382 |
| end_station_missing | 20399 |
| start_coords_invalid | 2997 |
| start_station_missing | 2997 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 1055301 |
| member | member | 3932752 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 1457242 |
| electric_bike | 3530811 |
