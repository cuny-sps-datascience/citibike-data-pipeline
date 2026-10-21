# Validation: 2025-03

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 4
- source rows: 3168271
- published rows: 3168271
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2025, 2, 28, 8, 43, 52, 291000), datetime.datetime(2025, 3, 31, 23, 56, 56, 495000))
- distinct start stations: 2245
- duration p50 / p99 / max / nonpositive: (485.26, 3128.92219999999, 93596.207, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2025 | 02 | 410 |
| 2025 | 03 | 3167861 |

## Quality flags

| flag | rows |
|---|---|
| crosses_dst | 195 |
| end_coords_invalid | 8441 |
| end_station_missing | 8454 |
| start_coords_invalid | 1053 |
| start_station_missing | 1053 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 447714 |
| member | member | 2720557 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 936779 |
| electric_bike | 2231492 |
