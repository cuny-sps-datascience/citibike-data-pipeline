# Validation: 2025-06

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 5
- source rows: 4759345
- published rows: 4759345
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2025, 5, 30, 23, 47, 29, 703000), datetime.datetime(2025, 6, 30, 23, 58, 25, 278000))
- distinct start stations: 2157
- duration p50 / p99 / max / nonpositive: (558.388, 3715.620119999989, 877764.51, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2025 | 05 | 886 |
| 2025 | 06 | 4758459 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 16921 |
| end_station_missing | 16943 |
| start_coords_invalid | 1992 |
| start_station_missing | 1992 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 951875 |
| member | member | 3807470 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 1379366 |
| electric_bike | 3379979 |
