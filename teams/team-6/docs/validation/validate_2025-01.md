# Validation: 2025-01

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 3
- source rows: 2124475
- published rows: 2124475
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2024, 12, 30, 23, 41, 25, 635000), datetime.datetime(2025, 1, 31, 23, 58, 14, 634000))
- distinct start stations: 2246
- duration p50 / p99 / max / nonpositive: (422.711, 2465.337039999999, 89997.26, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2024 | 12 | 207 |
| 2025 | 01 | 2124268 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 4317 |
| end_station_missing | 4322 |
| start_coords_invalid | 565 |
| start_station_missing | 564 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 201859 |
| member | member | 1922616 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 630924 |
| electric_bike | 1493551 |
