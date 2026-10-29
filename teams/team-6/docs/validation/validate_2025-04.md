# Validation: 2025-04

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 4
- source rows: 3724596
- published rows: 3724596
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2025, 3, 31, 2, 22, 50, 612000), datetime.datetime(2025, 4, 30, 23, 58, 20, 15000))
- distinct start stations: 2242
- duration p50 / p99 / max / nonpositive: (515.038, 3476.9964999999984, 90002.363, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2025 | 03 | 101 |
| 2025 | 04 | 3724495 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 9688 |
| end_station_missing | 9704 |
| start_coords_invalid | 1394 |
| start_station_missing | 1394 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 615368 |
| member | member | 3109228 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 1139159 |
| electric_bike | 2585437 |
