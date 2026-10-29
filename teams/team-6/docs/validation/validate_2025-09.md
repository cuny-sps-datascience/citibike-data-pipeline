# Validation: 2025-09

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 6
- source rows: 5287447
- published rows: 5287447
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2025, 8, 30, 23, 27, 40, 264000), datetime.datetime(2025, 9, 30, 23, 58, 26, 908000))
- distinct start stations: 2149
- duration p50 / p99 / max / nonpositive: (576.544, 3485.7401600000003, 90027.687, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2025 | 08 | 931 |
| 2025 | 09 | 5286516 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 12938 |
| end_station_missing | 12949 |
| start_coords_invalid | 2118 |
| start_station_missing | 2118 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 1001734 |
| member | member | 4285713 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 1584728 |
| electric_bike | 3702719 |
