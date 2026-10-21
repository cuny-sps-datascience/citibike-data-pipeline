# Validation: 2025-08

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 6
- source rows: 5121533
- published rows: 5121533
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2025, 7, 30, 23, 0, 23, 818000), datetime.datetime(2025, 8, 31, 23, 58, 17, 885000))
- distinct start stations: 2144
- duration p50 / p99 / max / nonpositive: (594.71, 4144.970119999997, 90026.04, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2025 | 07 | 290 |
| 2025 | 08 | 5121243 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 12845 |
| end_station_missing | 12856 |
| start_coords_invalid | 2112 |
| start_station_missing | 2112 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 1154291 |
| member | member | 3967242 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 1539141 |
| electric_bike | 3582392 |
