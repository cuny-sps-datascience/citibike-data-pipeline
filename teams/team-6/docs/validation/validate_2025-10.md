# Validation: 2025-10

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 5
- source rows: 4731900
- published rows: 4731900
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2025, 9, 30, 2, 10, 54, 819000), datetime.datetime(2025, 10, 31, 23, 58, 16, 325000))
- distinct start stations: 2169
- duration p50 / p99 / max / nonpositive: (535.915, 3286.2961399999967, 92727.112, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2025 | 09 | 639 |
| 2025 | 10 | 4731261 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 12975 |
| end_station_missing | 12983 |
| start_coords_invalid | 3143 |
| start_station_missing | 3143 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 799873 |
| member | member | 3932027 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 1393806 |
| electric_bike | 3338094 |
