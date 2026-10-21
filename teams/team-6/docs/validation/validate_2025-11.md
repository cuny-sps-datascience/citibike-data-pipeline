# Validation: 2025-11

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 4
- source rows: 3417052
- published rows: 3417052
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2025, 10, 31, 1, 0, 18, 978000), datetime.datetime(2025, 11, 30, 23, 57, 47, 774000))
- distinct start stations: 2189
- duration p50 / p99 / max / nonpositive: (497.5925, 3050.269959999999, 89998.407, 482)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2025 | 10 | 1097 |
| 2025 | 11 | 3415955 |

## Quality flags

| flag | rows |
|---|---|
| crosses_dst | 337 |
| duration_nonpositive | 482 |
| end_coords_invalid | 8758 |
| end_dst_ambiguous | 4567 |
| end_station_missing | 8770 |
| start_coords_invalid | 1344 |
| start_dst_ambiguous | 4124 |
| start_station_missing | 1344 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 506734 |
| member | member | 2910318 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 957904 |
| electric_bike | 2459148 |
