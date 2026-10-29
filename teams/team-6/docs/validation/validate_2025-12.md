# Validation: 2025-12

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 3
- source rows: 2091620
- published rows: 2091620
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2025, 11, 30, 15, 46, 22, 694000), datetime.datetime(2025, 12, 31, 23, 57, 28, 999000))
- distinct start stations: 2193
- duration p50 / p99 / max / nonpositive: (452.037, 2686.7182100000023, 89998.185, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2025 | 11 | 137 |
| 2025 | 12 | 2091483 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 5461 |
| end_station_missing | 5449 |
| start_coords_invalid | 1215 |
| start_station_missing | 1190 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 218988 |
| member | member | 1872632 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 572656 |
| electric_bike | 1518964 |
