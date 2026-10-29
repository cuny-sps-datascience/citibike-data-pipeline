# Validation: 2025-02

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 3
- source rows: 2031257
- published rows: 2031257
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2025, 1, 31, 9, 26, 50, 490000), datetime.datetime(2025, 2, 28, 23, 58, 2, 884000))
- distinct start stations: 2240
- duration p50 / p99 / max / nonpositive: (437.973, 2538.83488, 89998.119, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2025 | 01 | 273 |
| 2025 | 02 | 2030984 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 4773 |
| end_station_missing | 4777 |
| start_coords_invalid | 688 |
| start_station_missing | 673 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 192759 |
| member | member | 1838498 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 614585 |
| electric_bike | 1416672 |
