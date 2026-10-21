# Validation: 2026-04

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 4
- source rows: 3860371
- published rows: 3860371
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2026, 4, 1, 0, 0, 2, 969000), datetime.datetime(2026, 4, 30, 23, 59, 58, 765000))
- distinct start stations: 2236
- duration p50 / p99 / max / nonpositive: (539.557, 3518.852299999994, 90055.014, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2026 | 04 | 3860371 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 10473 |
| end_station_missing | 10498 |
| start_coords_invalid | 1939 |
| start_station_missing | 1939 |

## Membership

| group | original | rows |
|---|---|---|
| casual | casual | 614904 |
| member | member | 3245467 |

## Rideable type

| type | rows |
|---|---|
| classic_bike | 1069771 |
| electric_bike | 2790600 |
