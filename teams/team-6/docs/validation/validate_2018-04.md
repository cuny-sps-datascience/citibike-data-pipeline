# Validation: 2018-04

- reconciled: True
- schema_consistent: True
- selection_complete: True
- unlisted files under trips/ and quarantine/: 0
- stale ledger sources (published but not selected): 0
- selected sources: 1
- source rows: 1307543
- published rows: 1307543
- quarantined rows: 0
- record-level removals: 0 (schema_version 1 removes none)
- start date range: (datetime.datetime(2018, 4, 1, 0, 0, 0, 201000), datetime.datetime(2018, 4, 30, 23, 59, 35, 369000))
- distinct start stations: 766
- duration p50 / p99 / max / nonpositive: (595.071, 3635.5951800000016, 10172645.989, 0)
- duplicate ride_id: identical 0, conflicting 0

## Rows by trip month

| year | month | trips |
|---|---|---|
| 2018 | 04 | 1307543 |

## Quality flags

| flag | rows |
|---|---|
| end_coords_invalid | 1 |
| start_coords_invalid | 1 |

## Membership

| group | original | rows |
|---|---|---|
| casual | Customer | 149311 |
| member | Subscriber | 1158232 |

## Rideable type

| type | rows |
|---|---|
| None | 1307543 |
