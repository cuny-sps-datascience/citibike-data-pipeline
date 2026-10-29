# Data dictionary: normalized trips (schema_version 2)

Location: `FinalProject_processed/trips/trip_year=YYYY/trip_month=MM/*.parquet` (Hive partitions).
Partition month is the month of `started_at_local`, not the file's nominal release.

| column | type | meaning |
|---|---|---|
| started_at_local / ended_at_local | TIMESTAMP | Source wall-clock time, America/New_York, no UTC conversion. Legacy `starttime`/`stoptime` (also `Start Time`/`Stop Time`); modern `started_at`/`ended_at`. Formats accepted: ISO with or without fractions, `M/D/YYYY H:MM[:SS]`. |
| duration_seconds | DOUBLE | `ended_at_local - started_at_local`. Wall-clock based, so it is off by 3600 s across a DST change. |
| source_duration_seconds | DOUBLE | Legacy `tripduration`; null for modern sources. |
| start_station_id / end_station_id | VARCHAR | As in the source (`72`, `6322.01`). IDs are not equated across eras. Legacy IDs written as `NNN.0` (the re-exported 2019-01, 02, 03, 07 and 08 parts) are rewritten to `NNN`. Modern IDs with exactly one decimal digit (`^[0-9]+\.[0-9]$`) get a trailing `0` (`6948.1` -> `6948.10`): the exports sometimes drop it, and 104 of the 107 such IDs in 2025-2026 have a two-decimal twin with the same station name. No other modern ID is rewritten (`6322.01`, `JC032`, `HB503`, `SYS033`, `6432.10_Pillar` stay as they are). A modern ID such as `72.0` would also become `72.00` under this rule; no such modern ID has been observed. |
| start_station_name / end_station_name | VARCHAR | Trimmed source value. |
| start_lat, start_lng, end_lat, end_lng | DOUBLE | `TRY_CAST`; unparseable values become null and are flagged. |
| ride_id, rideable_type | VARCHAR | Modern sources only; null for legacy. |
| membership_group | VARCHAR | `member` / `casual`. Legacy `Subscriber` -> member, `Customer` -> casual. Anything else is null and flagged. |
| original_membership | VARCHAR | The source label, unchanged. |
| source_key, source_row | VARCHAR, BIGINT | Provenance: manifest key and 1-based record number (blank lines are not records). |
| schema_family, schema_version, run_id | VARCHAR, INTEGER, VARCHAR | Adapter used, normalization version, producing run. See "Schema versions" below. |
| quality_flags | VARCHAR[] | See below. Flags never remove rows. |
| eligible_departure | BOOLEAN | Valid start time and start station present. |
| eligible_arrival | BOOLEAN | Valid end time, end station present, duration >= 0. |
| trip_year / trip_month | VARCHAR (path) | Partition keys taken from the directory path, zero-padded strings (`trip_year=2025/trip_month=03`), the month of `started_at_local`. DuckDB infers their type from whichever partitions a given read touches: zero-padded values alone can yield `VARCHAR` (a read touching any of `trip_month=01` to `09` infers `VARCHAR` on DuckDB 1.5.6), while a read touching only `10` to `12` infers `BIGINT`. Always pin them with `hive_types`, e.g. `read_parquet(files, hive_partitioning = true, hive_types = {'trip_year': INTEGER, 'trip_month': INTEGER})`. |

Null tokens: empty string, `\N`, `NULL`.

### Planned bike-type demand features (Stage 3)

The October 5, 2026 revision to `2026-10-02-data622-station-demand-plan.md` makes bike type part of hourly/monthly station demand and as-of profiles. These aggregate outputs are planned, not yet built.

| Aggregate bucket | Raw `rideable_type` |
|---|---|
| `classic` | `classic_bike` |
| `electric` | `electric_bike` |
| `unknown` | Null or blank, including legacy trips without bike type |
| `other` | Any other nonblank value, retained in raw trips for review |

Each bucket has separate departure and arrival counts; their sums equal the corresponding total. Profiles add departure/arrival/net-flow means, departure counts and completed-departure shares by bucket. All type means use the same active-weekday denominator as totals, with zero-trip hours included. Shares divide by total completed departures and are null if there are none. Unknown and other shares remain visible.

Completed-trip shares describe observed ridership. Estimating a stocking mix that increases ridership also requires historical rentable inventory by type over matching dates. Stage 4 will audit that input and calculate departures per covered operating station-hour and departures per available bike-hour of each type. The station-demand plan defines coverage, publication cutoffs, sparse-data fallback and the limits of observational mix comparisons.

### Half-hour departure-mix report (implemented)

Location: `FinalProject_processed/reports/bike_mix_halfhour/date=YYYY-MM-DD/<unique-run>/`. Generate with `citibike bike-mix --date YYYY-MM-DD [--station ID] [--min-rides 20]`.

Each observed station/date has 48 rows, keyed by `trip_date, station_id, id_scheme, slot`. `interval_start_local` and `interval_end_local` define half-open 30-minute wall-clock intervals. Counts: `departures`, `classic_departures`, `electric_departures`, `unknown_departures`, `other_departures`, `known_type_departures` (classic + electric). All counts include only eligible departures.

`classic_pct` and `electric_pct` divide by known classic/electric departures; `known_type_pct` divides known-type departures by all departures. These percentages are null when their denominators are zero. `day_departures` and `day_electric_pct` repeat the station/day totals. `electric_change_from_day_pp` and `electric_change_from_previous_pp` report percentage-point differences; a zero-known-type slot makes its comparison undefined. `low_sample` is true below the requested known-type count threshold.

`station_summary.csv` lists daily departures and electric share, the number of qualifying intervals, and `electric_share_range_pp` (maximum minus minimum share over qualifying intervals; null with fewer than two). This range describes observed variation and does not test statistical significance. Empty slots indicate no observed eligible trips; operating status remains unknown. Fall-back repeats merge and spring-forward gaps stay empty in the fixed local-time grid.

A zero-duration trip (`duration_seconds = 0`) is flagged `duration_nonpositive` but still counts as
`eligible_arrival`, since that flag only requires `duration_seconds >= 0`.

## Quality flags

`start_station_missing`, `end_station_missing`, `end_ts_invalid`, `duration_nonpositive`,
`duration_mismatch` (legacy only: derived and source duration differ by more than 2 s; expected for
minute-resolution 2015 timestamps and DST changes), `start_coords_invalid` / `end_coords_invalid` (null or
outside lat 40.4 to 41.1, lng -74.4 to -73.6), `membership_missing`, `membership_unrecognized`,
`start_dst_ambiguous`, `start_dst_nonexistent`, `end_dst_ambiguous`, `end_dst_nonexistent`,
`crosses_dst` (the America/New_York UTC offset at the end differs from the start, so
`duration_seconds`, a wall-clock difference, is off by the offset change for these trips).

## Quarantine

`FinalProject_processed/quarantine/*.parquet`: `source_key`, `source_row`, `reason`, `raw_record`
(JSON array of the original fields). Reasons: `field_count` (wrong number of fields) and
`start_ts_invalid` (start time missing or unparseable, so no trip month can be assigned).
A source whose quarantine share exceeds `max_quarantine_fraction` is not published.

## Deliberately not carried

Legacy `bikeid`, `birth year`, `gender` (decision D6).

## Deduplication

Exact and re-quoted copies are removed at selection time (one source set per release).

Record-level removal (schema_version 2) covers one case only: a modern trip published by two releases.
Trips that start on the last day of a month and end in the next one can appear in both months'
releases (512 `ride_id`s in 2026-04 and 2026-05; 504 identical, 8 differing only by the one-decimal
station ID above). The rule, for a modern source of release R:

- A staged row is a spillover row when its start month (`trip_year`, `trip_month`) is not R.
- If a selected source of the release equal to that start month already published (ledger, dataset
  `trips`) a row with the
  same `ride_id` and the same `started_at_local`, `ended_at_local`, `start_station_id` and
  `end_station_id` (compared after the station ID rules above, nulls equal), the spillover row is
  removed: it is not written to `trips/` but to `removed/`.
- If the published copy differs in any of those fields, the row is kept (distinct records are never
  collapsed) and counted as a conflict in the normalize output (`conflicts=N`).
- A spillover row with no published copy in its start month's release is kept.
- Legacy sources (no `ride_id`) are never checked or removed.
- Published sources of the start month that are no longer selected are ignored: they are unpublished at
  the end of the run that replaces them, so a removal must not rely on their copies.

A release's check only sees what is already published, so `citibike normalize` processes releases in
chronological order. `citibike validate` still reports duplicate `ride_id`s across the published files
as identical or conflicting; that is the backstop for anything this rule does not see (for example a
start month's release normalized after the later one).

## Removed

`FinalProject_processed/removed/<slug>__<run_id>.parquet`, ledger dataset `removed`, one file per source
that removed rows: `source_key`, `source_row`, `ride_id`, `reason` (`duplicate_of_release:YYYY-MM`, the
release that keeps the trip), `kept_source_key` (the source whose published copy is kept; the smallest
key if several selected sources hold an identical copy), and the compared fields `started_at_local`,
`ended_at_local`, `start_station_id`, `end_station_id`. For every selected source, source rows =
published + quarantined + removed rows.

`citibike validate` checks every removed row of the validated sources against the trips currently
published by the selected sources of the release in its `reason` (same `ride_id` and compared fields).
A removed row without such a copy is an orphaned removal (the kept copy was later unpublished,
re-selected away or quarantined); any orphan fails reconciliation, and the fix is to re-normalize the
removing release.

## Schema versions

- 1: initial normalization.
- 2: one-decimal modern station IDs padded; cross-release duplicate removal. Every modern month is
  re-normalized under version 2. Legacy months keep their version 1 files until they are re-normalized;
  both rules leave legacy output unchanged, and `validate` does not compare `schema_version` values
  across files.
