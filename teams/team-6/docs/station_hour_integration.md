# Canonical station-hour integration

**Built:** October 21, 2026. **Status:** historical package; spatial-weather alignment superseded October 26. **Model activation:** none.

The [new historical Manhattan panel](historical_station_panel.md) implements the agreed March 2025–August 10, 2026 study as a separate version. The October 21 build below is preserved, but its weather horizon 1 targets the hour after the departure label. Do not use its spatial-weather fields for modeling. The corrected builder matches actual target timestamps, and validation now rejects that older misalignment. Its checksummed departure labels remain usable as bounded operation evidence; no combined model was trained from the misaligned features. Coverage figures below describe the original execution, not corrected weather eligibility.

The first canonical integration combines the validated 32-station operating-hour cohort with hourly trip flows, current bike/dock state, Farhod's spatial forecast handoff, and release-aware subway advisory features. Predictors and outcomes remain in separate Parquet files joined by `example_id`; the feature artifact contains no departure, return, or label-availability fields.

The immutable build is:

```text
/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/station_hour_integrations/20261021T-team6-v2
```

`CURRENT_STATION_HOUR` in the parent directory points to that build. The committed [validation receipt](validation/station_hour_integration_2026-10-21.json) records artifact hashes and measured coverage.

## Implemented integration

1. **Dated station identity.** Daily station-information snapshots provide an as-of GBFS UUID to Citi Bike `short_name` crosswalk. Every mapping must be available strictly before the forecast origin. The build contains 5,554 dated records for 32 UUID/short-name pairs and found no future-crosswalk use.
2. **Hourly rentals and returns.** Departures retain the previously reconciled supported station-hour labels. Returns are independently counted by eligible end station and end hour after scanning all 1,153 selected trip partitions that begin before October 2026. DST-ambiguous or nonexistent endpoint hours are excluded. The panel contains 326,992 departures and 328,922 returns; neither count appears in the feature file.
3. **Inventory state.** The latest fresh status reading strictly before each hourly origin supplies bikes, docks, e-bikes, disabled counts and dated capacity. Inventory is covered for 74,799 of 78,049 station-hours. A missing reading remains missing and does not imply zero inventory.
4. **Spatial forecast weather.** The compact handoff joins each GBFS UUID to one of 28 weather request classes and uses forecast horizon 1 for the next station hour. Fallback and missing-value indicators, run initialization, assumed availability, mapping distance and package/build identifiers are retained.
5. **Subway disruptions.** The existing station pilot's validated as-of advisory features are carried forward only when both the archived observation and GTFS archive predate the five-minute issue cutoff. Missing or semantically incomplete snapshots remain null instead of being encoded as normal service.
6. **Chronological partitions.** The frozen station protocol supplies April training, June validation, April–June refit membership and September test tags. Origins outside those windows remain `excluded`; no random split is created.

## Resulting coverage

| Split | Rows | Inventory | Spatial weather | Spatial sensitivity eligible | Strict as-of spatial weather | Disruption as-of eligible |
|---|---:|---:|---:|---:|---:|---:|
| Train | 16,789 | 16,064 | 16,258 | 13,471 | 0 | 0 |
| Validation | 9,941 | 9,532 | 9,817 | 8,230 | 0 | 0 |
| Test | 19,655 | 18,838 | 0 | 0 | 0 | 14,224 |
| Outside primary splits | 31,664 | 30,365 | 31,476 | 26,126 | 0 | 0 |

This completes the requested joins and split assignment, but it also establishes that the fully combined model comparison cannot yet run on common train/validation/test keys:

- The weather handoff ends June 30, so it has no September test rows.
- Its station mapping was built from an October 18 current snapshot. It is useful for a declared retrospective spatial sensitivity, but all historical rows fail strict as-of geography availability.
- Semantically complete subway advisory fields begin in August. The fixed April and June training/validation partitions therefore have no eligible disruption rows.

A later spatial-weather model needs a weather extension through a later untouched evaluation window and an effective-dated station mapping, or a predeclared sensitivity that accepts static retrospective geography. A subway-disruption model needs the teammate's new archive to contain semantically complete active periods/selectors and observed or publication timestamps in every split. The importer must preserve those timestamps and apply the same strict pre-cutoff rule.

## Artifacts

| File | Purpose |
|---|---|
| `features.parquet` | As-of identity, inventory, proxy/spatial weather, disruption fields and split tags |
| `labels.parquet` | Separate next-hour departures and returns with operation-support flags |
| `station_crosswalk.parquet` | Dated UUID/short-name mappings and their availability timestamps |
| `manifest.json` | Input versions, checksums, row counts, contracts, coverage and unresolved gates |

Farhod's ZIP is stored outside Git under `modeling_exports/three_hour/weather_handoffs/20261018T033852681910Z/`. Its independently computed SHA-256 is `b98e6120a46abaa1d1c8db18197fae44590e42dbae66233b3cae21c9e710f4cc`; the local receipt also hashes every extracted member.

## Shared Databricks handoff

The station-hour build and the complete extracted weather handoff were published to the existing Team 6 Free Edition workspace on October 21. All 172 source objects were downloaded again from the managed volume and matched their local byte counts and SHA-256 hashes.

```text
/Volumes/workspace/citibike_3h/inputs/station_hour_integrations/20261021T-team6-v2
/Volumes/workspace/citibike_3h/inputs/weather_handoffs/20261018T033852681910Z
```

Versioned Delta tables preserve the immutable publication, while stable views give notebooks a fixed name:

| Stable view | Versioned table | Rows |
|---|---|---:|
| `workspace.citibike_3h.station_hour_features_current` | `station_hour_features_20261021_team6_v2` | 78,049 |
| `workspace.citibike_3h.station_hour_labels_current` | `station_hour_labels_20261021_team6_v2` | 78,049 |
| `workspace.citibike_3h.station_crosswalk_current` | `station_crosswalk_20261021_team6_v2` | 5,554 |
| `workspace.citibike_3h.spatial_weather_classes_current` | `spatial_weather_classes_20261018` | 2,780,568 |
| `workspace.citibike_3h.station_weather_mapping_current` | `station_weather_mapping_20261018` | 2,519 |

The Databricks `account users` group has `USE CATALOG`, `USE SCHEMA`, inherited `SELECT`, and read/write access to the shared `inputs` volume. The existing SQL warehouse grants the same group `CAN USE`. This covers every active user in the workspace and permits teammates to query the tables or add an independently versioned subway contribution. Labels remain a separate table and no model, job, schedule, or stable model output was activated by this publication.

The committed [Databricks handoff receipt](validation/databricks_team_handoff_2026-10-21.json) records the remote locations, object verification, row-count checks, stable views, and effective access grants.

## Rebuild and validation

```sh
uv run python scripts/build_station_hour_integration.py \
  --station-package /absolute/station_pilot_package \
  --availability-package /absolute/availability_package \
  --weather-root /absolute/extracted_weather_build
```

```sh
uv run python scripts/build_station_hour_integration.py \
  --validate /absolute/station_hour_integration
```

The build rejects duplicate station-hour keys, identifier changes, missing as-of crosswalks, future advisory inputs, inconsistent inventory across horizons, weather count/key mismatches, feature/label row-count differences, and artifact checksum changes.
