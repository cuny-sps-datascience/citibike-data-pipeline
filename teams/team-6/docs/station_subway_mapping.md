# Citi Bike station to subway mapping

The October 16, 2026 reference links 2,518 current Citi Bike stations to nearby MTA station points at 250 and 500 meters. It is ready in the private Databricks workspace and as CSV and Parquet on the processed RAID. Randy operates this mapping; historical station geography, operation coverage and alert effects remain separate requirements.

## Sources and snapshot

Station identity and coordinates come from the public [Citi Bike GBFS discovery feed](https://gbfs.citibikenyc.com/gbfs/gbfs.json) and its English `station_information` endpoint. Subway coordinates, stop IDs, complex IDs and daytime routes come from [MTA Subway Stations](https://data.ny.gov/Transportation/MTA-Subway-Stations/39hk-dx4f/about_data), dataset `39hk-dx4f`. Its 496 reference stop records have 493 distinct MTA station IDs: 145 St, W 4 St and Queensboro Plaza each have separate line records sharing a station ID. GTFS stop ID is the unique reference key, so these lines and routes are preserved. The source also includes Staten Island Railway; it does not supply PATH or NJ Transit stations.

Mapping version: `20261016T135632Z-c198ce44`. Source retrieval completed at **2026-10-16 13:56:34 UTC** (09:56:34 New York); the local build finished at 13:56:36 UTC. Every row retains `available_from_utc`, a conservative lower bound from the latest source retrieval, and `mapping_version`. Historical validity and station operation are false. For actual prospective features, require both the snapshot availability and the completed mapping build to precede the input cutoff; the manifest retains both timestamps. Do not join this current geography to earlier training origins as if it were historical geography.

| Straight-line radius | All bike stations | With a subway reference point | Without a subway reference point |
|---|---:|---:|---:|
| 250 meters | 2,518 | 778 | 1,740 |
| 500 meters | 2,518 | 1,687 | 831 |

Coverage includes the whole current Citi Bike feed, including New Jersey stations. Unmatched stations stay in the coverage table. Zero nearby points means no reference point within that radius; it says nothing about departures, availability or continuous operation.

## Tables and data dictionary

Stable views are in `workspace.citibike_3h`. Each points to a Delta table with the mapping version appended. Full CSV and Parquet files use the short names below; identifiers must be imported as strings, including decimal-looking Citi Bike short names.

| View suffix after `station_subway_` | Rows | Unique key | Purpose |
|---|---:|---|---|
| `bike_stations` | 2,518 | `bike_station_id` | Every source bike station and its current coordinates |
| `station_proximity` | 4,696 | bike ID, radius, `gtfs_stop_id` | Every qualifying subway stop reference |
| `complex_proximity` | 4,025 | bike ID, radius, `mta_complex_id` | One nearest qualifying reference per complex |
| `route_proximity` | 6,608 | bike ID, radius, `route_id` | One row per nearby daytime route |
| `coverage` | 5,036 | bike ID, radius | Counts and nearest distance, including unmatched stations |

Common fields are `bike_station_id` (GBFS opaque identifier), `bike_short_name` (separate Citi Bike short-name scheme), `bike_name`, `bike_latitude`, `bike_longitude`, mapping version and availability, and the unverified history/operation flags. Scheme columns explicitly identify the namespaces. Do not equate a GBFS UUID with a trip short-name ID, or infer effective history from a matching current short name.

`distance_m` is haversine distance on a sphere of radius 6,371,008.8 meters. Reference points are not necessarily entrances, and straight-line distance is not walking distance. Both radii are fixed references for later sensitivity analysis. A complex uses its nearest point inside the radius. Route links use only qualifying points inside the radius, deduplicate repeated routes, and retain the nearest point/complex plus the distinct nearby-complex count. Distant members of the same complex cannot introduce additional routes.

`route_id` preserves the MTA **daytime route label**, with scheme `mta_subway_daytime_route`; it is not an asserted GTFS alert route crosswalk. In particular, generic shuttle labels must be resolved to the appropriate GTFS routes before joining route-level alerts. Preserve express variants and validate the actual feed namespace. Stop-level alerts also require explicit parent/directional-stop matching. Proximity alone does not prove that an alert affected this station or caused a departure change. `route_scope` records that reference routes do not establish all-hours service.

## Access and rebuild

The [forecast dashboard](https://dbc-d00cbd4c-c74c.cloud.databricks.com/editor/notebooks/2763628353985993) shows coverage by radius. The successful import is recorded in [extension validation](validation/mvp_extensions_2026-10-16.json), with cloud table counts and immutable source checksums.

Local package:

```text
/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/station_subway_mapping/20261016T135632Z-c198ce44
```

The directory contains five CSV files, five Parquet files, raw JSON responses and a manifest with URLs, retrieval times, hashes and row counts. `CURRENT_MAPPING` in its parent identifies the latest completed build. The [CSV preview](validation/station_subway_mapping_preview.csv) is an illustrative excerpt of links, not the full mapping.

```sh
uv run python scripts/build_station_subway_mapping.py
uv run python scripts/deploy_public_mvp.py mapping --package /absolute/new/mapping/build
```

Each build fetches fresh references into a new immutable directory. The collector reconciles MTA row counts before/after paging and verifies source update metadata did not change. Invalid coordinates or duplicate reference keys fail the build. The cloud importer checks every artifact checksum and row count, then replaces each stable current table and records the immutable package in `dataset_version`. Versioned packages remain in the volume and on the RAID. The source records and original trip archives remain intact. This reference does not retrain or alter the Manhattan model.

Example for a prospective feature query:

```sql
SELECT bike_station_id, bike_short_name, route_id, distance_m, mapping_version
FROM workspace.citibike_3h.station_subway_route_proximity
WHERE radius_m = 500
  AND to_timestamp(available_from_utc) < :input_cutoff_utc;
```

Also validate the manifest's build completion time and the alert's first-seen/publication cutoff. Historical alert coverage and timestamp timezone remain unresolved; this mapping does not remove those gates. Private workspace publication does not meet the course shared-catalog requirement.
