# Prospective station history and subway advisory datasheet

Randy Howk · October 16, 2026. These derived datasets preserve observed station configurations and nearby subway advisories for future forecasting research. They are deployed in the private `workspace.citibike_3h` schema. They do not supply historical station operation, verified disruption durations or departure labels. The deployed Manhattan model does not consume them.

## Sources and versions

Station information comes from feeds discovered through [Citi Bike GBFS](https://gbfs.citibikenyc.com/gbfs/gbfs.json). Advisories come from the [MTA all-alerts endpoint](https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fall-alerts). The hourly collector saves compressed raw snapshots, uncompressed SHA-256 checksums, collection IDs, observation times and errors. Raw data lives in private Unity Catalog volumes; no credentials or full snapshots belong in Git.

The initial reference uses mapping `20261016T135632Z-c198ce44` and regular subway GTFS reference `20261016T153134Z-5401b8c2`. The mapping contains 2,518 bicycle stations: 778 have a subway reference point within 250 meters and 1,687 within 500 meters. Distances use the haversine formula with Earth radius 6,371,008.8 meters. They are straight-line point distances, not walking distances or guaranteed accessible station entrances. See the [mapping contract](station_subway_mapping.md).

## Composition and keys

Counts below describe the **first full published batch**, not a continuously updated total. Later daily batches append observations. Logical keys also include `_processing_version`, which hashes the processing helper, static GTFS version and mapping version.

| Public view | First batch rows | Logical key within processing version |
|---|---:|---|
| `station_information_observations` | 47,858 | `snapshot_run_id`, `station_id` |
| `mta_alert_observations` | 9,078 | `snapshot_run_id`, `entity_id` |
| `mta_alert_selectors` | 43,888 | `snapshot_run_id`, `entity_id`, `selector_index` |
| `station_subway_alert_features_available` | 95,716 | `snapshot_run_id`, `bike_station_id`, `radius_m` |
| `prospective_source_coverage` | 38 | `snapshot_run_id`, `source` |

The batch covers 19 collected snapshots per source and 2,519 distinct observed bicycle station UUIDs. Advisory rows are entity sightings across snapshots, not 9,078 distinct service incidents. The station count differs from the earlier dated mapping, so newly observed stations remain visible with unknown exposure.

## Station observation fields

| Fields | Type and interpretation |
|---|---|
| `station_id`, `short_name` | Strings; GBFS station UUID and short-name scheme remain distinct |
| `name`, `latitude`, `longitude`, `capacity` | Observed name, WGS84 decimal degrees and nullable capacity count |
| `observed_at_utc`, `feed_timestamp_utc` | Retrieval observation and nullable provider update timestamps |
| `configuration_sha256` | Hash of observed short name, name, coordinates and capacity |
| `historical_operation_verified`, `effective_change_time_verified` | False; sampling does not establish operation or exact change time |
| `_raw_sha256`, `_batch_id`, `_processing_version`, `publication_completed_at_utc` | Raw lineage, processing identity and completed derived publication |

`station_geography_changes` compares consecutive configurations. Its first seven changes were all capacity updates; none changed coordinates, short name or station name. `station_geography_observed_intervals` groups consecutive matching configurations, including a return to an earlier configuration. First/last observations bound sightings, not exact effective intervals. `station_observation_gaps` and `prospective_source_gaps` flag gaps exceeding 90 minutes. Absence is unknown coverage, not closure.

## Advisory fields and interpretation

Entity observations retain `entity_id`, `revision_sha256`, `periods_json`, English `header_text`, planned-work indicator, cause/effect enums, feed age and freshness. Selector rows preserve agency, route, stop, route type, direction, trip restriction and `resolution`; `resolved_parent_count` audits matches. The [official alert dictionary](https://www.mta.info/document/90881) identifies `MTASBWY` as subway in this feed, while `MTA NYCT` and `MTABC` identify buses. Regular static subway GTFS uses `MTA NYCT`; the explicit crosswalk avoids bus/subway route collisions.

Fields in a selector combine with AND and selectors combine with OR, per the [GTFS reference](https://gtfs.org/documentation/realtime/reference/#message-entityselector). Route/stop/direction matching uses actual scheduled associations, retaining shuttle and express IDs. Unsupported trip restrictions stay unresolved. A resolved selector with no scheduled match remains an audit outcome and does not prove service was unaffected.

| Station/radius feature | Unit and meaning |
|---|---|
| `matched_active_alert_count` | Nullable count of distinct matched entities whose display periods include the snapshot instant |
| `matched_known_display_overlap_next_3h` | Nullable distinct count with explicit display periods overlapping the next three hours |
| `matched_alerts_without_explicit_period` | Nullable distinct count with no explicit display periods |
| `radius_m` | Integer 250 or 500; counts deduplicate entities across selectors and nearby subway points |
| `feed_fresh`, `gbfs_info_fresh`, `mapping_compatible` | Boolean source and mapping eligibility checks |
| `unresolved_subway_selectors` | Snapshot count of unsupported or unknown subway selectors |
| `reference_available_at_utc`, `available_from_utc` | Reference availability and earliest completed derived availability |

Counts become null when MTA provider age exceeds 15 minutes, GBFS information age exceeds 90 minutes, the station lacks a compatible mapping, short-name identifiers disagree or coordinates differ by more than 25 meters. Clock skew up to five minutes is tolerated. Null is unknown; zero means no resolved advisory matches in a usable snapshot. `normal_service_verified` remains false. Alert periods describe display, not actual impact, and hourly sampling may miss short advisories.

## Availability and quality controls

Derived availability is the greatest of source observation, reference availability and completed batch persistence. A raw snapshot collected earlier cannot make a later derived table historically available. A forecast consumer must filter `available_from_utc < cutoff`, require observations no older than 90 minutes, enforce freshness/mapping flags, and handle unresolved selectors. Provider publication times remain unverified.

Raw hashes, reference hashes, within-batch key uniqueness and imported row counts are checked before publication. Append-only raw tables join completed publication receipts; public views deduplicate logical keys. Partial batches remain hidden, and completed coverage receipts make replay idempotent. The first batch had no interior gaps above 90 minutes and no unsupported or unknown subway selectors. Those checks describe the sampled period only. See [execution evidence](validation/release_aware_evidence_2026-10-16.json) and [operating controls](release_aware_evidence_pipeline.md).

## Use and distribution

Preserve Citi Bike/Lyft and MTA attribution, provider terms, hashes and timing caveats when transferring these derivatives; public endpoints do not imply an unrestricted redistribution license. Station locations and advisory text describe public infrastructure and service messages. They contain no project-added rider identifiers. Raw advisory text is retained for audit and should be displayed as attributed provider text. Historical operation reconstruction, causal alert effects and class shared-catalog publication remain separate tasks.
