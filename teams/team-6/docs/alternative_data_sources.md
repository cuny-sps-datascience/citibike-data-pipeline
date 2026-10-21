# Alternatives to class station and subway data

Randy Howk · Investigated October 16, 2026.

**Execution follow-up:** The [public station pilot](station_history_pilot.md) has now acquired and audited all 534 files and trained independent station calendar/weather models in Free Edition. Weather MAE is 3.48 versus calendar 3.63 on 58,965 September examples across 31 eligible stations. The full audit also found a limitation missed by the September sample: complete alert fields begin August 7; the 122 earlier dates use a lossy schema. The fixed April–June alert-training/validation comparison remains incomplete. The investigation findings below describe its earlier sample-based checkpoint.

**A public route to a shorter 2026 station study is now credible.** ctbk.dev provides downloadable station-status history and dated station information; gtfsrt.io provides archived subway-alert observations with UTC collection times; Mobility Database provides dated subway schedules. Downloaded samples passed structural checks. This changes the next dependency from finding a source to auditing its full study-window coverage. The original March 2025–March 2026 training window remains unsupported by these newly verified station files.

The [sample and availability audit](validation/alternative_sources_2026-10-16.json) contains hashes, exact requests, access failures, row counts and quality measurements. Full files and publisher code snapshots stay on the RAID. The active model, canonical trip manifest, data pointers and station-operation eligibility were not changed.

## Best available sources

| Missing input | Preferred alternative | Verified access | Remaining condition |
|---|---|---|---|
| Historical station status | [ctbk.dev archive and collector](https://github.com/hudcostreets/ctbk.dev/blob/main/gbfs/worker/src/index.ts) | Three full-network daily Parquet samples; installation/renting/returning flags, inventory, provider timestamp and poll timestamp | Audit all days, source freshness, station identities and supported operating-hour brackets |
| Dated bike-station geography | [ctbk.dev daily information snapshots](https://data.ctbk.dev/gbfs/info/2026-06-15.json) | April, June and September JSON with UUID, short-name trip ID, coordinates and capacity | Establish observed configurations and change/gap rules; exact effective-change and retrieval times remain uncertain |
| Historical subway advisories | [gtfsrt.io inventory](https://storage.googleapis.com/parquet.gtfsrt.io/inventory.json) | September 15 Parquet; explicit UTC fetch timestamps and route/stop selectors | Audit freshness and compaction coverage; preserve selector semantics and full display periods |
| Historical subway geography and scheduled route associations | [Mobility Database NYC Subway history](https://mobilitydatabase.org/feeds/gtfs/mdb-516) | Four dated MTA GTFS ZIPs downloaded and CRC-checked | Select versions available before each cutoff and apply service calendars; schedules do not prove actual service |

Anonymous metadata requests found **178 of 178 daily files for each of station status, station information and subway alerts**, covering **April 7 through October 1, 2026, UTC**. All **534 HEAD requests** succeeded. October 1 is included because September 30 in New York extends into October 1 UTC. Accessible objects do not establish complete within-file observations or continuous operation.

The advertised combined size is about **12.8 decimal GB**: approximately 2.69 GB status, 0.24 GB information and 9.85 GB alerts. That is feasible on the RAID. Process daily partitions locally and upload compact model tables to Free Edition. The full history has not been downloaded by this investigation.

## Station status and geography findings

The publisher's [collector](https://github.com/hudcostreets/ctbk.dev/blob/8328e4332e81cd0a1dca5d657b81d8b90823888f/gbfs/worker/src/index.ts) records a whole station list when the provider feed advances, plus daily information. This sampling mechanism can retain unchanged and empty stations, avoiding the specific inventory-activity selection found in the previously audited CityBikes archive. Full-window representativeness still needs measurement.

| UTC archive partition | Rows | Distinct stations | Distinct poll times | Largest observed interior poll gap |
|---|---:|---:|---:|---:|
| April 7, 2026 | 3,216,680 | 2,360 | 1,363 | 122 seconds |
| June 15, 2026 | 3,471,840 | 2,411 | 1,440 | 64 seconds |
| September 15, 2026 | 3,461,106 | 2,519 | 1,374 | 180 seconds |

Every status UUID in these samples has a same-date information record and a short-name ID. There are no duplicate station/poll keys, invalid binary operation flags or negative bike/dock inventory values. April 7 starts at 01:16 UTC and is a partial UTC day. September's partition contains a poll just after midnight the next day; use actual timestamps instead of assuming every row belongs to its filename date.

Earlier files can repeat a provider timestamp across several polls. June has 144,660 repeated station/provider-timestamp groups while station/poll keys remain unique. Preserve `polled_at` as observation time and `ts` as provider time; count exposure by elapsed covered intervals rather than repeated rows. Some stations have very old `last_reported` values, so inspect station-report age along with operation flags. A fresh network response alone does not prove an individual station is reporting correctly.

Use the raw status files for the operating-history audit. The publisher's [frontend aggregation adapter](https://github.com/hudcostreets/ctbk.dev/blob/8328e4332e81cd0a1dca5d657b81d8b90823888f/www/src/query/stations.ts) fills operation flags with ones when converting chart summaries. Those synthesized flags cannot establish historical operation. Likewise, chart forward filling is not a project coverage rule. Empty inventory observations are not zero-departure labels; departures continue to come from the canonical trip archive.

Daily information carries a provider `last_updated`, but the downloaded JSON does not embed its exact collection time. Several old objects were modified during a later storage migration. Neither a migrated object timestamp nor a date filename proves an exact historical public release or station-movement time. Keep daily observed geography, reject ambiguous identity changes, and disclose any conservative availability/interpolation assumption. Preserve source attribution and applicable operator terms; anonymous access does not itself grant a redistribution license.

## Subway alert findings

The [archive inventory](https://storage.googleapis.com/parquet.gtfsrt.io/inventory.json) identifies the MTA subway-alert feed from **March 1 through October 14, 2026** at observation time. The retrieved September 15 daily file is **95,707,163 bytes**, containing **1,883,760 flattened selector rows**, **1,440 distinct fetches** and **247 distinct alert IDs**. These are sightings/selectors, not 1.88 million incidents. Arrow records `fetch_timestamp` explicitly in UTC, avoiding the floating-time ambiguity of the earlier Socrata archive.

The [compactor](https://github.com/JarvusInnovations/gtfs-realtime-archiver/blob/64d9d2a33f3b7dd0fc119746aca04f98d8cd1672/src/dagster_pipeline/defs/assets/compaction.py) emits one row per informed entity. Combine fields within that selector with AND, combine selectors with OR, then count distinct alert entities per snapshot/station/radius. Use `active_periods_json`, which preserves all periods; the compatibility start/end columns describe only the first period. Retain trip/direction restrictions and unsupported cases rather than broadening matches. Collection time establishes when an archived observer saw the alert; display-period starts do not establish publication or actual disruption duration.

The sample has no interior fetch gap above 120 seconds, but **220 of 1,440 fetches** have provider age above the existing 15-minute freshness threshold. Missing or stale exposure must remain unknown. A new archive does not justify replacing such periods with zero alerts or normal service.

The public Parquet download works without authentication. Raw protobuf access returned HTTP 403; the publisher's [reconciliation documentation](https://github.com/JarvusInnovations/gtfs-realtime-archiver/blob/64d9d2a33f3b7dd0fc119746aca04f98d8cd1672/dashboards/proto-parquet-reconciliation/README.md) explicitly describes the raw bucket as private. Its advertised protobuf hostname also failed TLS hostname verification. No access restriction or certificate check was bypassed. Use the verified Parquet path; raw/provenance reconciliation remains a separate access-dependent option.

## Historical subway references

Four [Mobility Database versions](https://mobilitydatabase.org/feeds/gtfs/mdb-516) were downloaded: February 27, May 14, May 27 and August 28, 2026. Their ZIP CRCs pass. Each has 29 routes and 1,488 stop records. Together their published version dates and service calendars supply candidates spanning the shorter 2026 study, including the May schedule transition. Select versions by actual archive availability and valid service date; do not reuse the October reference throughout historical training. Parse calendars, exceptions, parent stations, trip directions and route/stop associations for each applicable version.

These files improve historical subway mapping. They do not supply bicycle operation or verify an advisory's effect on actual train service. Historical bike locations come from the separate dated station-information archive.

## Other alternatives reviewed

| Source | Assessment |
|---|---|
| [CityBikes](https://data.citybik.es/) | Already acquired for November 2024–August 2026; stores status changes. The prior activity-selection audit still prevents unconditional station-operation claims. Useful corroboration, not the primary replacement. |
| [NYC Comptroller sample-data branch](https://github.com/NYCComptroller/citi-bike-gbfs/tree/sample-data) | Tree inventory contains 819 status/information pairs dated March 12–23, 2024, plus a prepared dataset. Genuine historical samples, outside the proposed 2025–2026 window; approximately 1.68 GB of raw JSON metadata-listed, not bulk-downloaded. |
| [Tom MacWright's archive notes](https://macwright.com/2023/09/17/bikeshare-1) | Describes five-minute capture beginning July 2023. No current anonymous bulk export or continuous 2025 coverage was verified; an owner-provided export could be a longer-history lead. |
| [Zenodo graph dataset](https://zenodo.org/records/13846868) | Published September 2024; availability series retain changes. No replacement for the needed 2025–2026 operating snapshots was established. |
| [digitalhen subway archive](https://huggingface.co/datasets/digitalhen/nyc-subway-realtime) | Promising 2026 service/alert tables with collection-quality records, but access requires approval. This is another access-dependent lead, not an immediately downloadable replacement. |
| [transitlab alerts](https://docs.transitlab.nyc/datasets/transit-alerts/) | Documents UTC change observations from October 2026 onward. Useful prospective complement; does not supply the earlier training period. |
| [CitiBikeHistory](https://citibikehistory.com/) | Public graphs show recent inventory history; a documented bulk export spanning the modeling window was not verified. |
| [Subway Data NYC](https://subwaydata.nyc/programmatic-access) | Train-arrival/service diagnostics; still not a verified substitute for an alert-version archive. |

No private-access requests were submitted, no messages were sent, and no gated dataset terms were accepted.

## Recommended next implementation

1. **Audit a full 2026 operating-history window.** Download the ctbk daily status/information files in bounded batches, beginning with a representative week. Check keys, timestamp ordering, gaps, active flags, station-report age, UUID/short-name correspondence and observed location changes. Establish a declared exposure policy before adding zero-departure station hours. Keep current main-evaluation gates closed until it passes.
2. **Build historical station/subway exposure.** Use dated bike information and appropriate GTFS versions. Read the archived alert snapshots preceding each input cutoff, enforce the current freshness policy, preserve all selectors/periods and deduplicate entities. Audit unavailable inputs independently of demand and never backfill a final revision into earlier forecasts.
3. **Freeze a shorter evaluation design.** A conservative candidate is training on supported April 2026 hours, May validation and September test. Require the maximum availability of every supplying-source cohort to precede the September fit cutoff under the existing 45-day assumption. June is not automatically a fully mature validation month: July releases can supply June spillover outcomes and mature only in September. Refitting may add individually mature later hours after the same check. Use calendar/station/weather predictors; any historical station rate additionally needs a supported exposure denominator and an available supplying release. This is a proposed design, not a measured model result.
4. **Run a bounded station pilot in Free Edition.** Choose a Manhattan cohort using pre-test metadata and coverage rules, compare the same station/hour keys with and without advisory features, then assess broader coverage. Keep the active Manhattan-total model separate. This investigates predictive contribution, without claiming causal subway effects.

Class data remains useful for longer historical coverage, and class access is still required for the assignment's workspace/shared-catalog delivery. It is no longer the only plausible source route for a station/subway study. The next blocker is validated historical exposure and feature construction over the shorter public-data window.

## Reproducing the research checks

The research package is `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/source_alternatives/20261016T174506Z-591dfc95`. Its manifest pins downloaded objects, publisher code revisions, metadata observations and failed requests. The small audit is stored in Git; bulk samples remain on the RAID.

```sh
uv run python scripts/probe_alternative_archive_coverage.py --root /absolute/research/package --start 2026-04-07 --end-exclusive 2026-10-02
uv run python scripts/audit_alternative_source_samples.py --root /absolute/research/package --output docs/validation/alternative_sources_2026-10-16.json
```

The first command makes rate-limited HEAD requests and records object presence, sizes and errors. `--reuse-existing` retains earlier observations with their original timestamps when extending a scan. The second verifies saved checksums and audits the explicitly named samples with bounded DuckDB memory. Neither command downloads the complete history, trains a model or promotes station eligibility.
