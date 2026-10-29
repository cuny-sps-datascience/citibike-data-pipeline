# September data refresh and midterm preparation

Randy Howk · October 16, 2026. September NYC trip data is downloaded, selected, normalized and validated. The complete hourly export and Manhattan outcomes now extend through September 30. Weather acquisition also extends through September, with a separate documented contribution that preserves the original model inputs. The midterm materials include an approximately 2,000-word technical whitepaper, three editable slides and a timed narration/navigation script.

## Validated data changes

| Item | Measured result |
|---|---|
| NYC release | `202609-citibike-tripdata.zip`, five modern CSV parts |
| Download | 972,151,835 bytes; ETag pinned during HTTPS retrieval; all selected member CRCs verified during profiling |
| Source SHA-256 | `81b5d756665ef7a69cc5189349511fb7ec973fcf20ab6ca7078732cb7e50537c` |
| September release rows | 4,982,855 profiled = 4,982,855 published; zero quarantined, removed or duplicate ride IDs |
| Combined August–September check | 10,229,091 source/published rows; complete selection, consistent schema; no duplicate ride IDs, unlisted files, stale ledger entries or orphaned removals |
| Complete observed export | 169 ledger-selected input files; 78 Parquet artifacts; 49,804,689 bytes |
| Observed station-hours | 21,167,148 rows; operation eligibility remains false |
| September eligible geographic input | 4,979,389 assigned departures across all published start locations |
| Manhattan label history | 18,984 hours, 63,393,151 assigned departures; four repeated fall-clock hours remain null |
| September Manhattan outcomes | 720 hours, 3,049,503 departures |
| Earlier-month revision | 549 additional eligible departures across all locations in August; Manhattan subset adds 331 departures across 17 August hours |
| Extended weather | 2,311 successful initialization runs of 2,320 requested; 110,928 normalized rows; all 120 September runs acquired |
| Weather gaps | Nine previously unavailable runs remain unavailable; null forecast values remain null |
| Portable weather ZIP | 768,530 bytes; SHA-256 `11183c4b04fc35f1544cb9aa6ecfc5a7509d2c2d9b80d3f466972b05ed871545` |

The provider's October 5 modification date is not a verified first-publication date. The public watcher first observed this object version on October 16 at 15:22:36 UTC; the local download completed at 17:15:47 UTC. Labels are prepared from selected published files, not from the release listing alone. Full ingestion reconciliation is in [the source validation](validation/september_ingestion_2026-10-16.json).

## Artifact locations

The common processed base is `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour`.

| Artifact | Path relative to that base |
|---|---|
| Observed-hour export | `builds/20261016T172712Z-eb309064` |
| Manhattan labels | `borough_labels/20261016T172924Z-8e688cba` |
| Extended weather table | `weather_reference_builds/20261016T173331Z-84df9caa` |
| Weather handoff directory | `handoffs/weather_reference_20261016T173331Z-84df9caa` |
| Weather portable ZIP | `handoffs/weather_reference_20261016T173331Z-84df9caa.zip` |

New raw trip downloads live separately under `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_downloads/objects/2026-09/<object_version>/`. The original `FinalProject` archive remains read-only. Raw weather responses and checksum-versioned old/new acquisition manifests remain under `source_candidates/weather`. Full datasets remain outside Git; the [runbook](runbook.md) provides the acquisition and rebuild commands.

`CURRENT_OBSERVED` and `CURRENT_BOROUGH_LABELS` select the new complete builds. `CURRENT_BOROUGH_FEATURES` and the active cloud model remain frozen on their original immutable packages. Original benchmark metrics are not recomputed against revised August outcomes. The weather-only normalizer checks hashes, declared units and initialization/valid-time uniqueness before creating the separate contribution.

## Cloud verification and demo evidence

The prepared outcome package passes local and cloud checks for complete calendars, unique keys, nonnegative counts, DST nulls, selected-input evidence and artifact hashes. The daily importer now reports **18,984 actual hours** through `2026-10-01T03:00:00Z` (September 30, 23:00 New York). Current imports replace the stable outcome and external-reference Delta tables and record the immutable package in `dataset_version`, leaving model-serving inputs unchanged. The [dated refresh receipt](validation/september_refresh_2026-10-16.json) preserves the table names and checksums recorded at execution time.

The refreshed health report at **17:32:31 UTC** is **OK**: all **16 due origins** have complete three-horizon forecasts published before their origins, with no invalid predictions, fallback rows, failed sources or stale sources in its monitored interval. The hourly, daily evidence and daily health schedules remain enabled. Matching October trip outcomes are still unpublished, so prospective evaluation correctly contains zero forecasts. September's new historical labels cannot supply October production accuracy.

The technical dry run refreshes dashboard outputs and checks authenticated notebook/job access, task completion, source/model hashes and temporal constraints. It verifies the application path without claiming a spoken rehearsal. The [demo script](midterm_demo_script.md) specifies five minutes of slide narration, five minutes of application navigation, an offline fallback and likely questions.

## Presentation and documentation

- [Whitepaper draft](technical_whitepaper_draft.md) and [PDF](presentations/data622_whitepaper_draft.pdf): problem, data, timing assumptions, models, results, architecture, reproducibility and limitations.
- [Three-slide PowerPoint](presentations/data622_mvp.pptx) and [PDF](presentations/data622_mvp.pdf): primary results now show release-aware monthly MAE 767.77 versus weekly profile 1,386.49 on 6,552 common rows, including June's stronger calendar comparison.
- [Departure label/feature datasheet](borough_departure_datasheet.md), [station/advisory datasheet](prospective_station_alert_datasheet.md) and [updated weather datasheet](weather_reference_datasheet.md): schemas, keys, provenance, missingness, timing, intended uses and distribution limits.
- [Demo notes](public_mvp_demo.md) and [timed script](midterm_demo_script.md): refreshed claims, navigation, questions and spoken-practice instructions.

Local verification passes **238 tests in 30.52 seconds**, doctor, CLI help and changed-file compilation. The deck has exactly three slides with speaker notes and a matching three-page PDF; all three slide previews were visually reviewed. The whitepaper has selectable text and source links. The final receipt records artifact and repository-link checks.

## Remaining work

Randy still needs to rehearse aloud and adjust delivery timing. Class-workspace deployment and confirmation that another team can read the weather contribution require class access. Historical station operation/geography, supported alert history and station/alert model evaluation remain open research dependencies. Production accuracy requires matching released outcomes and a sustained observation period. These tasks are not completed by this historical data refresh or the whitepaper draft.
