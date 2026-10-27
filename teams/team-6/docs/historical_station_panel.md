# Historical Manhattan station departure panel

**Reviewed October 26, 2026.** This separate retrospective package implements the decisions recorded in [the protocol](historical_station_panel_protocol.json): Manhattan, March 1, 2025–August 10, 2026 New York dates, explicitly retrospective station geography, a five-minute issue cutoff, and unknown counts wherever zero-hour support is absent. Inventory coverage is not a requirement. No model is fitted or activated.

The immutable RAID build is `modeling_exports/three_hour/historical_station_panels/20261027T015443Z-80ae1edf` under the configured processed root. Its UTC build identifier falls on October 27; the local review date is October 26. The [validation receipt](validation/historical_station_panel_2026-10-26.json) pins its manifest and inputs. The build contains **26,178,555 station-origin-horizon rows**, **689 stations**, and 18 monthly feature/label pairs. Artifacts total 46,901,471 bytes. Existing dataset pointers and frozen experiments remain in place.

## Inputs and identity

The existing ledger-derived observed-hour export already covers March 2025 onward. This build reuses its checksummed monthly Parquet instead of reprocessing the trip archive. It retains source-release maturity under the existing conservative, unverified month-end-plus-45-day assumption, including all supplying releases in each target month. Released station demand-profile means still require valid exposure denominators; this package does not invent them.

The newer weather/subway input is Farhod's [October 23 five-table release](weather_subway_handoff.md), build `20261023T174914038037Z`. Its ZIP SHA-256 matches the published digest, and all 14 Parquet members match their manifest sizes and hashes. Recipient-side checks reconcile all five table counts and unique keys: 3,031,560 weather rows, 2,519 weather mappings, 331 bridge pairs, and 5,995,296 rows each for origin alert features and retrospective exposure. The retrospective exposure table is audited as an input artifact but never joined into predictors.

The weather and subway station keys are GBFS IDs, including numeric-looking strings; trip keys are Citi Bike `short_name` strings. The crosswalk uses the pinned October 15 GBFS reference, unique identifiers, and a maximum 25 m coordinate difference from the October 18 weather mapping. A pinned NYC borough polygon identifies 690 Manhattan weather mappings; one fails identity/location agreement, leaving 689 stations. This is a current-snapshot cohort. It can omit stations retired before October and include stations that had not opened in March 2025. A row in its historical grid does not prove the station existed or operated then.

The delivered subway bridge selects the nearest dock per subway location within 200 m. It does not cover every dock within 200 m. Unmapped stations stay missing. When the archive has no publications in the recent window, counts stay null; zero indicates no recent matching publication during a non-silent archive window, not confirmed normal service.

## Exact time alignment

For nominal origin `T`, horizon `h` predicts departures in `[T+(h-1) hours,T+h hours)`. All comparisons and joins use UTC; calendar features use New York time.

- Weather source origin is `T-1h`, with source horizon 1, 2 or 3. Its actual `target_hour_utc` must equal the departure target hour. Run initialization, assumed availability, fallback, missingness and build identifiers are retained. Availability must be strictly before `T-5min`.
- Subway source origin is `T-1h`. Its recent-publication window is therefore `[T-4h,T-1h)`, under the producer's zero-minute ingestion-delay assumption. The one-hour-old snapshot preserves the five-minute cutoff without trying to reconstruct event histories from an already aggregated table. Publication timestamps and source origin remain explicit.
- Observed monthly trip geography appears only in labels as a retrospective consistency diagnostic. It is excluded from predictors, as are target counts, label availability, and measured disruption exposure.

The original 32-station builder joined source weather horizon 1 at origin `T` to departures for hour `T`, although that weather actually targets `T+1h`. Its October 21 receipt is retained as historical structural/checksum evidence; its spatial-weather fields must not be used for model comparison. The corrected builder now selects the preceding weather origin and verifies target equality. Its validator rejects the archived misalignment. This defect did not activate a combined model. The old departure labels remain a separate, checksum-pinned source of bounded operation evidence.

## Splits and coverage

The original proposal's training and validation dates are retained; test ends August 10 because that is the agreed common source endpoint. An origin is omitted if any part of its three target intervals extends past a split or study endpoint. Each retained station-origin has exactly three rows.

| Split, New York dates | Rows | Weather eligible | Subway eligible | Both predictors | Known count | Unknown count |
|---|---:|---:|---:|---:|---:|---:|
| Train: March 2025–March 2026 | 19,638,567 | 19,390,527 | 3,473,820 | 3,428,460 | 12,524,578 | 7,113,989 |
| Validation: April–May 2026 | 3,021,954 | 3,021,954 | 535,248 | 535,248 | 2,096,694 | 925,260 |
| Test: June 1–August 10, 2026 | 3,518,034 | 3,480,828 | 625,212 | 618,408 | 2,610,061 | 907,973 |

These are retrospective development partitions. Earlier months have already been inspected in other studies; they are not a new untouched confirmation. Predictor coverage is not equivalent to evaluable labels.

**Main station evaluation remains gated.** The full training period has no independently supported operating-hour grid. Positive observed counts are retained; absent trips remain null. The preserved 32-station pilot supplies bounded operation evidence for 112,407 validation and 60,463 test rows, including 37,883 and 15,233 zero-count labels respectively. These counts repeat target hours across forecast origins/horizons; they are not unique hours. They cannot establish unconditional coverage for 689 stations or the full training history. All `historical_operation_verified` and `main_evaluation_eligible` flags remain false. Both repeated fall-back hours are excluded from usable labels when the original trip offset is unknowable.

Strict historical geography eligibility is zero in every split, as expected under the authorized retrospective mapping. Static geography, early ECMWF hindcasts, assumed source availability, archive silence and survivor selection remain limitations. The November stockout confirmation protocol, models, cohorts and schedules are unchanged.

## Artifacts and commands

| Artifact | Purpose |
|---|---|
| `features/YYYY-MM.parquet` | Station/origin/horizon keys, target/calendar fields, spatial forecasts and prior-origin publication features |
| `labels/YYYY-MM.parquet` | Separate departure counts, source-release maturity, bounded operation flags and monthly geography diagnostic |
| `station_crosswalk.parquet` | String UUID/short-name identity and retrospective mapping provenance |
| `protocol.json`, `manifest.json` | User decisions, splits, hashes, source versions, coverage and unresolved gates |

```sh
uv run --locked python scripts/build_historical_station_panel.py
uv run --locked python scripts/build_historical_station_panel.py --validate /absolute/build/path
```

The builder writes a new directory and refuses overwriting it. It verifies all source member hashes and table keys, then processes one origin month at a time with a 3 GB DuckDB memory cap and two threads. Validation checks artifact hashes, unique natural keys, exact feature/label key correspondence, all three horizons, target/issue timing, split purges, predictor/label separation and the unsupported-zero policy. No bulk data is committed to Git.

## Shared workspace handoff: pending authentication

The local package is validated. Workspace upload could not start because the `DATA622` OAuth refresh token expired. The following is the prepared destination and reading example, to use **after** a successful upload; it is separate from the older `station_hour_*_current` views:

```python
spark.conf.set("spark.sql.session.timeZone", "UTC")
panel_root = "/Volumes/workspace/citibike_3h/inputs/historical_station_panels/20261027T015443Z-80ae1edf"
features = spark.read.parquet(f"{panel_root}/features")
labels = spark.read.parquet(f"{panel_root}/labels")
crosswalk = spark.read.parquet(f"{panel_root}/station_crosswalk.parquet")
# Join labels only for analysis; keep them separate during feature preparation.
panel = features.join(labels, ["station_id", "origin_utc", "horizon", "target_hour_utc"], "left")
```

The [receipt](validation/historical_station_panel_2026-10-26.json) records the local validation and pending upload. After reauthentication, publish and independently download/hash-check every object with:

```sh
databricks auth login --profile DATA622
uv run --locked python scripts/publish_historical_station_panel.py \
  --package /Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/historical_station_panels/20261027T015443Z-80ae1edf \
  --receipt docs/validation/historical_station_panel_publication_2026-10-26.json
```

Access will use the existing shared `inputs` volume permissions. The publisher refuses different-content overwrites and changes no stable table/view, model, job or schedule. The former 32-station feature views still refer to the preserved historical build and carry the weather-alignment limitation above.
