# Public station-history pilot

Randy Howk · October 16, 2026 · Independent of the live Manhattan model.

The public station archive supplies a route to a smaller 2026 station forecast study without class data. This pilot reconstructs three hourly departure targets for 32 stations selected from dated information before looking at station outcomes. It does not restore the original 2025 training window. Exact subway-alert ablation has an additional archive-schema gate described below.

The same cohort now also has a separate [availability MVP](availability_mvp.md) that forecasts observed bikes/docks and shortage probabilities from status history. Its inventory errors and direct observation timing are different from the departure targets and monthly release rule below. Neither model yet estimates unmet demand or optimal movements.

## Executed result

The [Databricks Free Edition run](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/runs/140677514025666) succeeded. On **58,965 common September forecast rows across 31 eligible stations**, weather Poisson achieved **MAE 3.4803 departures per station-hour**, compared with **3.6328** for station/calendar Poisson: a descriptive **4.20% reduction**. Both families selected alpha 0.1 using June validation. These are conditional retrospective results; no station model was activated.

| September metric | Calendar | Weather |
|---|---:|---:|
| MAE | 3.6328 | 3.4803 |
| RMSE | 5.0197 | 4.8892 |
| Poisson deviance | 4.9819 | 4.6885 |
| Mean prediction minus actual | −0.1498 | −0.1526 |

The test has 690 distinct origins, 19,717 distinct station/target hours and 18,351 zero-label forecast rows. Repeated horizons share outcomes and are not independent observations. Weather MAE by horizon 0/1/2 is 3.4766 / 3.4776 / 3.4866. The mean actual count is 4.3090. No uncertainty/significance claim is made from this one-month, geographically restricted study.

Training contains 50,243 examples; validation 29,806; refit 175,253. All 32 cohort stations have training/refit examples. Greenwich St & Rector St (`5073.07`) has no eligible September examples, so 31 stations enter the test. This is a coverage exclusion, not a prediction of zero demand. No test station is absent from refit.

The complete acquisition contains **534 files / 12,784,149,798 bytes**, including 620,683,413 network status observations and 344,159,765 flattened alert rows. Cohort processing retains 7,908,087 unique station/poll observations, including 51,505 fresh renting observations with empty inventory. There are 123,148 bounded supported station-hours across the full archive. The modeling windows yield 78,287 unique supported labels, including 24,106 zeros, and 234,238 forecast examples. The build excludes 26,489 unsupported-operation examples and 1,545 missing-weather examples; no monthly geography diagnostic exceeds 100 meters.

The dated bike/subway mapping contains **20,856 configuration/reference pairs**. Of 718 September origin cutoffs, 528 have semantically complete, fresh advisory snapshots and 190 fail freshness. April–June's 2,014 origins fail the legacy-schema gate. Thus there are 42,792 rich advisory examples in test and **zero in training/validation/refit**. The exact alert ablation did not run; no missing alert input was encoded as zero.

The [evidence receipt](validation/station_pilot_2026-10-16.json), [package audit](validation/station_pilot_package_2026-10-16.json) and [independent result audit](validation/station_pilot_results_2026-10-16.json) record versions and measurements. All 13 result checks pass; recomputed MAEs match saved predictions. Save/reload matches exactly on 100 held-out rows. **256 local tests pass**. The original proposal checksum and live aggregate model pointer are unchanged.

Future cloud runs replace the stable current tables `station_pilot_predictions`, `station_pilot_features` and `station_pilot_mapping`; every row records the immutable result package in `dataset_version`. The dated validation receipt retains the original table names recorded by the completed pilot. Those superseded catalog tables were later removed after their immutable volume and RAID artifacts were verified.

Models, selections and reports are saved under `/Volumes/workspace/citibike_3h/exports/station_pilots/20261016T201856Z-d67ba148`; MLflow run `3883a342bd6a4027ba34dd1761da2380`. Local immutable inputs are `station_pilot_packages/20261016T200304Z-7c2377e9`, with the results mirrored under `station_pilot_results/20261016T201856Z-d67ba148` in the processed RAID's three-hour export root.

## Fixed study design

The [protocol](station_pilot_protocol.json) records the station-selection rule, predictors, temporal windows, candidates and quality conditions. The [first protocol](station_pilot_protocol_v1.json) is retained. Its amendment precedes all station validation/test metrics and rejects older alert files whose omitted fields prevent complete matching. Station calendar/weather training proceeds independently of that alert gate.

The cohort is the first 32 Manhattan string short-name identifiers in lexical order among stations with unique UUID/short-name pairs, positive recorded capacity and stable locations in the April 6/7 information snapshots. It includes lower Manhattan and Governors Island; it is not a representative Manhattan sample. Membership does not depend on trip counts or September performance. Coordinates must remain within 25 meters of both the previous observed configuration and the initial cohort location. Known missing/ambiguous IDs invalidate the earlier mapping. Monthly trip-coordinate means are diagnostics only; a discrepancy above 100 meters rejects that station-month rather than establishing its geography.

| Stage | Target hours, New York time | Model/selection cutoff |
|---|---|---|
| Initial training | April 8–30 | June 15, 23:55; individually mature labels only |
| Validation | June 16–30 | Labels must be mature before August 31, 23:55 |
| Refit | April 8–June 30 | August 31, 23:55; individually mature labels only |
| Test | September 1–30 | August 31, 23:55; no test-driven tuning |

Cutoffs in the table are local time. Source-month end plus 45 days at 00:00 UTC is the assumed trip-release rule. A label's availability is the latest of its own target-month release and every source cohort supplying its trips. This applies to zero labels too. April labels can train a June 16 model, but cannot train a forecast issued in May under this rule. July spillovers that mature September 14 cannot enter September 1 selection/refitting. Every included origin's three target intervals stay within its partition.

No station labels were used to select the cohort. September aggregate outcomes were previously inspected during the aggregate project; this station study is reconstructed retrospective evidence rather than a newly issued forecast or a claim of an untouched project-wide test set.

## Supported hours and geographic assumptions

[ctbk.dev](https://github.com/hudcostreets/ctbk.dev) supplies raw full-network status observations. `polled_at` is collection time; `ts` is provider time. Installation/renting flags must both equal one. Provider age must be between −300 and 120 seconds; station-report age must be between −300 and 600 seconds. Consecutive good polls must be at most 120 seconds apart. Only whole hours completely inside a connected chain qualify. Missing/bad polls split the chain; there is no extrapolation before or after it.

An empty bike inventory does not remove a fresh renting station. Departures, including supported zeros, come from complete ledger-selected trip inputs, not inventory changes. The observed archive and its canonical station eligibility remain unchanged. This pilot uses a **conditional operating-hour population under bounded interpolation**, not proof that the station stayed continuously open between observations.

Daily information has no retained exact historical collection time. The explicit scenario makes a file available no earlier than 01:00 UTC after its nominal date, and never before its provider timestamp. The most recent strictly eligible, stable observation is used; older configurations expire after 49 hours. These assumed availability times do not establish the first public release or the exact intraday time of a station move.

Dated MTA schedules from [Mobility Database](https://mobilitydatabase.org/feeds/gtfs/mdb-516) supply 496 parent-station points. Archive versions must predate the cutoff; service calendars and exceptions filter route/platform associations on the origin's New York date. Bike-to-subway distances use each eligible dated bike configuration, with 500 meters primary and 250 meters descriptive. Proximity and scheduled associations do not prove a train was operating at the target hour.

## Alert schema gate

The [gtfsrt.io inventory](https://storage.googleapis.com/parquet.gtfsrt.io/inventory.json) provides public daily flattened subway-alert Parquet with UTC fetch times. A complete snapshot needs the full `active_periods_json`, entity-selector `direction_id`, trip descriptor restrictions, incrementality and deletion fields. Fields within a selector are ANDed; selectors are ORed; entities are counted once per bike station. A snapshot must be collected strictly before the forecast cutoff, at most two minutes earlier, and have provider age between −300 and 900 seconds. Stale or unsupported observations remain null, never zero alerts or normal service.

**The full audit finds 122 older-schema dates, April 7–August 6; complete fields begin August 7, covering 56 dates.** All 534 object checksums pass. The publisher's [field audit, issue 91](https://github.com/JarvusInnovations/gtfs-realtime-archiver/issues/91) documents discarded extra display periods and selector direction fields. Adding nullable columns does not reconstruct what those files discarded. The September sample's richer schema therefore cannot establish equivalent April–June training-history semantics. Raw protobuf access was denied in the earlier investigation; no access bypass is attempted. See the [full source audit](validation/station_pilot_sources_2026-10-16.json).

The complete advisory comparison runs only if release-eligible examples with preserved semantics exist in every training, validation, refit and test partition. Otherwise the package retains supported station/weather examples, maps subway proximity, audits richer alert history, and reports the exact alert comparison as incomplete.

To clear this gate without class data, the archive publisher could recompact April 7–June 30 subway-alert raw snapshots using the current extractor, preserving all selectors, periods and feed headers. Alternatively supply an equivalently timestamped full-field archive. The publisher describes re-materialization as the supported backfill route while raw files remain within retention. This is a specific source-field requirement, not a Databricks account-access issue. No maintainer message or access request has been sent.

A different future study could fit fixed advisory candidates on the rich August observations after their labels mature and then issue prospective forecasts. Those August labels cannot train a September 1 forecast under the 45-day rule. Such a prospective comparison needs later issued predictions and outcomes; it is not a substitute for missing April–June fields in this fixed historical fold.

## Models and validation

The October 20 [ensemble result](ensemble_results.md) compares bounded random forests and histogram boosting with these Poisson references in calendar and weather feature arms. On the same 58,965 September rows, weather histogram boosting MAE is 2.1338, random forest 2.8964 and Poisson 3.4803. June validation selected boosting before those metrics. This later comparison reproduces the Poisson result and remains inspected development evidence; it does not alter this completed pilot, activate a station model or clear the alert gate.

Candidate Poisson models use station/hour, weekday/hour and horizon categories. The weather family adds forecast temperature, temperature squared, precipitation, wind and a wet indicator. The optional advisory family adds distinct matched entities with current display periods, periods overlapping the next three hours and unspecified periods. Existing archived ECMWF IFS forecast runs use the Manhattan point proxy, initialization plus seven hours strictly before the five-minute origin cutoff, and the original precipitation interval alignment.

Each family selects regularization from 0.1 and 1.0 using common-key June MAE. Selections are written before September evaluation. Selected families are refitted on individually mature April–June labels. Report MAE, RMSE, Poisson deviance, bias, zero-label counts, horizon metrics and paired daily errors. Alert comparisons, when eligible, use identical station/origin/horizon keys. Matched advisories are not measured disruptions or causal effects.

Full raw archives and narrow checkpoints stay under the processed RAID's `modeling_exports/three_hour/station_pilot_sources` and `station_pilot_packages`. Portable inputs, separate model files and MLflow evidence are submitted to the existing private Databricks Free Edition workspace. Stable current prediction/mapping tables carry `dataset_version`; versioned packages remain in volumes and on the RAID. Serverless environment 5 pins scikit-learn 1.7.2. Nothing activates a station model or changes recurring production jobs.

## Reproduction

Run from the canonical project root with the locked environment. Local state files contain absolute package paths, not credentials.

```sh
uv run python scripts/collect_station_pilot_sources.py \
  --research "$(cat .local/execution/alternative_research_root.txt)" \
  --root "$(cat .local/execution/station_pilot_sources.txt)"
```

```sh
uv run python scripts/audit_station_pilot_sources.py \
  --sources "$(cat .local/execution/station_pilot_sources.txt)" \
  --output docs/validation/station_pilot_sources_2026-10-16.json
```

```sh
uv run python scripts/build_station_pilot.py \
  --sources "$(cat .local/execution/station_pilot_sources.txt)" \
  --root "$(cat .local/execution/station_pilot_package.txt)"
```

```sh
uv run python scripts/audit_station_pilot.py \
  --package "$(cat .local/execution/station_pilot_package.txt)" \
  --output docs/validation/station_pilot_package_2026-10-16.json
```

```sh
uv run python scripts/deploy_station_pilot.py \
  --package "$(cat .local/execution/station_pilot_package.txt)"
```

```sh
uv run python scripts/fetch_station_pilot_result.py
uv run python scripts/audit_station_pilot.py \
  --package "$(cat .local/execution/station_pilot_package.txt)" \
  --result "$(cat .local/execution/station_pilot_result_root.txt)" \
  --output docs/validation/station_pilot_results_2026-10-16.json
uv run python scripts/record_station_pilot_validation.py
```

Acquisition resumes from checksummed receipts. Keep download, RAID integrity audit and build stages sequential. A build resumes narrow partitions and advisory checkpoints only with the same pinned inputs/protocol. Another cloud submission creates a separate retrospective result version, not a production release. Record its run status before describing results as trained or validated.
