# Station availability MVP

Randy Howk · October 16, 2026.

**The availability pilot is built and trained in Databricks Free Edition.** It predicts usable bike and dock counts, calibrated empty/no-dock probabilities and nominal 90% prediction intervals at 15, 30, 60 and 180 minutes. It uses the existing 32-station cohort independently of the live departure model. It does not estimate attempted demand or prescribe bike movements.

Open the [availability dashboard notebook](https://dbc-d00cbd4c-c74c.cloud.databricks.com/editor/notebooks/2830703976677819). Choose a station by its cross-street/name label (the stable station number appears in brackets) and a horizon, then **Run all** to refresh. The notebook has valid package/model defaults; it does not retrain models. A completed dashboard run contains saved outputs, while the notebook is the manual refresh surface. Inspect the reading/issue timestamps before treating a saved output as current. No new recurring schedule is enabled.

## What the results say

The [training run](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/runs/239441304535843) succeeded. The test contains **82,286 September station/issue/horizon examples across 31 stations**. Both bike and dock count models selected the 31-leaf histogram boosting candidate using August validation. Selection is across all four horizons; the model does not switch candidates after seeing September errors.

Some horizons share future observations and adjacent examples are dependent. These row counts are not independent customer observations; comparisons are descriptive, with no statistical-significance claim.

| Horizon | Bike MAE, model | Bike MAE, unchanged inventory | Dock MAE, model | Dock MAE, unchanged inventory |
|---|---:|---:|---:|---:|
| 15 minutes | 1.212 | **1.076** | 1.163 | **1.060** |
| 30 minutes | 1.632 | **1.613** | **1.582** | 1.591 |
| 60 minutes | **2.285** | 2.455 | **2.222** | 2.415 |
| 180 minutes | **3.932** | 5.042 | **3.858** | 4.974 |

At three hours, model MAE is about 22% lower for both outcomes. At 15 minutes, persistence is better. This is useful evidence for longer-horizon forecasting, not a claim that a learned model always improves on the current inventory. A future version can predeclare selection by horizon and evaluate it on a later window; this version retains its frozen selection rule.

September interval coverage is **89.67%–90.47%** across outcome/horizon combinations. Mean interval widths grow from 5.88 bikes / 5.43 docks at 15 minutes to 18.79 bikes / 16.59 docks at three hours. These are intervals for individual outcome/horizon forecasts, not simultaneous guarantees for every station and time. Time dependence and drift limit formal coverage guarantees, particularly for October live predictions calibrated on August.

Separate classifiers estimate the chance of zero usable bikes or zero usable docks at each target timestamp. Sigmoid calibration uses August 15–31. September Brier scores are lower than the “current zero/nonzero state stays unchanged” risk baseline at every horizon. For example, three-hour no-dock Brier is **0.0628 versus 0.1146**. Empty-bike events are rare: only 77–91 positive examples per horizon, so neither high overall classification accuracy nor this one-month study establishes reliable detection of every shortage. Calibration bins, average precision and prevalence baselines are saved in the result receipt.

## First live publication

The first successful refresh observed the public status feed at **21:00:03 UTC**, issued forecasts at **21:00:03 UTC**, and completed publication at **21:00:09 UTC** on October 16. It saved **124 forecasts across 31 fresh eligible stations**, with all target times still in the future. The remaining station had missing/ambiguous information and received no forecast. This validates collection/scoring/publication timing; it does not yet measure live predictive accuracy. See the preserved [first-refresh receipt](validation/availability_first_refresh_2026-10-16.json).

The dashboard shows current station status/counts, forecast counts/ranges, empty/no-dock probabilities, selected-station interval charts and the September comparison table. Unknown, closed, stale and changed-identity stations appear separately from empty/full states. Very small positive probabilities are not rounded into assertions of zero risk. Refresh snapshots and raw payload checksums are preserved in their own cloud volume directory.

## Data and target contract

The [frozen protocol](availability_protocol.json) predates model evaluation. The builder reads the previously acquired ctbk.dev raw status and dated information through the source manifest. It retains the existing cohort's string short-name identifiers and UUIDs; no trip-volume or performance-based reselection occurs. The raw archives and original trip pipeline remain unchanged.

There are **492,270 accepted examples out of 540,672 potential station/issue/horizon combinations**. Exclusion diagnostics overlap and must not be summed. Predictors and labels are stored separately and joined by an immutable `example_id`. The predictor allowlist includes station index, current bikes/docks/ebikes/disabled counts, dated capacity, calendar and horizon. Optional lag observations are retained for audit/future extensions, but excluded from this first model so a manual live refresh does not require unavailable minute-level history. There are no future trip endpoints, target inventories, weather or subway predictors in this version.

Historical issue times are hourly. Input is the latest actual station poll **strictly before issue**, no more than 120 seconds old. Provider age must be between −300 and 120 seconds; station-report age between −300 and 600 seconds. Counts must be nonnegative. Both rentals and returns must be enabled, and the station installed. Select the most recent observation before quality filtering; an older good reading cannot replace a newer stale/closed reading.

The label is the first actual poll at or after issue plus the requested horizon, no more than 120 seconds later. It must independently meet the same freshness/service checks. There is no interpolation through missing target readings. Future closed states are censored and counted, not labeled as zero inventory. Thus evaluation is **conditional on continued service and endpoint coverage**, not an unconditional probability of being able to complete a trip. Empty/no-dock risk refers to the target timestamp, not any shortage that might occur between issue and target.

Dated station information uses the prior pilot's conservative scenario: availability no earlier than day-end plus one hour or its provider timestamp, whichever is later. Identity/location must be stable against previous and initial observations; unknown identifiers invalidate earlier mappings; metadata expires after 49 hours. Strict reference boundaries can exclude origins rather than using newly available information at the exact cutoff. Exact intraday moves and historical metadata publication times remain unverified. For live scoring, actual retrieval timestamps establish availability of fresh current information.

These are reconstructed historical observations, not forecasts actually issued during April–September. Inventory includes unidentified operator and maintenance effects. Counts are forecast separately; the model does not enforce a controlled fleet-flow balance or identify how inventory would change under a new rebalancing policy.

## Training and uncertainty

The October 20 [ensemble result](ensemble_results.md) compares persistence, the existing histogram boosting candidates and bounded random forests, selecting counts separately by outcome/horizon on August validation and recalibrating intervals afterward. No forest route is selected. Bikes use persistence at 15/30 minutes and 31-leaf boosting at 60/180; docks use persistence at 15 and boosting thereafter. This uses inspected September development evidence and retains the existing boosting risk specification. It does not replace the completed MVP or change the manual dashboard model before later confirmation.

| Partition | New York dates | Accepted model rows |
|---|---|---:|
| Initial training | April 8–July 31 | 321,420 |
| Model selection | August 1–14 | 39,460 |
| Refit | April 8–August 14 | 361,128 |
| Calibration | August 15–31 | 48,116 |
| Test | September 1–30 | 82,286 |

Origins whose maximum horizon plus the two-minute label tolerance crosses a partition boundary are purged. Every supplying target observation must precede that partition's endpoint. Refit can recover examples at the internal training/validation boundary, so its count need not equal their sum. These directly observed inventory labels do not require the 45-day monthly trip-file lag.

Count candidates are unchanged inventory and histogram gradient boosting with 15 or 31 leaves, 60 iterations, fixed seed and no random early-stopping validation split. Candidates select on validation MAE separately for bikes/docks. Fixed 15-leaf classifiers fit the zero-count events. The selected learners refit through August 14; August 15–31 supplies probability calibration and finite-sample 90% absolute-residual quantiles by outcome/horizon. No model is refit after calibration. Selections, calibrated components and models are persisted before September scoring.

Intervals have nonnegative lower bounds and no unsupported hard upper clipping to a possibly changing reported capacity. The dashboard rounds interval bounds outward. A range extending past today's reported capacity is not a recommendation to stock that many bikes.

## Artifacts and verification

The [evidence receipt](validation/availability_mvp_2026-10-16.json), [input audit](validation/availability_package_2026-10-16.json) and [independent result audit](validation/availability_results_2026-10-16.json) record hashes, runs, timestamps and scores. Twelve input checks validate source polls, service flags, inventory values, temporal keys and feature/label separation. Independent SQL reproduces all eight outcome/horizon count, interval and Brier-score summaries. Model save/reload matches exactly on 100 held-out examples. **261 tests pass**. The original proposal and active departure pointer are unchanged.

Local package: `modeling_exports/three_hour/availability_packages/20261016T205258Z-ce84473b` under the processed RAID. Cloud model/report: `/Volumes/workspace/citibike_3h/exports/availability_pilots/20261016T205658Z-53093e33`. MLflow run: `5704a1f2a5a54a06abd2ad978a823f27`.

Current imports use the stable tables `workspace.citibike_3h.availability_test_predictions` and `workspace.citibike_3h.availability_live_predictions`; `dataset_version` identifies their immutable model or refresh package. The dated validation receipts retain the original table names recorded when this run was first executed.

Live observations and publication receipts are stored under `/Volumes/workspace/citibike_3h/exports/availability_live/<refresh-version>`; `exports/availability_dashboard_latest.json` identifies the latest successful manual refresh. Its report is required when interpreting publication timing. The existing departure, health and public-evidence schedules are not changed.

## Reproduction

Use the canonical root and locked environment. Keep bulk RAID stages sequential. The saved package is immutable after cloud upload; a changed builder/protocol requires a new package directory.

```sh
uv run python scripts/build_availability_pilot.py \
  --sources "$(cat .local/execution/station_pilot_sources.txt)" \
  --cohort-root "$(cat .local/execution/station_pilot_package.txt)" \
  --root /absolute/new/availability_package
uv run python scripts/audit_availability_pilot.py \
  --package /absolute/new/availability_package \
  --output /absolute/input-audit.json
uv run python scripts/deploy_availability_pilot.py train --package /absolute/new/availability_package
uv run python scripts/fetch_availability_result.py train
```

```sh
uv run python scripts/deploy_availability_pilot.py dashboard \
  --package "$(cat .local/execution/availability_package.txt)"
uv run python scripts/fetch_availability_result.py dashboard
uv run python scripts/audit_availability_results.py \
  --result "$(cat .local/execution/availability_result_root.txt)" \
  --output docs/validation/availability_results_2026-10-16.json
uv run python scripts/record_availability_validation.py
```

Ordinary use only needs **Run all** in the dashboard notebook. It refreshes readings, scores the frozen availability model and records a new snapshot; training is a separate action. Attempted-demand estimation, movement optimization and operator trials remain subsequent work.
