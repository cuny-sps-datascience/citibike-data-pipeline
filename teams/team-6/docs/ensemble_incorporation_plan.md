# Ensemble model incorporation plan and execution

Randy Howk · Planned October 19, 2026 · Executed October 20, 2026

Add random forests and histogram gradient boosting to the station departure comparison, then compare random forests with the availability MVP's existing boosting models. The primary question is whether flexible station, calendar and weather interactions improve forecasts enough to justify their compute and serving costs. A useful result can be a retained Poisson or persistence baseline.

The implementable development stages are complete. The [result report](ensemble_results.md) records the local/cloud comparisons, selection outcomes and evidence. Separate [station](station_ensemble_protocol.json) and [availability](availability_ensemble_protocol.json) protocols preserve the new freeze; the earlier protocols and artifacts remain unchanged. No model was activated. Later confirmation remains pending because its outcomes do not yet exist.

## Scope and order

| Priority | Experiment | Comparison | Deliverable |
|---|---|---|---|
| 1 complete | Conditional station departures | Calendar and weather Poisson versus random forest and histogram boosting | Weather histogram boosting selected; cloud result independently audited |
| 2 complete | Conditional station inventory | Persistence and existing histogram boosting versus random forest | Per-horizon persistence/boosting routing selected; no forest route selected |
| 3 deferred | Zero inventory risk | Existing calibrated boosting versus a random forest classifier | Separate probability study after later count confirmation |
| Deferred | Manhattan departures, weighted blends and stacking | Extend only after the station comparison is complete | Separate protocol and measured compute budget |

Random forests average randomized trees; gradient boosting fits trees sequentially to improve earlier predictions. Both are ensemble methods. The [availability MVP](availability_mvp.md) already implements boosting for counts and zero-count classification. The [three-hour plan](three_hour_mvp_databricks_plan.md) already identifies histogram boosting with Poisson loss as a departure extension. See the [scikit-learn 1.7 ensemble guide](https://scikit-learn.org/1.7/modules/ensemble.html).

Retain the 32 metadata-selected stations, string identifiers, conditional operating-hour/endpoint policies and dated geography. Eligibility can yield fewer evaluation stations. Do not select stations using errors or trip volume. The original 2025 scope, neighborhood exporter and exact alert ablation keep their existing gates. Missing/stale advisories cannot become zero alerts. Inventory forecasts do not measure departures, unmet demand or the effect of rebalancing.

## Station departure experiment

### Features and comparisons

Use the existing [station pilot](station_history_pilot.md) package as the immutable source. Keep its target intervals, label counts, source-month availability checks, weather proxy, five-minute origin cutoff and conditional operation rules. Reproduce the existing Poisson outputs before adding estimators.

Run two feature arms on the same weather-covered station/origin/horizon keys: calendar, and calendar plus weather. Within each arm compare Poisson, random forest and histogram boosting. This separates the benefit of a model family from the benefit of adding weather. Calendar candidates also use the common weather-covered keys in the primary comparison; any broader calendar-only evaluation is reported separately.

Poisson retains its existing `station_hour`, `dow_hour` and horizon categories. Trees receive explicit station ID, target local hour, target weekday and horizon, with the existing temperature, squared temperature, precipitation, wind and wet indicator in the weather arm. Derive hour and weekday from the target timestamp using `America/New_York` and verify agreement with the existing categories. These are different representations of the same information; label the comparison accordingly. Any later addition of profiles, holidays or inventory is a separate feature experiment.

Fit category encoders on the fitting partition only. Use one-hot station identity for forests and an explicit training-fitted categorical mapping for histogram boosting. Do not treat station IDs as ordered numbers. Preserve feature names, order and unknown-category behavior in the model artifact; report previously unseen stations separately and use the existing serving eligibility policy. Scaling remains part of the Poisson pipeline; trees do not require it. Do not add recent trip lags: they are unavailable under the monthly trip-release contract.

### Bounded candidates

| Family | Configurations per feature arm | Fixed settings |
|---|---:|---|
| Poisson | 2 | Existing alpha 0.1 and 1.0; existing convergence checks |
| Random forest | 2 | 150 trees; depth 10 or 16; at most 256 leaves/tree; minimum leaf size 80; `max_features=0.7`; bootstrap with `max_samples=0.7`; `criterion="poisson"`; `n_jobs=4`; seed 622 |
| Histogram boosting | 1 | Poisson loss; 60 iterations; 15 leaves; minimum leaf size 80; L2 regularization 1.0; learning rate 0.1; `early_stopping=False`; seed 622 |

This permits ten initial fits across the two arms and up to six family/arm refits. Fit on the same rows for every candidate. Require nonnegative labels and positive total training counts for Poisson-loss candidates; record a failed prerequisite rather than changing labels. The [forest API](https://scikit-learn.org/1.7/modules/generated/sklearn.ensemble.RandomForestRegressor.html) supports Poisson splitting; the [histogram boosting API](https://scikit-learn.org/1.7/modules/generated/sklearn.ensemble.HistGradientBoostingRegressor.html) supports Poisson loss. Neither choice supplies calibrated departure intervals. Interval estimation is outside this first departure extension.

### Development windows and selection

Reuse the completed study's windows for a descriptive development comparison:

| Stage | New York target dates | Availability requirement |
|---|---|---|
| Training | April 8–30, 2026 | Each label available before the June 15, 23:55 fit cutoff |
| Validation | June 16–30, 2026 | Each label available before the August 31, 23:55 selection/refit cutoff |
| Refit | April 8–June 30, 2026 | Each label individually available before August 31, 23:55 |
| Development evaluation | September 1–30, 2026 | Already inspected; additional comparisons are retrospective development evidence |

Retain strict availability checks using the latest release of the target month and every supplying source cohort, under the existing month-end-plus-45-days assumption. Purge origins whose three target intervals cross a partition boundary. Historical cutoffs are reconstruction scenarios; new artifacts record their actual creation times as well.

Choose the configuration with lowest pooled validation MAE within each family/arm, then identify the overall validation winner. For ties within absolute MAE tolerance `1e-9`, prefer Poisson, histogram boosting, then forest; within Poisson prefer alpha 1.0, within forests prefer depth 10, then use lexical candidate ID. Resource failures disqualify a candidate before selection and remain visible. Save the complete selection record before refitting or computing September metrics. Refit the selected family/arm candidates, preserving their configurations, and report every selected comparison rather than choosing a new winner from September.

Do not use shuffled cross-validation, random early-stopping partitions or forest out-of-bag scores for forecast selection or final accuracy claims. All stations and horizons at an origin belong to the same temporal partition. Bootstrap sampling inside the forest's training set remains permitted. The [scikit-learn cross-validation guide](https://scikit-learn.org/1.7/modules/cross_validation.html) explains why time-dependent samples require chronological evaluation.

The current station runtime's candidate names, convergence metadata and fitting logic assume Poisson. Extend these through explicit estimator-family configuration; do not infer a forest configuration from an alpha-shaped name or require Poisson iteration metadata from every learner.

## Availability count experiment

Keep the [availability protocol's](availability_protocol.json) predictors, strict issue-time input selection, 120-second target tolerance, closure censoring and separate bike/dock targets. Retain the April 8–July 31 training, August 1–14 selection, April 8–August 14 refit, August 15–31 calibration and September development-evaluation windows. Purge maximum-horizon-plus-tolerance boundary crossings exactly as in the current runtime.

For each outcome, fit the existing 15- and 31-leaf, 60-iteration histogram candidates plus the two bounded forest configurations above, using `criterion="squared_error"` for forests. Keep the existing boosting settings. Persistence requires no fit. This is eight initial count fits and at most eight unique count refits across both outcomes. Forests encode station identity in their saved pipeline; boosting retains its explicit categorical station treatment. Weather, advisories and optional lag observations remain outside this count experiment.

For the new version, choose the lowest validation MAE candidate separately for each outcome/horizon. Pool horizons for training, then select using each horizon's validation rows; do not train a new model for every horizon. Refit each selected outcome/configuration once and store the outcome/horizon routing map. Ties within `1e-9` prefer persistence, then smaller boosting, then shallower forest. The completed MVP's pooled selection rule remains unchanged in its original artifacts.

Calibrate nominal 90% absolute-residual intervals by outcome/horizon on August 15–31 predictions from the selected routing map. No calibration rows enter model selection, and no learner refits after calibration. Keep nonnegative lower bounds and the existing policy against unsupported upper clipping to today's capacity. Time dependence and drift limit coverage guarantees. Tree-to-tree variation is not a calibrated interval.

Retain the existing fixed boosting risk specification and separate sigmoid calibration while testing count models. Freeze all fitted components before development scoring. The dashboard must distinguish a new count-model version from its risk-model version. Later risk experiments need their own protocol, classifier selection and subsequent calibration windows, rare-event counts, Brier scores, average precision and reliability bins. Class weighting or resampling cannot silently alter the probability target.

## Confirmation on future outcomes

All June–September periods above are development data for this extension. A timestamped protocol must name the final candidates, selected configurations, features, thresholds, model hashes and confirmation dates before any confirmation outcomes are inspected. An independent result audit checks reproduction; it does not make a reused month an untouched test.

The earliest planned confirmation window is November 1–30, 2026, in New York time, conditional on freezing before November 1 and obtaining the required source coverage. If the freeze is later, use the next complete New York calendar month after freezing. Record the chosen dates in the new protocol before that window begins. Preserve the existing cohort and report exclusions rather than changing membership to improve coverage. Source availability for this future window is a prerequisite, not an assumption that the current April–September archives already cover it.

The initial confirmation uses the development-selected refitted models, with the inventory calibration above, frozen before the window. If fresher training or calibration is needed, define and freeze new chronological partitions and mature-label cutoffs in a new protocol before confirmation starts. Do not adapt models or selection using confirmation outcomes.

For a prospective claim, issue and persist predictions before their targets in a separately versioned workflow and record actual publication times. Manual inventory refreshes support only their observed issuance cadence; they do not establish complete hourly coverage. A later reconstruction using archived inputs must be labeled retrospective even if its model was frozen in advance. Changes to collection cadence or operational deployment are a separate implementation task.

Inventory labels mature at observed endpoint polls. Departure confirmation waits for complete validated monthly inputs and all supplying cohorts; under the 45-day assumption, November labels cannot be considered fully available before January 2027, and later spillovers can delay them further. Consequently November departure accuracy is not a promised December project deliverable. Earlier verified publication evidence can support a separately documented actual-release analysis. Inventory confirmation may be available sooner if endpoint observations are acquired. Until outcomes mature, report coverage and pending counts.

## Metrics and interpretation

Use identical eligible keys within every paired comparison. Report forecast-row counts, distinct origins, unique station/target hours, stations, missing-source exclusions and label maturity. A model failure must not silently remove difficult evaluation rows; mark the candidate incomplete or apply only a fallback declared before scoring.

For departures, report MAE, RMSE, Poisson deviance and prediction-minus-actual bias, pooled and by horizon, wet/dry conditions, target hour and training-defined station activity. For inventory, report MAE/RMSE against persistence and the original frozen boosting model by outcome/horizon, plus interval coverage and mean width. Retain risk metrics separately. Never compare departure and inventory MAE as if they measured the same outcome.

Save paired daily error differences and station-level breakdowns. For descriptive uncertainty, bootstrap contiguous seven-day blocks of origin dates, retaining all stations and horizons together, with 1,000 replicates and seed 622. Report the short number of effective time blocks and sensitivity to dependence; a one-month interval alone does not establish significance or generalization to other stations. Use held-out feature-group ablations to discuss weather's predictive contribution. Feature importance is associative, not causal evidence of weather or transit effects.

Keep the operational incumbent unless the declared validation rule selects a usable alternative. Describe September gains as development results. A final recommendation considers later confirmation, per-horizon regressions, coverage, runtime, memory and artifact size; a lower pooled error alone does not justify operational replacement. Record unsuccessful candidates and retained baselines in the report.

## Implementation sequence and verification

All paths below are relative to the canonical repository. Proposed files are marked as new and are not existing commands.

| Stage | Implementation work | Exit condition |
|---|---|---|
| 1. Freeze inputs and protocols | New `docs/station_ensemble_protocol.json` and `docs/availability_ensemble_protocol.json`; pin existing manifests, feature arms, candidates, cutoffs and confirmation policy | Complete; historical protocols/artifacts preserved and new hashes recorded |
| 2. Add departure estimators | Versioned `station_ensemble_runtime.py`, package builder, audit/deploy scripts and notebook 24 | Complete; Poisson reproduced and estimator-specific artifacts verified |
| 3. Run departure development | Build a new versioned package, run local audit, submit sequential cloud fits, fetch and independently audit predictions | Complete; selection saved before September scoring and cloud metrics reproduced |
| 4. Add inventory comparison | Versioned `availability_ensemble_runtime.py`, horizon routing, recalibration, package/audit/deploy scripts and notebook 25 | Complete; routing and intervals survive save/reload; dashboard activation remains pending confirmation |
| 5. Freeze confirmation | Record exact future dates, hashes, source/issuance plan and pending-label rules in each new protocol | No inspection of confirmation outcomes before freeze; deployment requirements resolved before prospective issuance |
| 6. Report and integrate | Update model reports, project documentation and the whitepaper after evidence exists | Complete for development evidence; dashboard integration awaits confirmation |

Add meaningful checks in `tests/test_station_history.py` and `tests/test_availability.py`, with new runtime-focused tests if needed: category handling and feature order; same-key comparisons; strict label maturity and partition purges; validation-only selection; outcome/horizon routing; calibration isolation; finite nonnegative predictions; and save/reload parity for every estimator family. Compare preserved Poisson outputs to catch unintended feature changes. Independently recompute metrics from saved predictions, then run the existing suite using the repository's locked `uv run` workflow.

Use separate versioned runtimes, notebooks and scripts so completed protocols still reproduce. Fail clearly when a reader receives an unsupported result schema. Store estimator family, feature arm, configuration, routing, training cutoffs, protocol/package hashes, dependency versions, code hash, selection receipt and prediction artifact together. New packages may reference unchanged immutable data, but must not overwrite the old package or claim its historical freeze time.

## Compute and artifact controls

Run candidates sequentially with at most four fitting threads; keep RAID stages sequential and original sources read-only. Begin with a deterministic training-only smoke sample to verify imports, encoding and save/reload; its errors are not model-selection evidence. Use the full existing compact training partitions for the declared experiment. If they exceed budget, freeze an amended sampling protocol with the same training rows for all families before the full comparison, and disclose the change. Always evaluate all eligible holdout keys in batches.

Initial project targets are fifteen minutes per fit, approximately 3 GB process memory, 100 MB per serialized estimator and two minutes per batch scoring run. These are measured stop/review budgets, not platform quotas. Record wall time, peak memory, artifact size and failures. Do not silently reduce a failed candidate's settings or choose a sampling scheme after inspecting evaluation errors; resource-driven changes receive a new protocol version. Total candidate/refit budgets must also fit available workspace usage before submission.

Save bulk inputs, models and predictions in new versioned directories under the processed RAID's `modeling_exports/three_hour/` and matching private cloud volume exports. Commit only code, protocols, documentation and small audit receipts. Follow the [catalog retention policy](catalog_retention_policy.md), using stable tables with `dataset_version` when imports are implemented; retain immutable artifacts without creating a timestamp-named table for every fit. Development artifacts do not change active model pointers, dashboard defaults or recurring jobs. Operational integration must preserve existing fallback and prediction-idempotency contracts and have a concrete rollback pointer.

## Completion checklist

- [x] New protocols and immutable input references recorded.
- [x] Existing Poisson behavior reproduced; bounded departure candidates implemented and verified.
- [x] Development selections, paired metrics and compute measurements independently reproduced.
- [x] Inventory count comparison and horizon routing implemented; intervals recalibrated separately from selection.
- [ ] Exact later confirmation window frozen; sources, issuance status and pending outcomes documented.
- [x] Development report distinguishes available evidence from pending confirmation; retained candidates and fallbacks explained.

The remaining unchecked item requires future source coverage and outcomes. It cannot be completed from the October 20 evidence.
