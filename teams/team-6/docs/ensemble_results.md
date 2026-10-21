# Ensemble model development results

Randy Howk · October 20, 2026

The bounded ensemble comparisons are implemented and executed locally and in Databricks Free Edition. Histogram boosting is the strongest station-departure candidate. Random forest improves substantially over Poisson for station departures but does not match boosting. Random forest is not selected for any inventory outcome/horizon; the availability result retains a mixture of persistence and the existing 31-leaf boosting model.

These are retrospective development results on previously inspected September outcomes. They do not activate a model, change a schedule or supply future confirmation. The [incorporation plan](ensemble_incorporation_plan.md), [station protocol](station_ensemble_protocol.json) and [availability protocol](availability_ensemble_protocol.json) define the frozen candidates and gates.

## Station departure comparison

June validation selected the 15-leaf weather histogram boosting candidate overall. Within each family and feature arm, validation selected Poisson alpha 0.1, the fixed 15-leaf boosting model and the depth-16 forest. The selections were saved before refitting or reading September metrics.

All selected models use the same 58,965 September station/origin/horizon rows across 31 eligible stations. The Poisson results reproduce the October 16 pilot within numerical roundoff.

| Feature arm and model | Validation MAE | September MAE | September RMSE | Poisson deviance |
|---|---:|---:|---:|---:|
| Calendar Poisson | 3.9126 | 3.6328 | 5.0197 | 4.9819 |
| Calendar random forest | 3.2299 | 2.7385 | 4.1328 | 3.1604 |
| Calendar histogram boosting | **2.7563** | **2.1976** | **3.4534** | **2.2086** |
| Weather Poisson | 3.8650 | 3.4803 | 4.8892 | 4.6885 |
| Weather random forest | 3.2614 | 2.8964 | 4.4052 | 3.4186 |
| Weather histogram boosting | **2.6677** | **2.1338** | **3.3219** | **2.0516** |

Weather histogram boosting reduces September MAE by **38.69%** relative to weather Poisson. Weather random forest reduces it by **16.78%**. Adding weather improves the selected boosting MAE by **2.90%** relative to calendar boosting, while weather forest performs **5.77% worse** than calendar forest. Thus the evidence supports nonlinear ensembles for this station problem, but it supports histogram boosting more strongly than random forest.

Seven-day block resampling uses only five September blocks and is descriptive. Its 95% interval for weather boosting minus calendar Poisson MAE is −1.697 to −1.246 departures per station-hour. This does not establish generalization beyond this month and cohort.

The cloud refits use Python 3.12.3, scikit-learn 1.7.2 and NumPy 2.1.3. The largest selected refit took 68.91 seconds; observed process peak RSS reached 1,080.87 MB. The six-model artifact is 10,348,800 bytes. Save/reload maximum prediction difference is below `1e-12`. Cloud metrics match local metrics within `3e-12`.

Databricks run: [769399725196061](https://dbc-d00cbd4c-c74c.cloud.databricks.com/?o=7474644096185196#job/830335479884342/run/769399725196061). MLflow run: `8f895b718dcb4d93bbdd5158069e1991`. Package manifest: `71846e30f8ee2bc63abfdcf715fab7a298ca320a102f9541828a7153e34217c6`. See the [validation receipt](validation/station_ensemble_2026-10-20.json).

## Availability count comparison

Candidates train across all horizons and select separately by outcome/horizon using August 1–14 validation MAE. Neither depth-10 nor depth-16 random forest wins a route. The frozen routing is:

| Horizon | Bikes | Docks |
|---|---|---|
| 15 minutes | Persistence | Persistence |
| 30 minutes | Persistence | 31-leaf boosting |
| 60 minutes | 31-leaf boosting | 31-leaf boosting |
| 180 minutes | 31-leaf boosting | 31-leaf boosting |

September development metrics for the routed count model are:

| Horizon | Bike MAE | Bike persistence | Dock MAE | Dock persistence |
|---|---:|---:|---:|---:|
| 15 minutes | 1.076 | 1.076 | 1.060 | 1.060 |
| 30 minutes | 1.613 | 1.613 | **1.582** | 1.591 |
| 60 minutes | **2.285** | 2.455 | **2.222** | 2.415 |
| 180 minutes | **3.932** | 5.042 | **3.858** | 4.974 |

The routing makes the earlier horizon observation operational: persistence is preferable for very short forecasts, while boosting helps at longer horizons. The new calibration uses August 15–31 residuals from that routing. September interval coverage is 89.67%–93.03%, depending on outcome/horizon. The fixed boosting risk models and their separate sigmoid calibration remain in place; no random forest risk claim is made.

On validation, both forests lose to the selected candidate for every outcome/horizon. Forest fits took 150.18–164.78 seconds in the cloud, compared with 2.44–3.96 seconds for boosting candidates, and did not justify their additional compute. Peak process RSS reached 1,188.58 MB. The routed artifact is 792,513 bytes. Cloud count metrics match local execution within `1e-15`.

Databricks run: [757022510974412](https://dbc-d00cbd4c-c74c.cloud.databricks.com/?o=7474644096185196#job/750503436174345/run/757022510974412). MLflow run: `fbb3b0c2ace34abaa27f8bab12536b84`. Package manifest: `86706dc2913aa64fc854d51c07a02a58e44413dcc9eb91047c7b9e5efe516766`. See the [validation receipt](validation/availability_ensemble_2026-10-20.json).

## Operational decision and remaining confirmation

The station result supports carrying weather histogram boosting forward as the candidate for later confirmation, with weather Poisson retained as the interpretable reference and fallback candidate. The result does not replace the active Manhattan model or establish a station production model. The exact subway advisory ablation remains gated.

The availability development result supports per-horizon routing between persistence and 31-leaf boosting. It does not change the frozen manual dashboard model because September was already inspected and no later confirmation period is available yet. A future dashboard version also needs an explicit migration and rollback step for its routing and recalibrated intervals.

The earliest planned confirmation window remains November 1–30, 2026, conditional on a separate pre-outcome freeze and complete source coverage. Inventory outcomes can mature from observed polls. Departure labels remain subject to validated monthly releases and supplying-cohort maturity; under the existing 45-day assumption, November departure outcomes cannot be fully mature before January 2027. Until then, the cloud runs are development evidence only.

## Compute choice

These experiments do not justify a migration to Spark ML. The prepared packages contain 234,238 and 492,270 rows, peak process RSS remained below 1.2 GB, and all runs completed within the existing single-node budgets. Spark would introduce scheduling, serialization and model-parity work without resolving the source-coverage or later-confirmation gates. The [Spark scaling decision](spark_scaling_decision.md) records the measured rationale and defines a future path in which Spark is evaluated first for distributed feature construction and incremental refreshes, then for model training only if training data exceeds single-node capacity.
