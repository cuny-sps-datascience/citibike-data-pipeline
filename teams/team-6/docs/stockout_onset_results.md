# Thirty-minute stockout-onset comparison

## Outcome and population

This experiment adds a separate outcome to the completed inventory forecasts: whether a station with both bikes and docks available at issue time reaches zero bikes or zero docks anywhere in the next 30 minutes. It does not change the existing point-in-time inventory counts or zero-count probabilities.

The frozen [protocol](stockout_onset_protocol.json) requires the existing 32-station cohort, fresh service-eligible input, fresh stable metadata, nonzero bikes and docks at issue, a first future poll within 120 seconds, no future gap over 120 seconds, eligible polls throughout the interval and a confirming observation from minute 30 through minute 32. Current stockouts, closures, stale observations and incomplete windows are excluded. Observed zero inventory is not interpreted as unmet demand.

The new package contains 106,194 common-key examples from 109,531 candidate 30-minute origins. It covers all 32 stations and contains 807 bike-onset and 7,037 dock-onset events. The package and result are immutable RAID artifacts:

```text
/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/stockout_onset_packages/20261021T010515Z-v1
/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/stockout_onset_results/20261021T010835Z-local-v1
```

The independent reload audit is [recorded here](validation/stockout_onset_2026-10-20.json). It verifies artifact checksums, predictor/label keys, chronological cutoffs, the pre-test selection record, unique prediction keys and bounded finite probabilities.

## Candidate comparison

Three candidates use identical chronological keys:

- Fixed persistence/no-onset baseline: probability zero because every eligible origin begins stocked.
- Regularized station logistic regression: station one-hot effects plus standardized numeric inventory, capacity, calendar and cyclic-hour predictors. This ports the shared pipeline's partial-pooling logistic pattern into the Team 6 split and audit contract.
- Fifteen-leaf histogram boosting: the existing Team 6 classifier family applied to the separate onset label.

Candidates were trained through July and selected independently for bikes and docks by August 1–14 Brier score. Both learned candidates were refit through August 14 and sigmoid-calibrated on August 15–31. The selection record was written before September metrics were read.

| Outcome | Candidate | Validation Brier | Validation AP | September Brier | September AP |
|---|---|---:|---:|---:|---:|
| Bike onset | No-onset baseline | 0.00795 | 0.00795 | 0.00615 | 0.00615 |
| Bike onset | Logistic | 0.00608 | 0.34076 | 0.00458 | 0.39732 |
| Bike onset | Histogram boosting | **0.00557** | **0.47985** | **0.00432** | **0.44836** |
| Dock onset | No-onset baseline | 0.05866 | 0.05866 | 0.06113 | 0.06113 |
| Dock onset | Logistic | 0.03757 | 0.46946 | 0.03929 | 0.49503 |
| Dock onset | Histogram boosting | **0.03445** | **0.52555** | **0.03648** | **0.53945** |

Histogram boosting is the frozen validation winner for both outcomes. The logistic model materially improves on the fixed baseline and remains a useful transparent comparator, but it is not selected. September includes 17,553 examples, 108 bike-onset events and 1,073 dock-onset events. These are development results because September outcomes were already inspected in earlier Team 6 work.

No model was registered, activated or scheduled. The result does not alter the existing horizon-routed inventory model.

## Reproduction

```sh
uv run --locked python scripts/build_stockout_onset_package.py \
  --base-package /absolute/availability-package \
  --sources /absolute/pinned-station-source-root \
  --output /absolute/new-stockout-onset-package

uv run --locked python scripts/run_stockout_onset_experiment.py \
  --package /absolute/new-stockout-onset-package \
  --output /absolute/new-result

uv run --locked python scripts/audit_stockout_onset.py \
  --package /absolute/new-stockout-onset-package \
  --result /absolute/new-result
```
