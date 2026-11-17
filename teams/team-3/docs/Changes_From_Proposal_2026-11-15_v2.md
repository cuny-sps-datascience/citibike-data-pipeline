# Changes from the proposal (version 2, November 15, 2026)

Earlier versions are kept. This page uses saved results only and adds no new scoring. The pilot publishes each window to the team's shared storage area (see the pilot monitoring summary for the windows covered).

| Proposal said | What was done | Reason or limitation |
|---|---|---|
| All stations in the network | Fixed cohort of 100 stations | Compute and account limits. Results do not show network-wide performance. |
| Primary measure: area under the precision-recall curve | Primary test measure: confirmed shortage snapshots caught with up to five alerts per outcome per update. Average precision and probability metrics (log loss, Brier score) reported on development data | Closer to the operating question. A snapshot is not a distinct incident or a prevented shortage. |
| Several alert budgets | 3, 5 and 10 alerts per update on development data; only 5 in the test | The test comparison was fixed in advance. More alerts catch more, with lower precision. |
| Calibration plots and Brier score | Ten-band calibration table and plot and Brier score on development data | Scores are used for ranking only; one development week does not establish calibration. |
| Baselines: persistence, station hour-of-week, low-count | All three, plus the original logistic model, in development. The test compared the frozen candidate with the original logistic model and the low-count rule only | The test comparison was fixed in advance. Persistence and hour-of-week caught fewer snapshots than the candidate at every budget in development. |
| Income and no-vehicle quintiles | Thirds of poverty share and no-vehicle share for station areas with estimates | 100 stations are too few for five stable groups; income has missing and top-coded values. Groups describe station areas, not riders. |
| Shortage rate per station-day by group | Shortage rate per snapshot by group (known shortage snapshots divided by known candidate snapshots) | The saved development table is per snapshot, and no new runs were allowed. Not comparable with a per-station-day rate. |
| ACS (American Community Survey) used for description | Six ACS measures are also model inputs | Shuffled-control results suggest ACS acts mainly as a station profile; they do not establish cause. |
| Weather and trip features | Tested and excluded | Availability, coverage and ranking limitations. |
| Fall back to the low-count rule if the model fails | Falls back to the original logistic model | A tested model was available. Every published pilot window so far used the frozen candidate with no fallback. |
| Logistic model for the MVP, boosted model later | Boosted model frozen before a one-time test-week score | Ahead of the proposal's timeline. The test week was partly exposed through earlier aggregate label counts and is presented with that label. |
| Scoring all day | Two daily pilot windows (morning and evening), published to shared storage; no automatic retraining | Account limits. Pilot results monitor operations and are not model evaluation. |
| Shared catalog tables | Files in S3 (Amazon Simple Storage Service) under the team prefix, including the ACS package (10 files) | The team moved to individual accounts. |
| Exact reproducibility (implied) | Frozen scores and the main development tables reproduced exactly in a repeat run | The lasso (L1) logistic sensitivity rows differed slightly between runs, likely because the solver shuffles rows without a fixed seed (not directly tested). |
| Birth-death model (professor's suggestion) | Considered and deferred | Needs arrival and departure rates from trip data whose timing was not established. |

**Station-area group results:** presented neutrally, with both comparison methods and their uncertainty. Poverty share is both a model input and the grouping variable. The results do not establish causes.

Sources: project proposal; approved freeze record; saved one-time test result; corrected development results and review-run comparison; pilot window reports and published files; ACS publish manifest; group results decision.
