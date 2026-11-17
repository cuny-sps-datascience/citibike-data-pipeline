# Citi Bike: ACS controls, intervals and neighborhood audit (development results)

November 10, 2026. Exploratory development results on three previously used weeks. Not an independent test. The reserved October 5 to 11 week was not read or scored.

Source: saved development audit outputs. Figures below were read from the saved summary; the audit was not rerun for this report.

## Reproduction checks (all passed)

- Labeled rows and candidate pools matched the earlier reports exactly: 944,792 training rows and 192,544 validation rows per outcome; 185,592 bike and 174,735 dock validation candidates.
- Catch counts for station-only boosted, ACS boosted and the low-count rule matched every earlier reported value (18 of 18).
- Saved models refit on the same window gave identical probabilities (maximum difference 0) for station-only and ACS, bike and dock.
- The saved ACS table equals the table embedded in the principal notebook (0 differing cells; embedded JSON SHA-256 18f55c17...).

## Early stopping (item 5)

- Every earlier boosted model used scikit-learn's default `early_stopping='auto'`, which is on for training sets above 10,000 rows. So early stopping was already active: 10% of training rows were held aside internally to monitor the loss.
- In every fit, the model ran all 100 rounds (`n_iter_` = 100). Early stopping never ended training early. It still matters, because the 10% held-aside rows are not used to fit the trees.
- Setting `early_stopping=True` explicitly reproduced the "auto" fits exactly: maximum probability difference 0 and identical alert keys in all six week and outcome fits. No material difference. The new manifest records `early_stopping: true`.

## Development results (item 1 and 2)

Confirmed shortage snapshots among the top five alerts per outcome per update. Counts are snapshots, not distinct incidents. Unknown outcomes are not counted as catches. Differences versus station-only boosted are pooled over the three weeks, with 95% station-day resampling intervals.

| Model | Bike Sep14 | Bike Sep21 | Bike Sep28 | Bike pooled vs station-only | Dock Sep14 | Dock Sep21 | Dock Sep28 | Dock pooled vs station-only |
|---|---:|---:|---:|---|---:|---:|---:|---|
| Low-count rule | 1,830 | 1,347 | 1,575 | n/a | 2,341 | 2,033 | 2,179 | n/a |
| Station-only boosted | 2,165 | 1,520 | 1,715 | reference | 2,819 | 2,376 | 2,362 | reference |
| **ACS candidate** | 2,197 | 1,592 | 1,814 | +203 (+51 to +375) | 3,164 | 2,505 | 2,656 | +768 (+465 to +1,079) |
| Permuted ACS, seed 101 | 2,211 | 1,605 | 1,808 | +224 (+50 to +407) | 3,029 | 2,443 | 2,628 | +543 (+236 to +839) |
| Permuted ACS, seed 202 | 2,190 | 1,606 | 1,807 | +203 (+37 to +375) | 2,987 | 2,411 | 2,565 | +406 (+141 to +712) |
| Permuted ACS, seed 303 | 2,212 | 1,574 | 1,814 | +200 (+39 to +377) | 3,012 | 2,442 | 2,672 | +569 (+265 to +852) |
| Prior shortage rate | 2,217 | 1,598 | 1,770 | +185 (−5 to +364) | 2,992 | 2,370 | 2,597 | +402 (+115 to +701) |

ACS candidate versus the low-count rule, pooled: bike +851 (+596 to +1,115); dock +1,772 (+1,340 to +2,208).

Permutation detail: complete ACS rows were shuffled among the 100 stations. Stations keeping their own ACS row: 1, 1 and 0 for the three seeds.

**Resampling unit and meaning.** The unit is a station-day: one station on one New York calendar day, including eligible station-days where neither model alerted. Each draw resamples station-days with replacement and sums the paired difference in confirmed catches. The interval describes how much the observed difference depends on which station-days happened to occur. It conditions on the fitted models, does not capture dependence across stations on the same day or across days for one station, and does not reflect retraining or repeated model selection on these weeks. Per-week intervals are in `station_day_intervals.parquet`.

**Interpretation.**
- **Bike:** randomly reassigned ACS vectors matched the real ACS gain (pooled +200 to +224 versus +203). For bikes, ACS is functioning as a station profile: any fixed per-station tag gives the trees the same lift. These results do not show that neighborhood traits predict bike shortages.
- **Dock:** the permutation controls recovered roughly half to three quarters of the ACS gain (+406 to +569 versus +768), and the prior-rate control +402. Most of the dock gain also looks like a station-profile effect. The remaining gap between real and permuted ACS was not given its own interval, and the development weeks have been examined repeatedly, so it should not be read as evidence that neighborhood traits predict or cause dock shortages.
- Per your decision, ACS stays in the candidate regardless.

## ACS provenance and missingness (item 3)

- Six measures, in model order: `estimated_population_500m`, `bachelors_plus_pct_500m`, `households_no_vehicle_pct_500m`, `poverty_pct_500m`, `bicycle_commute_pct_500m`, `school_enrollment_pct_500m`. Provisional 2020 to 2024 ACS five-year estimates, area-weighted within about 500 meters of each station.
- File: private workspace `Citi Bike - ACS and July Trip Development - e1eb6e26/acs_station_features.parquet`, 13,770 bytes, SHA-256 `3332babcb605f43c85f60b979bcda82c66e1c5c1199167118f1a333dbc9b84f9`. The upstream Census download that produced it was not re-verified this week. A related local script, `Download_Citi_Bike_ACS_2024.R`, is in the Final Project folder; it was not rerun.
- Missing values: one station, **Yankee Ferry Terminal** (short name 4440.02, station ID `35915f48-31c5-499a-b845-2ca2fb541904`), lacks all five percentage fields; its population estimate is present. It contributes 11,876 development rows.
- Training: these NaN values (not-a-number markers) were passed to `HistGradientBoostingClassifier` unchanged. The model learns which branch missing values follow. No imputation.
- Live scoring: the v14 scorer passes the same NaN values unchanged, so training and scoring treat this station the same way. Tested with synthetic models; to be confirmed on the trained candidate in the key-level replay.
- Known limitations, recorded and not fixed this week: area weights include water area; tract-level bicycle commute share is unreliable; derived margins of error are not available. These are neighborhood estimates, not rider characteristics.

## Neighborhood audit (item 4)

**Overlap stated plainly:** poverty share is both a model input and the audit grouping variable. Stations were split into thirds of the cohort (33 each) by poverty share; Yankee Ferry Terminal is a separate "missing" group. Pooled over three weeks. Capture = confirmed catches divided by known shortage snapshots in that group.

Poverty-share groups:

| Outcome | Model | Lower | Middle | Higher | Higher share of alerts vs share of known shortages |
|---|---|---:|---:|---:|---|
| Bike | ACS candidate | 59.7% | 64.1% | 50.9% | 33.5% vs 27.6% |
| Bike | Station-only | 55.4% | 63.4% | 49.6% | 34.9% vs 27.6% |
| Bike | Low-count | 45.4% | 50.6% | 54.9% | 46.0% vs 27.6% |
| Dock | ACS candidate | 47.2% | 48.1% | 47.9% | 31.0% vs 30.9% |
| Dock | Station-only | 41.5% | 43.8% | 45.0% | 32.9% vs 30.9% |
| Dock | Low-count | 35.1% | 42.0% | 35.7% | 31.0% vs 30.9% |

Higher minus lower capture, pooled, 95% station-cluster interval (3,000 draws, stations resampled within each group):
- Bike: ACS −8.8 points (−23.9 to +7.9); station-only −5.8 (−17.0 to +8.0); low-count +9.6 (−1.4 to +23.2).
- Dock: ACS +0.7 (−11.7 to +11.3); station-only +3.4 (−3.3 to +10.0); low-count +0.6 (−12.3 to +13.2).
- Concentration: in the higher-poverty group, three stations account for 977 of 2,622 known bike shortage snapshots.

Sensitivity, no-vehicle-share groups, higher minus lower capture:
- Bike: ACS +3.8 (−7.4 to +16.7); station-only +4.8 (−6.5 to +16.3); low-count −5.0 (−14.9 to +4.5).
- Dock: ACS −7.8 (−20.2 to +3.7); station-only −6.4 (−15.7 to +2.1); low-count −20.8 (−34.1 to −8.4).

Unknown outcome rates were at most 0.5% of eligible snapshots or alerts in every group.

**Reading this.** The ACS candidate's lower bike capture in the higher-poverty group appears in station-only too, so it is not introduced by ACS. The interval includes zero and the group is concentrated in a few stations. Grouping by no-vehicle share reverses its sign. The low-count rule's lower dock capture in high no-vehicle areas is the only interval that excludes zero, and it concerns the comparison rule, not the candidate. Treat all of this as signals for review, not as proof of a cause.
