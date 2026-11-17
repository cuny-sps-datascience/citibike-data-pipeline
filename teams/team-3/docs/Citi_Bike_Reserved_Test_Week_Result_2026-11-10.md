# Citi Bike: reserved test week result (Oct 5 to 11, 2026)

Single evaluation under the approved freeze record (SHA-256 0118cb65...). Scored once on November 11, 2026. A marker blocks reruns. Numbers below were read from the saved summary (SHA-256 37a4b49c...). No tuning or model change followed.

Known limitations: the week was partly exposed earlier through aggregate label counts; availability times are simulated from collection times; counts are snapshots, not distinct incidents or prevented shortages.

## Primary result

190,006 test examples, 1,996 polls. Up to five alerts per outcome per update from the same candidate pools.

| Outcome | Method | Alerts | Confirmed catches | Known non-shortage | Unknown | Precision among known |
|---|---|---:|---:|---:|---:|---:|
| Bike | ACS candidate (frozen A) | 9,980 | **1,924** | 8,039 | 17 | 19.3% |
| Bike | Original logistic | 9,980 | 1,640 | 8,324 | 16 | 16.5% |
| Bike | Low-count rule | 9,980 | 1,614 | 8,347 | 19 | 16.2% |
| Dock | ACS candidate (frozen A) | 9,980 | **2,603** | 7,362 | 15 | 26.1% |
| Dock | Original logistic | 9,980 | 2,234 | 7,728 | 18 | 22.4% |
| Dock | Low-count rule | 9,980 | 2,144 | 7,816 | 20 | 21.5% |

Known shortage snapshots in the candidate pools: bike 3,265, dock 5,117.

Paired station-day resampling (1,000 draws, fixed seed, 672 station-days):
- Bike: ACS minus logistic +284 (+140 to +435); ACS minus low-count +310 (+156 to +465).
- Dock: ACS minus logistic +369 (+160 to +584); ACS minus low-count +459 (+212 to +709).

All four intervals exclude zero. Within this pre-registered comparison, the frozen candidate caught more confirmed shortage snapshots than both comparison methods for both outcomes. The intervals condition on the fitted models and do not capture all cross-station or cross-day dependence.

## Neighborhood audit (signals for review, not evidence of cause)

Poverty share is both a model input and the grouping variable. Thirds of the 99 stations with estimates; groups describe neighborhood estimates, not riders. Unknown rates were at most 0.3% in every group.

Bike capture by poverty-share group: ACS candidate lower 64.9%, middle 56.9%, higher 49.2%. Logistic 44.9%, 50.8%, 61.9%. Low-count 47.6%, 46.0%, 62.3%. The higher group holds only 17.1% of known bike shortages this week (559 snapshots) and receives 23.6% of ACS alerts.

Higher minus lower capture, station-cluster resampling (3,000 draws):
- Poverty share, bike: ACS −15.7 points (−34.9 to +4.1); logistic +17.0 (−1.8 to +32.2); low-count +14.7 (−8.6 to +33.9).
- Poverty share, dock: ACS +4.7 (−9.5 to +17.1); logistic +16.3 (+4.8 to +27.0); low-count +12.6 (−2.9 to +27.8).
- No-vehicle share, bike: ACS +0.7 (−15.6 to +15.2); logistic +14.3 (−0.5 to +27.9); low-count −8.6 (−24.9 to +7.2).
- No-vehicle share, dock: ACS −3.4 (−15.8 to +8.8); logistic −18.6 (−30.0 to −6.0); low-count −13.2 (−25.7 to −1.2).

Reading: the ACS candidate's lower bike capture in the higher-poverty third appeared again (development pooled −8.8 points, test −15.7). Neither interval excludes zero, the group had relatively few known shortages this week, and station concentration was not recomputed for the test week. The comparison methods show the opposite pattern on bikes. This is a consistent signal worth review before or during the pilot. It is not evidence that the model causes or reflects a neighborhood effect. On the no-vehicle grouping, the candidate shows no gap while both comparison methods under-capture docks in higher no-vehicle areas.
