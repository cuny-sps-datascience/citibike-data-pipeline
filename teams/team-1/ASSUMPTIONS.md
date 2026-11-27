# Assumptions log

Every modelling choice that could change results. Update the Evidence column after running 00 and 02.

| # | Assumption | Value | Why | Evidence / date |
|---|---|---|---|---|
| 1 | Prediction grid | Every 15 min, UTC :00/:15/:30/:45 | Matches the 15-min horizon; reduces autocorrelation between rows | — |
| 2 | State at time T | Latest snapshot in (T − 10 min, T] | Point-in-time correct; tolerates feed jitter | Fill in from 00 cadence audit (median/p90 gap) |
| 3 | Horizon | 15 min; label snapshot falls in (t+5, t+15] | Tolerance in #2 applied to the target time; `horizon_offset_min` stored per row | 02 check "label outside (t+5, t+15]" = 0 |
| 4 | Empty | `num_bikes_available == 0` | GBFS counts e-bikes inside `num_bikes_available`; a rider sees no bike | — |
| 5 | Full | `num_docks_available == 0` | Rider cannot return | — |
| 6 | Exclusions | No fresh state at t or t+15; station not installed/renting/returning at either | Outages are not prediction targets; counted by `exclusion_reason` | 02 exclusion table |
| 7 | Stations | 100: 50 highest (empty+full) rate + 50 seeded random; coverage ≥ 90%; present in live feed | Enough positives to learn, plus a representative half; report metrics by stratum | 02 stratum table |
| 8 | Split | Last 7 days test, previous 7 validation, rest train; 60-min embargo | Chronological; embargo covers the 60-min lag + 15-min label windows | `split_config` table |
| 9 | Selection | Model and threshold (max F1) chosen on validation only; test scored once | Avoids test-set tuning | 03 output |
| 10 | Priors | Station × local hour × weekend empty/full rates, train split only, Bayesian smoothing m=20 | Encodes station identity without leakage | `station_priors` |
| 11 | Snapshot time (live) | Feed `last_updated`, falling back to fetch time | Time the observation was true, not when we happened to fetch it | — |
| 12 | Snapshot time (historical) | Column mapped in `STATUS_COLMAP["snapshot_ts"]` | Must mean the same thing as #11 — confirm with professor's notebook | — |
| 13 | Weather | Published as `nyc_weather_hourly` (P §2, §7); not a model feature in the MVP | NOAA arrives ~60 h late; reanalysis values can be revised; neither is a dependable real-time input at 15 min | 07_weather, Nov 12: 2,016 hours, 0 missing, 100% join match |
| 14 | Weather source | Open-Meteo Historical Weather API (ECMWF reanalysis), centroid point 40.708, −73.955 | PR #61 (NOAA GHCNh) closed unmerged; Open-Meteo reachable from Free Edition, no key, CC BY 4.0 | 07_weather, Nov 12 |
| 15 | Station habits for training rows | Out-of-fold by 7-day blocks | In-sample habits leaked labels (v1) | 06 ablation, Nov 11: empty hgb 0.779 → 0.867 with fix |
| 16 | Full-network scope | `scope=full` widget in 02/03/06: every station with coverage ≥ 90% **in the training period** and present in the live feed; outputs and models in `workspace.citibike_full` | Professor feedback (Nov 20): cover all ~2,500 stations. Separate schema keeps the live MVP jobs untouched. Builds on Maxfield's commit 21dd8fd | 02 station count; `run_timings` |
| 17 | Split fixed before selection (full) | 02 computes and saves `split_config`; station quality uses training rows only; 03 reads the saved split | Choosing stations on validation/test coverage would let the evaluation period shape the sample | `split_config` in `citibike_full` |
| 18 | Priors counted in Spark | Station × hour × weekend counts on **all** training rows (and per 7-day fold for out-of-fold); pandas sample used only for model fitting | At full scale a 1.5M-row sample leaves ~600 rows per station across 48 cells; counting in Spark removes that thinning. Identical to the pandas version when nothing is sampled | Local test: MVP results match the previous code to 1e-13 |
| 19 | Evaluation rows (full) | All validation/test rows up to `EVAL_MAX_ROWS` (1.5M per split); above that, a deterministic hash sample of whole station-days | Memory guard for serverless pandas; whole station-days keep empty/full episodes and onsets intact; same rows on every run | 03 prints kept share per split |
| 20 | Strata (full) | Event-rate tertiles (low / mid / high) from training-period quality, stored in `selection_stratum` | The MVP's high-event vs random strata do not exist when every station is kept; tiers still show where the model helps | 06 §3 `analysis_by_stratum` |
| 21 | Cost proxy | Wall-clock seconds per step on serverless, appended to `run_timings` (scope, rows, runtime versions) | Free Edition shows no DBU bill; time per step at 100 vs full-network scale is the comparable measure | `run_timings` in each scope's schema |
