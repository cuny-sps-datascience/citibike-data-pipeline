# Executing the Citi Bike Neighborhood Proposal on Databricks Free Edition

**Planning date:** October 10, 2026
**Scope:** One Free Edition account or four team members' Free Edition accounts on the basis of the GCP documentation. This is an execution plan; cloud capabilities and runtimes must be verified in the team's workspaces.

Use local DuckDB for the large trip archive, Census downloads, geographic joins, and historical feature preparation. Use Databricks for compact Delta tables, controlled model experiments, daily batch forecasts, evaluation, and a dashboard. **Start with one workspace. Add three development workspaces only if team members need independent experiments; keep one workspace responsible for the final scheduled system.**

This implements the neighborhood proposal in `docs/neighborhood_demand_proposal.md`. It does not require the earlier empty/full-station classification project.

## Governing Free Edition constraints

The linked [GCP Free Edition limitations](https://docs.databricks.com/gcp/en/getting-started/free-edition-limitations) specify:

| Constraint | Execution choice |
|---|---|
| Serverless only; limited compute and usage | Small Python models; pilot before scaling |
| Restricted outbound internet | Download public inputs locally and upload prepared files |
| One SQL warehouse, at most 2X-Small | Query prepared dashboard tables on demand |
| Five concurrent job tasks per account | Serialize scheduled tasks |
| One workspace and metastore per account | Exchange versioned files between independent accounts |
| Apps stop after 24 hours | Use a dashboard or notebook as the MVP interface |
| Quota exhaustion can stop compute for the day or month | Preserve outputs and resume after reset |

The page does not publish a universal numeric daily allowance. The runtime targets below are project budgets, not guaranteed platform capacity. The Model Serving restrictions are listed under endpoints; the proposed forecasting job executes Python prediction in a notebook and must pass the initial pilot. No serving endpoint is required.

[Serverless documentation](https://docs.databricks.com/gcp/en/compute/serverless/limitations) also specifies Spark Connect APIs, limited DBFS access, unsupported Spark cache APIs, and a default UTC session timezone. Use DataFrames/SQL, Unity Catalog volumes, persisted Delta intermediates, and explicit `America/New_York` date handling. Configure dependencies through supported notebook/environment settings. Verify scikit-learn and MLflow imports before committing to a runtime. Avoid cluster-specific initialization scripts.

## Local preparation and the cloud data package

The existing validation report reconciles 323,817,126 published trips. The implementation log reports about 9.8 GB of normalized Parquet. Those inputs stay on the RAID. Use the manifest's selected published-file list rather than scanning arbitrary archive copies. Local aggregation remains a development task; the current CLI does not yet implement the neighborhood modeling workflow.

Build a reproducible export under the existing processed-data root, separate from original sources. Include January 2025 through August 2026 targets, plus earlier published trips needed for the eight-week historical profiles. Start with one borough. Expand only after the full workflow runs.

| Proposed dataset | Grain and purpose |
|---|---|
| `station_day_observed` | Station/date; departure count, separate arrivals, morning/evening counts, operation evidence, coverage flags |
| `station_historical_profile` | Station/weekday/profile cutoff; historical counts and bike/member shares available at that cutoff |
| `nyc_tract_neighborhood_profile` | Tract/ACS vintage; Census counts, estimates, uncertainty, land area and derived shares |
| `station_tract_crosswalk` | Station/identifier scheme/effective interval; tract, borough, coordinates, match status |
| `source_release_availability` | Source release; availability date, vintage and evidence or documented assumption |
| `model_feature_snapshot` | Station/forecast origin/target date; release-safe features, joined neighborhood values and eligibility flags |

Keep observed target counts logically separate from forecast features. A historical profile cannot include a trip merely because it precedes the target date: its source release must also have been available at the forecast origin. Census vintages follow the same rule.

Publish each package with a manifest containing schema version, feature version, code revision or code hash, row counts, date ranges, source cutoffs, file sizes, SHA-256 checksums, and split definitions. All accounts use the same package and station/date evaluation keys. Account-local table names are configuration, not hard-coded source references.

**Sizing estimates:** 2,000–3,000 stations across 608 calendar days would produce 1.22–1.82 million station-day rows before eligibility filtering. These are planning estimates, not verified station counts. Aim for a compressed cloud package below 500 MB and a pilot of roughly 100,000–200,000 rows. Measure actual bytes and memory. If necessary, partition by borough and date instead of collecting the whole feature table into pandas.

Upload Parquet, supporting CSV/JSON, and code to a managed Unity Catalog volume, then create managed Delta tables. The [volume documentation](https://docs.databricks.com/gcp/en/volumes/volume-files) supports UI upload/download and a 5 GB limit per UI-uploaded file. Our proposed files are much smaller. UI transfer is the initial route; automation through workspace tools is an optional later improvement after authentication is verified.

## One account execution

One account is the lowest-complexity option. Its workspace contains the authoritative tables, experiment results, accepted model, scheduled job, and dashboard. Team members exchange source through the project repository or another agreed versioned folder. Verify workspace invitations and each person's actual permissions if collaborative editing is needed; nobody needs to exchange passwords.

Use an available catalog with a `citibike_project` schema, plus `inputs`, `models`, and `exports` volumes. Catalog names must be discovered in the workspace. Keep raw uploads versioned and validate them before making a new package current.

| Step | Deliverable | Completion check |
|---|---|---|
| 1. Capability pilot | Small input table, model fit, saved artifact, one scheduled run | Read/write Delta, dependencies, model reload and forecast persistence succeed |
| 2. Import borough package | Six prepared tables | Checksums, keys, row counts and aggregate totals match local exports |
| 3. Baseline | Eight-week weekday average with pooled fallback | Evaluation uses only available releases and eligible station-days |
| 4. Poisson comparison | Models with and without neighborhood features | Same observations, cutoffs, preprocessing and evaluation keys |
| 5. MVP deployment | Daily job, evaluation table, dashboard/notebook | Forecasts are saved before their target day; pending outcomes are explicit |
| 6. Model extension | Two bounded boosted-tree experiments | Improvement assessed using the same validation set |
| 7. Expansion | Additional boroughs and rolling backtests | Runtime, memory and coverage remain acceptable |

Run experiments sequentially. For the first full comparison, allow four trained candidates: Poisson with/without Census and boosted trees with/without Census, plus the arithmetic baseline. Choose any limited hyperparameter settings using the pilot/validation period; avoid AutoML or a large grid search.

There is an important modeling adjustment: Census features that stay constant for each station are collinear with a full set of station indicators in a linear model. For the primary Poisson neighborhood comparison, use historical station usage features and calendar variables without a full station-ID dummy matrix. Treat a station-indicator model as a separate forecasting benchmark. Neighborhood coefficients remain associations, not evidence of individual rider attributes.

Use scikit-learn `PoissonRegressor` first. A bounded `HistGradientBoostingRegressor` with Poisson loss is a suitable extension; start with approximately 100 boosting iterations and small trees, then measure runtime. Fit scaling, imputation and any feature selection on training rows only. Persist the model, preprocessing, dependency versions, feature order and release manifest together. Use MLflow if the pilot verifies it; otherwise use a model artifact in the managed volume and an explicit Delta model-metadata table.

## Four account division of work

Four workspaces help separate contributors' experiments. They do not combine into a larger cluster or automatically share a catalog. Assign work by function, rather than giving each account a different borough model that cannot be directly compared.

| Account | Responsibility | Inputs | Handoff |
|---|---|---|---|
| A | Integration and scheduled forecasts | Approved feature package and candidate configuration | Authoritative tables, baseline, accepted model, daily predictions and job logs |
| B | Poisson modeling | Frozen package, feature definitions and splits | Two Poisson comparisons, validation predictions, configuration and reproducible training code |
| C | Boosted-tree modeling | Same frozen package and splits | Two bounded tree comparisons, validation predictions, configuration and training code |
| D | Independent evaluation and presentation | Data package and candidate validation predictions | Coverage audit, metric tables, tract-holdout analysis, dashboard specification and report |

Local archive processing and geographic preparation happen once before this split. Account A owns the release manifest and integration decisions. Account D computes metrics independently to catch mismatched rows and leakage; it does not select configurations using the final test period. Account D can prepare visualizations, but the final dashboard reads Account A's authoritative tables.

A handoff is a small versioned package: notebook/Python source, environment versions, parameters, manifest hash, validation predictions, metrics, and optional model artifact. Use the team's existing shared folder or repository for code and small manifests, with downloadable Parquet files for larger tables. Do not rely on direct cross-account SQL, shared temporary views, or an unverified Delta Sharing setup.

Account A imports the winning configuration and refits or reloads it under the verified production environment. It checks predictions against a fixed validation sample before promotion. B and C do not need their experiments to run during daily production. If one development workspace hits its limit, that work waits or runs locally; daily forecasting continues independently in A if its compute remains available.

**Choose four accounts when** separate contributors genuinely need to develop simultaneously or independent evaluation is valuable. **Choose one account when** transfer and environment coordination would take more time than the small model experiments. The final deployed footprint is the same in both arrangements.

## Scheduled operation and runtime targets

Use one daily job with three sequential notebook tasks: check/build features, score, and record evaluation/status. Set maximum concurrent runs to one. Put monthly import/training in a separate manually triggered job initially; automate it only after source delivery is reliable. Jobs support run timeouts and concurrency controls in the [configuration reference](https://docs.databricks.com/gcp/en/jobs/configure-job).

| Workload | Proposed cadence | Initial project target |
|---|---|---|
| Validate package and import prepared data | New monthly release | Under 10 minutes |
| Train/promote candidate | Monthly, after validation | Under 15 minutes for selected candidate |
| Build next-day rows and score | Daily at 6 p.m. America/New_York | Under 5 minutes for the whole job |
| Match saved forecasts to new observations | After new trip release | Under 5 minutes |
| Refresh dashboard summaries | After scoring or evaluation | Part of the batch job |
| View dashboard | During review and demonstration | On demand; shortest available warehouse auto-stop |

These are stop-and-review targets. Configure matching job timeouts after the pilot, and begin with no automatic retry loop; investigate a failure before rerunning. They are not Databricks quotas or promised runtimes. Five minutes daily plus 15 minutes training and 15 minutes import/evaluation is about 180 minutes of job wall time over 30 days, excluding development, startup and dashboard use. Wall time cannot be converted to a guaranteed Free Edition allowance or DBU budget without account-specific measurements.

Each live forecast stores station ID, forecast origin in UTC, NYC target date, model version, profile/source cutoff, input package hash, prediction and data-quality status. It is immutable after creation. Retries must recognize an existing forecast, and late catch-up records must be marked as reconstructions rather than live forecasts. Persist run status, freshness, scoring duration and eligible-station coverage.

Use Delta dashboard summaries rather than repeatedly joining all feature rows. Show the latest forecast, top starting stations/tracts, neighborhood profiles, matched-outcome error, and the proportion of outcomes still pending. During a quota interruption, preserve the last forecast and its timestamp; record the missed scheduled run instead of inventing an on-time output. Maintain local exports of accepted models, predictions, metric tables and job definitions for reproducible recovery.

The latest complete trip files in the local project currently cover August 2026. They can demonstrate the historical workflow, but live October forecasts would initially use older profiles. Import September and subsequent published releases when available. The dashboard should show the profile cutoff so readers can assess freshness. Predicting next-day departures does not require daily access to new completed-trip counts, but delayed profiles may reduce accuracy.

## Evaluation and shared catalog delivery

Retain the proposal's chronological design, with availability-aware rolling origins. A forecast made September 1 cannot train on August labels if the August release arrives afterward. Similarly, a January test forecast cannot use hyperparameters selected from December validation outcomes that have not yet been published. Freeze settings using only matured validation labels at the first tested origin; if that is impossible, move the first scored origin forward and document the excluded dates.

Run the baseline and accepted model on exactly the same held-out rows. Report MAE, RMSE, Poisson deviance, top-20 overlap, borough/activity-group breakdowns, and tract match coverage. Neighborhood-feature comparisons use otherwise identical inputs and splits. Make a tract-holdout study a secondary experiment after the chronological MVP works. Evaluate the full eligible holdout even if memory constraints require a smaller training subset.

Monthly files delay prospective evaluation. If the course expects all four weeks of deployment to have scored outcomes by submission, those files may not arrive in time. Historical backtests demonstrate the evaluation mechanism, but cannot be presented as live accuracy. During the capability audit, inspect whether the class offers daily trip records or another validated source of actual departures. Changes in station-status inventory alone do not identify departures because returns and rebalancing also change inventory. Without fresher labels, retain live pending forecasts and report the measured subset honestly.

**The required class shared table needs an explicit delivery route.** A table in a personal account does not by itself satisfy the requirement to make data available in the class shared catalog. Publish `nyc_tract_neighborhood_profile` and the documented crosswalk into the instructor's catalog using authorized access. If that access is absent, prepare Parquet, a data dictionary, source manifest, and import SQL for an authorized class-workspace user. Completion requires another team to query the documented table, not merely receive a file. The shared class workspace may be the best final integration destination if it is available and its permissions permit the jobs.

## Delivery sequence and acceptance gates

Use the sequence below relative to the start of implementation; session counts are estimates, not course deadlines.

| Working session | One account | Four accounts | Exit condition |
|---|---|---|---|
| 1 | Capability pilot | A pilots integration; others verify dependencies | Small model persists and scores in a scheduled notebook |
| 2–3 | Local aggregation, Census integration and release dates | Common local preparation; D reviews coverage | Borough package reconciles and has documented keys/cutoffs |
| 4 | Import and baseline | A imports; B/C receive frozen package | Baseline metrics reproducible on agreed validation keys |
| 5–6 | Poisson comparison, then bounded trees | B/C model; D independently evaluates | Results comparable; winning configuration selected without test leakage |
| 7 | Scheduled MVP and interface | A integrates; D supplies summaries | Daily forecast written before target day and retry behavior verified |
| 8 | Shared-catalog publication and review | A publishes; all review | Another team can query Census table and documentation |
| 9–10 | Scale and backtests if pilot budget allows | B/C extend; A keeps production stable | Additional coverage fits measured compute budget |

The minimum accepted delivery is one borough, a baseline and Poisson comparison, the documented external table, a saved model, a scheduled forecast, historical evaluation, and a readable dashboard/notebook. Boosted trees, LODES, clustering, all-borough coverage, model serving, and a continuously running App are extensions.

Before expanding, verify unique keys and nonnegative counts; station-day totals against eligible local departures; effective dates and tract joins; missing-coverage handling; release-safe train/forecast features; model reload parity; prediction idempotency; and the class table's actual query permissions. If model training exceeds the pilot budget, reduce the borough/training window or train locally and upload the verified artifact. Scheduled forecasting and saved outcomes can still run in the final workspace.

## Proposal wording to make the scope executable

Use this operational description when revising the proposal:

> We will prepare historical trip aggregates and Census geographic joins locally, then publish compact documented tables to Databricks. A serverless notebook job will produce and store next-day station departure forecasts daily. Training will occur in monthly batches after new releases are validated. The MVP will cover one borough using a historical baseline and Poisson regression, with a dashboard or notebook and availability-aware evaluation. Four team workspaces may support independent model development, but one integration workspace will own the scheduled system. Live performance will be evaluated only when validated departure outcomes become available; the required external dataset will be published in the class shared catalog.
