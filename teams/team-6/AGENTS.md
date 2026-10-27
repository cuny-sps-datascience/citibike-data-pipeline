# DATA 622 Final Project

This directory is the canonical Team 6 root for all DATA 622 Final Project code, configuration, tests, documentation, proposals and export planning. Its local path is `/Users/randyhowk/Sites/CUNY/citibike-data-pipeline/teams/team-6`, inside the shared `cuny-sps-datascience/citibike-data-pipeline` repository. Work from the `team-6` branch and run project commands here using `uv run --locked`.

## Current work

- The October 26 PR 58 follow-up is `docs/historical_station_panel.md` and `docs/historical_station_panel_protocol.json`: a separate 689-station Manhattan retrospective panel for March 2025–August 10, 2026. The user authorized static retrospective geography, the five-minute issue cutoff with previous-origin subway inputs, and unknown unsupported zero counts. Main station evaluation remains gated. The original October 21 spatial-weather feature join is superseded due to a one-hour target mismatch; preserve its labels and receipts but do not use its old weather fields for modeling. Do not alter the November confirmation.
- The untouched stockout-onset confirmation window is frozen for November 1–30 in `docs/stockout_onset_confirmation_protocol.json`. Do not inspect outcomes, change the cohort/model hash, retrain, recalibrate or tune thresholds before the window closes. Fresh station information is a hard gate. Farhod's April–June spatial-weather handoff is received and structurally validated; use it only under the coverage and static-geography limits in `docs/station_hour_integration.md`.
- The project moved into the shared pipeline repository on October 20, 2026. `docs/shared_pipeline_migration.md` defines the ownership boundary, retained archives and reviewed paths to shared catalog and public S3 data. Keep Team 6-owned work in this directory unless a change is deliberately intended for every team.
- The October 19 ensemble incorporation request was executed October 20; see `docs/ensemble_results.md` and `docs/ensemble_incorporation_plan.md`. Weather histogram boosting is the station validation winner; inventory routes use persistence at short horizons and existing 31-leaf boosting later, with no forest route selected. These are inspected September development results. Keep all completed protocols/artifacts intact. No model, stable table or schedule was activated; later confirmation needs a separate pre-outcome freeze and future coverage.
- The current prepared ensemble tables fit in one process and cloud peak RSS stayed below 1.2 GB. Do not migrate the present model runtimes to Spark ML without a measured scaling constraint. `docs/spark_scaling_decision.md` defines reconsideration triggers and the preferred future sequence: benchmark Spark first for cloud-resident feature preparation and incremental refreshes while preserving the existing package contract.
- The active October 15 request executes the three-hour weather/subway proposal in `docs/citibike_proposal_4_pages_for_revisions.docx`. Use `docs/three_hour_mvp_databricks_plan.md` and `docs/three_hour_mvp_data_sources.md`; Randy operates all workstreams for now.
- The user subsequently authorized work without class data. `docs/public_only_mvp.md` documents the executed Manhattan-total benchmark and cloud operations; `docs/manhattan_benchmark_model_card.md` records its results. This reduces spatial scope and does not complete the station model or alert ablation. CityBikes change-only coverage remains conditional and cannot justify unconditional station zeros. Preserve the original proposal and those eligibility gates.
- The October 16 public-history follow-up is `docs/station_history_pilot.md`: an independent 32-station conditional calendar/weather pilot trained in Free Edition; 31 stations enter September evaluation. It uses bounded fresh ctbk.dev polling and dated geography assumptions. Exact advisory ablation remains gated: full fields start August 7, with none in the fixed April–June training/validation window. Preserve the live aggregate model, original 2025 scope gate and canonical station eligibility; this pilot does not globally verify continuous operation.
- The subsequently authorized availability MVP is `docs/availability_mvp.md` and `docs/availability_protocol.json`. It directly predicts future GBFS bikes/docks and point-in-time zero-count risks for the same cohort, using separate chronological train/selection/calibration/test windows. Unlike monthly trip labels, inventory labels become known at their observed poll times. Forecasts are conditional on continued service and endpoint freshness. The manual-refresh notebook `22_availability_dashboard` uses a frozen availability model; do not confuse inventory MAE with departure MAE, claim unmet demand/rebalancing effects, change existing schedules or activate a departure model when operating it.
- The earlier neighborhood proposal is `docs/neighborhood_demand_proposal.md` and its PDF.
- Read `docs/modeling_export_plan.md` for the modeling dataset contract and implementation sequence.
- Read `docs/databricks_free_execution_plan.md` for one or four Free Edition workspaces.
- Read `docs/implementation_status.md` and `docs/runbook.md` before operating the pipeline.
- Earlier station-availability proposals and aggregate plans remain reference material; the modeling-export plan governs the existing neighborhood export. New three-hour work must preserve that exporter and keep unsupported zero-hour counts and model evaluation gated.

## Data and repositories

- Keep original sources read-only at `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject`.
- The normalized archive and manifest remain at `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed`. Select input files from the published-file ledger and selected sources, not directory globs.
- Put full modeling exports under that processed root's `modeling_exports` directory. Commit code, configuration, documentation and small audit summaries, not bulk data or environments.
- Preserve normalized audit fields. Produce narrow modeling tables as a separate derived dataset; never remove columns from the canonical trip archive just to reduce an upload.
- The independent illustrative sample repository remains at `https://github.com/Vaene/CUNY622FinalProjectPublic`; it is not embedded in this shared checkout.
- The previous canonical checkout is preserved locally at `/Users/randyhowk/Sites/CUNY/CUNY622FinalProject` and remotely at `https://github.com/cuny-sps-datascience/team6`. Treat it as a migration archive. Continue active work here.
- Shared pipeline code and notebooks live at the repository root, two directories above this file. Keep shared and Team 6 changes separately reviewable.

## Modeling rules

- Keep labels separate from predictors. A future trip's duration, destination, membership or bike type cannot be used to predict that trip before it happens. Released historical aggregates may be used with an explicit availability cutoff.
- Modern `source_duration_seconds` is intentionally null. Do not replace it with zero or infer an unknown bike type as classic.
- Departure and arrival eligibility are different. Missing destinations do not automatically invalidate departures.
- Duration threshold policy is undecided. Use the measured audit and sensitivity comparisons before selecting it; retain baseline counts and document missing, nonpositive and DST-affected durations.
- Include zero-trip station-days only with documented operation and input coverage. Trip endpoints alone do not prove continuous station operation.
- Keep station IDs as strings and identifier schemes explicit. Use effective-dated station geography and published Census vintages.
- Use conservative memory and sequential archive reads on the RAID. Only one manifest writer may run at a time; audits use read-only connections.

## Verification

After code or environment relocation, verify `uv run citibike doctor`, CLI help, import locations and the existing test suite. For documentation-only edits, check paths, links and status claims without reprocessing the full archive.
