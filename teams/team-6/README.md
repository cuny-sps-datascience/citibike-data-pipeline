# CUNY DATA 622 Final Project

The Team 6 project lives in the shared `cuny-sps-datascience/citibike-data-pipeline` repository on the `team-6` branch. Its local project root is `/Users/randyhowk/Sites/CUNY/citibike-data-pipeline/teams/team-6`. Run Team 6 commands there; shared pipeline assets remain at the repository root.

## Start here

- [Ensemble model results](docs/ensemble_results.md) and [incorporation plan](docs/ensemble_incorporation_plan.md): October 20 local/cloud comparisons. Weather histogram boosting reduces station September MAE to 2.134; inventory forests are not selected, and per-horizon routing retains persistence/boosting. Later confirmation is pending.
- [Shared-repository migration](docs/shared_pipeline_migration.md): active branch, directory boundaries, retained archives and paths for adopting shared pipeline data.
- [Shared station source audit](docs/shared_station_source_audit.md): pinned S3 objects, 32-station overlap comparison and the enforced station-information freshness cutoff.
- [Thirty-minute stockout-onset comparison](docs/stockout_onset_results.md): strict continuous-window labels and common-key persistence/logistic/boosting results; boosting wins validation for bike and dock onset.
- [Spark scaling decision](docs/spark_scaling_decision.md): current prepared tables fit in about 1.2 GB or less and complete within budget, so Spark ML would add overhead and validation work. Future iterations can introduce Spark first for cloud-resident, multi-year feature construction and incremental partition refreshes when measured scaling triggers are reached.
- [Public-only benchmark and runbook](docs/public_only_mvp.md): executed Manhattan-total forecasting without class data; [model card and measured performance](docs/manhattan_benchmark_model_card.md). Randy operates all workstreams.
- [Three-hour execution status](docs/three_hour_execution_status.md): verified exports, cloud training/forecasting, and remaining station-level/class dependencies.
- [MVP slides](docs/presentations/data622_mvp.pptx) and [PDF](docs/presentations/data622_mvp.pdf): three slides with speaker notes; [demo sequence](docs/public_mvp_demo.md).
- [Technical whitepaper draft](docs/technical_whitepaper_draft.md) and [PDF](docs/presentations/data622_whitepaper_draft.pdf): methods, results and reflection; [timed demo script](docs/midterm_demo_script.md).
- [September refresh](docs/september_refresh.md): validated new trip/outcome coverage and extended weather handoff; [departure dataset datasheet](docs/borough_departure_datasheet.md) and [station/advisory datasheet](docs/prospective_station_alert_datasheet.md).
- [Alternatives to class station and subway data](docs/alternative_data_sources.md): verified public 2026 station/status and alert archives, historical subway schedules, sample-quality findings and a proposed shorter station study.
- [Executed public station-history pilot](docs/station_history_pilot.md): 178 days acquired; 32-station training in Databricks, 31-station September evaluation, weather MAE 3.48 versus calendar 3.63. Exact alert ablation remains gated by older archive fields; the live aggregate model is unchanged.
- [Availability MVP and dashboard](docs/availability_mvp.md): bike/dock counts, empty/no-dock probabilities and uncertainty at 15/30/60/180 minutes. Three-hour MAE improves about 22% over unchanged inventory; persistence remains better at 15 minutes. Separate manual refresh and frozen departure models.
- [Station to subway mapping](docs/station_subway_mapping.md): dated 250m/500m public reference, CSV/Parquet and cloud views; [daily health monitoring](docs/daily_forecast_health.md): enabled at 13:15 UTC.
- [Release aware evaluation and public evidence](docs/release_aware_evidence_pipeline.md): monthly fitting/selection cutoffs, daily trip-release queue and outcome imports, observed station history and matched subway advisories. Daily evidence runs at 13:00 UTC.
- [Three-hour MVP plan for Databricks Free Edition](docs/three_hour_mvp_databricks_plan.md): October 15 planning for the supplied [weather and subway disruption proposal](docs/citibike_proposal_4_pages_for_revisions.docx).
- [Three-hour MVP data sources](docs/three_hour_mvp_data_sources.md): verified endpoints, alert coverage gaps, weather timing and acquisition steps; [API audit](docs/validation/data622_mvp_source_audit_2026-10-15.json).
- [Earlier neighborhood proposal](docs/neighborhood_demand_proposal.md) and [three-page proposal PDF](docs/neighborhood_demand_proposal.pdf).
- [Modeling export plan](docs/modeling_export_plan.md): datasets, columns, duration-policy review, release cutoffs, validation and implementation steps.
- [Executed export and measured sizes](docs/validation/model_export_2026-10-11.md): 46.2 MB portable ZIP, validation, import instructions and remaining inputs.
- [Databricks Free Edition plan](docs/databricks_free_execution_plan.md): one integration workspace or four development accounts.
- [Runbook](docs/runbook.md), [data dictionary](docs/data_dictionary.md) and [implementation status](docs/implementation_status.md).
- [Project consolidation record](docs/project_consolidation.md).

## Local commands

```sh
cd /Users/randyhowk/Sites/CUNY/citibike-data-pipeline/teams/team-6
uv sync --locked
uv run citibike doctor
uv run citibike --help
uv run python scripts/audit_model_inputs.py
uv run citibike model-export --dry-run
uv run citibike model-export --start 2025-01-01 --end-exclusive 2026-09-01
uv run python scripts/manage_three_hour_workspace.py status
```

`model-export` creates a validated, provisional observed-day handoff on the RAID, including a portable ZIP and Databricks import notebook. Verified Census/tract geography, historical station operation/zero-day coverage and source publication evidence remain required before the full model pilot. Read each build's data dictionary and validation report. The existing `aggregate` and `publish-plan` entries remain placeholders. `bike-mix` provides retrospective half-hour trip shares.

For the active three-hour proposal, `forecast-sources` collects timestamped external inputs and `hourly-export` writes validated observed station-hour counts under `modeling_exports/three_hour/`. A public-only Manhattan-total model is trained and deployed alongside hourly collection at minute 55 UTC. It is a reduced-scope benchmark; the station-level model and alert ablation remain gated. The earlier synthetic capability model remains separately labeled.

## Locations

| Item | Location |
|---|---|
| Original CSV and ZIP archive | `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject` |
| Versioned new release downloads | `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_downloads` |
| Normalized trips, manifest and reports | `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed` |
| Modeling packages and portable ZIPs | `FinalProject_processed/modeling_exports/builds/<build_id>/` and `modeling_exports/handoffs/` |
| Local shortcut to RAID modeling exports | `data/modeling_exports/` (ignored symlink) |
| Independent public sample repository | [Vaene/CUNY622FinalProjectPublic](https://github.com/Vaene/CUNY622FinalProjectPublic) |
| Small relocated CSV example | `data/samples/citibike_april_2025_sample.csv` |
| Preserved pre-migration checkout | `/Users/randyhowk/Sites/CUNY/CUNY622FinalProject` |

Full datasets stay on the RAID. The public repository contains illustrative excerpts and has its own Git history. Earlier availability proposals and PDF specifications remain available as reference material.
