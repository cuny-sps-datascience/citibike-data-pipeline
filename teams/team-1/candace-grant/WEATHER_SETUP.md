# Team 1 weather integration — setup and handoff

Prepared November 18, 2026. This adds forecast collection, a time-aware weather join, and a preliminary experiment. The existing 100-station selection and deployed bike models remain the MVP baseline. Expanding to all stations and deploying a weather-trained model are later upgrades.

## What is in Git versus Databricks

Git contains notebooks, Python code and instructions. It does not share the Delta tables, collected forecasts, registered models or scheduled jobs across separate Databricks workspaces. Each workspace must create its inputs, or receive access to a shared data source. Pulling the code alone does not recreate the forecast history already collected.

The weather collector and join notebooks are under `teams/team-1/weather_forecasts/`. The integration files are under `teams/team-1/candace-grant/`. If either set is still in a pending pull request, obtain the reviewed files from that branch. Do not assume both are already merged into main.

## 1. Check prerequisites

Use a workspace with the existing bike pipeline configured. Its selected-station table, `C.T["mvp_stations"]`, should contain the original 100 stations (50 high-event-rate and 50 seeded-random). Do not rerun the all-station upgrade for this handoff.

The collector/examples workflow uses these source tables:

- `workspace.default.live_station_status`: `station_id`, `fetched_at`, `station_reported_at`, availability counts and operational flags.
- `workspace.default.live_station_info`: `station_id`, `fetched_at`, station name, capacity, `lat`, `lon`.

These differ from the teammate collector's normalized `snapshot_ts`/`last_reported` schema. The weather-example notebook handles the conversion. Check which collector writes which tables before scheduling anything.

In `config.py`, retain the existing station selection and seed (622), and include:

```python
# Point the shared weather join to the collected forecast tables.
WEATHER_FORECAST_TABLE = "workspace.default.weather_forecast_history"
WEATHER_MAPPING_TABLE = "workspace.default.station_forecast_mapping"
```

After editing imported Python files, restart Python and rerun imports, or reload `config` and `citibike_lib` before use.

## 2. Run the collector once

Run `11_collect_area_forecasts` from top to bottom. It uses station coordinates to create requested forecast locations, retrieves nine hourly weather variables from Open-Meteo using `gfs_seamless`, records when each response was retrieved, and appends the forecasts.

It creates/updates:

- `workspace.default.weather_forecast_locations`
- `workspace.default.station_forecast_mapping`
- `workspace.default.weather_forecast_history`

The latest successful collection covered 102 requested locations. These are requested coordinates, not necessarily 102 distinct provider grid cells. Mapping/location tables describe current locations; forecast history accumulates snapshots.

## 3. Configure collection jobs

In Databricks Jobs & Pipelines, create or edit a notebook task pointing to `11_collect_area_forecasts`. Run it manually once and verify success before enabling its schedule.

Suggested initial schedule: hourly, UTC, maximum concurrent runs 1. Every three hours is a lower-compute alternative while actively developing. Check for an existing collector job first; do not create a duplicate. Monitor failures and queued runs, particularly on Free Edition.

Keep the existing bike status collector running at its established cadence (approximately five minutes). Avoid scheduling two collectors into the same table. Forecast collection and bike polling are separate jobs.

A successful manual run does not establish that a schedule is active. Confirm the trigger is active and inspect a subsequent scheduled run.

## 4. Build weather-enriched examples

Run `08_prepare_weather_examples` in the `candace-grant` folder. It reads the selected stations and recent bike observations, builds features and labels with the shared bike library, attaches weather through `L.add_weather`, and writes:

`workspace.citibike.weather_model_examples`

This dedicated output is overwritten on refresh; raw forecast history is retained. Run it manually for the MVP. A daily refresh can be added later, after collection is reliable and compute allows. Rerunning lets newly observed outcomes become available; the newest observations may still lack future labels.

Expected checks from the completed run: 26,100 rows, 24,597 eligible examples, 26,100 rows with forecasts, and zero forecasts retrieved after prediction time. Counts will change as data accumulate.

The join chooses a forecast for the hour containing prediction time + 15 minutes, using the latest snapshot retrieved no later than prediction time. It preserves rows with unavailable weather as nulls. Never replace unknown weather with zero. Input keys must be unique by station and prediction time.

Hourly weather is an approximation for a 15-minute prediction. Precipitation/snowfall are hourly accumulation variables, not totals for the next 15 minutes. Historical rows before collection began generally cannot be populated from this archive. Current station mappings are not a historical record of station relocations.

## 5. Run the experiment separately

Run `09_test_weather_features` (the experiment notebook) manually after preparing examples. It compares bike-only and bike-plus-weather histogram gradient boosting models against persistence on the same rows, with seed 622. It excludes station-prior features for this short experiment.

Training: 16,751 examples before November 17, 23:00 UTC. Validation: 7,458 examples from November 18, 00:00 UTC; one-hour gap. Eligible data covered 98 of the 100 selected stations. The experiment currently evaluates empty-station outcomes only.

| Method | Average precision ↑ | Brier score ↓ | Precision | Recall | F1 |
|---|---:|---:|---:|---:|---:|
| Bike only | 0.8560 | 0.0129 | 0.8214 | 0.8239 | 0.8227 |
| Bike + weather | 0.8767 | 0.0126 | 0.8169 | 0.8388 | 0.8277 |
| Persistence | 0.6977 | 0.0152 | 0.8343 | 0.8269 | 0.8306 |

Classification threshold: 0.5. Among 7,126 station-times with bikes initially, 58 became empty at the target: bike only caught 1 with 8 false warnings; bike + weather caught 5 with 10 false warnings; persistence caught none. These are station-time examples, not necessarily separate shortage episodes.

This is preliminary evidence from approximately three dates, with no independent test set. Weather improved probability metrics and detected four additional transitions, but missed 53 of 58 transitions. Persistence retained the highest overall F1. The experiment models remain separate from deployed models; this code does not automatically register or promote them.

## 6. Commit only the intended work

In the Databricks Git panel on `team1-weather-integration`, clear the select-all checkbox. Select the new experiment notebook and this guide after adding it to the project folder. Inspect each diff. The previously committed integration files should only be selected again if they have intentional new changes.

Suggested commit: `Add preliminary weather experiment and setup guide`.

Do not include unrelated setup/training changes, scratch notebooks, archives, partial edits to `04_score_live.py`, data exports or model files. Commit & Push. If this branch already has an open pull request, the new commit joins it; otherwise create a pull request into the agreed integration branch (main for the current teammate integration workflow). Check the base before submission.

## After the MVP

Collect a longer forecast archive; evaluate both empty and full outcomes on later held-out dates; expand to all eligible stations with manageable sampling; compare models and tune warning thresholds on validation only. Integrate weather into both training and live feature preparation, retrain, and validate before promoting a model. Keep persistence comparisons, shortage-transition metrics and monitoring in the deployed workflow.
