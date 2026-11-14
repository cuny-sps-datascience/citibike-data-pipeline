# Team 1 · Citi Bike 15-minute stockout MVP (Sean)

Predicts, for every Citi Bike station, the chance it **runs out of bikes** or **runs out of docks** within the next **15 minutes**. Results are shown on a map app that replays a held-out week, with what actually happened, or shows live predictions.

The data and features come from the professor's Free Edition pipeline in `free_edition_models/`. This folder adds four things:
- the 15-minute horizon from our proposal
- an evaluation against baselines
- playback scoring of the test week
- a map app

The professor's files are not modified.

AI disclosure: this folder was built with help from Claude (Anthropic). Sean ran and checked it in Databricks.

## Pipeline

```
free_edition_models/1-download-silver-FE   → silver_station_status, silver_station_info   (class S3 archive)
free_edition_models/3-gold-features-FE     → gold_station_predictors                       (professor's features)
sean_mvp/1-gold-targets-15                 → team1_gold_targets_15                         (15-minute targets)
sean_mvp/2-train-evaluate-15               → team1_model_eval_15, team1_model_info_15, models @champion
sean_mvp/3-score-playback-15               → team1_playback_15                             (test week, for the app)
free_edition_models/0-live-station-poller-FE → live_station_status, live_station_info
sean_mvp/4-predict-live-15                 → team1_live_predictions_15                     (live, for the app)
sean_mvp/app/                              → Databricks App (map)
```

| File | Purpose |
|---|---|
| `mvp_config.py` | Every setting: schema, table names, horizon, split lengths, sample sizes |
| `mvp_lib.py` | Split, history baseline, metrics, and map status. Imports the professor's `station_features.py`; it doesn't copy it. |
| `app/` | Streamlit app: `app.py`, `data.py`, `ui.html`, `app.yaml` |

All notebooks have a `run_mode` dropdown. `dev` (the default) calculates and shows results but writes nothing. Choose `production` to write tables and register models.

## Run order (Free Edition)

1. Run `free_edition_models/1-download-silver-FE` and then `3-gold-features-FE` in production, if they haven't been run already.
2. Run `1-gold-targets-15` in production.
3. Run `2-train-evaluate-15` in production. This takes about 30–60 minutes, mostly the cross-validation of the logistic regression.
4. Run `3-score-playback-15` in production.
5. Set up live predictions as a job (**Jobs & Pipelines → Create job**):
   - Task `poll`: notebook `free_edition_models/0-live-station-poller-FE`
   - Task `predict`: notebook `teams/team-1/sean_mvp/4-predict-live-15`, depends on `poll`, with parameter `run_mode` = `production`
   - Schedule every 10–15 minutes. Free Edition limits compute, so check the run history after a day.
6. Deploy the app (**Compute → Apps → Create app → Custom**):
   - Resource: a SQL warehouse, permission **Can use**, key **`sql-warehouse`**
   - Deploy from `teams/team-1/sean_mvp/app` in your Git folder
   - Let the app read the tables. Use the service principal ID from the app's Authorization tab:
     ```sql
     GRANT USE CATALOG ON CATALOG workspace TO `<service-principal-id>`;
     GRANT USE SCHEMA, SELECT ON SCHEMA workspace.default TO `<service-principal-id>`;
     ```

## Target, split, and evaluation

- **Target** (the professor's `build_targets` with `horizon_minutes=15`): 1 if a station that currently has bikes (docks) shows 0 at any poll in the next 15 minutes. It is null if the station is already out, not operating, or the window has gaps longer than 10 minutes.
- **Split by time:**
  - Test is the newest 7 days and validation the 7 days before; training is everything earlier.
  - A row is used only if its whole label window falls inside its period.
  - Inside training, the penalty `C` is tuned with the professor's time-gap cross-validation.
- **Models:**

  | Model | What it is |
  |---|---|
  | `count_only` | Baseline: logistic regression on the bikes/docks left |
  | `history` | Baseline: the station's own onset rate by hour and weekend, from training |
  | `logreg_geo` | The professor's model |
  | `gbm` | Gradient boosting with latitude/longitude |

  The chosen model is the one with the lowest **validation** log loss.
- **Persistence isn't a usable baseline here.** Targets only exist for stations that aren't out yet, so "stays the same" always predicts 0.
- **Metrics:** log loss, Brier score, PR-AUC (the base rate is a few percent, so accuracy is meaningless), precision/recall/F1 at the validation-chosen threshold, and calibration curves.
- **Map status:**

  | Status | Meaning |
  |---|---|
  | Empty risk / Full risk | Probability is at or above the validation-chosen threshold |
  | Watch | Probability is at or above half the threshold |
  | Empty now / Full now | The station is already out |
  | Offline | The station isn't operating or has no data |

## Not done yet

- Weather features: Maxfield's lagged weather, plus a with/without comparison.
- Rolling-origin evaluation over several test weeks.
- Event-level lead time.
- Per-station fairness and coverage review: are some neighborhoods predicted worse?
