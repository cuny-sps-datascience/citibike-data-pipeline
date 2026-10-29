# Midterm presentation and live demo script

Randy Howk · Prepared October 16, 2026. Use the [three-slide PowerPoint](presentations/data622_mvp.pptx) or [matching PDF](presentations/data622_mvp.pdf). The course allocates approximately five minutes to slides and five minutes to the application. The script below is ready for spoken practice; a technical navigation check does not substitute for Randy rehearsing aloud.

## Preparation

Open the [dashboard](https://dbc-d00cbd4c-c74c.cloud.databricks.com/editor/notebooks/2763628353985993), [hourly job](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/1072725421467387), [daily evidence job](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/537646732304684) and [daily health job](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/899230762229374) in separate authenticated tabs. Keep the [model card](manhattan_benchmark_model_card.md), [September refresh](september_refresh.md), [weather datasheet](weather_reference_datasheet.md) and [station/advisory datasheet](prospective_station_alert_datasheet.md) available locally. Use successful completed runs; avoid waiting for cloud startup during the presentation.

Refresh the dashboard before presenting with `uv run python scripts/deploy_public_mvp.py dashboard`, then confirm its returned run succeeds. Inspect all three enabled schedules and the latest health report. A failed or late forecast should be discussed as an observed operational issue. Preserve actual result dates. Do not retrain, activate a new model or describe September outcomes as October forecast accuracy. If authentication or compute is unavailable, switch to the PDF, saved plot and dated validation reports.

## Five minute slide narration

### Slide 1 from 0:00 to 1:30

“This project forecasts Citi Bike departures across Manhattan for the next three hours. Each output is a separate hourly count. It measures served trips, so it does not measure people who wanted a bicycle but could not get one.

“My original proposal predicts individual stations using weather and subway disruptions. The public station archive creates a coverage problem: it records changes, and quiet periods are poorly represented. Only 0.147 percent of conservatively covered station hours have zero departures. Treating those observations as complete operation history would make evaluation depend on activity.

“I therefore built a Manhattan-total benchmark while preserving the original station research as an extension. Published trips provide geographic labels, forecast weather provides predictors, and Databricks runs hourly inference. The application also collects prospective station and subway evidence and checks forecast delivery daily. I am operating all workstreams for now. This is a working aggregate system with specific limits on what its results establish.”

### Slide 2 from 1:30 to 3:10

“The large trip files stay on the RAID. A manifest selects the published files; normalization reconciles every source row. Each departure is assigned using its actual start coordinates. Ambiguous autumn clock hours have null labels.

“Compact labels, features and saved models move into Databricks. Predictors use older released departure profiles, calendar information and individual forecast weather runs. There are no recent unavailable trip lags or future trip properties. Inputs freeze five minutes before the forecast origin. Historical trip availability assumes month end plus 45 days, and weather availability assumes initialization plus seven hours. These are declared assumptions; early weather archives include hindcasts.

“The stronger evaluation repeats fitting, validation selection and refitting month by month, using only labels available under that rule. The hourly serving job collects sources at minute 55, scores three intervals and preserves the first published predictions during retries. Calendar fallback covers unusable weather. Separate daily jobs watch releases, import validated outcomes, normalize station/advisory observations and monitor delivery. A data update does not change the frozen active model.”

### Slide 3 from 3:10 to 5:00

“Across June through August, the release-aware monthly analysis has mean absolute error about 768 departures per Manhattan hour, versus 1,386 for the weekly profile, on 6,552 common forecast rows. The monthly errors are 734, 840 and 727. June is a useful counterexample: the calendar model performs better than the validation-selected weather model, at 650. Weather does not improve every month.

“The original frozen model test is 761 on a slightly different set of 6,564 rows. The monthly analysis adds boundary purges and availability controls; it does not replace the active model. Both analyses are retrospective, and these months were already inspected. Overlapping targets mean the rows are not independent trials.

“September trips and weather extend the historical data. They cannot evaluate live forecasts issued in October. Genuine production accuracy waits for those matching monthly outcomes. The current subway mapping and advisory history support future research, without establishing historical station operation or causal effects. Remaining course work includes the class deployment, accessible shared weather contribution and sustained production observation. I will now show the forecast records, scheduled jobs and evidence behind these claims.”

Aim for 120–135 spoken words per minute, with pauses for the diagram and results. Use the time marks as limits; shorten explanation before rushing numbers. Record actual spoken duration during Randy's rehearsal.

## Five minute application navigation

| Clock | Action | Evidence to point to |
|---|---|---|
| 0:00–1:00 | Open dashboard forecasts | Three separate targets, model version, New York/UTC times, publication before origin, fallback reason |
| 1:00–2:00 | Open release-aware results | 6,552 common keys; 767.77 versus 1,386.49; June calendar advantage; original evaluation kept separate |
| 2:00–3:00 | Open hourly job and completed run | Collection then scoring, :55 UTC, one concurrent run, bounded timeout and actual task status |
| 3:00–4:00 | Return to daily evidence and data coverage | Validated September outcomes, first-seen release evidence, prospective station/advisory tables; weather handoff ready for class transfer |
| 4:00–5:00 | Show health and outcome evaluation | Delivery status and freshness; retry preserves original predictions; zero matching October outcomes remains a valid pending state |

## Questions to prepare

**Why no station accuracy?** The available archive does not establish complete station operation and quiet hours. Aggregate geography avoids inventing that denominator, but reduces the original scope.

**Is weather better?** It improves pooled retrospective MAE over the fitted calendar comparison in the original test. June's monthly result favors calendar. Forecast availability and hindcast assumptions still limit inference.

**Why is production accuracy empty?** The first live forecasts are in October; published September trips precede them. Evaluation requires matching released departures and forecasts published before their origins.

**What do subway counts mean?** Distinct matched advisories near mapped reference points, with explicit freshness and availability checks. They are not verified disruption duration, normal-service indicators or causal effects.

**What meets the sharing requirement?** The weather table has a documented portable package and verified private import. Completion requires publishing into the authorized class catalog and confirming another team can read it.

**What is reproducible?** The source ledger, checksums, immutable packages, selected models, MLflow report, temporal tests, job definitions and saved predictions. Provider history and Free Edition uptime remain external constraints.

## Rehearsal record

The [September validation receipt](validation/september_refresh_2026-10-16.json) records the technical dry run, including refreshed cloud outputs, dataset checks and presentation checks. It does not claim a spoken rehearsal. Randy should practice the slide narration once, time the five navigation segments, and repeat with the offline PDF to confirm recovery within ten minutes overall.
