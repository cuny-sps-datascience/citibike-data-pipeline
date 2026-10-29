# Daily Manhattan forecast health monitoring

The [daily health job](https://dbc-d00cbd4c-c74c.cloud.databricks.com/jobs/899230762229374) is enabled at **13:15 UTC every day**, which is 09:15 in New York during daylight saving time and 08:15 during standard time. Randy is the operator. A manual cloud run passed before scheduling was enabled; the first scheduled run is October 17. Hourly forecast collection/scoring continues at minute 55 UTC.

The first health report, October 16 at 13:57:34 UTC, returned **OK**: 12 due origins had all three valid forecasts published before their nominal origins, with no fallback rows, failed source requests or stale source retrievals. These checks measure delivered forecasts, not live prediction accuracy. There are zero matured live forecast outcomes. The [saved validation](validation/mvp_extensions_2026-10-16.json) includes the report and verified schedule.

## Checks and status

The notebook checks a rolling 24-hour window, beginning no earlier than the first scheduled scored origin, October 16 at 02:00 UTC. It monitors the currently frozen active model. Each origin becomes due ten minutes after the hour; this reporting grace allows pending writes, while on-time publication still strictly requires persistence completion **before the nominal origin** for all three horizons.

| Check | Threshold | Status effect |
|---|---|---|
| Forecast completeness | Exactly horizons 1, 2 and 3 for each due origin | ERROR on missing/duplicate rows |
| Publication timing | Every canonical forecast persisted before its origin | ERROR on late/unverified publication |
| Prediction values | Finite, nonnegative values | ERROR on invalid values |
| Calendar fallback | Count all fallback rows in due origins | WARN when present |
| Source requests | Count failed HTTP requests in the last 24 hours | WARN when present |
| Retrieval freshness | Last successful retrieval of each of five sources no older than 90 minutes | WARN if stale/missing |
| Outcome availability | Count forecasts with genuine imported outcomes | Informational; no accuracy until outcomes mature |

The five sources are GBFS discovery, station information, station status, MTA alerts and forecast weather. Freshness refers to successful retrieval time; an HTTP success does not establish provider payload age, continuous coverage or the assumed weather-release delay. A request failure remains visible in the daily window even after recovery. A report with no due origins is WARN.

`ERROR` preserves the detailed report and Delta summary before failing the job, making the failure visible in Databricks Jobs. `WARN` preserves the report and lets the job succeed; inspect the report's status, rather than equating job success with clean source health. There are no email, Slack or third-party notifications.

## Reports and dashboard

`workspace.citibike_3h.borough_daily_health` appends one summary per monitoring run. The `borough_daily_health_latest` view selects the newest report. The [forecast dashboard](https://dbc-d00cbd4c-c74c.cloud.databricks.com/editor/notebooks/2763628353985993) displays it beside the dated subway mapping coverage. Detailed JSON includes every due origin and per-source counts, timestamps and freshness.

```text
/Volumes/workspace/citibike_3h/exports/health/<monitor_run_id>/report.json
/Volumes/workspace/citibike_3h/exports/borough_health_latest.json
```

Each run has a distinct immutable report directory; the latest JSON is a mutable convenience pointer. The helper module is uploaded by content hash and verified before execution. The job has one concurrent run, a 600-second timeout and no automatic retries. It runs in serverless environment 5; it neither trains the model nor rescans trip archives. It needs no local OAuth file inside Databricks because it reads the project's Delta tables directly.

```sql
SELECT * FROM workspace.citibike_3h.borough_daily_health_latest;

SELECT checked_at_utc, status, incomplete_origins, late_origins,
       fallback_rows, source_failures, stale_sources, matured_forecasts
FROM workspace.citibike_3h.borough_daily_health
ORDER BY checked_at_utc DESC;
```

## Investigation and operation

For ERROR, inspect the origin details and compare the hourly job's collection/scoring tasks with `borough_forecasts` and `collection_log`. Preserve first forecasts; a late rerun cannot make a missed origin on time. For WARN, inspect fallback reasons and the latest source attempts. Calendar fallback can maintain forecast availability while weather recovers. Matching published trip outcomes must be imported separately before calculating prospective MAE.

Daily monitoring uses canonical persisted forecasts, so it does not measure the timing of every retry or every scheduler task. The separate SDK audit inspects actual attempt completion and scheduler run states:

```sh
uv run python scripts/audit_three_hour_operations.py --output docs/validation/operational_health_YYYY-MM-DD.json
```

To deploy a revised monitor, run its manual cloud check and then enable the schedule:

```sh
uv run python scripts/deploy_public_mvp.py health
uv run python scripts/deploy_public_mvp.py health --schedule
```

Wait for the first command's run to succeed before scheduling; the second command enforces that gate and does not launch another manual run. Deployment state is in `.local/execution/databricks_health.json`. Use the daily job's Databricks schedule controls to pause monitoring independently of hourly scoring. To stop all recurring project work, pause **all three** schedules: hourly forecasting, daily health and daily public evidence. `manage_three_hour_workspace.py pause-collector` pauses only hourly collection/scoring.

The [public evidence job](release_aware_evidence_pipeline.md) runs at 13:00 UTC. It observes trip releases, imports prepared outcomes and normalizes station/MTA evidence. Forecast health continues to measure delivered forecasts and source retrieval freshness; the evidence job reports its own task failures and backlog.

Before activating a different model version, define its monitoring start and version coverage explicitly. The current monitor checks the frozen active version from the configured deployment origin; changing the active pointer without adjusting that contract would report older origins as missing for the new version. The monitor does not automatically backfill forecasts or retune model thresholds.
