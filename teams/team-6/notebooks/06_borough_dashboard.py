# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Manhattan three-hour forecast dashboard
# MAGIC Predictions are **three separate hourly totals across Manhattan**, not station counts. This public-only benchmark preserves the proposal's temporal splits. Weather availability uses an assumed seven-hour delay; monthly release dates use an assumed 45-day delay. Early weather archives include hindcasts. No historical subway-alert effect is claimed. Prospective accuracy waits for published trip outcomes.

# COMMAND ----------
display(spark.sql('''SELECT scope,origin_utc,horizon,target_start_utc,target_end_utc,
    from_utc_timestamp(target_start_utc,'America/New_York') AS target_start_new_york,
    prediction,selected_model,profile_release,weather_initialization_utc,
    fallback_reason,publication_completed_at_utc,published_before_target,
    publication_completed_at_utc<origin_utc AS published_before_origin
    FROM workspace.citibike_3h.borough_forecasts ORDER BY origin_utc DESC,horizon LIMIT 72'''))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Retrospective model comparison
# MAGIC MAE and RMSE are departures per Manhattan hour. Model choice uses validation only. Test figures describe retrospective development, with the release assumptions above.

# COMMAND ----------
display(spark.sql('SELECT * FROM workspace.citibike_3h.borough_model_metrics ORDER BY model_version DESC,split,mae'))

# COMMAND ----------
import json
from pathlib import Path
import pyarrow.parquet as pq
import matplotlib.pyplot as plt

metadata=json.loads(Path('/Volumes/workspace/citibike_3h/models/borough_active.json').read_text())
test=pq.read_table(Path(metadata['model_root'])/'test_predictions.parquet').to_pylist()
subset=sorted([r for r in test if r['horizon']==1],key=lambda r:r['target_start_utc'])[-168:]
fig,ax=plt.subplots(figsize=(12,4))
hours=[r['target_start_utc'] for r in subset]
ax.plot(hours,[r['departures'] for r in subset],label='Published departures',linewidth=1.5)
ax.plot(hours,[r['prediction'] for r in subset],label='Selected model',linewidth=1.2)
ax.set(title='Manhattan total: final seven days of the retrospective test window',
       xlabel='Target hour (UTC)',ylabel='Departures per hour')
ax.legend();fig.autofmt_xdate();fig.tight_layout()
fig.savefig('/Volumes/workspace/citibike_3h/exports/borough_test_final_week.png',dpi=160)
display(fig)

# COMMAND ----------
display(spark.sql('''SELECT source,count(*) AS attempts,sum(CASE WHEN success THEN 1 ELSE 0 END) AS successes,
    max(observed_at_utc) AS latest_observation FROM workspace.citibike_3h.collection_log GROUP BY source'''))
display(spark.sql('''SELECT split,horizon,count(*) AS rows,
    sum(CASE WHEN weather_covered THEN 1 ELSE 0 END) AS weather_covered
    FROM workspace.citibike_3h.borough_features GROUP BY split,horizon ORDER BY split,horizon'''))
if spark.catalog.tableExists('workspace.citibike_3h.borough_prospective_evaluation'):
    display(spark.sql('''SELECT model_version,horizon,count(*) AS matured_forecasts,
        avg(absolute_error) AS live_mae,sqrt(avg(squared_error)) AS live_rmse
        FROM workspace.citibike_3h.borough_prospective_evaluation GROUP BY ALL'''))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Daily health and prospective subway reference
# MAGIC Health checks delivered forecasts and source retrieval freshness. Operational health does not establish live predictive accuracy. The station/subway mapping is a dated, straight-line reference; historical geography and subway disruption effects remain unverified.

# COMMAND ----------
if spark.catalog.tableExists('workspace.citibike_3h.borough_daily_health_latest'):
    display(spark.table('workspace.citibike_3h.borough_daily_health_latest'))
if spark.catalog.tableExists('workspace.citibike_3h.station_subway_coverage'):
    display(spark.sql('''SELECT mapping_version,radius_m,count(*) AS bike_stations,
        sum(CASE WHEN nearby_station_points>0 THEN 1 ELSE 0 END) AS with_subway_station,
        sum(CASE WHEN nearby_routes>0 THEN 1 ELSE 0 END) AS with_daytime_route
        FROM workspace.citibike_3h.station_subway_coverage GROUP BY ALL ORDER BY radius_m'''))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Release-aware backtests and public evidence
# MAGIC Rolling results gate fitting and selection by assumed label release times. These previously inspected historical months remain retrospective evidence. The release watcher queues files for local validation; listing does not approve complete outcomes. Station observations and matched subway advisories are prospective references, with derived publication times and coverage flags.

# COMMAND ----------
if spark.catalog.tableExists('workspace.citibike_3h.borough_rolling_metrics'):
    display(spark.table('workspace.citibike_3h.borough_rolling_metrics').orderBy('fold_month','mae'))
if spark.catalog.tableExists('workspace.citibike_3h.trip_release_candidates'):
    display(spark.table('workspace.citibike_3h.trip_release_candidates').where(
        "readiness='awaiting_local_ingestion_and_label_validation'"))
if spark.catalog.tableExists('workspace.citibike_3h.borough_evaluation_daily'):
    display(spark.table('workspace.citibike_3h.borough_evaluation_daily').orderBy('checked_at_utc',ascending=False).limit(7))
if spark.catalog.tableExists('workspace.citibike_3h.borough_actual_hourly'):
    display(spark.sql('''SELECT count(*) AS prepared_outcome_hours,min(hour_utc) AS first_hour_utc,
        max(hour_utc) AS latest_hour_utc,
        sum(CASE WHEN NOT label_assignable THEN 1 ELSE 0 END) AS unassignable_hours
        FROM workspace.citibike_3h.borough_actual_hourly'''))
if spark.catalog.tableExists('workspace.citibike_3h.station_information_observations'):
    display(spark.sql('''SELECT count(*) AS station_observations,count(DISTINCT station_id) AS distinct_station_ids,
        min(observed_at_utc) AS first_observed_utc,max(observed_at_utc) AS latest_observed_utc
        FROM workspace.citibike_3h.station_information_observations'''))
    display(spark.table('workspace.citibike_3h.station_geography_changes').orderBy('observed_at_utc',ascending=False).limit(20))
    display(spark.table('workspace.citibike_3h.prospective_source_gaps'))
if spark.catalog.tableExists('workspace.citibike_3h.station_subway_alert_features_available'):
    display(spark.sql('''SELECT observed_at_utc,radius_m,count(*) AS bike_stations,
        count(matched_active_alert_count) AS rows_with_usable_reference,
        sum(CASE WHEN matched_active_alert_count>0 THEN 1 ELSE 0 END) AS with_matched_advisories,
        max(available_from_utc) AS derived_features_available_from
        FROM workspace.citibike_3h.station_subway_alert_features_available
        GROUP BY ALL ORDER BY observed_at_utc DESC,radius_m LIMIT 12'''))
    display(spark.sql('''SELECT resolution,count(*) AS selectors FROM workspace.citibike_3h.mta_alert_selectors
        WHERE agency_id='MTASBWY' GROUP BY resolution'''))
dbutils.notebook.exit('Manhattan forecast dashboard queries succeeded')
