# Databricks notebook source
# MAGIC %md
# MAGIC # Citi Bike three hour data readiness
# MAGIC Randy currently operates all project workstreams. These are source observations and coverage checks.
# MAGIC Departure forecasting and historical accuracy remain gated until historical operating exposure is documented.

# COMMAND ----------
display(spark.sql('''SELECT 'subway station references' AS dataset,count(*) AS rows
    FROM workspace.citibike_3h.subway_stations
    UNION ALL SELECT 'historical subway alert revisions',count(*)
    FROM workspace.citibike_3h.subway_alert_archive
    UNION ALL SELECT 'archived weather hours',count(*)
    FROM workspace.citibike_3h.nyc_weather_forecast_run
    UNION ALL SELECT 'current Citi Bike station references',count(*)
    FROM workspace.citibike_3h.gbfs_station_information'''))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Historical alert coverage
# MAGIC An absent record does not prove normal service. The timestamp timezone and feed continuity require verification.

# COMMAND ----------
display(spark.sql('''SELECT substring(date,1,7) AS month,count(*) AS revisions,
    min(date) AS earliest_source_timestamp,max(date) AS latest_source_timestamp
    FROM workspace.citibike_3h.subway_alert_archive GROUP BY 1 ORDER BY 1'''))

# COMMAND ----------
display(spark.sql('''SELECT initialization_utc, min(valid_time_utc) AS first_valid_hour,
    max(valid_time_utc) AS last_valid_hour,count(*) AS hours,
    count_if(precipitation IS NULL) AS missing_precipitation_hours
    FROM workspace.citibike_3h.nyc_weather_forecast_run GROUP BY 1 ORDER BY 1'''))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Remaining inputs
# MAGIC Historical station operation and dated geography are required for valid zero-hour counts.
# MAGIC The current weather package is a four-run access pilot. It does not cover the entire training period.
# MAGIC A table in this personal workspace still needs an authorized import into the class shared catalog.
