# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Read the saved development data, calculate predictors, and build 30-minute stockout targets.
# MAGIC Output keys are station_id and fetched_at. Predictors and targets are saved separately.
# MAGIC Feature calculations are loaded from shared-station-features. This driver uses the saved development inputs.
# MAGIC This is intended to build the dataset we will use to train and validate our stockout and dock full prediction
# MAGIC models
# MAGIC

# COMMAND ----------

# MAGIC %run ./shared-station-feature-pipeline

# COMMAND ----------

# Team 3 reading copy with comments added (November 14, 2026). The code is unchanged from the shared pipeline original at the repository root, as saved in the workspace that day.
# This is a teammate-owned upstream notebook from the shared pipeline. Team 3 did not rerun it.
# The checked frozen-model code reads the station exports directly and builds its own 60-minute targets,
# so this notebook (30-minute targets) is not a direct input. The full history of the exports was not traced.
# The comments that were already here are the author's; the added ones follow the same top-to-bottom steps.
#
# The above run of the notebook is quite finicky, keep it in its own cell

## Packages and run settings
from pyspark.sql import functions as F, Window



spark.conf.set("spark.sql.session.timeZone", "UTC")
# Scheduled jobs set run_mode=production.
# Manual development runs default to calculating without writing tables.
dbutils.widgets.dropdown(
    "run_mode", "development", ["development", "production"]
)

RUN_MODE = dbutils.widgets.get("run_mode")
PUBLISH = RUN_MODE == "production"

## Input and output tables
STATUS_TABLE = "citibike_project.citibike.silver_station_status"
INFO_TABLE = "citibike_project.citibike.silver_station_info"

PREDICTOR_TABLE = "citibike_project.citibike.gold_station_predictors"
TARGET_TABLE = "citibike_project.citibike.gold_station_targets"

print(f"Run mode: {RUN_MODE}")
print(f"Write gold tables: {PUBLISH}")

## Constants: New York time, 30-minute target horizon, largest allowed gap between polls, minimum history
LOCAL_TZ = "America/New_York"
HORIZON_MIN = 30
MAX_OBSERVATION_GAP_MIN = 10
MIN_HISTORY_MIN = 20
MICROSECONDS_PER_MINUTE = 60000000

## Load silver status and silver station information
status = spark.table(STATUS_TABLE).select(
    "station_id", "fetched_at", "station_reported_at",
    "num_bikes_available", "num_bikes_disabled",
    "num_docks_available", "num_docks_disabled",
    "is_installed", "is_renting", "is_returning",
)
info = spark.table(INFO_TABLE).select(
    "station_id", "fetched_at", "name", "capacity", "lat", "lon"
).distinct()

# Check missing and duplicate keys in one aggregation per table.
for name, frame in [("status", status), ("information", info)]:
    invalid_keys = frame.groupBy("station_id", "fetched_at").count().filter(
        "station_id IS NULL OR fetched_at IS NULL OR count > 1"
    )
    if invalid_keys.take(1):
        display(invalid_keys.limit(10))
        raise ValueError(f"{name}: missing or duplicate station/poll keys")


# We build the predictors table

## Build the predictors with the shared feature functions (loaded by the %run cell above)
predictors = build_predictors(
    status,
    info,
    local_timezone=LOCAL_TZ,
    max_gap_minutes=MAX_OBSERVATION_GAP_MIN,
    min_history_minutes=MIN_HISTORY_MIN,
)
# Write and reload predictors before calculating targets. We do this for speed because
# we don't want to trigger the lazy eval twice

if PUBLISH:
    predictors.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(PREDICTOR_TABLE)
    predictors = spark.table(PREDICTOR_TABLE)

## Build the 30-minute stockout targets from the predictors
targets = build_targets(
    predictors,
    horizon_minutes=HORIZON_MIN,
    max_gap_minutes=MAX_OBSERVATION_GAP_MIN,
)

if PUBLISH:
    targets.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TARGET_TABLE)
    targets = spark.table(TARGET_TABLE)
else:
    print("PUBLISH=False: output tables were not written.")

## Display a summary: polls, labeled polls and the share that ran out, for bikes and docks
display(targets.agg(
    F.count("*").alias("polls"),
    F.count("bike_target").alias("bike_labeled_polls"),
    F.avg("bike_target").alias("bike_onset_share"),
    F.count("dock_target").alias("dock_labeled_polls"),
    F.avg("dock_target").alias("dock_onset_share"),
))



