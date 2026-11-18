# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 · Station selection, labels, and features
# MAGIC Builds `features_labels_15m`, one row per `(station_id, prediction_time_utc)` on a 15-minute UTC grid.
# MAGIC
# MAGIC | Item | Definition |
# MAGIC |---|---|
# MAGIC | State at T | latest snapshot with `snapshot_ts` in `(T − 10 min, T]` |
# MAGIC | `empty_label` | `num_bikes_available == 0` at t + 15 (e-bikes count as bikes, as in GBFS) |
# MAGIC | `full_label` | `num_docks_available == 0` at t + 15 |
# MAGIC | Excluded | no fresh state at t or t + 15, or station not installed/renting/returning at either |
# MAGIC | Features | state at t; Δbikes/Δdocks over 15/30/60 min; capacity; fill ratio; e-bike share; report age; local time-of-day and day-of-week (cyclic) |
# MAGIC
# MAGIC Every feature uses only data at or before t. Station × hour priors are fit later on the train split only (03).

# COMMAND ----------

import os, sys
sys.path.insert(0, os.getcwd())
import importlib, config as C, citibike_lib as L
importlib.reload(C); importlib.reload(L)
L.init_spark(spark)
from pyspark.sql import functions as F

status = spark.table(C.T["std_status_hist"])
info = spark.table(C.T["std_info"])

# COMMAND ----------

# MAGIC %md ## 1 · Full-network pass for station quality
# MAGIC Coverage and event rates for every station, used only to choose the MVP stations.

# COMMAND ----------

import pandas as pd

fl_all = L.build_features_labels(status, info, C)

bounds = fl_all.agg(
    F.min("prediction_time_utc").alias("first_time"),
    F.max("prediction_time_utc").alias("last_time")
).first()

split = L.compute_split(
    pd.Series([bounds["first_time"], bounds["last_time"]]),
    C
)

training_cutoff = (
    split["val_start_utc"]
    - pd.Timedelta(minutes=C.EMBARGO_MIN)
)

selection_rows = fl_all.where(
    F.col("prediction_time_utc") < F.lit(training_cutoff.to_pydatetime())
)

quality = L.station_quality(selection_rows)

print("Validation starts:", split["val_start_utc"])
print("Test starts:", split["test_start_utc"])
print("Training ends before:", training_cutoff)
quality.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T["station_quality"])
display(spark.table(C.T["station_quality"]).orderBy(F.desc("coverage")).limit(20))

# COMMAND ----------

split_record = {
    "val_start_utc": split["val_start_utc"].to_pydatetime(),
    "test_start_utc": split["test_start_utc"].to_pydatetime(),
    "embargo_min": int(split["embargo_min"]),
    "data_start_utc": bounds["first_time"],
    "data_end_utc": bounds["last_time"],
}

(
    spark.createDataFrame([split_record])
    .withColumn("created_at", F.current_timestamp())
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(C.T["split_config"])
)

# COMMAND ----------

# MAGIC %md ## 2 · Use All eligible MVP stations

# COMMAND ----------

live_ids = None
if spark.catalog.tableExists(C.SOURCE_LIVE_STATUS_TABLE):
    live_ids = (
        spark.table(C.SOURCE_LIVE_STATUS_TABLE)
        .select("station_id")
        .distinct()
    )
    overlap = spark.table(C.T["station_quality"]).join(live_ids, "station_id").count()
    print(f"historical stations also in live feed: {overlap}")
    if overlap == 0:
        raise ValueError("No station_id overlap between historical silver data and the live feed. "
                         "The two sources use different ID schemes; map them (e.g. via short_name/legacy_id) before continuing.")
else:
    print("WARNING: live_station_status not found yet; selecting without the live-feed filter. Re-run once the collector has data.")

stations = L.select_stations(spark.table(C.T["station_quality"]), live_ids, C)
stations.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T["mvp_stations"])
n = spark.table(C.T["mvp_stations"]).count()
print("MVP stations:", n)
if n < 50:
    raise ValueError(f"Only {n} stations met coverage >= {C.MIN_COVERAGE}. Lower MIN_COVERAGE in config.py and record why in ASSUMPTIONS.md.")
display(spark.table(C.T["mvp_stations"]).groupBy("selection_stratum").agg(
    F.count("*").alias("stations"), F.avg("empty_rate").alias("empty_rate"), F.avg("full_rate").alias("full_rate")))

# COMMAND ----------

# MAGIC %md ## 3 · Features and labels for MVP stations

# COMMAND ----------

mvp = spark.table(C.T["mvp_stations"]).select("station_id")
fl = L.build_features_labels(status.join(mvp, "station_id"), info, C)
(fl.write.mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable(C.T["features_labels"]))

spark.sql(f"COMMENT ON TABLE {C.T['features_labels']} IS "
          "'Point-in-time features at t and empty/full labels at t+15 for MVP stations. Key: station_id, prediction_time_utc.'")

# COMMAND ----------

# MAGIC %md ## 4 · Data-quality checks (fail loudly)

# COMMAND ----------

fl = spark.table(C.T["features_labels"])
dup = fl.groupBy("station_id", "prediction_time_utc").count().where("count > 1").count()
leak = fl.where("snapshot_ts > prediction_time_utc").count()
bad_h = fl.where(f"eligible = 1 AND (horizon_offset_min > {C.HORIZON_MIN} "
                 f"OR horizon_offset_min <= {C.HORIZON_MIN - C.ASOF_TOLERANCE_MIN})").count()
neg = fl.where("num_bikes_available < 0 OR num_docks_available < 0").count()
summary = fl.agg(F.count("*").alias("rows"), F.sum("eligible").alias("eligible_rows"),
                 F.avg(F.when(F.col("eligible") == 1, F.col("empty_label"))).alias("empty_rate"),
                 F.avg(F.when(F.col("eligible") == 1, F.col("full_label"))).alias("full_rate"),
                 F.min("prediction_time_utc").alias("t_min"), F.max("prediction_time_utc").alias("t_max")).first()
print(summary)
display(fl.groupBy("exclusion_reason").count().orderBy(F.desc("count")))

checks = {"duplicate keys": dup, "feature snapshot after t (leakage)": leak,
          "label outside (t+5, t+15]": bad_h, "negative counts": neg}
failed = {k: v for k, v in checks.items() if v}
assert not failed, f"Data-quality checks failed: {failed}"
print("All data-quality checks passed:", checks)