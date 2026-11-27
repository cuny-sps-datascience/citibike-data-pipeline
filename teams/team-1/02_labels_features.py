# Databricks notebook source
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
# MAGIC
# MAGIC **Scope widget** (top of the notebook):
# MAGIC * `mvp`: the original 100 stations (50 high event-rate + 50 seeded random). Writes to `workspace.citibike`, the live system's schema.
# MAGIC * `full`: every station with ≥ 90% coverage **in the training period** that is also in the live feed. Writes to
# MAGIC   `workspace.citibike_full` with its own models, so the live jobs are untouched. The split is fixed here and saved,
# MAGIC   so station selection never looks at validation or test weeks. Builds on Maxfield's full-station commit (21dd8fd).

# COMMAND ----------

dbutils.widgets.dropdown("scope", "mvp", ["mvp", "full"], "Station scope")
import os, sys
os.environ["CITIBIKE_SCOPE"] = dbutils.widgets.get("scope")
sys.path.insert(0, os.getcwd())
import importlib, datetime as dt, config as C, citibike_lib as L
importlib.reload(C); importlib.reload(L)
L.init_spark(spark)
import pandas as pd
from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C.CATALOG}.{C.OUT_SCHEMA}")
print(f"scope={C.SCOPE} -> outputs in {C.CATALOG}.{C.OUT_SCHEMA}")
timer = L.StepTimer("02_labels_features")

status = spark.table(C.T["std_status_hist"])
info = spark.table(C.T["std_info"])

def save(df, key):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T[key])
    return spark.table(C.T[key])

# COMMAND ----------

# MAGIC %md ## 1 · Full-network pass for station quality
# MAGIC Coverage and event rates for every station, used only to choose stations.
# MAGIC In `full` scope the features for every station are saved once (`features_labels_15m_all`) and reused,
# MAGIC the split is fixed from the data and saved, and quality is measured on **training rows only**.

# COMMAND ----------

timer.start("features_all_stations")
fl_all = L.build_features_labels(status, info, C)
if C.SCOPE == "full":
    fl_all = save(fl_all, "features_labels_all")
    timer.stop(rows=fl_all.count())

    elig_all = fl_all.where("eligible = 1")
    bounds = elig_all.agg(F.min("prediction_time_utc").alias("tmin"), F.max("prediction_time_utc").alias("tmax")).first()
    split = L.compute_split(pd.Series([bounds["tmin"], bounds["tmax"]]), C)
    split_row = {"val_start_utc": split["val_start_utc"].to_pydatetime(),
                 "test_start_utc": split["test_start_utc"].to_pydatetime(),
                 "embargo_min": int(split["embargo_min"]),
                 "data_start_utc": bounds["tmin"], "data_end_utc": bounds["tmax"],
                 "created_at": dt.datetime.utcnow()}
    save(spark.createDataFrame([split_row]), "split_config")
    print("split fixed and saved:", split_row)
    train_cut = split_row["val_start_utc"] - dt.timedelta(minutes=split_row["embargo_min"])
    quality_src = fl_all.where(F.col("prediction_time_utc") < F.lit(train_cut))
else:
    quality_src = fl_all

timer.start("station_quality")
quality = save(L.station_quality(quality_src), "station_quality")
timer.stop(rows=quality.count())
display(quality.orderBy(F.desc("coverage")).limit(20))

# COMMAND ----------

# MAGIC %md ## 2 · Select stations
# MAGIC * `mvp`: 50 high event-rate + 50 seeded random, coverage ≥ 90%, present in the live feed.
# MAGIC * `full`: all stations with coverage ≥ 90% present in the live feed; `selection_stratum` = event-rate tier (low / mid / high).

# COMMAND ----------

live_ids = L.live_station_ids(spark, C)
if live_ids is not None:
    overlap = quality.join(live_ids, "station_id").count()
    print(f"historical stations also in live feed: {overlap}")
    if overlap == 0:
        raise ValueError("No station_id overlap between historical silver data and the live feed. "
                         "The two sources use different ID schemes; map them (e.g. via short_name/legacy_id) before continuing.")
else:
    print("WARNING: no live_station_status table found; selecting without the live-feed filter. Re-run once the collector has data.")

if C.SCOPE == "full":
    stations = save(L.select_stations_full(quality, live_ids, C), "mvp_stations")
else:
    stations = save(L.select_stations(quality, live_ids, C), "mvp_stations")
n = stations.count()
print(f"{C.SCOPE} stations: {n:,}  (of {quality.count():,} with history)")
if n < 50:
    raise ValueError(f"Only {n} stations met coverage >= {C.MIN_COVERAGE}. Lower MIN_COVERAGE in config.py and record why in ASSUMPTIONS.md.")
display(stations.groupBy("selection_stratum").agg(
    F.count("*").alias("stations"), F.avg("empty_rate").alias("empty_rate"), F.avg("full_rate").alias("full_rate"))
    .orderBy("selection_stratum"))

# COMMAND ----------

# MAGIC %md ## 3 · Features and labels for the selected stations

# COMMAND ----------

timer.start("features_selected")
sel = spark.table(C.T["mvp_stations"]).select("station_id")
if C.SCOPE == "full":
    fl = fl_all.join(sel, "station_id")                     # reuse the saved all-station features
else:
    fl = L.build_features_labels(status.join(sel, "station_id"), info, C)
fl = save(fl, "features_labels")
timer.stop(rows=fl.count())

spark.sql(f"COMMENT ON TABLE {C.T['features_labels']} IS "
          f"'Point-in-time features at t and empty/full labels at t+15 ({C.SCOPE} scope). Key: station_id, prediction_time_utc.'")

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
timer.save(spark)
