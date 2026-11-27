# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Freshness, volume, and live-outcome monitoring
# MAGIC Third task in the job. Writes one row per check to `monitoring_log`, refreshes
# MAGIC `live_outcomes` (predictions joined to what actually happened at t+15), and
# MAGIC **raises** on a failed check so the job's failure email fires.

# COMMAND ----------

import os, sys, datetime as dt
sys.path.insert(0, os.getcwd())
import importlib, config as C, citibike_lib as L
importlib.reload(C); importlib.reload(L)
L.init_spark(spark)
from pyspark.sql import functions as F

now = dt.datetime.utcnow()
checks = []

def check(name, value, ok, detail="", fatal=True):
    """fatal=False -> logged and shown on the dashboard, but does not fail the job."""
    checks.append((now, name, float(value) if value is not None else None, bool(ok), detail, fatal))

# COMMAND ----------

# Collector freshness and volume
live = spark.table(C.T["live_status"])
last = live.agg(F.max("snapshot_ts").alias("ts")).first()["ts"]
age = (now - last).total_seconds() / 60 if last else None
check("snapshot_age_min", age, age is not None and age <= C.MAX_SNAPSHOT_AGE_MIN, f"newest snapshot {last}")

n_latest = live.where(F.col("snapshot_ts") == F.lit(last)).count() if last else 0
check("stations_in_latest_snapshot", n_latest, n_latest >= C.MIN_LIVE_STATIONS)

runs = spark.table(C.T["collector_runs"]).where(F.col("fetched_at") >= F.lit(now - dt.timedelta(hours=24)))
r = runs.agg(F.count("*").alias("n"), F.sum(F.when(F.col("status") == "OK", 1).otherwise(0)).alias("ok")).first()
fail_rate = 1 - (r["ok"] or 0) / r["n"] if r["n"] else None
check("collector_fail_rate_24h", fail_rate, fail_rate is not None and fail_rate <= 0.1, f"{r['n']} runs in 24h")

# COMMAND ----------

# Prediction freshness
if spark.catalog.tableExists(C.T["predictions"]):
    p_last = spark.table(C.T["predictions"]).agg(F.max("scored_at")).first()[0]
    p_age = (now - p_last).total_seconds() / 60 if p_last else None
    check("prediction_age_min", p_age, p_age is not None and p_age <= C.MAX_PREDICTION_AGE_MIN, f"last scored {p_last}")

    # Realized outcomes: live labels from the same library function used in training.
    mvp = spark.table(C.T["mvp_stations"]).select("station_id")
    recent = live.where(F.col("snapshot_ts") >= F.lit(now - dt.timedelta(days=3))).join(mvp, "station_id")
    labels = (L.build_features_labels(recent, spark.table(C.T["live_info"]), C, with_labels=True)
              .where("eligible = 1")
              .select("station_id", "prediction_time_utc", "empty_label", "full_label"))
    outcomes = spark.table(C.T["predictions"]).join(labels, ["station_id", "prediction_time_utc"], "inner")
    (outcomes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T["live_outcomes"]))

    o = outcomes.where(F.col("prediction_time_utc") >= F.lit(now - dt.timedelta(hours=24))).agg(
        F.count("*").alias("n"),
        F.avg((F.col("p_empty") - F.col("empty_label")) ** 2).alias("brier_empty"),
        F.avg((F.col("p_full") - F.col("full_label")) ** 2).alias("brier_full"),
        F.avg("empty_label").alias("empty_rate"), F.avg("p_empty").alias("mean_p_empty"),
        F.avg("full_label").alias("full_rate"), F.avg("p_full").alias("mean_p_full")).first()
    if o["n"]:
        check("live_brier_empty_24h", o["brier_empty"], True, f"n={o['n']}")
        check("live_brier_full_24h", o["brier_full"], True, f"n={o['n']}")
        # calibration-in-the-large drift: mean prediction vs observed rate
        check("live_empty_rate_gap_24h", abs(o["mean_p_empty"] - o["empty_rate"]), abs(o["mean_p_empty"] - o["empty_rate"]) < 0.05, fatal=False)
        check("live_full_rate_gap_24h", abs(o["mean_p_full"] - o["full_rate"]), abs(o["mean_p_full"] - o["full_rate"]) < 0.05, fatal=False)

# COMMAND ----------

log = spark.createDataFrame(checks, "checked_at timestamp, check string, value double, ok boolean, detail string, fatal boolean")
log.write.mode("append").saveAsTable(C.T["monitoring_log"])
display(log)
failed = [c for c in checks if not c[3] and c[5]]
if failed:
    raise RuntimeError("Monitoring checks failed: " + "; ".join(f"{c[1]}={c[2]} ({c[4]})" for c in failed))
