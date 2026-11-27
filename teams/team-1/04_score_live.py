# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Batch-score live snapshots
# MAGIC Runs right after the collector in the same job. Builds features for the newest
# MAGIC 15-minute grid time with the **same library function used in training**, loads
# MAGIC both `@champion` models from Unity Catalog, and MERGEs predictions keyed on
# MAGIC `(station_id, prediction_time_utc)`. Re-running is a no-op for an already-scored time.

# COMMAND ----------

import os, sys, datetime as dt
sys.path.insert(0, os.getcwd())
import importlib, config as C, citibike_lib as L
importlib.reload(C); importlib.reload(L)
L.init_spark(spark)
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# COMMAND ----------

def champion(target):
    try:
        mv = client.get_model_version_by_alias(C.MODEL_NAMES[target], C.MODEL_ALIAS)
    except Exception as e:
        return None, None, None
    model = mlflow.sklearn.load_model(f"models:/{C.MODEL_NAMES[target]}@{C.MODEL_ALIAS}")
    return model, mv.version, float(mv.tags.get("threshold", 0.5))

models = {t: champion(t) for t in C.TARGETS}
if any(m[0] is None for m in models.values()):
    dbutils.notebook.exit("No champion model registered yet; run 03_train_evaluate. Skipping scoring.")

# COMMAND ----------

# Only the window needed for the longest lag + tolerance, for MVP stations.
lookback_min = max(C.LAG_STEPS) * C.GRID_MIN + C.ASOF_TOLERANCE_MIN + C.GRID_MIN
mvp = spark.table(C.T["mvp_stations"]).select("station_id")
live = (spark.table(C.T["live_status"])
        .where(F.col("snapshot_ts") >= F.expr(f"current_timestamp() - INTERVAL {lookback_min + 30} MINUTES"))
        .join(mvp, "station_id"))
info = spark.table(C.T["live_info"])

feats = L.build_features_labels(live, info, C, with_labels=False)
latest_t = feats.agg(F.max("prediction_time_utc")).first()[0]
if latest_t is None:
    dbutils.notebook.exit("No recent live snapshots for MVP stations; nothing to score.")

pdf = feats.where((F.col("prediction_time_utc") == F.lit(latest_t)) & (F.col("eligible") == 1)).toPandas()
if pdf.empty:
    # Fail loudly: a silent skip here hides a broken pipeline (e.g. collector cadence too slow
    # for ASOF_TOLERANCE_MIN). The job's failure email will fire.
    diag = (feats.where(F.col("prediction_time_utc") == F.lit(latest_t))
                 .groupBy("exclusion_reason").count().collect())
    newest = live.agg(F.max("snapshot_ts")).first()[0]
    raise RuntimeError(
        f"No eligible stations at {latest_t}. Exclusions: {[(r[0], r[1]) for r in diag]}. "
        f"Newest live snapshot: {newest}. A state at t needs a snapshot within "
        f"{C.ASOF_TOLERANCE_MIN} min before t; check that the collector runs every 5 minutes.")
priors = spark.table(C.T["station_priors"]).toPandas()
pdf = L.join_priors(pdf, priors)
X = L.to_pandas_features(pdf)
print(f"scoring {len(pdf)} stations at {latest_t}")

# COMMAND ----------

out = pd.DataFrame({
    "station_id": pdf["station_id"],
    "prediction_time_utc": pd.to_datetime(pdf["prediction_time_utc"]),
    "target_time_utc": pd.to_datetime(pdf["prediction_time_utc"]) + pd.Timedelta(minutes=C.HORIZON_MIN),
    "snapshot_ts": pd.to_datetime(pdf["snapshot_ts"]),
    "state_age_min": pdf["state_age_min"].astype(float),
    "num_bikes_available": pdf["num_bikes_available"].astype("int64"),
    "num_docks_available": pdf["num_docks_available"].astype("int64"),
})
for t, (model, version, thr) in models.items():
    p = model.predict_proba(X)[:, 1]
    out[f"p_{t}"] = p
    out[f"pred_{t}"] = (p >= thr).astype(int)
    out[f"threshold_{t}"] = thr
    out[f"model_version_{t}"] = str(version)
out["scored_at"] = dt.datetime.utcnow()

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C.T['predictions']} (
  station_id STRING, prediction_time_utc TIMESTAMP, target_time_utc TIMESTAMP, snapshot_ts TIMESTAMP,
  state_age_min DOUBLE, num_bikes_available BIGINT, num_docks_available BIGINT,
  p_empty DOUBLE, pred_empty INT, threshold_empty DOUBLE, model_version_empty STRING,
  p_full DOUBLE, pred_full INT, threshold_full DOUBLE, model_version_full STRING,
  scored_at TIMESTAMP
) COMMENT 'Live 15-minute-ahead empty/full probabilities for MVP stations'
""")
sdf = spark.createDataFrame(out).select(*spark.table(C.T["predictions"]).columns)
sdf.createOrReplaceTempView("new_preds")
spark.sql(f"""
MERGE INTO {C.T['predictions']} t USING new_preds s
ON t.station_id = s.station_id AND t.prediction_time_utc = s.prediction_time_utc
WHEN NOT MATCHED THEN INSERT *
""")
print(f"wrote {len(out)} predictions for {latest_t}; "
      f"flagged empty={int(out['pred_empty'].sum())}, full={int(out['pred_full'].sum())}")
