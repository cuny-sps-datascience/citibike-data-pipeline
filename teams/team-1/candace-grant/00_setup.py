# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup and source validation
# MAGIC Creates the project schema and volume, inspects the professor's silver tables,
# MAGIC validates the column map in `config.py`, and writes standardized copies.
# MAGIC
# MAGIC Run once (and again whenever the professor's tables are refreshed).

# COMMAND ----------

import os, sys
sys.path.insert(0, os.getcwd())   # notebook folder: config.py and citibike_lib.py live here
import importlib, config as C, citibike_lib as L
importlib.reload(C); importlib.reload(L)
L.init_spark(spark)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C.CATALOG}.{C.SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {C.CATALOG}.{C.SCHEMA}.{C.VOLUME}")
print("schema + volume ready:", C.CATALOG, C.SCHEMA, C.VOLUME_PATH)

# COMMAND ----------

# MAGIC %md ## Inspect the professor's tables
# MAGIC Check the printed columns against `STATUS_COLMAP` / `INFO_COLMAP`. If a name differs, edit `config.py` and re-run.

# COMMAND ----------

status_src = spark.table(C.SOURCE_STATUS_TABLE)
info_src = spark.table(C.SOURCE_INFO_TABLE)
status_src.printSchema()
info_src.printSchema()
display(status_src.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F
flags = [c for c in C.SOURCE_STATUS_QUALITY_FLAGS if c in status_src.columns]
unknown = [c for c in status_src.columns if c.startswith("_") and c not in flags
           and c != C.SOURCE_LAST_REPORTED_PLACEHOLDER_FLAG]
print("quality flags applied:", flags, "| other underscore columns (not filtered):", unknown)
n_before = status_src.count()
for c in flags:
    status_src = status_src.where(~F.coalesce(F.col(c), F.lit(False)))
print(f"rows kept after quality flags: {status_src.count():,} of {n_before:,}")

ph = C.SOURCE_LAST_REPORTED_PLACEHOLDER_FLAG
if ph and ph in status_src.columns and C.STATUS_COLMAP.get("last_reported") in status_src.columns:
    lr = C.STATUS_COLMAP["last_reported"]
    status_src = status_src.withColumn(lr, F.when(F.coalesce(F.col(ph), F.lit(False)), F.lit(None)).otherwise(F.col(lr)))
    print(f"{lr} set to NULL where {ph} is true")

std_status, missing = L.standardize_status(status_src, C.STATUS_COLMAP, C.SOURCE_STATUS_TABLE)
if missing:
    print("WARNING optional columns not found, filled with defaults:", missing)
std_info = L.standardize_info(info_src, C.INFO_COLMAP)

if C.HIST_START_UTC:
    std_status = std_status.where(f"snapshot_ts >= '{C.HIST_START_UTC}'")
if C.HIST_END_UTC:
    std_status = std_status.where(f"snapshot_ts <  '{C.HIST_END_UTC}'")

(std_status.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(C.T["std_status_hist"]))
(std_info.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(C.T["std_info"]))

# COMMAND ----------

# MAGIC %md ## Snapshot cadence audit (proposal §2)
# MAGIC The as-of tolerance in `config.py` must exceed the typical gap between snapshots.

# COMMAND ----------

audit = spark.sql(f"""
WITH s AS (
  SELECT station_id, snapshot_ts,
         (unix_timestamp(snapshot_ts) - unix_timestamp(LAG(snapshot_ts) OVER (PARTITION BY station_id ORDER BY snapshot_ts))) / 60.0 AS gap_min
  FROM {C.T['std_status_hist']}
)
SELECT
  MIN(snapshot_ts) AS first_snapshot, MAX(snapshot_ts) AS last_snapshot,
  COUNT(DISTINCT station_id) AS stations, COUNT(*) AS rows,
  percentile_approx(gap_min, 0.5)  AS median_gap_min,
  percentile_approx(gap_min, 0.9)  AS p90_gap_min,
  percentile_approx(gap_min, 0.99) AS p99_gap_min,
  AVG(CASE WHEN gap_min > {C.ASOF_TOLERANCE_MIN} THEN 1 ELSE 0 END) AS share_gaps_over_tolerance
FROM s
""")
display(audit)
a = audit.first()
if a["median_gap_min"] is not None and a["median_gap_min"] > C.ASOF_TOLERANCE_MIN:
    raise ValueError(f"Median snapshot gap {a['median_gap_min']:.1f} min exceeds ASOF_TOLERANCE_MIN "
                     f"({C.ASOF_TOLERANCE_MIN}). Raise the tolerance or GRID_MIN in config.py, and log it in ASSUMPTIONS.md.")
