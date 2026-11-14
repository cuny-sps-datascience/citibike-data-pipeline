# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Live GBFS collector
# MAGIC Fetches Citi Bike `station_status` (+ `station_information`) and appends one
# MAGIC snapshot per station to `live_station_status`.
# MAGIC
# MAGIC * **Idempotent:** MERGE on `(station_id, snapshot_ts)`, so a retried or duplicated run never double-counts.
# MAGIC * **Auditable:** raw JSON lands in the UC volume; every run writes a row to `collector_runs`, success or failure.
# MAGIC * **Schedule:** Lakeflow Job, every 15 minutes (see RUNBOOK.md). Free Edition allows 5 concurrent job tasks.

# COMMAND ----------

import os, sys, json, uuid, datetime as dt
sys.path.insert(0, os.getcwd())
import importlib, config as C, citibike_lib as L
importlib.reload(C); importlib.reload(L)
L.init_spark(spark)
from pyspark.sql import functions as F

run_id = str(uuid.uuid4())
fetched_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)

# COMMAND ----------

# Safe to run before 00_setup: create the schema and volume if they don't exist yet.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C.CATALOG}.{C.SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {C.CATALOG}.{C.SCHEMA}.{C.VOLUME}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C.T['live_status']} (
  station_id STRING NOT NULL, snapshot_ts TIMESTAMP NOT NULL, last_reported TIMESTAMP,
  num_bikes_available INT, num_ebikes_available INT, num_docks_available INT,
  num_bikes_disabled INT, num_docks_disabled INT,
  is_installed INT, is_renting INT, is_returning INT,
  feed_last_updated TIMESTAMP, fetched_at TIMESTAMP NOT NULL, source_url STRING, run_id STRING NOT NULL
) CLUSTER BY (snapshot_ts, station_id)
COMMENT 'Live Citi Bike GBFS station_status snapshots, one row per station per feed update. Source: Citi Bike / Lyft GBFS.'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C.T['collector_runs']} (
  run_id STRING, fetched_at TIMESTAMP, status STRING, source_url STRING,
  feed_last_updated TIMESTAMP, stations_in_payload INT, rows_inserted BIGINT,
  feed_age_sec DOUBLE, error STRING
)""")

def log_run(status, source_url=None, feed_ts=None, n_payload=None, n_inserted=None, error=None):
    age = (fetched_at - feed_ts).total_seconds() if feed_ts else None
    spark.createDataFrame(
        [(run_id, fetched_at, status, source_url, feed_ts, n_payload, n_inserted, age, (error or "")[:4000])],
        "run_id string, fetched_at timestamp, status string, source_url string, feed_last_updated timestamp, "
        "stations_in_payload int, rows_inserted bigint, feed_age_sec double, error string",
    ).write.mode("append").saveAsTable(C.T["collector_runs"])

# COMMAND ----------

try:
    payload, url = L.fetch_json(C.GBFS_STATUS_URLS, C.HTTP_TIMEOUT_S, C.HTTP_RETRIES)
    rows = L.parse_status_payload(payload, fetched_at, url, run_id)
    if len(rows) < C.MIN_STATIONS_EXPECTED:
        raise ValueError(f"Only {len(rows)} stations in payload (< {C.MIN_STATIONS_EXPECTED}); refusing to write a partial snapshot.")
    feed_ts = rows[0][11]
except Exception as e:
    log_run("FAILED", error=repr(e))
    raise   # fail the job run so the job's failure notification fires

if C.SAVE_RAW_JSON:
    day = fetched_at.strftime("%Y-%m-%d")
    path = f"{C.VOLUME_PATH}/gbfs/station_status/dt={day}/{fetched_at.strftime('%H%M%S')}_{run_id[:8]}.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f)

# COMMAND ----------

batch = spark.createDataFrame(rows, L.LIVE_STATUS_SCHEMA).dropDuplicates(["station_id", "snapshot_ts"])
batch.createOrReplaceTempView("batch")
before = spark.table(C.T["live_status"]).count()
spark.sql(f"""
MERGE INTO {C.T['live_status']} t
USING batch s
ON t.station_id = s.station_id AND t.snapshot_ts = s.snapshot_ts
WHEN NOT MATCHED THEN INSERT *
""")
inserted = spark.table(C.T["live_status"]).count() - before

# station_information changes rarely; refresh it every run (≈2k rows, cheap)
try:
    info_payload, _ = L.fetch_json(C.GBFS_INFO_URLS, C.HTTP_TIMEOUT_S, C.HTTP_RETRIES)
    (spark.createDataFrame(L.parse_info_payload(info_payload, fetched_at), L.LIVE_INFO_SCHEMA)
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T["live_info"]))
except Exception as e:
    print("station_information refresh failed (non-fatal):", repr(e))

log_run("OK", url, feed_ts, len(rows), inserted)
print(f"run {run_id}: {len(rows)} stations from {url}; feed_ts={feed_ts}; inserted={inserted} "
      f"({'duplicate feed version, nothing new' if inserted == 0 else 'new snapshot'})")
