# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# 01. Citi Bike trip history -> monthly trips per station -> first-year demand target
#
# Reads the public monthly trip zips (bucket "tripdata", files like 202501-citibike-tripdata.zip),
# keeps only trips per start station per month, and builds the KNN target:
# average monthly trips during a station's first (up to) 12 months.
#
# Safe to re-run: each month is saved once as a small parquet file and skipped next time.
# Set dry_run = true (the default) to only check that the files are reachable.
#
# Writes (workspace.default): team9_monthly_station_trips, team9_station_targets, team9_seasonal_share

# COMMAND ----------

import gc, glob, os, sys, tempfile
import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config

sys.path.append(os.getcwd())
from trips_io import zip_to_monthly, combine_monthly, build_targets, seasonal_share, normalize_station_id

dbutils.widgets.text("first_month", "202501")
dbutils.widgets.text("last_month", "202610")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.text("schema", "workspace.default")
dbutils.widgets.text("volume_dir", "/Volumes/workspace/default/citibike/team9")

FIRST, LAST = dbutils.widgets.get("first_month"), dbutils.widgets.get("last_month")
DRY_RUN = dbutils.widgets.get("dry_run") == "true"
SCHEMA, VOLUME_DIR = dbutils.widgets.get("schema"), dbutils.widgets.get("volume_dir")
MONTHLY_DIR = f"{VOLUME_DIR}/monthly"
BUCKET = "tripdata"

months = pd.period_range(pd.Period(FIRST, "M"), pd.Period(LAST, "M"), freq="M")
s3 = boto3.client("s3", region_name="us-east-1", config=Config(signature_version=UNSIGNED))

# COMMAND ----------

# Check that every file exists and is reachable (no download yet)
missing = []
for p in months:
    key = f"{p.strftime('%Y%m')}-citibike-tripdata.zip"
    try:
        size_mb = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] / 1e6
        print(f"{key:36s} {size_mb:8.0f} MB")
    except Exception as e:
        missing.append(key)
        print(f"{key:36s} NOT REACHABLE: {type(e).__name__}")
print("\nmissing:", missing)

if DRY_RUN:
    dbutils.notebook.exit("dry run finished: set dry_run = false to download and aggregate")

# COMMAND ----------

# Download one month at a time, aggregate it, delete the zip. Only a small parquet is kept.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.citibike")
os.makedirs(MONTHLY_DIR, exist_ok=True)

for p in months:
    tag = p.strftime("%Y%m")
    out = f"{MONTHLY_DIR}/{tag}.parquet"
    if os.path.exists(out):
        print(tag, "already done")
        continue
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, f"{tag}.zip")
        s3.download_file(BUCKET, f"{tag}-citibike-tripdata.zip", local)
        monthly = zip_to_monthly(local)
    monthly.to_parquet(out)
    print(tag, f"{monthly['trips'].sum():,} trips, {monthly['start_station_id'].nunique()} stations")
    del monthly
    gc.collect()

# COMMAND ----------

# Combine all months, build targets, save tables
frames = [pd.read_parquet(f"{MONTHLY_DIR}/{p.strftime('%Y%m')}.parquet") for p in months
          if os.path.exists(f"{MONTHLY_DIR}/{p.strftime('%Y%m')}.parquet")]
for f in frames:    # months saved by an earlier version may still have ids like "3834.1"
    f["start_station_id"] = normalize_station_id(f["start_station_id"])
monthly = combine_monthly(frames)
targets = build_targets(monthly, history_start=months[0].to_timestamp())
# Seasonality uses every month file saved in the folder (even months outside first_month..last_month)
all_files = sorted(glob.glob(f"{MONTHLY_DIR}/*.parquet"))
season = seasonal_share(combine_monthly([pd.read_parquet(f) for f in all_files]))  # ids not needed here
season = season.rename("share").reset_index().rename(columns={"month": "month_of_year"})

for name, df in [("team9_monthly_station_trips", monthly), ("team9_station_targets", targets),
                 ("team9_seasonal_share", season)]:
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable(f"{SCHEMA}.{name}")

# COMMAND ----------

# Sanity checks. The number of NEW stations is what makes the backtest meaningful.
print("trips per month (look for odd months, e.g. 202604 and 202610 were small files):")
print(monthly.groupby("month")["trips"].sum().to_string())
print("\nstations with a target:", len(targets))
print("opened before/at history start (used as neighbors only):", int(targets["is_pre_history"].sum()))
new = targets[~targets["is_pre_history"]]
print("opened during the history (possible test stations):", len(new))
print("  with at least 6 months of first-year data:", int((new["n_months"] >= 6).sum()))
display(new.sort_values("first_month").head(20))