# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# 02. Station context table (our new shared dataset)
#
# One row per station: location, size, distance to the nearest subway entrance, number of
# nearby stations, and the first-year demand target from notebook 01.
# External data added: MTA Subway Entrances and Exits 2024 (data.ny.gov, dataset i9wp-a4ja).
#
# Trip files identify stations by a code like "6140.05". That code is the "short_name" in the
# GBFS station information, so we join on short_name. The match rate is printed below.
#
# Writes: team9_subway_entrances, team9_station_context (documented with column comments)

# COMMAND ----------

import os, re, sys
import numpy as np
import pandas as pd

sys.path.append(os.getcwd())
from knn_demand import add_features
from trips_io import normalize_station_id

dbutils.widgets.text("schema", "workspace.default")
dbutils.widgets.text("info_table", "workspace.default.silver_station_info")
dbutils.widgets.text("subway_url", "https://data.ny.gov/api/views/i9wp-a4ja/rows.csv?accessType=DOWNLOAD")
dbutils.widgets.text("subway_fallback_csv", "/Volumes/workspace/default/citibike/team9/subway_entrances.csv")
SCHEMA = dbutils.widgets.get("schema")
INFO_TABLE = dbutils.widgets.get("info_table")

# COMMAND ----------

# Subway entrances. If the download is blocked, upload the CSV from data.ny.gov to the
# fallback path above and re-run.
try:
    sub = pd.read_csv(dbutils.widgets.get("subway_url"))
except Exception as e:
    print("download failed, using fallback file:", type(e).__name__)
    sub = pd.read_csv(dbutils.widgets.get("subway_fallback_csv"))

def find(words):
    for c in sub.columns:
        if all(w in c.lower() for w in words):
            return c

lat_c = find(["entrance", "latitude"]) or find(["latitude"])
lon_c = find(["entrance", "longitude"]) or find(["longitude"])
if lat_c and lon_c:
    pts = pd.DataFrame({"lat": sub[lat_c], "lon": sub[lon_c]})
else:   # some versions only have a "POINT (lon lat)" text column
    geo = find(["georeference"]) or find(["point"])
    if geo is None:
        raise ValueError(f"No coordinates found. Columns: {list(sub.columns)}")
    xy = sub[geo].astype(str).str.extract(r"POINT \(\s*(-?[\d.]+)\s+(-?[\d.]+)\s*\)").astype(float)
    pts = pd.DataFrame({"lat": xy[1], "lon": xy[0]})
pts = pts.dropna().query("40.4 < lat < 41.0 and -74.3 < lon < -73.6").drop_duplicates().reset_index(drop=True)
print("subway entrances:", len(pts))
spark.createDataFrame(pts).write.mode("overwrite").saveAsTable(f"{SCHEMA}.team9_subway_entrances")

# COMMAND ----------

# Latest information per station from the silver table
info = spark.sql(f"""
    SELECT station_id, short_name, name, lat, lon, capacity FROM (
      SELECT *, row_number() OVER (PARTITION BY station_id ORDER BY fetched_at DESC) AS rn
      FROM {INFO_TABLE}) WHERE rn = 1
""").toPandas()
info = (info.dropna(subset=["short_name", "lat", "lon", "capacity"])
            .query("40.4 < lat < 41.0 and -74.3 < lon < -73.6")
            .drop_duplicates("short_name").reset_index(drop=True))
info["capacity"] = info["capacity"].astype(int)
info["short_name"] = normalize_station_id(info["short_name"])
feat = add_features(info, pts["lat"], pts["lon"])
feat["subway_dist_m"] = np.expm1(feat["log_subway_dist"])

# COMMAND ----------

# Join to the demand targets from notebook 01
targets = spark.table(f"{SCHEMA}.team9_station_targets").toPandas()
ctx = feat.merge(targets, left_on="short_name", right_on="start_station_id", how="inner").drop(columns="start_station_id")

print(f"target stations: {len(targets)}   matched to station info: {len(ctx)} "
      f"({len(ctx) / len(targets):.0%})")
unmatched = targets[~targets["start_station_id"].isin(feat["short_name"])]
print("examples of unmatched trip station ids:", unmatched["start_station_id"].head(10).tolist())

spark.createDataFrame(ctx).write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable(f"{SCHEMA}.team9_station_context")

# COMMAND ----------

# Data dictionary stored on the table itself
TABLE = f"{SCHEMA}.team9_station_context"
docs = {
    "station_id": "GBFS station id",
    "short_name": "Station code used in trip files (joins to start_station_id)",
    "name": "Station name",
    "lat": "Latitude",
    "lon": "Longitude",
    "capacity": "Docks reported in the latest station information",
    "x_m": "Easting in meters from a point in NYC (local approximation)",
    "y_m": "Northing in meters from a point in NYC (local approximation)",
    "log_capacity": "log(1 + capacity)",
    "log_subway_dist": "log(1 + meters to nearest subway entrance)",
    "stations_400m": "Other stations within 400 m (today, not at opening)",
    "subway_dist_m": "Meters to nearest subway entrance (2024 MTA data)",
    "first_month": "First month with at least 30 trips from the station",
    "target": "Average monthly trips in the 12 months after the first month",
    "n_months": "Months of data behind the target (up to 12)",
    "is_pre_history": "True if already open when our trip history starts (first-year not observed)",
}
spark.sql(f"COMMENT ON TABLE {TABLE} IS 'Team 9: one row per Citi Bike station with KNN features and first-year demand. Subway, density and capacity are current-day values.'")
for col, text in docs.items():
    spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN {col} COMMENT '{text}'")
display(spark.table(TABLE).limit(10))