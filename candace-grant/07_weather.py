# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Publish `nyc_weather_hourly`
# MAGIC Hourly temperature, wind speed and precipitation for the study area, published as a documented table
# MAGIC (proposal deliverable). **Not a model input:** the MVP models do not use weather (see the paper, §9).
# MAGIC
# MAGIC * **Source:** Open-Meteo Historical Weather API (`archive-api.open-meteo.com`), ECMWF reanalysis
# MAGIC   ("best match": IFS / ERA5 / ERA5-Land), no API key. Data licence CC BY 4.0.
# MAGIC   Attribution: *Weather data by Open-Meteo.com (CC BY 4.0); generated using Copernicus Climate Change Service information.*
# MAGIC * **Why not NOAA GHCNh (PR #61):** that pipeline was closed without merging; NOAA observations also arrive ~60 h late.
# MAGIC * **Location:** one point at the centroid of the 100 MVP stations. City-scale weather varies little across the sample.
# MAGIC * **Time:** hourly, UTC, aligned to the top of the hour (the value covers the preceding hour for precipitation).

# COMMAND ----------

import os, sys, json, datetime as dt, time
sys.path.insert(0, os.getcwd())
import importlib, config as C, citibike_lib as L
importlib.reload(C); importlib.reload(L)
L.init_spark(spark)
import requests
import pandas as pd
from pyspark.sql import functions as F

TABLE = C.fq("nyc_weather_hourly")
START = "2026-08-21"                                        # first day of the bike archive
END = (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
VARS = ["temperature_2m", "wind_speed_10m", "precipitation"]

# COMMAND ----------

# Centroid of the study stations (falls back to midtown Manhattan if info is missing)
c = (spark.table(C.T["mvp_stations"]).join(spark.table(C.T["std_info"]), "station_id")
     .agg(F.avg("lat").alias("lat"), F.avg("lon").alias("lon")).first())
LAT, LON = (round(c["lat"], 4), round(c["lon"], 4)) if c and c["lat"] else (40.7549, -73.9840)
print(f"weather point: {LAT}, {LON} · {START} → {END}")

# COMMAND ----------

def fetch_archive(lat, lon, start, end, retries=3):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {"latitude": lat, "longitude": lon, "start_date": start, "end_date": end,
              "hourly": ",".join(VARS), "timezone": "UTC", "wind_speed_unit": "kmh", "precipitation_unit": "mm"}
    last = None
    for a in range(retries):
        try:
            r = requests.get(url, params=params, timeout=60, headers={"User-Agent": "cuny-data622-group1"})
            r.raise_for_status()
            js = r.json()
            if "hourly" not in js:
                raise ValueError(f"unexpected payload: {str(js)[:300]}")
            return js
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 ** a)
    raise RuntimeError(
        f"Open-Meteo not reachable from this workspace ({type(last).__name__}: {last}). "
        "Free Edition only allows trusted outbound domains; if this host is blocked, record weather as a known gap.")


def to_frame(js) -> pd.DataFrame:
    h = js["hourly"]
    df = pd.DataFrame({"obs_hour_utc": pd.to_datetime(h["time"]),
                       "temperature_c": h["temperature_2m"],
                       "wind_speed_kmh": h["wind_speed_10m"],
                       "precipitation_mm": h["precipitation"]})
    df = df.dropna(subset=["temperature_c", "wind_speed_kmh", "precipitation_mm"], how="all")
    df["latitude"], df["longitude"] = float(js["latitude"]), float(js["longitude"])
    df["source"] = "Open-Meteo Historical Weather API (best match: ECMWF IFS / ERA5 / ERA5-Land)"
    df["retrieved_at_utc"] = pd.Timestamp.utcnow().tz_localize(None)
    return df.sort_values("obs_hour_utc").drop_duplicates("obs_hour_utc").reset_index(drop=True)

# COMMAND ----------

payload = fetch_archive(LAT, LON, START, END)
raw_path = f"{C.VOLUME_PATH}/weather/open_meteo_{START}_{END}.json"
os.makedirs(os.path.dirname(raw_path), exist_ok=True)
with open(raw_path, "w") as f:
    json.dump(payload, f)

wx = to_frame(payload)
print(f"{len(wx):,} hourly rows · {wx.obs_hour_utc.min()} → {wx.obs_hour_utc.max()} · raw JSON {raw_path}")

# Quality gates: hourly continuity and plausible ranges for NYC
expected = int((wx.obs_hour_utc.max() - wx.obs_hour_utc.min()) / pd.Timedelta(hours=1)) + 1
checks = {
    "missing hours": expected - len(wx),
    "temperature outside -30..45 C": int(((wx.temperature_c < -30) | (wx.temperature_c > 45)).sum()),
    "negative wind or precipitation": int(((wx.wind_speed_kmh < 0) | (wx.precipitation_mm < 0)).sum()),
}
print(checks)
assert checks["temperature outside -30..45 C"] == 0 and checks["negative wind or precipitation"] == 0, checks

# COMMAND ----------

(spark.createDataFrame(wx).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE))
spark.sql(f"""COMMENT ON TABLE {TABLE} IS
'Hourly NYC weather at the centroid of the 100 study stations. Source: Open-Meteo Historical Weather API (ECMWF reanalysis, best match), CC BY 4.0 — Weather data by Open-Meteo.com; generated using Copernicus Climate Change Service information. Times are UTC, top of the hour. Published as a project deliverable; not a model input in the MVP.'""")
for col, txt in {"obs_hour_utc": "Hour (UTC). Precipitation is the total over the preceding hour.",
                 "temperature_c": "Air temperature at 2 m, degrees C.",
                 "wind_speed_kmh": "Wind speed at 10 m, km/h.",
                 "precipitation_mm": "Total precipitation (rain + snow water equivalent) over the preceding hour, mm."}.items():
    spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN {col} COMMENT '{txt}'")
display(spark.table(TABLE).orderBy(F.desc("obs_hour_utc")).limit(5))

# COMMAND ----------

# MAGIC %md ## Example join (documented for other teams)
# MAGIC Attach the latest weather hour that had **finished** by the prediction time. With `LAG_HOURS = 1`, a prediction at 09:15
# MAGIC uses the hour ending 08:00. Reanalysis values for recent hours can be revised; for a live model, use a forecast feed instead.

# COMMAND ----------

LAG_HOURS = 1
example = spark.sql(f"""
SELECT f.station_id, f.prediction_time_utc, f.empty_label, f.full_label,
       w.obs_hour_utc AS weather_hour_utc, w.temperature_c, w.wind_speed_kmh, w.precipitation_mm
FROM {C.T['features_labels']} f
LEFT JOIN {TABLE} w
  ON w.obs_hour_utc = date_trunc('HOUR', f.prediction_time_utc - INTERVAL {LAG_HOURS} HOURS)
WHERE f.eligible = 1
""")
display(example.limit(10))
cov = example.agg(F.avg(F.col("temperature_c").isNotNull().cast("int")).alias("share_with_weather")).first()[0]
print(f"share of eligible station-times with a weather match: {cov:.1%}")
