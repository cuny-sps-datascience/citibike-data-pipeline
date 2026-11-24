# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# 04. DEMO: enter a latitude, longitude and size for a possible new station.
# Change the widgets at the top, then press "Run all". Output: predicted monthly trips with a
# P10-P90 range, the most similar existing stations (table and map), a seasonal budget,
# and crew hours. Crew hour numbers use placeholder assumptions, not Citi Bike cost data.

# COMMAND ----------

# MAGIC %pip install folium -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json, os, sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

sys.path.append(os.getcwd())
from knn_demand import (KNNDemand, site_features, to_xy, seasonal_budget, crew_plan,
                        TRIPS_PER_BIKE_CHECK, HOURS_PER_BIKE_CHECK, BASE_HOURS_PER_MONTH)

dbutils.widgets.text("schema", "workspace.default")
dbutils.widgets.text("volume_dir", "/Volumes/workspace/default/citibike/team9")
dbutils.widgets.text("lat", "40.7527")
dbutils.widgets.text("lon", "-73.9772")
dbutils.widgets.text("capacity", "35")
SCHEMA, VOLUME_DIR = dbutils.widgets.get("schema"), dbutils.widgets.get("volume_dir")
LAT, LON, CAP = float(dbutils.widgets.get("lat")), float(dbutils.widgets.get("lon")), int(dbutils.widgets.get("capacity"))

ctx = spark.table(f"{SCHEMA}.team9_station_context").toPandas()
sub = spark.table(f"{SCHEMA}.team9_subway_entrances").toPandas()
share = spark.table(f"{SCHEMA}.team9_seasonal_share").toPandas().set_index("month_of_year")["share"]
params = json.load(open(f"{VOLUME_DIR}/model_params.json"))

# COMMAND ----------

# Fit on every station and predict the new site
model = KNNDemand(params["k"], params["weights"]).fit(ctx, ctx["target"])
site = site_features(LAT, LON, CAP, ctx, sub["lat"], sub["lon"])
pred = model.predict(site).iloc[0]
if "interval" in params:      # calibrated range from notebook 03
    pred["p10"], pred["p90"] = pred["pred"] * params["interval"][0], pred["pred"] * params["interval"][1]

print(f"Site ({LAT}, {LON}), {CAP} docks")
print(f"  nearest subway entrance: {np.expm1(site['log_subway_dist'][0]):.0f} m, "
      f"existing stations within 400 m: {int(site['stations_400m'][0])}")
print(f"Predicted trips per month: {pred['pred']:,.0f}   (P10 {pred['p10']:,.0f} to P90 {pred['p90']:,.0f})")
print(f"Predicted first-year trips: {pred['pred'] * 12:,.0f}")

# COMMAND ----------

# The similar stations behind the prediction
nb = ctx.iloc[pred["neighbor_idx"]].copy()
sx, sy = to_xy(LAT, LON)
nb["meters_away"] = np.hypot(nb["x_m"] - sx, nb["y_m"] - sy).round(0)
display(nb[["name", "capacity", "meters_away", "subway_dist_m", "stations_400m", "target"]]
        .rename(columns={"target": "avg_monthly_trips_first_year"}).round(0))

# COMMAND ----------

# Seasonal budget and crew hours
bud = seasonal_budget(pred["pred"], share)
bud = pd.concat([bud, crew_plan(bud["trips"])], axis=1).round(1)
display(bud)
print(f"Assumptions (placeholders): 1 bike check per {TRIPS_PER_BIKE_CHECK} trips, "
      f"{HOURS_PER_BIKE_CHECK} h per check, {BASE_HOURS_PER_MONTH} base h per month")
fig, ax = plt.subplots(figsize=(7, 3))
ax.bar(bud["month"], bud["trips"]); ax.set_xlabel("month"); ax.set_ylabel("trips")
ax.set_title("Seasonal profile of predicted trips")
display(fig)

# COMMAND ----------

# Map: new site (red) and its neighbors (blue, bigger = busier)
try:
    import folium
    m = folium.Map(location=[LAT, LON], zoom_start=14)
    folium.Marker([LAT, LON], tooltip="New site", icon=folium.Icon(color="red")).add_to(m)
    for _, r in nb.iterrows():
        folium.CircleMarker([r["lat"], r["lon"]], radius=4 + r["target"] ** 0.5 / 4, color="blue",
                            fill=True, tooltip=f"{r['name']}: {r['target']:.0f} trips/month").add_to(m)
    displayHTML(m._repr_html_())
except ImportError:
    print("folium not available; skipping the map")