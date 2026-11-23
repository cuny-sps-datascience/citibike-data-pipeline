# Databricks notebook source
# MAGIC %md
# MAGIC # Train the Team 6 next-hour station demand models
# MAGIC Set an output directory in a Unity Catalog volume. Each training run writes a new model version.

# COMMAND ----------

# MAGIC %pip install -r ../requirements-databricks.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pathlib import Path
import sys

import matplotlib.pyplot as plt
import pandas as pd

MVP_ROOT = Path.cwd().parent
sys.path.insert(0, str(MVP_ROOT / "scripts"))
from next_hour import fit_and_save

dbutils.widgets.text("data_dir", "", "Data volume path (blank uses Git-folder MVP data)")
dbutils.widgets.text("artifact_root", "/Volumes/workspace/citibike_3h/exports/team6_next_hour",
                     "Output directory in an existing Unity Catalog volume")

data_dir = Path(dbutils.widgets.get("data_dir").strip() or MVP_ROOT / "data")
artifact_root = dbutils.widgets.get("artifact_root").strip()
if not artifact_root.startswith("/Volumes/"):
    raise ValueError("Set artifact_root to a writable /Volumes/<catalog>/<schema>/<volume>/... path.")

run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ")
model_dir = Path(artifact_root) / "models" / run_id
summary = fit_and_save(data_dir, model_dir)
print(f"MODEL_DIR={model_dir}")
display(pd.DataFrame(summary["metrics"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demand curves
# MAGIC New York time. The index is 100 at a station's overall hourly average.

# COMMAND ----------

curve = pd.read_parquet(model_dir / "demand_curves.parquet")
station = pd.read_parquet(model_dir / "stations.parquet")[["station_id", "station_name"]]
curve = curve.merge(station, on="station_id").sort_values(["station_id", "t_dow", "t_hour"])
display(curve)
display(pd.read_parquet(model_dir / "metrics_by_station_hour.parquet"))

curves = (curve.assign(day_type=curve.t_dow.lt(5).map({True: "Weekday", False: "Weekend"}))
          .groupby(["station_name", "day_type", "t_hour"]).typical_departures.mean().reset_index())
fig, axes = plt.subplots(2, 3, figsize=(14, 7), sharex=True, layout="constrained")
for axis, (name, group) in zip(axes.flat, curves.groupby("station_name")):
    for label, points in group.groupby("day_type"):
        axis.plot(points.t_hour, points.typical_departures, label=label)
    axis.set_title(name)
    axis.set_xticks([0, 6, 12, 18, 23])
    axis.set_ylim(bottom=0)
    axis.grid(alpha=0.2)
axes[0, 0].legend()
fig.supxlabel("Hour in New York")
fig.supylabel("Typical departures per hour")
plt.show()
