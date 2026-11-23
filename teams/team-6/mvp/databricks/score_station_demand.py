# Databricks notebook source
# MAGIC %md
# MAGIC # Score next-hour station demand
# MAGIC Set the model directory printed by the training notebook. Leave the station blank to score all six.

# COMMAND ----------

# MAGIC %pip install -r ../requirements-databricks.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pathlib import Path
import sys

import pandas as pd

MVP_ROOT = Path.cwd().parent
sys.path.insert(0, str(MVP_ROOT / "scripts"))
from live_weather import collect_forecasts, save_forecasts
from next_hour import score_next_hour

dbutils.widgets.text("model_dir", "", "Model directory printed by training")
dbutils.widgets.text("station_id", "", "Trip station ID (blank scores all stations)")
dbutils.widgets.dropdown("mode", "both", ["both", "calendar", "weather"], "Model inputs")
dbutils.widgets.text("weather_snapshot", "", "Optional staged forecast Parquet")

model_path = dbutils.widgets.get("model_dir").strip()
if not model_path.startswith("/Volumes/"):
    raise ValueError("Set model_dir to the /Volumes/... directory printed by training.")
model_dir = Path(model_path)
artifact_root = model_dir.parent.parent
mode = dbutils.widgets.get("mode")
station_id = dbutils.widgets.get("station_id").strip()
weather_snapshot = dbutils.widgets.get("weather_snapshot").strip()

forecasts = None
if mode != "calendar":
    if weather_snapshot:
        forecasts = pd.read_parquet(weather_snapshot)
    else:
        forecasts, errors = collect_forecasts()
        for error in errors:
            print(error)
        if not forecasts.empty:
            print(f"Weather snapshot: {save_forecasts(forecasts, artifact_root / 'weather_forecasts')}")

as_of = pd.Timestamp.now(tz="UTC")
predictions = score_next_hour(model_dir, as_of, mode, station_id, forecasts)
prediction_dir = artifact_root / "predictions"
prediction_dir.mkdir(parents=True, exist_ok=True)
stamp = as_of.strftime("%Y%m%dT%H%M%S%fZ")
prediction_path = prediction_dir / f"next_hour_{stamp}.parquet"
predictions.to_parquet(prediction_path, index=False)
print(f"PREDICTIONS={prediction_path}")
display(predictions[[
    "station_id", "station_name", "target_new_york", "model_used", "predicted_departures",
    "typical_departures", "demand_index", "share_of_typical_day", "relative_to_typical",
]])
