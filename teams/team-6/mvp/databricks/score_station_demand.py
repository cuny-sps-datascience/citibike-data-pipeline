# Databricks notebook source
# MAGIC %md
# MAGIC # Score next-hour station demand
# MAGIC The default alias is champion. Provide a bundle directory to score a saved volume version.

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
from forecast_registry import MODEL_NAME, forecast_request, score_registered

dbutils.widgets.text("model_dir", "", "Optional saved model bundle directory")
dbutils.widgets.text("registered_model", MODEL_NAME, "Unity Catalog registered model")
dbutils.widgets.dropdown("model_alias", "champion", ["champion", "challenger"], "Registry alias")
dbutils.widgets.text("artifact_root", "/Volumes/workspace/citibike_3h/exports/team6_next_hour",
                     "Output root for registry scoring")
dbutils.widgets.text("station_id", "", "Trip station ID (blank scores all stations)")
dbutils.widgets.dropdown("mode", "both", ["both", "calendar", "weather"], "Model inputs")
dbutils.widgets.text("weather_snapshot", "", "Optional staged forecast Parquet")

model_path = dbutils.widgets.get("model_dir").strip()
output_path = dbutils.widgets.get("artifact_root").strip()
if not (model_path or output_path).startswith("/Volumes/"):
    raise ValueError("Set the model or output directory to a writable Unity Catalog volume.")
artifact_root = Path(model_path).parent.parent if model_path else Path(output_path)
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
if model_path:
    from next_hour import score_next_hour

    predictions = score_next_hour(Path(model_path), as_of, mode, station_id, forecasts)
    predictions = predictions.assign(
        model_name="team6_volume_bundle", model_alias="explicit",
        model_version=predictions.bundle_version,
    )
else:
    predictions = score_registered(
        dbutils.widgets.get("registered_model").strip(), dbutils.widgets.get("model_alias"),
        forecast_request(as_of, mode, station_id, forecasts),
    )
prediction_dir = artifact_root / "predictions"
prediction_dir.mkdir(parents=True, exist_ok=True)
stamp = as_of.strftime("%Y%m%dT%H%M%S%fZ")
prediction_path = prediction_dir / f"next_hour_{stamp}.parquet"
prediction_path.write_bytes(predictions.to_parquet(index=False))
print(f"PREDICTIONS={prediction_path}")
display(predictions[[
    "station_id", "station_name", "target_new_york", "model_used", "predicted_departures",
    "typical_departures", "relative_to_typical", "model_alias", "model_version", "bundle_version",
]])
