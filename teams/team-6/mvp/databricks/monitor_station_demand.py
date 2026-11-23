# Databricks notebook source
# MAGIC %md
# MAGIC # Monitor saved station demand forecasts
# MAGIC Refresh after staging a validated hourly outcome table and its release receipts.

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
from monitor_departures import evaluate_predictions
from release_backtest import load_release_receipts

dbutils.widgets.text("artifact_root", "/Volumes/workspace/citibike_3h/exports/team6_next_hour",
                     "Root with saved predictions")
dbutils.widgets.text("outcomes_path", "", "Validated hourly station outcome Parquet")
dbutils.widgets.text("release_receipts", "", "Verified release CSV (blank leaves outcomes pending)")

artifact_root = Path(dbutils.widgets.get("artifact_root").strip())
if not str(artifact_root).startswith("/Volumes/"):
    raise ValueError("Provide the prediction and monitoring Unity Catalog volume directory.")
paths = sorted((artifact_root / "predictions").glob("next_hour_*.parquet"))
if not paths:
    raise ValueError("Score at least one forecast before refreshing monitoring.")
outcomes = dbutils.widgets.get("outcomes_path").strip()
receipt_path = dbutils.widgets.get("release_receipts").strip()
if not outcomes and receipt_path:
    raise ValueError("A release receipt needs a validated outcome dataset.")
predictions = pd.concat([pd.read_parquet(path) for path in paths], ignore_index=True)
actuals = pd.read_parquet(outcomes) if outcomes else pd.DataFrame({
    "station_id": pd.Series(dtype=str), "hour_utc": pd.Series(dtype="datetime64[ns, UTC]"),
    "departures": pd.Series(dtype="Int64"),
})
receipts = load_release_receipts(receipt_path) if receipt_path else pd.Series(dtype="datetime64[ns, UTC]")
as_of = pd.Timestamp.now(tz="UTC")
rows, metrics = evaluate_predictions(predictions, actuals, receipts, as_of)

output = artifact_root / "monitoring" / as_of.strftime("%Y%m%dT%H%M%S%fZ")
output.mkdir(parents=True, exist_ok=False)
(output / "forecast_outcomes.parquet").write_bytes(rows.to_parquet(index=False))
(output / "metrics_by_station_hour.parquet").write_bytes(metrics.to_parquet(index=False))
print(f"MONITORING_DIR={output}")
display(rows.status.value_counts().rename_axis("status").reset_index(name="forecast_rows"))
display(metrics)
