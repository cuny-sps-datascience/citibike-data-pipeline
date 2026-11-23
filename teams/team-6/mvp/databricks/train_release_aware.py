# Databricks notebook source
# MAGIC %md
# MAGIC # Release-aware training and model registration
# MAGIC Development runs log the comparison. Registry runs publish the Poisson challenger.

# COMMAND ----------

# MAGIC %pip install -r ../requirements-databricks.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pathlib import Path
import sys

import mlflow
import pandas as pd
from mlflow import MlflowClient

MVP_ROOT = Path.cwd().parent
sys.path.insert(0, str(MVP_ROOT / "scripts"))
from forecast_registry import MODEL_NAME, existing_alias, publish_version
from release_backtest import load_release_receipts, run_backtest

dbutils.widgets.text("data_dir", "", "Input volume directory (blank uses Git-folder MVP data)")
dbutils.widgets.text("artifact_root", "/Volumes/workspace/citibike_3h/exports/team6_next_hour",
                     "Model output root")
dbutils.widgets.text("fit_cutoff_utc", "", "Historical/deployment cutoff (blank uses now)")
dbutils.widgets.text("release_receipts", "", "Optional verified release CSV")
dbutils.widgets.dropdown("run_mode", "dev", ["dev", "register"], "Run mode")
dbutils.widgets.text("registered_model", MODEL_NAME, "Unity Catalog registered model")
dbutils.widgets.text("champion_bootstrap_dir", "", "Previously verified bundle for first registration")

data_dir = Path(dbutils.widgets.get("data_dir").strip() or MVP_ROOT / "data")
artifact_root = dbutils.widgets.get("artifact_root").strip()
if not artifact_root.startswith("/Volumes/"):
    raise ValueError("Set artifact_root to a writable Unity Catalog volume directory.")

now = pd.Timestamp.now(tz="UTC")
fit_cutoff = dbutils.widgets.get("fit_cutoff_utc").strip() or now.isoformat()
receipt_path = dbutils.widgets.get("release_receipts").strip()
receipts = load_release_receipts(receipt_path) if receipt_path else None
register = dbutils.widgets.get("run_mode") == "register"
model_name = dbutils.widgets.get("registered_model").strip()
bootstrap = dbutils.widgets.get("champion_bootstrap_dir").strip()
run_id = now.strftime("%Y%m%dT%H%M%S%fZ")
model_dir = Path(artifact_root) / "models" / run_id

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
experiment = f"/Users/{user}/team6-next-hour" + ("" if register else "-dev")
mlflow.set_experiment(experiment)
client = MlflowClient()

with mlflow.start_run(run_name="release-aware-next-hour") as run:
    report = run_backtest(data_dir, model_dir, fit_cutoff, receipts)
    mlflow.log_params({
        **report["settings"], "release_basis": report["release_basis"],
        "candidate_alpha": report["candidate_fit"]["alpha"],
        "fit_cutoff_utc": report["fit_cutoff_utc"], "bundle_version": report["bundle_version"],
    })
    for step, fold in enumerate(report["folds"], start=1):
        for metric in fold["metrics"]:
            mlflow.log_metrics({f"fold_{metric['model']}_{name}": metric[name] for name in (
                "poisson_log_loss", "weighted_poisson_log_loss", "mae", "rows",
            )}, step=step)
    for metric in report["metrics"]:
        mlflow.log_metrics({f"pooled_{metric['model']}_{name}": metric[name] for name in (
            "poisson_log_loss", "weighted_poisson_log_loss", "mae", "rows",
        )})
    mlflow.log_dict(report, "folds_and_metrics.json")
    mlflow.set_tag("volume_model_dir", str(model_dir))
    if register:
        challenger = publish_version(model_dir, model_name)
        client.set_registered_model_alias(model_name, "challenger", challenger)
        mlflow.set_tag("challenger_version", challenger)
        print(f"CHALLENGER_VERSION={challenger}")

if register and bootstrap and existing_alias(client, model_name, "champion") is None:
    with mlflow.start_run(run_name="bootstrap-previously-verified-lightgbm"):
        champion = publish_version(Path(bootstrap), model_name)
        client.set_registered_model_alias(model_name, "champion", champion)
        mlflow.set_tag("source_bundle", bootstrap)
        mlflow.set_tag("champion_version", champion)
        print(f"CHAMPION_VERSION={champion}")

print(f"MODEL_DIR={model_dir}")
print(f"MLFLOW_RUN_ID={run.info.run_id}")
display(pd.DataFrame(report["metrics"]))
display(pd.DataFrame([
    {"target_month": fold["target_month"], "validation_month": fold["validation_month"], **metric}
    for fold in report["folds"] for metric in fold["metrics"]
]))
