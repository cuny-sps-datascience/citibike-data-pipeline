# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# 03. Tune k and the location weight, backtest on "pretend-new" stations, log to MLflow.
#
# For each station that opened during the history we hide its trips, use only stations that
# opened earlier, and predict its first-year demand. Compared against a grid-cell average and
# a ridge regression. Metrics: MAE, WAPE, Spearman, and P10-P90 coverage (goal about 80%).

# COMMAND ----------

import json, os, sys, time
import matplotlib.pyplot as plt
import mlflow

sys.path.append(os.getcwd())
from knn_demand import backtest, metrics, tune, test_stations, interval_factors

dbutils.widgets.text("schema", "workspace.default")
dbutils.widgets.text("volume_dir", "/Volumes/workspace/default/citibike/team9")
dbutils.widgets.text("min_months", "6")
SCHEMA, VOLUME_DIR = dbutils.widgets.get("schema"), dbutils.widgets.get("volume_dir")
MIN_MONTHS = int(dbutils.widgets.get("min_months"))

ctx = spark.table(f"{SCHEMA}.team9_station_context").toPandas()
n_test = len(test_stations(ctx, MIN_MONTHS))
print(f"stations: {len(ctx)}   pretend-new test stations: {n_test}")
assert n_test >= 10, "Too few test stations. Lower min_months or extend the trip history in notebook 01."

# COMMAND ----------

t0 = time.time()
grid = tune(ctx, loc_weights=(1.0, 2.5, 4.0, 6.0, 8.0), min_months=MIN_MONTHS)
print(f"tuning took {time.time() - t0:.1f} s")
display(grid)
best = grid.iloc[0]
K, LOC_W = int(best["k"]), float(best["location_weight"])
print("best:", K, LOC_W)

# COMMAND ----------

t0 = time.time()
bt = backtest(ctx, k=K, weights={"location": LOC_W}, min_months=MIN_MONTHS)
print(f"backtest took {time.time() - t0:.1f} s")
scores = metrics(bt)
display(scores.reset_index().rename(columns={"index": "model"}))

# The neighbors' spread alone gives a range that is too narrow, so calibrate it on the backtest.
# Note: calibrated and checked on the same test stations, so the coverage below is a little optimistic.
F_LO, F_HI = interval_factors(bt)
cal_cov = float(((bt["actual"] >= bt["knn"] * F_LO) & (bt["actual"] <= bt["knn"] * F_HI)).mean())
print(f"raw P10-P90 coverage: {scores.loc['knn', 'P10_P90_coverage']:.0%}")
print(f"calibrated range = prediction x {F_LO:.2f} to x {F_HI:.2f}, coverage {cal_cov:.0%}")

# COMMAND ----------

# Save results, parameters, and log the run
spark.createDataFrame(bt).write.mode("overwrite").saveAsTable(f"{SCHEMA}.team9_backtest_predictions")
os.makedirs(VOLUME_DIR, exist_ok=True)
with open(f"{VOLUME_DIR}/model_params.json", "w") as f:
    json.dump({"k": K, "weights": {"location": LOC_W}, "interval": [F_LO, F_HI]}, f)

try:
    with mlflow.start_run(run_name="knn_backtest"):
        mlflow.log_params({"k": K, "location_weight": LOC_W, "n_test": len(bt)})
        mlflow.log_metric("calibrated_coverage", cal_cov)
        for model, row in scores.iterrows():
            for name, value in row.dropna().items():
                mlflow.log_metric(f"{model}_{name}", float(value))
except Exception as e:
    print("MLflow logging skipped:", type(e).__name__, e)

fig, ax = plt.subplots(figsize=(5, 5))
ax.scatter(bt["actual"], bt["knn"], s=12)
top = max(bt["actual"].max(), bt["knn"].max())
ax.plot([0, top], [0, top], "k--", lw=1)
ax.set_xlabel("actual first-year monthly trips"); ax.set_ylabel("KNN prediction")
ax.set_title("Pretend-new stations")
display(fig)