# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Train, evaluate, register
# MAGIC Two binary classifiers (empty at t+15, full at t+15) compared against persistence.
# MAGIC
# MAGIC * **Split:** chronological train / validation / test with a 60-minute embargo at each boundary.
# MAGIC * **Selection:** model choice and decision threshold use **validation only**. Test metrics are computed once, after selection, and reported for every candidate.
# MAGIC * **Metrics:** PR-AUC (primary; events are rare), ROC-AUC, Brier, precision/recall/F1 at the validation-tuned threshold, reliability bins.
# MAGIC * **Registry:** the validation winner for each target is registered in Unity Catalog with alias `champion`.

# COMMAND ----------

import os, sys, json, datetime as dt
sys.path.insert(0, os.getcwd())
import importlib, config as C, citibike_lib as L
importlib.reload(C); importlib.reload(L)
L.init_spark(spark)
import numpy as np, pandas as pd
import mlflow
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/{C.EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md ## 1 · Split (computed from data unless fixed in config) and persisted

# COMMAND ----------

fl = spark.table(C.T["features_labels"]).where("eligible = 1")
bounds = fl.agg(F.min("prediction_time_utc").alias("tmin"), F.max("prediction_time_utc").alias("tmax")).first()
split = L.compute_split(pd.Series([bounds["tmin"], bounds["tmax"]]), C)
split_row = {"val_start_utc": split["val_start_utc"].to_pydatetime(),
             "test_start_utc": split["test_start_utc"].to_pydatetime(),
             "embargo_min": int(split["embargo_min"]),
             "data_start_utc": bounds["tmin"], "data_end_utc": bounds["tmax"],
             "created_at": dt.datetime.utcnow()}
spark.createDataFrame([split_row]).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T["split_config"])
print(split_row)

vs, ts, emb = split_row["val_start_utc"], split_row["test_start_utc"], dt.timedelta(minutes=split_row["embargo_min"])
train_sdf = fl.where(F.col("prediction_time_utc") < F.lit(vs - emb))
eval_sdf = fl.where(F.col("prediction_time_utc") >= F.lit(vs))

n_train = train_sdf.count()
if n_train > C.TRAIN_MAX_ROWS:
    train_sdf = train_sdf.sample(fraction=C.TRAIN_MAX_ROWS / n_train, seed=C.RANDOM_STATE)
    print(f"train sampled {n_train:,} -> ~{C.TRAIN_MAX_ROWS:,} rows")

cols = ["station_id", "prediction_time_utc", *L.FEATURE_COLS_SPARK, "empty_label", "full_label"]
pdf = pd.concat([train_sdf.select(*cols).toPandas(), eval_sdf.select(*cols).toPandas()], ignore_index=True)
pdf["prediction_time_utc"] = pd.to_datetime(pdf["prediction_time_utc"])
pdf["split"] = L.assign_split(pdf["prediction_time_utc"], split)
pdf = pdf[pdf["split"] != "embargo"].reset_index(drop=True)
print(pdf.groupby("split").agg(rows=("station_id", "size"), empty_rate=("empty_label", "mean"), full_rate=("full_label", "mean")))

# COMMAND ----------

# MAGIC %md ## 2 · Station × hour priors from TRAIN only
# MAGIC * **Validation, test, live:** priors fit on all training rows (saved to `station_priors`, used by `04_score_live`).
# MAGIC * **Training rows:** out-of-fold priors. Each 7-day block of training gets priors fit on the *other* blocks, so no
# MAGIC   row's own label is inside its prior. (v1 used in-sample priors here; `06_analysis` showed this target leakage
# MAGIC   cost gradient boosting ~0.09 validation PR-AUC on empty.)

# COMMAND ----------

priors = L.fit_priors(pdf[pdf["split"] == "train"], C.PRIOR_SMOOTHING_M)
spark.createDataFrame(priors).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T["station_priors"])
pdf = L.join_priors(pdf, priors)

is_tr = (pdf["split"] == "train").to_numpy()
pe_oof, pf_oof = L.fit_priors_oof(pdf.loc[is_tr], C.PRIOR_SMOOTHING_M, C.PRIOR_OOF_FOLD_DAYS)
for col, oof in (("prior_empty", pe_oof), ("prior_full", pf_oof)):
    in_sample = pdf.loc[is_tr, col].to_numpy()
    print(f"{col}: train mean in-sample {in_sample.mean():.4f} vs out-of-fold {oof.mean():.4f}; "
          f"corr with label in-sample {np.corrcoef(in_sample, pdf.loc[is_tr, col.replace('prior_', '') + '_label'])[0, 1]:.3f} "
          f"vs out-of-fold {np.corrcoef(oof, pdf.loc[is_tr, col.replace('prior_', '') + '_label'])[0, 1]:.3f}")
    pdf.loc[is_tr, col] = oof
assert pdf.loc[is_tr, ["prior_empty", "prior_full"]].notna().all().all()
tr, va, te = (pdf[pdf["split"] == s] for s in ("train", "val", "test"))
Xtr, Xva, Xte = (L.to_pandas_features(d) for d in (tr, va, te))

# COMMAND ----------

# MAGIC %md ## 3 · Fit and compare

# COMMAND ----------

comparison, calib_rows, test_pred_frames, champions = [], [], [], {}
run_ts = dt.datetime.utcnow()

for target in C.TARGETS:
    ycol = f"{target}_label"
    ytr, yva, yte = tr[ycol].values, va[ycol].values, te[ycol].values
    now_col = "is_empty_now" if target == "empty" else "is_full_now"
    rates = L.persistence_rates(tr, target)

    # candidate -> (val scores, test scores, fitted estimator or None)
    cands = {
        "persistence": (va[now_col].astype(float).values, te[now_col].astype(float).values, None),
        "persistence_calibrated": (va[now_col].map(rates).values, te[now_col].map(rates).values, None),
        "station_hour_prior": (va[f"prior_{target}"].values, te[f"prior_{target}"].values, None),
    }
    for name, est in L.make_models(C.RANDOM_STATE).items():
        est.fit(Xtr, ytr)
        cands[name] = (est.predict_proba(Xva)[:, 1], est.predict_proba(Xte)[:, 1], est)

    for name, (pva, pte, est) in cands.items():
        thr = L.best_f1_threshold(yva, pva)
        mv = L.classification_metrics(yva, pva, thr)
        mt = L.classification_metrics(yte, pte, thr)
        comparison.append({"target": target, "model": name, **{f"val_{k}": v for k, v in mv.items()},
                           **{f"test_{k}": v for k, v in mt.items()}, "run_ts": run_ts})
        cb = L.calibration_table(yte, pte)
        cb["target"], cb["model"] = target, name
        calib_rows.append(cb)

    learned = [r for r in comparison if r["target"] == target and r["model"] in ("logreg", "hgb")]
    best = max(learned, key=lambda r: r["val_pr_auc"])
    base = next(r for r in comparison if r["target"] == target and r["model"] == "persistence_calibrated")
    champions[target] = {"model": best["model"], "threshold": best["val_threshold"],
                         "beats_persistence_val": best["val_pr_auc"] > base["val_pr_auc"],
                         "est": cands[best["model"]][2]}
    test_pred_frames.append(pd.DataFrame({
        "station_id": te["station_id"].values, "prediction_time_utc": te["prediction_time_utc"].values,
        "target": target, "label": yte, "p": cands[best["model"]][1],
        "p_persistence": cands["persistence_calibrated"][1], "model_name": best["model"]}))
    print(f"[{target}] champion={best['model']} val PR-AUC={best['val_pr_auc']:.3f} "
          f"vs calibrated persistence {base['val_pr_auc']:.3f}; threshold={best['val_threshold']:.3f}")

cmp_df = pd.DataFrame(comparison)
display(cmp_df[["target", "model", "val_pr_auc", "test_pr_auc", "test_roc_auc", "test_brier",
                "test_precision", "test_recall", "test_f1", "test_threshold", "test_prevalence"]])

# COMMAND ----------

# MAGIC %md ## 4 · Persist evaluation artifacts

# COMMAND ----------

spark.createDataFrame(cmp_df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T["model_comparison"])
spark.createDataFrame(pd.concat(calib_rows, ignore_index=True)).write.mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(C.T["calibration_bins"])

# Cross-reference contract: one row per (station, prediction time), both targets side by side.
tp = pd.concat(test_pred_frames, ignore_index=True)
wide = (tp.pivot_table(index=["station_id", "prediction_time_utc"], columns="target",
                       values=["label", "p", "p_persistence"], aggfunc="first"))
wide.columns = [f"{a}_{b}" for a, b in wide.columns]
wide = wide.reset_index().rename(columns={"label_empty": "empty_label", "label_full": "full_label",
                                          "p_empty": "p_empty", "p_full": "p_full"})
wide["model_empty"] = champions["empty"]["model"]
wide["model_full"] = champions["full"]["model"]
wide["author"] = user
spark.createDataFrame(wide).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T["test_predictions"])
export_path = f"{C.VOLUME_PATH}/exports/test_predictions_{user.split('@')[0]}.parquet"
os.makedirs(os.path.dirname(export_path), exist_ok=True)
wide.to_parquet(export_path, index=False)
print("cross-reference export:", export_path)

# COMMAND ----------

# MAGIC %md ## 5 · Log to MLflow and register champions (Unity Catalog)

# COMMAND ----------

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

client = MlflowClient()
cal = pd.concat(calib_rows, ignore_index=True)
for target, ch in champions.items():
    est = ch["est"]
    with mlflow.start_run(run_name=f"{target}_{ch['model']}") as run:
        mlflow.log_params({"target": target, "model": ch["model"], "threshold": ch["threshold"],
                           "grid_min": C.GRID_MIN, "horizon_min": C.HORIZON_MIN,
                           "asof_tol_min": C.ASOF_TOLERANCE_MIN, "n_stations": C.N_STATIONS,
                           "val_start_utc": str(vs), "test_start_utc": str(ts), "embargo_min": C.EMBARGO_MIN,
                           "features": json.dumps(L.MODEL_FEATURES), "runtime": L.runtime_info()})
        for r in comparison:
            if r["target"] == target:
                mlflow.log_metrics({f"{r['model']}_{k}": v for k, v in r.items()
                                    if isinstance(v, (int, float)) and not (isinstance(v, float) and np.isnan(v))
                                    and k.startswith(("val_", "test_"))})
        # reliability plot
        fig, ax = plt.subplots(figsize=(5, 5))
        for m in ("persistence_calibrated", "logreg", "hgb"):
            c = cal[(cal["target"] == target) & (cal["model"] == m)]
            ax.plot(c["mean_pred"], c["obs_rate"], marker="o", label=m)
        ax.plot([0, 1], [0, 1], "k--", lw=1)
        ax.set_xlabel("mean predicted probability"); ax.set_ylabel("observed rate")
        ax.set_title(f"Reliability · {target} at t+15 (test)"); ax.legend()
        mlflow.log_figure(fig, f"reliability_{target}.png"); plt.close(fig)

        sig = infer_signature(Xtr.head(500), est.predict_proba(Xtr.head(500))[:, 1])
        kw = dict(signature=sig, input_example=Xtr.head(5), registered_model_name=C.MODEL_NAMES[target])
        try:
            info = mlflow.sklearn.log_model(est, name="model", **kw)            # MLflow 3
        except TypeError:
            info = mlflow.sklearn.log_model(est, artifact_path="model", **kw)   # MLflow 2.x
        mv = getattr(info, "registered_model_version", None) or max(
            int(v.version) for v in client.search_model_versions(f"name='{C.MODEL_NAMES[target]}'"))
        client.set_registered_model_alias(C.MODEL_NAMES[target], C.MODEL_ALIAS, mv)
        client.set_model_version_tag(C.MODEL_NAMES[target], mv, "threshold", str(ch["threshold"]))
        client.set_model_version_tag(C.MODEL_NAMES[target], mv, "beats_persistence_val", str(ch["beats_persistence_val"]))
        print(f"registered {C.MODEL_NAMES[target]} v{mv} @{C.MODEL_ALIAS} (run {run.info.run_id})")
