# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Train, evaluate, register
# MAGIC Two binary classifiers (empty at t+15, full at t+15) compared against persistence.
# MAGIC
# MAGIC * **Split:** chronological train / validation / test with a 60-minute embargo at each boundary.
# MAGIC * **Selection:** model choice and decision threshold use **validation only**. Test metrics are computed once, after selection, and reported for every candidate.
# MAGIC * **Metrics:** PR-AUC (primary; events are rare), ROC-AUC, Brier, precision/recall/F1 at the validation-tuned threshold, reliability bins.
# MAGIC * **Registry:** the validation winner for each target is registered in Unity Catalog with alias `champion`.
# MAGIC * **Scope widget:** `mvp` (100 stations, live system's schema and models) or `full` (whole network, `workspace.citibike_full`, separate models). See `02`.

# COMMAND ----------

dbutils.widgets.dropdown("scope", "mvp", ["mvp", "full"], "Station scope")
import os, sys, json, datetime as dt
os.environ["CITIBIKE_SCOPE"] = dbutils.widgets.get("scope")
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
print(f"scope={C.SCOPE} -> outputs in {C.CATALOG}.{C.OUT_SCHEMA}; models {C.MODEL_NAMES}")
timer = L.StepTimer("03_train_evaluate")

# COMMAND ----------

# MAGIC %md ## 1 · Split and data
# MAGIC * `mvp`: split computed from the data (unless fixed in config) and saved, as before.
# MAGIC * `full`: split read from `split_config`, fixed in `02` before stations were chosen.
# MAGIC * Train: all training rows feed the priors (counted in Spark); a sample of up to `TRAIN_MAX_ROWS` is pulled for model fitting.
# MAGIC * Validation and test: every row, or (full scope, if over `EVAL_MAX_ROWS`) a deterministic sample of whole station-days.

# COMMAND ----------

fl = spark.table(C.T["features_labels"]).where("eligible = 1")
if C.SCOPE == "full":
    split_row = spark.table(C.T["split_config"]).first().asDict()
else:
    bounds = fl.agg(F.min("prediction_time_utc").alias("tmin"), F.max("prediction_time_utc").alias("tmax")).first()
    split = L.compute_split(pd.Series([bounds["tmin"], bounds["tmax"]]), C)
    split_row = {"val_start_utc": split["val_start_utc"].to_pydatetime(),
                 "test_start_utc": split["test_start_utc"].to_pydatetime(),
                 "embargo_min": int(split["embargo_min"]),
                 "data_start_utc": bounds["tmin"], "data_end_utc": bounds["tmax"],
                 "created_at": dt.datetime.utcnow()}
    spark.createDataFrame([split_row]).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T["split_config"])
split = {"val_start_utc": pd.Timestamp(split_row["val_start_utc"]),
         "test_start_utc": pd.Timestamp(split_row["test_start_utc"]),
         "embargo_min": int(split_row["embargo_min"])}
print(split_row)

vs, ts, emb = split_row["val_start_utc"], split_row["test_start_utc"], dt.timedelta(minutes=split_row["embargo_min"])
t = F.col("prediction_time_utc")
train_all = fl.where(t < F.lit(vs - emb))
frames = {"val": fl.where((t >= F.lit(vs)) & (t < F.lit(ts - emb))), "test": fl.where(t >= F.lit(ts))}
n_stations = fl.select("station_id").distinct().count()

timer.start("pull_to_pandas")
n_train = train_all.count()
train_sdf = train_all
if n_train > C.TRAIN_MAX_ROWS:
    train_sdf = train_all.sample(fraction=C.TRAIN_MAX_ROWS / n_train, seed=C.RANDOM_STATE)
    print(f"train sampled {n_train:,} -> ~{C.TRAIN_MAX_ROWS:,} rows for model fitting (priors still use all {n_train:,})")
eval_frac = {}
for name in ("val", "test"):
    frames[name], eval_frac[name] = L.sample_station_days(frames[name], C.EVAL_MAX_ROWS, C.RANDOM_STATE)
    if eval_frac[name] < 1:
        print(f"{name}: kept {eval_frac[name]:.1%} of station-days (deterministic hash) to stay under {C.EVAL_MAX_ROWS:,} rows")

cols = ["station_id", "prediction_time_utc", *L.FEATURE_COLS_SPARK, "empty_label", "full_label"]
pdf = pd.concat([train_sdf.select(*cols).toPandas(), frames["val"].select(*cols).toPandas(),
                 frames["test"].select(*cols).toPandas()], ignore_index=True)
pdf["prediction_time_utc"] = pd.to_datetime(pdf["prediction_time_utc"])
pdf["split"] = L.assign_split(pdf["prediction_time_utc"], split)
pdf = pdf[pdf["split"] != "embargo"].reset_index(drop=True)
timer.stop(rows=len(pdf), n_train_all=n_train, n_stations=n_stations, eval_frac=eval_frac)
print(f"stations: {n_stations:,}")
print(pdf.groupby("split").agg(rows=("station_id", "size"), empty_rate=("empty_label", "mean"), full_rate=("full_label", "mean")))

# COMMAND ----------

# MAGIC %md ## 2 · Station × hour priors from TRAIN only (counted in Spark on all training rows)
# MAGIC * **Validation, test, live:** priors fit on all training rows (saved to `station_priors`, used by `04_score_live`).
# MAGIC * **Training rows:** out-of-fold priors. Each 7-day block of training gets priors fit on the *other* blocks, so no
# MAGIC   row's own label is inside its prior. (v1 used in-sample priors here; `06_analysis` showed this target leakage
# MAGIC   cost gradient boosting ~0.09 validation PR-AUC on empty.)
# MAGIC * Counting in Spark means priors are not thinned by the pandas training sample, which matters at full-network scale
# MAGIC   (about 600 sampled rows per station would otherwise feed 48 station × hour × weekend cells).

# COMMAND ----------

timer.start("priors")
t0 = train_all.agg(F.min("prediction_time_utc")).first()[0]
counts = L.prior_counts_spark(train_all, t0, C.PRIOR_OOF_FOLD_DAYS)
priors = L.priors_from_counts(counts, C.PRIOR_SMOOTHING_M)
spark.createDataFrame(priors).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T["station_priors"])
pdf = L.join_priors(pdf, priors)

is_tr = (pdf["split"] == "train").to_numpy()
pe_oof, pf_oof = L.oof_priors_from_counts(pdf.loc[is_tr], counts, C.PRIOR_SMOOTHING_M, t0, C.PRIOR_OOF_FOLD_DAYS)
for col, oof in (("prior_empty", pe_oof), ("prior_full", pf_oof)):
    in_sample = pdf.loc[is_tr, col].to_numpy()
    y_tr = pdf.loc[is_tr, col.replace("prior_", "") + "_label"]
    print(f"{col}: train mean in-sample {in_sample.mean():.4f} vs out-of-fold {oof.mean():.4f}; "
          f"corr with label in-sample {np.corrcoef(in_sample, y_tr)[0, 1]:.3f} vs out-of-fold {np.corrcoef(oof, y_tr)[0, 1]:.3f}")
    pdf.loc[is_tr, col] = oof
assert pdf.loc[is_tr, ["prior_empty", "prior_full"]].notna().all().all()
timer.stop(rows=len(priors), folds=int(counts["fold"].nunique()))
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
        timer.start(f"fit_{target}_{name}")
        est.fit(Xtr, ytr)
        timer.stop(rows=len(Xtr))
        timer.start(f"score_{target}_{name}")
        cands[name] = (est.predict_proba(Xva)[:, 1], est.predict_proba(Xte)[:, 1], est)
        timer.stop(rows=len(Xva) + len(Xte))

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
wide["scope"] = C.SCOPE
spark.createDataFrame(wide).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.T["test_predictions"])
suffix = "" if C.SCOPE == "mvp" else f"_{C.SCOPE}"
export_path = f"{C.VOLUME_PATH}/exports/test_predictions_{user.split('@')[0]}{suffix}.parquet"
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
                           "asof_tol_min": C.ASOF_TOLERANCE_MIN, "n_stations": n_stations, "scope": C.SCOPE,
                           "n_train_all": n_train, "n_train_fit": len(Xtr), "eval_frac": json.dumps(eval_frac),
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

# COMMAND ----------

# MAGIC %md ## 6 · Runtimes (evidence for the cost section)
# MAGIC Appended to `run_timings` in this scope's schema. Free Edition shows no DBU bill, so wall-clock time per step on
# MAGIC serverless is the cost proxy; compare the `mvp` and `full` rows.

# COMMAND ----------

display(timer.save(spark))
