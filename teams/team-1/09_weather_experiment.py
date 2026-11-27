# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Does weather improve the 15-minute forecast?
# MAGIC Controlled experiment: the champion model (gradient boosting) is trained **twice on identical rows**,
# MAGIC once with the current features and once with weather added. Everything else is held fixed:
# MAGIC the split persisted by `03` (train < Oct 12, validation Oct 12–19, test Oct 19–26, 60-min embargo),
# MAGIC out-of-fold station × hour priors, hyperparameters, and threshold selection on validation.
# MAGIC
# MAGIC * **Weather source:** `nyc_weather_hourly` from `07_weather` (Open-Meteo reanalysis, hourly, full history).
# MAGIC * **No look-ahead:** each row uses the last *completed* hour before its prediction time
# MAGIC   (`obs_hour = floor_hour(t − 1 h)`), the same rule as the example join in `07`.
# MAGIC * **Uncertainty:** paired block bootstrap over station-days of the test week (both models scored on the same resample).
# MAGIC * **Read-only for production:** nothing is registered and live scoring is unchanged. Results go to
# MAGIC   `analysis_weather` and `analysis_weather_bootstrap`, plus an MLflow run.
# MAGIC
# MAGIC Limitation stated up front: reanalysis is published with a delay, so live use would need forecasts
# MAGIC (Maxfield's collector, `08_prepare_weather_examples`). This notebook answers the modelling question
# MAGIC "is weather predictive at a 15-minute horizon?" on the full history.

# COMMAND ----------

import os, sys, json, datetime as dt
sys.path.insert(0, os.getcwd())
import importlib, config as C, citibike_lib as L
importlib.reload(C); importlib.reload(L)
L.init_spark(spark)
import numpy as np, pandas as pd
import mlflow
from pyspark.sql import functions as F
from sklearn.metrics import average_precision_score, brier_score_loss

user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/{C.EXPERIMENT_NAME}")

WEATHER_TABLE = C.fq("nyc_weather_hourly")
OUT_TABLE = C.fq("analysis_weather")
BOOT_TABLE = C.fq("analysis_weather_bootstrap")
LAG_HOURS = 1
N_BOOT = 300
RAIN_MM = 0.1   # an hour counts as "raining" at or above this
WEATHER_FEATURES = ["temperature_c", "wind_speed_kmh", "precipitation_mm", "precip_3h_mm", "is_raining"]

# COMMAND ----------

# MAGIC %md ## 1 · Weather features (one row per hour)

# COMMAND ----------

wx = spark.table(WEATHER_TABLE).toPandas()
wx["obs_hour_utc"] = pd.to_datetime(wx["obs_hour_utc"])
full_index = pd.date_range(wx["obs_hour_utc"].min(), wx["obs_hour_utc"].max(), freq="h")
wx = wx.set_index("obs_hour_utc").reindex(full_index)
wx.index.name = "obs_hour_utc"
# precipitation_mm covers the hour ending at obs_hour, so a 3-hour total ending at obs_hour is known at obs_hour.
wx["precip_3h_mm"] = wx["precipitation_mm"].rolling(3, min_periods=1).sum()
wx["is_raining"] = (wx["precipitation_mm"] >= RAIN_MM).astype(float)
wx = wx.reset_index()[["obs_hour_utc", *WEATHER_FEATURES]]
print(f"weather hours {len(wx):,}: {wx.obs_hour_utc.min()} -> {wx.obs_hour_utc.max()}; "
      f"missing hours {int(wx.temperature_c.isna().sum())}; raining hours {int(wx.is_raining.sum())}")

# COMMAND ----------

# MAGIC %md ## 2 · Same rows, split and priors as `03`

# COMMAND ----------

sp = spark.table(C.T["split_config"]).first()
split = {"val_start_utc": pd.Timestamp(sp["val_start_utc"]), "test_start_utc": pd.Timestamp(sp["test_start_utc"]),
         "embargo_min": int(sp["embargo_min"])}
vs, emb = sp["val_start_utc"], dt.timedelta(minutes=int(sp["embargo_min"]))
print("split (from 03):", split)

fl = spark.table(C.T["features_labels"]).where("eligible = 1")
train_sdf = fl.where(F.col("prediction_time_utc") < F.lit(vs - emb))
eval_sdf = fl.where(F.col("prediction_time_utc") >= F.lit(vs))
n_train = train_sdf.count()
if n_train > C.TRAIN_MAX_ROWS:
    train_sdf = train_sdf.sample(fraction=C.TRAIN_MAX_ROWS / n_train, seed=C.RANDOM_STATE)

cols = ["station_id", "prediction_time_utc", *L.FEATURE_COLS_SPARK, "empty_label", "full_label"]
pdf = pd.concat([train_sdf.select(*cols).toPandas(), eval_sdf.select(*cols).toPandas()], ignore_index=True)
pdf["prediction_time_utc"] = pd.to_datetime(pdf["prediction_time_utc"])
pdf["split"] = L.assign_split(pdf["prediction_time_utc"], split)
pdf = pdf[pdf["split"] != "embargo"].reset_index(drop=True)

# Weather join: last completed hour before the prediction time.
pdf["obs_hour_utc"] = (pdf["prediction_time_utc"] - pd.Timedelta(hours=LAG_HOURS)).dt.floor("h")
pdf = pdf.merge(wx, on="obs_hour_utc", how="left", validate="many_to_one")
# No look-ahead: the whole weather hour ends at or before the prediction time.
assert (pdf["obs_hour_utc"] + pd.Timedelta(hours=1) <= pdf["prediction_time_utc"]).all()
coverage = pdf.groupby("split")["temperature_c"].apply(lambda s: s.notna().mean())
print("weather coverage by split:\n", coverage)
assert coverage.min() > 0.99, "weather table does not cover the modelling window; rerun 07_weather"

priors = L.fit_priors(pdf[pdf["split"] == "train"], C.PRIOR_SMOOTHING_M)
pdf = L.join_priors(pdf, priors)
is_tr = (pdf["split"] == "train").to_numpy()
pe_oof, pf_oof = L.fit_priors_oof(pdf.loc[is_tr], C.PRIOR_SMOOTHING_M, C.PRIOR_OOF_FOLD_DAYS)
pdf.loc[is_tr, "prior_empty"], pdf.loc[is_tr, "prior_full"] = pe_oof, pf_oof

tr, va, te = (pdf[pdf["split"] == s].reset_index(drop=True) for s in ("train", "val", "test"))
print(pdf.groupby("split").agg(rows=("station_id", "size"), empty=("empty_label", "mean"), full=("full_label", "mean"),
                                raining=("is_raining", "mean")))

def X(d, with_weather):
    feats = L.MODEL_FEATURES + (WEATHER_FEATURES if with_weather else [])
    return d[feats].apply(pd.to_numeric, errors="coerce").astype("float64")

# COMMAND ----------

# MAGIC %md ## 3 · Train both variants and score validation / test

# COMMAND ----------

def onset_stats(d, p, thr, target):
    """Rows not in the state now: does the state appear at t+15? Persistence scores zero here by definition."""
    now = "is_empty_now" if target == "empty" else "is_full_now"
    m = (d[now] == 0).to_numpy()
    y, q = d[f"{target}_label"].to_numpy()[m], p[m]
    flagged = q >= thr
    return {"onset_events": int(y.sum()),
            "onset_pr_auc": float(average_precision_score(y, q)) if 0 < y.sum() < len(y) else float("nan"),
            "onset_caught": int((flagged & (y == 1)).sum()),
            "onset_precision": float((y[flagged] == 1).mean()) if flagged.any() else float("nan")}

results, scores, fitted = [], {}, {}
for target in C.TARGETS:
    ycol = f"{target}_label"
    rates = L.persistence_rates(tr, target)
    now = "is_empty_now" if target == "empty" else "is_full_now"
    p_pers_te = te[now].map(rates).to_numpy()
    brier_pers = brier_score_loss(te[ycol], p_pers_te)
    for variant, ww in (("hgb_no_weather", False), ("hgb_weather", True)):
        est = L.make_models(C.RANDOM_STATE)["hgb"]
        est.fit(X(tr, ww), tr[ycol].to_numpy())
        pva, pte = est.predict_proba(X(va, ww))[:, 1], est.predict_proba(X(te, ww))[:, 1]
        thr = L.best_f1_threshold(va[ycol].to_numpy(), pva)
        mv = L.classification_metrics(va[ycol].to_numpy(), pva, thr)
        mt = L.classification_metrics(te[ycol].to_numpy(), pte, thr)
        rain = (te["is_raining"] == 1).to_numpy()
        row = {"target": target, "variant": variant, "threshold": thr,
               "val_pr_auc": mv["pr_auc"], "test_pr_auc": mt["pr_auc"], "test_brier": mt["brier"],
               "test_brier_skill_vs_persistence": 1 - mt["brier"] / brier_pers,
               "test_precision": mt["precision"], "test_recall": mt["recall"],
               "test_rain_rows": int(rain.sum()),
               "test_rain_pr_auc": float(average_precision_score(te[ycol][rain], pte[rain])) if te[ycol][rain].nunique() == 2 else float("nan"),
               "test_rain_brier": float(brier_score_loss(te[ycol][rain], pte[rain])) if rain.any() else float("nan"),
               **{f"test_{k}": v for k, v in onset_stats(te, pte, thr, target).items()},
               "run_ts": dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)}
        results.append(row)
        scores[(target, variant)] = pte
        fitted[(target, variant)] = est
        print(f"[{target}] {variant:15s} val PR-AUC {row['val_pr_auc']:.3f} | test PR-AUC {row['test_pr_auc']:.3f} "
              f"| Brier {row['test_brier']:.4f} | skill {row['test_brier_skill_vs_persistence']:.1%} "
              f"| onsets caught {row['test_onset_caught']}/{row['test_onset_events']}")

res = pd.DataFrame(results)
display(res)

# COMMAND ----------

# MAGIC %md ## 4 · Is the difference real? Paired block bootstrap over station-days (test week)

# COMMAND ----------

rng = np.random.default_rng(C.RANDOM_STATE)
blocks = (te["station_id"].astype(str) + "|" + te["prediction_time_utc"].dt.strftime("%Y-%m-%d")).to_numpy()
uniq, inv = np.unique(blocks, return_inverse=True)
idx_by_block = [np.flatnonzero(inv == b) for b in range(len(uniq))]
boot_rows = []
for target in C.TARGETS:
    y = te[f"{target}_label"].to_numpy()
    p0, p1 = scores[(target, "hgb_no_weather")], scores[(target, "hgb_weather")]
    for b in range(N_BOOT):
        pick = np.concatenate([idx_by_block[i] for i in rng.integers(0, len(uniq), len(uniq))])
        yb = y[pick]
        if yb.sum() == 0 or yb.sum() == len(yb):
            continue
        boot_rows.append({"target": target, "rep": b,
                          "d_pr_auc": average_precision_score(yb, p1[pick]) - average_precision_score(yb, p0[pick]),
                          "d_brier": brier_score_loss(yb, p1[pick]) - brier_score_loss(yb, p0[pick])})
boot = pd.DataFrame(boot_rows)
summary = (boot.groupby("target")
               .agg(d_pr_auc_mean=("d_pr_auc", "mean"),
                    d_pr_auc_lo=("d_pr_auc", lambda s: s.quantile(0.025)),
                    d_pr_auc_hi=("d_pr_auc", lambda s: s.quantile(0.975)),
                    d_brier_mean=("d_brier", "mean"),
                    d_brier_lo=("d_brier", lambda s: s.quantile(0.025)),
                    d_brier_hi=("d_brier", lambda s: s.quantile(0.975)),
                    reps=("rep", "size"))
               .reset_index())
summary["pr_auc_ci_excludes_0"] = (summary["d_pr_auc_lo"] > 0) | (summary["d_pr_auc_hi"] < 0)
summary["brier_ci_excludes_0"] = (summary["d_brier_lo"] > 0) | (summary["d_brier_hi"] < 0)
display(summary)

# COMMAND ----------

# MAGIC %md ## 5 · How much does the weather model rely on weather? (validation permutation importance)

# COMMAND ----------

imp_rows = []
for target in C.TARGETS:
    est, y = fitted[(target, "hgb_weather")], va[f"{target}_label"].to_numpy()
    Xv = X(va, True)
    base = average_precision_score(y, est.predict_proba(Xv)[:, 1])
    for name, group in (("all weather", WEATHER_FEATURES), ("current state", ["is_empty_now", "is_full_now", "num_bikes_available", "num_docks_available", "fill_ratio"])):
        drops = []
        for r in range(3):
            Xp = Xv.copy()
            perm = rng.permutation(len(Xp))
            Xp[group] = Xp[group].to_numpy()[perm]
            drops.append(base - average_precision_score(y, est.predict_proba(Xp)[:, 1]))
        imp_rows.append({"target": target, "group": name, "val_pr_auc": base, "mean_drop": float(np.mean(drops))})
imp = pd.DataFrame(imp_rows)
display(imp)

# COMMAND ----------

# MAGIC %md ## 6 · Save, log, and state the conclusion

# COMMAND ----------

res_out = res.merge(summary, on="target", how="left")
spark.createDataFrame(res_out).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(OUT_TABLE)
spark.createDataFrame(boot).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(BOOT_TABLE)

with mlflow.start_run(run_name="weather_experiment"):
    mlflow.log_params({"weather_table": WEATHER_TABLE, "lag_hours": LAG_HOURS, "weather_features": json.dumps(WEATHER_FEATURES),
                       "n_boot": N_BOOT, "val_start_utc": str(split["val_start_utc"]), "test_start_utc": str(split["test_start_utc"])})
    for r in results:
        mlflow.log_metrics({f"{r['target']}_{r['variant']}_{k}": float(v) for k, v in r.items()
                            if k.startswith(("val_", "test_")) and isinstance(v, (int, float)) and np.isfinite(v)})
    for _, s in summary.iterrows():
        mlflow.log_metrics({f"{s['target']}_d_pr_auc_mean": s["d_pr_auc_mean"], f"{s['target']}_d_brier_mean": s["d_brier_mean"]})

for target in C.TARGETS:
    a = res[(res.target == target) & (res.variant == "hgb_no_weather")].iloc[0]
    b = res[(res.target == target) & (res.variant == "hgb_weather")].iloc[0]
    s = summary[summary.target == target].iloc[0]
    verdict = ("improves" if s["d_pr_auc_lo"] > 0 else "hurts" if s["d_pr_auc_hi"] < 0 else "does not measurably change")
    print(f"{target.upper()}: weather {verdict} the forecast. Test PR-AUC {a.test_pr_auc:.3f} -> {b.test_pr_auc:.3f} "
          f"(Δ {s.d_pr_auc_mean:+.4f}, 95% CI {s.d_pr_auc_lo:+.4f} to {s.d_pr_auc_hi:+.4f}); "
          f"Brier {a.test_brier:.4f} -> {b.test_brier:.4f}; rainy-hour PR-AUC {a.test_rain_pr_auc:.3f} -> {b.test_rain_pr_auc:.3f} "
          f"({int(a.test_rain_rows):,} rainy rows).")
print(f"\nSaved {OUT_TABLE} and {BOOT_TABLE}. Production models and live scoring are unchanged.")
