# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Analysis for the report
# MAGIC Read-only with respect to the pipeline: it does **not** change the registered models, `predictions`, or the jobs.
# MAGIC
# MAGIC | § | Analysis | Data used | Question it answers |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Exploratory analysis | all eligible rows | When and where are stations empty or full? How long do episodes last? How do states change in 15 min? |
# MAGIC | 2 | Transition metrics | **test** week | Can the model catch stations that are about to *change* state (which persistence never can)? |
# MAGIC | 3 | Results by stratum | **test** week | Does the model help every stratum equally? (mvp: high-event vs random; full: event-rate tiers) |
# MAGIC | 4 | Skill scores | **test** week | What share of the reference forecast's error does the model remove? |
# MAGIC | 5 | Grouped permutation importance | **validation** week | Which kinds of information does each champion rely on? |
# MAGIC | 6 | Feature-group ablation | train → **validation** | How much does each feature group add when models are retrained with it? |
# MAGIC
# MAGIC §5–6 use the validation week only, so studying features never touches the test week.
# MAGIC Outputs: `analysis_*` tables in the project schema, PNGs in `/Volumes/.../raw/analysis/`, and one MLflow run `report_analysis`.

# COMMAND ----------

dbutils.widgets.dropdown("scope", "mvp", ["mvp", "full"], "Station scope")
import os, sys, json, datetime as dt
os.environ["CITIBIKE_SCOPE"] = dbutils.widgets.get("scope")
sys.path.insert(0, os.getcwd())
import importlib, config as C, citibike_lib as L
importlib.reload(C); importlib.reload(L)
L.init_spark(spark)
import numpy as np, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow
from pyspark.sql import functions as F, Window
from sklearn.metrics import average_precision_score, brier_score_loss, precision_recall_curve

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/{C.EXPERIMENT_NAME}")

FIG_DIR = f"{C.VOLUME_PATH}/analysis" + ("" if C.SCOPE == "mvp" else f"_{C.SCOPE}")
os.makedirs(FIG_DIR, exist_ok=True)
A = lambda name: C.fq_out(f"analysis_{name}")      # analysis table names (scope's schema)
print(f"scope={C.SCOPE}: tables -> {C.CATALOG}.{C.OUT_SCHEMA}.analysis_*, figures -> {FIG_DIR}")

# Chart styling: validated categorical order (blue, orange, aqua, yellow); references in neutral gray, dashed.
COL = {"s1": "#2a78d6", "s2": "#eb6834", "s3": "#1baf7a", "s4": "#eda100", "ref": "#8a8985",
       "ink": "#0b0b0b", "ink2": "#52514e", "grid": "#e4e3df"}
plt.rcParams.update({
    "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb", "axes.edgecolor": COL["grid"],
    "axes.labelcolor": COL["ink2"], "xtick.color": COL["ink2"], "ytick.color": COL["ink2"],
    "axes.grid": True, "grid.color": COL["grid"], "grid.linewidth": 0.8, "axes.spines.top": False,
    "axes.spines.right": False, "axes.titlesize": 11, "axes.titleweight": "bold", "axes.titlecolor": COL["ink"],
    "legend.frameon": False, "lines.linewidth": 2, "font.size": 9,
})
figures = {}
def save(fig, name):
    path = f"{FIG_DIR}/{name}.png"
    fig.savefig(path, dpi=160, bbox_inches="tight")
    figures[name] = fig
    display(fig)
    return path

def write_table(pdf, name):
    spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(A(name))

# COMMAND ----------

# MAGIC %md ## Helpers (pure pandas / numpy)

# COMMAND ----------

FEATURE_GROUPS = {
    "current_state": ["num_bikes_available", "num_ebikes_available", "num_docks_available",
                      "num_bikes_disabled", "num_docks_disabled", "capacity", "fill_ratio",
                      "is_empty_now", "is_full_now", "ebike_share"],
    "momentum": [c for c in L.MODEL_FEATURES if c.startswith(("d_bikes_", "d_docks_"))],
    "time": ["is_weekend", "tod_sin", "tod_cos", "dow_sin", "dow_cos"],
    "station_habits": ["prior_empty", "prior_full"],
    "freshness": ["report_age_min"],
}
_assigned = sum(FEATURE_GROUPS.values(), [])
assert sorted(_assigned) == sorted(L.MODEL_FEATURES), set(L.MODEL_FEATURES) ^ set(_assigned)
ABLATION_ORDER = ["current_state", "momentum", "time", "station_habits", "freshness"]


def safe_ap(y, p):
    y = np.asarray(y).astype(int)
    return float(average_precision_score(y, p)) if 0 < y.sum() < len(y) else float("nan")


def transition_metrics(df, target, thr, p_col, now_col, y_col):
    """Metrics on the two transition subsets for one target.

    onset:    state NOT present now  -> does it appear at t+15?  (persistence predicts 'no' for all)
    recovery: state present now      -> does it clear by t+15?   (persistence predicts 'no change')
    """
    out = []
    for subset, mask, y, p in [
        ("onset", df[now_col] == 0, df[y_col], df[p_col]),
        ("recovery", df[now_col] == 1, 1 - df[y_col], 1 - df[p_col]),   # event = state clears
    ]:
        y_s, p_s = y[mask].to_numpy(), p[mask].to_numpy()
        thr_s = thr if subset == "onset" else 1 - thr
        yhat = (p_s >= thr_s).astype(int)
        tp, fp, fn = int(((yhat == 1) & (y_s == 1)).sum()), int(((yhat == 1) & (y_s == 0)).sum()), int(((yhat == 0) & (y_s == 1)).sum())
        out.append({
            "target": target, "subset": subset, "rows": int(mask.sum()), "events": int(y_s.sum()),
            "event_rate": float(y_s.mean()) if len(y_s) else float("nan"),
            "pr_auc_model": safe_ap(y_s, p_s),
            "pr_auc_no_skill": float(y_s.mean()) if len(y_s) else float("nan"),
            "precision_model": tp / (tp + fp) if tp + fp else float("nan"),
            "recall_model": tp / (tp + fn) if tp + fn else float("nan"),
            "recall_persistence": 0.0,                                   # persistence never predicts a change
            "brier_model": float(np.mean((p[mask].to_numpy() - y[mask].to_numpy()) ** 2)) if len(y_s) else float("nan"),
        })
    return out


def run_lengths(states: pd.Series) -> list[int]:
    """Lengths of consecutive runs of 1 in an ordered 0/1/NaN series (NaN breaks a run)."""
    lengths, n = [], 0
    for v in states.to_numpy():
        if v == 1:
            n += 1
        else:
            if n:
                lengths.append(n)
            n = 0
    if n:
        lengths.append(n)
    return lengths


def grouped_permutation_importance(model, X, y, groups, n_repeats, seed):
    """Drop in validation PR-AUC when a whole feature group is shuffled together.
    Shuffling correlated features jointly (e.g. d_bikes and d_docks) avoids the
    understatement that per-feature permutation gives for redundant features."""
    rng = np.random.default_rng(seed)
    base = safe_ap(y, model.predict_proba(X)[:, 1])
    rows = []
    for g, cols in groups.items():
        drops = []
        for _ in range(n_repeats):
            Xp = X.copy()
            idx = rng.permutation(len(Xp))
            Xp[cols] = Xp[cols].to_numpy()[idx]
            drops.append(base - safe_ap(y, model.predict_proba(Xp)[:, 1]))
        rows.append({"group": g, "n_features": len(cols), "base_pr_auc": base,
                     "drop_mean": float(np.mean(drops)), "drop_std": float(np.std(drops))})
    return pd.DataFrame(rows).sort_values("drop_mean", ascending=False)

# COMMAND ----------

# MAGIC %md ## Load shared inputs

# COMMAND ----------

split_row = spark.table(C.T["split_config"]).first().asDict()
split = {"val_start_utc": pd.Timestamp(split_row["val_start_utc"]),
         "test_start_utc": pd.Timestamp(split_row["test_start_utc"]),
         "embargo_min": int(split_row["embargo_min"])}
fl = spark.table(C.T["features_labels"])
stations = spark.table(C.T["mvp_stations"]).select("station_id", "selection_stratum")
cmp_df = spark.table(C.T["model_comparison"]).toPandas()
tp = spark.table(C.T["test_predictions"]).toPandas()
champ = {"empty": tp["model_empty"].iloc[0], "full": tp["model_full"].iloc[0]}
thr = {t: float(cmp_df[(cmp_df.target == t) & (cmp_df.model == champ[t])]["val_threshold"].iloc[0]) for t in C.TARGETS}
print("split:", split, "| champions:", champ, "| thresholds:", thr)

# COMMAND ----------

# MAGIC %md ## 1 · Exploratory analysis
# MAGIC Uses the station's state **at t** (`is_empty_now`, `is_full_now`) on eligible rows, i.e. how often a rider arriving at that moment finds the station empty or full.

# COMMAND ----------

elig = fl.where("eligible = 1").join(stations, "station_id")

hourly = (elig.groupBy("selection_stratum", "is_weekend", "hour_local")
              .agg(F.avg("is_empty_now").alias("empty_rate"), F.avg("is_full_now").alias("full_rate"), F.count("*").alias("rows"))
              .toPandas().sort_values(["selection_stratum", "is_weekend", "hour_local"]))
write_table(hourly, "hourly_rates")

# Strata: mvp = high_event_rate / seeded_random; full = event-rate tiers (high / mid / low).
order = ["high_event_rate", "mid_event_rate", "low_event_rate", "seeded_random"]
strata = sorted(hourly.selection_stratum.unique(), key=lambda x: order.index(x) if x in order else len(order))
titles = {"high_event_rate": "High-event stations", "mid_event_rate": "Mid-event stations",
          "low_event_rate": "Low-event stations", "seeded_random": "Random stations"}
fig, axes = plt.subplots(len(strata), 2, figsize=(11, 3.25 * len(strata)), sharex=True, squeeze=False)
for col, (state, label) in enumerate([("empty_rate", "Empty"), ("full_rate", "Full")]):
    for row, stratum in enumerate(strata):
        title = titles.get(stratum, stratum)
        ax = axes[row, col]
        for wk, color, name in [(0, COL["s1"], "Weekday"), (1, COL["s2"], "Weekend")]:
            d = hourly[(hourly.selection_stratum == stratum) & (hourly.is_weekend == wk)]
            ax.plot(d.hour_local, d[state] * 100, color=color, label=name)
        ax.set_title(f"{label} · {title}")
        ax.set_ylabel("% of station-times")
        if row == len(strata) - 1: ax.set_xlabel("Hour of day (New York)")
        ax.set_xticks(range(0, 24, 3))
axes[0, 0].legend(loc="upper right")
fig.suptitle("When are stations empty or full? (state at t, eligible rows, Aug 21 – Oct 26)", fontsize=11, color=COL["ink"])
fig.tight_layout()
save(fig, "01_hourly_rates")

# COMMAND ----------

daily = (elig.withColumn("date_local", F.to_date(F.from_utc_timestamp("prediction_time_utc", C.LOCAL_TZ)))
             .groupBy("date_local").agg(F.avg("is_empty_now").alias("empty_rate"), F.avg("is_full_now").alias("full_rate"),
                                        F.count("*").alias("rows"))
             .toPandas().sort_values("date_local"))
write_table(daily, "daily_rates")

fig, axes = plt.subplots(1, 2, figsize=(11, 3.4))
for ax, (state, label, color) in zip(axes, [("empty_rate", "Empty", COL["s1"]), ("full_rate", "Full", COL["s2"])]):
    ax.plot(pd.to_datetime(daily.date_local), daily[state] * 100, color=color)
    for ts, name in [(split["val_start_utc"], "val"), (split["test_start_utc"], "test")]:
        ax.axvline(ts, color=COL["ref"], ls="--", lw=1)
        ax.text(ts, ax.get_ylim()[1], f" {name}", color=COL["ink2"], va="top", fontsize=8)
    ax.set_title(f"{label} rate by day")
    ax.set_ylabel("% of station-times")
    ax.tick_params(axis="x", rotation=30)
fig.tight_layout()
save(fig, "02_daily_rates_drift")

# COMMAND ----------

# MAGIC %md ### Episode length
# MAGIC Consecutive 15-minute grid times in the same state. Resolution is 15 minutes, so an episode shorter than one step is either missed or counted as 15 min.

# COMMAND ----------

# Runs are found in Spark (gaps-and-islands), so this scales to the full network; same result as run_lengths per station.
ep_src = fl.select("station_id", "prediction_time_utc",
                   F.when(F.col("eligible") == 1, F.col("is_empty_now")).alias("empty_now"),
                   F.when(F.col("eligible") == 1, F.col("is_full_now")).alias("full_now"))
ep_rows = []
for state in ("empty_now", "full_now"):
    lens = L.episode_lengths_spark(ep_src, state)["steps"].astype(int) * C.GRID_MIN
    ep_rows.append({"state": state.replace("_now", ""), "episodes": int(len(lens)),
                    "median_min": float(lens.median()), "p75_min": float(lens.quantile(.75)),
                    "p90_min": float(lens.quantile(.9)), "share_single_step": float((lens == C.GRID_MIN).mean()),
                    "share_over_2h": float((lens > 120).mean())})
    ep_rows[-1]["_lens"] = lens
episodes = pd.DataFrame([{k: v for k, v in r.items() if k != "_lens"} for r in ep_rows])
write_table(episodes, "episode_lengths")
display(episodes)

fig, ax = plt.subplots(figsize=(6.5, 3.6))
for r, color in zip(ep_rows, [COL["s1"], COL["s2"]]):
    lens = np.sort(r["_lens"].to_numpy())
    ccdf = 1 - np.arange(len(lens)) / len(lens)
    ax.step(lens / 60, ccdf * 100, where="post", color=color, label=f"{r['state'].title()} episodes")
ax.set_xscale("log"); ax.set_xlabel("Episode length (hours, log scale)")
ax.set_ylabel("% of episodes at least this long"); ax.set_title("How long do empty and full episodes last?")
ax.legend()
save(fig, "03_episode_lengths")

# COMMAND ----------

# MAGIC %md ### 15-minute transition matrix
# MAGIC Row = state at t, column = state at t+15. The off-diagonal cells are every change persistence gets wrong.

# COMMAND ----------

state_t = F.when(F.col("is_empty_now") == 1, "empty").when(F.col("is_full_now") == 1, "full").otherwise("neither")
state_h = F.when(F.col("empty_label") == 1, "empty").when(F.col("full_label") == 1, "full").otherwise("neither")
tm = (fl.where("eligible = 1").select(state_t.alias("state_t"), state_h.alias("state_t15"))
        .groupBy("state_t", "state_t15").count().toPandas())
tm_counts = tm.pivot(index="state_t", columns="state_t15", values="count").fillna(0).reindex(
    index=["empty", "neither", "full"], columns=["empty", "neither", "full"]).fillna(0)
tm_probs = tm_counts.div(tm_counts.sum(axis=1), axis=0)
write_table(tm_probs.reset_index().rename(columns={"index": "state_t"}), "transition_matrix")
print("Counts:"); display(tm_counts.reset_index())
print("Row shares (P(state at t+15 | state at t)):"); display((tm_probs * 100).round(2).reset_index())

# COMMAND ----------

# MAGIC %md ## 2 · Transition metrics (test week)
# MAGIC **Onset**: not empty now → empty at t+15 (and the same for full). **Recovery**: empty now → has a bike at t+15.
# MAGIC Persistence predicts "no change" everywhere, so its recall on both subsets is 0 by construction. The no-skill PR-AUC equals the event rate.

# COMMAND ----------

now = (fl.where(F.col("prediction_time_utc") >= F.lit(split["test_start_utc"].to_pydatetime()))   # test week only
         .select("station_id", "prediction_time_utc", "is_empty_now", "is_full_now").toPandas())
tp["prediction_time_utc"] = pd.to_datetime(tp["prediction_time_utc"])
now["prediction_time_utc"] = pd.to_datetime(now["prediction_time_utc"])
test = tp.merge(now, on=["station_id", "prediction_time_utc"], how="left")
assert test["is_empty_now"].notna().all(), "test rows missing current state"

trans = pd.DataFrame(
    transition_metrics(test, "empty", thr["empty"], "p_empty", "is_empty_now", "empty_label")
    + transition_metrics(test, "full", thr["full"], "p_full", "is_full_now", "full_label"))
write_table(trans, "transition_metrics")
display(trans.round(4))

fig, axes = plt.subplots(1, 2, figsize=(11, 4))
for ax, (target, p_col, now_col, y_col) in zip(axes, [("empty", "p_empty", "is_empty_now", "empty_label"),
                                                       ("full", "p_full", "is_full_now", "full_label")]):
    sub = test[test[now_col] == 0]
    prec, rec, _ = precision_recall_curve(sub[y_col], sub[p_col])
    ap = safe_ap(sub[y_col], sub[p_col])
    ax.plot(rec, prec, color=COL["s1"], label=f"{champ[target]} (PR-AUC {ap:.2f})")
    ax.axhline(sub[y_col].mean(), color=COL["ref"], ls="--", lw=1.5,
               label=f"No skill / persistence ({sub[y_col].mean():.3f})")
    ax.set_xlim(0, 1); ax.set_ylim(0, 1.02)
    ax.set_xlabel("Recall (share of new events caught)"); ax.set_ylabel("Precision (share of flags that were right)")
    ax.set_title(f"Predicting onset: not {target} now → {target} at t+15")
    ax.legend(loc="upper right")
fig.tight_layout()
save(fig, "04_transition_pr_curves")

# COMMAND ----------

# MAGIC %md ## 3 · Results by stratum (test week)

# COMMAND ----------

ts_ = test.merge(stations.toPandas(), on="station_id", how="left")
rows = []
for stratum, d in [("all", ts_)] + list(ts_.groupby("selection_stratum")):
    for target in C.TARGETS:
        y, p, pp = d[f"{target}_label"], d[f"p_{target}"], d[f"p_persistence_{target}"]
        b_m, b_p = brier_score_loss(y, p), brier_score_loss(y, pp)
        rows.append({"stratum": stratum, "target": target, "rows": len(d), "event_rate": float(y.mean()),
                     "pr_auc_persistence": safe_ap(y, pp), "pr_auc_model": safe_ap(y, p),
                     "brier_persistence": b_p, "brier_model": b_m, "brier_skill_vs_persistence": 1 - b_m / b_p})
by_stratum = pd.DataFrame(rows)
write_table(by_stratum, "by_stratum")
display(by_stratum.round(4))

# COMMAND ----------

# MAGIC %md ## 4 · Brier skill scores (test week)
# MAGIC Skill = 1 − Brier(model) / Brier(reference). 1 = perfect, 0 = no better than the reference, negative = worse.
# MAGIC The climatology reference predicts the **training-period** event rate for every row (no test information used).

# COMMAND ----------

train_rates = (fl.where("eligible = 1").where(F.col("prediction_time_utc") < F.lit(split["val_start_utc"].to_pydatetime()))
                 .agg(F.avg("empty_label").alias("empty"), F.avg("full_label").alias("full")).first().asDict())
skill = []
for target in C.TARGETS:
    y = test[f"{target}_label"].to_numpy()
    b_clim = brier_score_loss(y, np.full(len(y), train_rates[target]))
    b_pers = brier_score_loss(y, test[f"p_persistence_{target}"])
    for model in ["persistence_calibrated", "station_hour_prior", "logreg", "hgb"]:
        b = float(cmp_df[(cmp_df.target == target) & (cmp_df.model == model)]["test_brier"].iloc[0])
        skill.append({"target": target, "model": model, "champion": model == champ[target], "brier": b,
                      "skill_vs_climatology": 1 - b / b_clim, "skill_vs_persistence": 1 - b / b_pers,
                      "climatology_rate_train": train_rates[target]})
skill = pd.DataFrame(skill)
write_table(skill, "skill_scores")
display(skill.round(4))

# COMMAND ----------

# MAGIC %md ## 5 · Grouped permutation importance (validation week, champion models)
# MAGIC Shuffle one feature group at a time and measure the drop in validation PR-AUC. Bigger drop = the model relies on that information more.

# COMMAND ----------

cols = ["station_id", "prediction_time_utc", *L.FEATURE_COLS_SPARK, "empty_label", "full_label"]
emb = dt.timedelta(minutes=split["embargo_min"])
train_sdf = fl.where("eligible = 1").where(F.col("prediction_time_utc") < F.lit((split["val_start_utc"] - emb).to_pydatetime()))
val_sdf = fl.where("eligible = 1").where((F.col("prediction_time_utc") >= F.lit(split["val_start_utc"].to_pydatetime()))
                                         & (F.col("prediction_time_utc") < F.lit((split["test_start_utc"] - emb).to_pydatetime())))
priors = spark.table(C.T["station_priors"]).toPandas()
val = L.join_priors(val_sdf.select(*cols).toPandas(), priors)
Xva = L.to_pandas_features(val)

imp = []
for target in C.TARGETS:
    model = mlflow.sklearn.load_model(f"models:/{C.MODEL_NAMES[target]}@{C.MODEL_ALIAS}")
    r = grouped_permutation_importance(model, Xva, val[f"{target}_label"].to_numpy(), FEATURE_GROUPS, n_repeats=3, seed=C.RANDOM_STATE)
    r["target"], r["model"] = target, champ[target]
    imp.append(r)
imp = pd.concat(imp, ignore_index=True)
write_table(imp, "permutation_importance")
display(imp.round(4))

fig, axes = plt.subplots(1, 2, figsize=(11, 3.6))
for ax, target in zip(axes, C.TARGETS):
    d = imp[imp.target == target].sort_values("drop_mean")
    ax.barh(d.group.str.replace("_", " "), d.drop_mean, xerr=d.drop_std, color=COL["s1"], height=0.6,
            error_kw={"ecolor": COL["ink2"], "lw": 1})
    ax.set_xlabel("Drop in validation PR-AUC when shuffled")
    ax.set_title(f"{target.title()} · {champ[target]}")
    for y_, v in enumerate(d.drop_mean):
        ax.text(v, y_, f" {v:.3f}", va="center", color=COL["ink2"], fontsize=8)
fig.tight_layout()
save(fig, "05_permutation_importance")

# COMMAND ----------

# MAGIC %md ## 6 · Feature-group ablation (train → validation)
# MAGIC Retrains both model types with feature groups added cumulatively. Train is sampled to 300k rows to keep runtime reasonable; validation is complete.
# MAGIC Station habits are fit on the training split only, exactly as in `03`.

# COMMAND ----------

n_train = train_sdf.count()
frac = min(1.0, 300_000 / n_train)
train = L.join_priors(train_sdf.select(*cols).sample(fraction=frac, seed=C.RANDOM_STATE).toPandas(), priors)
# Training rows get out-of-fold priors, exactly as in 03 (in-sample priors leak each row's own label).
train["prior_empty"], train["prior_full"] = L.fit_priors_oof(train, C.PRIOR_SMOOTHING_M, C.PRIOR_OOF_FOLD_DAYS)
Xtr = L.to_pandas_features(train)
print(f"ablation train rows: {len(train):,} (sampled {frac:.1%} of {n_train:,}); validation rows: {len(val):,}")

ab = []
for target in C.TARGETS:
    ytr, yva = train[f"{target}_label"].to_numpy(), val[f"{target}_label"].to_numpy()
    now_col = "is_empty_now" if target == "empty" else "is_full_now"
    ab.append({"target": target, "model": "persistence", "step": 0, "groups": "state now (rule)",
               "n_features": 1, "val_pr_auc": safe_ap(yva, val[now_col]), "val_brier": brier_score_loss(yva, val[now_col])})
    used = []
    for step, g in enumerate(ABLATION_ORDER, start=1):
        used += FEATURE_GROUPS[g]
        for name, est in L.make_models(C.RANDOM_STATE).items():
            est.fit(Xtr[used], ytr)
            p = est.predict_proba(Xva[used])[:, 1]
            ab.append({"target": target, "model": name, "step": step, "groups": " + ".join(ABLATION_ORDER[:step]),
                       "n_features": len(used), "val_pr_auc": safe_ap(yva, p), "val_brier": brier_score_loss(yva, p)})
        print(target, step, g, "done")
ab = pd.DataFrame(ab)
write_table(ab, "ablation")
display(ab.round(4))

fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
labels = ["state\nnow"] + ["+ " + g.replace("_", "\n") if i else "current\nstate" for i, g in enumerate(ABLATION_ORDER)]
for ax, target in zip(axes, C.TARGETS):
    d = ab[ab.target == target]
    base = d[d.model == "persistence"].val_pr_auc.iloc[0]
    ax.axhline(base, color=COL["ref"], ls="--", lw=1.5, label=f"Persistence ({base:.3f})")
    for name, color in [("logreg", COL["s1"]), ("hgb", COL["s2"])]:
        m = d[d.model == name].sort_values("step")
        ax.plot(m.step, m.val_pr_auc, marker="o", ms=6, color=color, label=name)
    ax.set_xticks(range(0, len(ABLATION_ORDER) + 1)); ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Validation PR-AUC"); ax.set_title(f"{target.title()}: adding feature groups")
    ax.legend(loc="lower right")
fig.tight_layout()
save(fig, "06_ablation")

# COMMAND ----------

# MAGIC %md ## 7 · Reliability of the champions (test week)

# COMMAND ----------

cal = spark.table(C.T["calibration_bins"]).toPandas()
fig, axes = plt.subplots(1, 2, figsize=(10, 4.4))
for ax, target in zip(axes, C.TARGETS):
    ax.plot([0, 1], [0, 1], color=COL["ref"], ls="--", lw=1, label="Perfect calibration")
    for model, color, name in [("persistence_calibrated", COL["s2"], "Persistence (calibrated)"),
                               (champ[target], COL["s1"], f"{champ[target]} (champion)")]:
        c = cal[(cal.target == target) & (cal.model == model) & (cal.n >= 30)].sort_values("mean_pred")
        ax.plot(c.mean_pred, c.obs_rate, marker="o", ms=6, color=color, label=name)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.set_xlabel("Mean predicted probability"); ax.set_ylabel("Observed rate")
    ax.set_title(f"Reliability · {target} at t+15 (bins with ≥30 rows)")
    ax.legend(loc="upper left")
fig.tight_layout()
save(fig, "07_reliability")

# COMMAND ----------

# MAGIC %md ## Log everything to one MLflow run

# COMMAND ----------

with mlflow.start_run(run_name="report_analysis"):
    for name, fig in figures.items():
        mlflow.log_figure(fig, f"figures/{name}.png")
    for name, df in {"hourly_rates": hourly, "episode_lengths": episodes, "transition_matrix": tm_probs.reset_index(),
                     "transition_metrics": trans, "by_stratum": by_stratum, "skill_scores": skill,
                     "permutation_importance": imp, "ablation": ab}.items():
        mlflow.log_table(df, f"tables/{name}.json")
    mlflow.log_params({"champion_empty": champ["empty"], "champion_full": champ["full"],
                       "threshold_empty": thr["empty"], "threshold_full": thr["full"]})
print("Figures:", FIG_DIR)
print("Tables:", [A(n) for n in ["hourly_rates", "daily_rates", "episode_lengths", "transition_matrix",
                                  "transition_metrics", "by_stratum", "skill_scores", "permutation_importance", "ablation"]])
