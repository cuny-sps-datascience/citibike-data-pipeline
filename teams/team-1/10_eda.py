# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Exploratory data analysis and feature screening
# MAGIC **Question:** which of the features known at time *t* carry information about whether a station is empty or full
# MAGIC at *t + 15 min*, and which features repeat each other?
# MAGIC
# MAGIC **The target is observed, not predicted.** In `features_labels_15m`, `empty_label` / `full_label` record what the
# MAGIC station actually showed 15 minutes after *t*. The model learns to predict them; in this historical table they are
# MAGIC facts, so ordinary feature-vs-target EDA applies.
# MAGIC
# MAGIC **Training rows only.** Everything here uses the training period (before the validation week, minus the embargo),
# MAGIC so looking at the data cannot influence anything that is later scored on validation or test.
# MAGIC
# MAGIC | § | Output | What it answers |
# MAGIC |---|---|---|
# MAGIC | 1 | Data dictionary | What each variable means, its range, how often it is missing |
# MAGIC | 2 | Target overview | How rare empty/full are, by hour, weekday and station tier |
# MAGIC | 3 | Feature vs target curves | How the empty/full rate at t+15 changes across each feature's range |
# MAGIC | 4 | Univariate ranking | How well each feature ranks events on its own (PR-AUC lift, mutual information) |
# MAGIC | 5 | Correlation and redundancy | Which features carry the same information (Spearman, clustering) |
# MAGIC | 6 | Onset view | What separates "about to go empty/full" from "stays fine" (the cases persistence always misses) |
# MAGIC | 7 | Feature-selection candidates | One table combining §4–5, with a suggestion per feature, to test in `11_model_comparison` |
# MAGIC
# MAGIC Scope widget: `mvp` (100 stations) or `full` (whole network), as in `02`/`03`/`06`.

# COMMAND ----------

dbutils.widgets.dropdown("scope", "mvp", ["mvp", "full"], "Station scope")
import os, sys, datetime as dt
os.environ["CITIBIKE_SCOPE"] = dbutils.widgets.get("scope")
sys.path.insert(0, os.getcwd())
import importlib, config as C, citibike_lib as L
importlib.reload(C); importlib.reload(L)
L.init_spark(spark)
import numpy as np, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from pyspark.sql import functions as F
from sklearn.metrics import average_precision_score
from sklearn.feature_selection import mutual_info_classif

EDA_SAMPLE_ROWS = 400_000      # training rows pulled to pandas for plots/ranking (whole station-days, deterministic)
MI_ROWS = 60_000               # mutual information is slow on continuous features; subsample of the sample
REDUNDANT_RHO = 0.90           # |Spearman| at or above this -> features flagged as near-duplicates

FIG_DIR = f"{C.VOLUME_PATH}/eda" + ("" if C.SCOPE == "mvp" else f"_{C.SCOPE}")
os.makedirs(FIG_DIR, exist_ok=True)
E = lambda name: C.fq_out(f"eda_{name}")
timer = L.StepTimer("10_eda")
print(f"scope={C.SCOPE}: tables -> {C.CATALOG}.{C.OUT_SCHEMA}.eda_*, figures -> {FIG_DIR}")

# Chart styling (same validated palette as 06): empty = blue, full = orange; references gray, dashed.
COL = {"empty": "#2a78d6", "full": "#eb6834", "s3": "#1baf7a", "s4": "#eda100", "ref": "#8a8985",
       "ink": "#0b0b0b", "ink2": "#52514e", "grid": "#e4e3df", "bg": "#fcfcfb"}
plt.rcParams.update({
    "figure.facecolor": COL["bg"], "axes.facecolor": COL["bg"], "axes.edgecolor": COL["grid"],
    "axes.labelcolor": COL["ink2"], "xtick.color": COL["ink2"], "ytick.color": COL["ink2"],
    "axes.grid": True, "grid.color": COL["grid"], "grid.linewidth": 0.8, "axes.spines.top": False,
    "axes.spines.right": False, "axes.titlesize": 10, "axes.titleweight": "bold", "axes.titlecolor": COL["ink"],
    "legend.frameon": False, "lines.linewidth": 2, "font.size": 8.5,
})
SEQ = LinearSegmentedColormap.from_list("seq", ["#f2f1ee", "#9cc3ef", "#2a78d6", "#123f75"])
DIV = LinearSegmentedColormap.from_list("div", ["#2a78d6", "#f2f1ee", "#eb6834"])
figures = {}
def save(fig, name):
    fig.savefig(f"{FIG_DIR}/{name}.png", dpi=160, bbox_inches="tight")
    figures[name] = fig
    display(fig)

def write_table(pdf, name):
    spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(E(name))

# What each variable means (all measured at prediction time t, from data at or before t).
DESCRIPTIONS = {
    "num_bikes_available": ("current state", "Bikes available now (classic + e-bike)"),
    "num_ebikes_available": ("current state", "E-bikes available now"),
    "num_docks_available": ("current state", "Open docks now"),
    "num_bikes_disabled": ("current state", "Broken / disabled bikes in docks"),
    "num_docks_disabled": ("current state", "Disabled docks"),
    "capacity": ("station", "Total docks at the station (station info)"),
    "fill_ratio": ("current state", "bikes / (bikes + open docks); 0 = empty, 1 = full"),
    "is_empty_now": ("current state", "1 if no bikes now"),
    "is_full_now": ("current state", "1 if no open docks now"),
    "ebike_share": ("current state", "Share of available bikes that are e-bikes"),
    "report_age_min": ("freshness", "Minutes since the station last reported"),
    "d_bikes_15": ("momentum", "Change in bikes over the last 15 min"),
    "d_docks_15": ("momentum", "Change in open docks over the last 15 min"),
    "d_bikes_30": ("momentum", "Change in bikes over the last 30 min"),
    "d_docks_30": ("momentum", "Change in open docks over the last 30 min"),
    "d_bikes_60": ("momentum", "Change in bikes over the last 60 min"),
    "d_docks_60": ("momentum", "Change in open docks over the last 60 min"),
    "hour_local": ("time", "Hour of day, New York (0-23)"),
    "dow_local": ("time", "Day of week, 1 = Sunday ... 7 = Saturday"),
    "is_weekend": ("time", "1 on Saturday/Sunday"),
    "tod_sin": ("time", "Time of day, cyclic encoding (sin)"),
    "tod_cos": ("time", "Time of day, cyclic encoding (cos)"),
    "dow_sin": ("time", "Day of week, cyclic encoding (sin)"),
    "dow_cos": ("time", "Day of week, cyclic encoding (cos)"),
}
FEATURES = [c for c in L.FEATURE_COLS_SPARK if c in DESCRIPTIONS]
TARGETS = {"empty": "empty_label", "full": "full_label"}

# COMMAND ----------

# MAGIC %md ## Load training rows

# COMMAND ----------

timer.start("load")
split_row = spark.table(C.T["split_config"]).first().asDict()
train_cut = split_row["val_start_utc"] - dt.timedelta(minutes=split_row["embargo_min"])
fl = spark.table(C.T["features_labels"]).where("eligible = 1").where(F.col("prediction_time_utc") < F.lit(train_cut))
stations = spark.table(C.T["mvp_stations"]).select("station_id", "selection_stratum")
n_train = fl.count()
sample_sdf, frac = L.sample_station_days(fl, EDA_SAMPLE_ROWS, C.RANDOM_STATE, n=n_train)
df = (sample_sdf.join(stations, "station_id", "left")
                .select("station_id", "prediction_time_utc", "selection_stratum", *FEATURES, *TARGETS.values())
                .toPandas())
timer.stop(rows=len(df), n_train=n_train, frac=frac)
print(f"training rows: {n_train:,}; EDA sample: {len(df):,} rows ({frac:.1%} of station-days), "
      f"{df.station_id.nunique():,} stations; training period ends {train_cut}")

# COMMAND ----------

# MAGIC %md ## 1 · Data dictionary
# MAGIC Statistics on the EDA sample. `missing_%` is mostly lag features at the start of a station's history or after a gap.

# COMMAND ----------

rows = []
for c in FEATURES:
    s = df[c]
    group, desc = DESCRIPTIONS[c]
    rows.append({"feature": c, "group": group, "description": desc, "dtype": str(s.dtype),
                 "missing_pct": round(100 * s.isna().mean(), 2), "distinct": int(s.nunique()),
                 "mean": s.mean(), "std": s.std(), "min": s.min(), "p50": s.median(), "max": s.max()})
for t, y in TARGETS.items():
    rows.append({"feature": y, "group": "target", "description": f"1 if the station is {t} at t + 15 min (observed)",
                 "dtype": str(df[y].dtype), "missing_pct": 0.0, "distinct": 2, "mean": df[y].mean(),
                 "std": df[y].std(), "min": 0, "p50": df[y].median(), "max": 1})
dictionary = pd.DataFrame(rows)
write_table(dictionary, "data_dictionary")
display(dictionary.round(3))

# COMMAND ----------

# MAGIC %md ## 2 · Target overview
# MAGIC Base rates are computed in Spark on **all** training rows (not the sample).

# COMMAND ----------

overall = fl.agg(F.count("*").alias("rows"), F.avg("empty_label").alias("empty_rate"), F.avg("full_label").alias("full_rate"),
                 F.avg("is_empty_now").alias("empty_now_rate"), F.avg("is_full_now").alias("full_now_rate")).toPandas()
display(overall)

by_hd = (fl.groupBy("dow_local", "hour_local")
           .agg(F.avg("empty_label").alias("empty_rate"), F.avg("full_label").alias("full_rate"), F.count("*").alias("rows"))
           .toPandas())
write_table(by_hd, "target_by_hour_dow")
day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

fig, axes = plt.subplots(1, 2, figsize=(12, 3.6))
for ax, t in zip(axes, TARGETS):
    m = by_hd.pivot(index="dow_local", columns="hour_local", values=f"{t}_rate").reindex(index=range(1, 8), columns=range(24))
    im = ax.imshow(m.values * 100, aspect="auto", cmap=SEQ)
    ax.set_yticks(range(7), day_names); ax.set_xticks(range(0, 24, 3)); ax.grid(False)
    ax.set_xlabel("Hour of day (New York)")
    ax.set_title(f"% of station-times {t} at t+15")
    fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label="%")
fig.tight_layout()
save(fig, "01_target_by_hour_weekday")

# COMMAND ----------

by_station = (fl.groupBy("station_id").agg(F.avg("empty_label").alias("empty_rate"), F.avg("full_label").alias("full_rate"))
                .join(stations, "station_id", "left").toPandas())
write_table(by_station, "target_by_station")
fig, axes = plt.subplots(1, 2, figsize=(11, 3.4))
for ax, t in zip(axes, TARGETS):
    v = np.sort(by_station[f"{t}_rate"].to_numpy())[::-1] * 100
    ax.plot(np.arange(1, len(v) + 1) / len(v) * 100, v, color=COL[t])
    ax.set_xlabel("Stations, ranked (% of stations)"); ax.set_ylabel(f"% of time {t} at t+15")
    top10 = by_station[f"{t}_rate"].nlargest(max(1, len(v) // 10)).sum() / max(by_station[f"{t}_rate"].sum(), 1e-12)
    ax.set_title(f"{t.title()} is concentrated: top 10% of stations hold {top10:.0%} of it")
fig.tight_layout()
save(fig, "02_target_concentration_by_station")
display(by_station.groupby("selection_stratum")[["empty_rate", "full_rate"]].agg(["count", "mean", "median"]).round(4))

# COMMAND ----------

# MAGIC %md ## 3 · How each feature relates to the target
# MAGIC Each panel: the share of rows that are empty (blue) or full (orange) at t+15, across the feature's range
# MAGIC (deciles for continuous features, values for discrete ones). A flat line means the feature says little on its own;
# MAGIC a steep or U-shaped line means it carries signal. Cyclic encodings are summarised by `hour_local` / `dow_local`.

# COMMAND ----------

def rate_curve(d: pd.DataFrame, col: str, max_levels: int = 24) -> pd.DataFrame:
    x = d[col]
    if x.nunique() <= max_levels:
        key = x
    else:
        key = pd.qcut(x.rank(method="first"), 10, labels=False)          # equal-count deciles
    g = d.assign(_k=key).dropna(subset=["_k"]).groupby("_k")
    out = g.agg(x_mid=(col, "median"), rows=(col, "size"), empty_rate=("empty_label", "mean"),
                full_rate=("full_label", "mean")).reset_index(drop=True)
    out["feature"] = col
    return out

PLOT_FEATURES = [c for c in FEATURES if c not in ("tod_sin", "tod_cos", "dow_sin", "dow_cos")]
curves = pd.concat([rate_curve(df, c) for c in PLOT_FEATURES], ignore_index=True)
write_table(curves, "feature_target_curves")

ncol = 4
nrow = int(np.ceil(len(PLOT_FEATURES) / ncol))
fig, axes = plt.subplots(nrow, ncol, figsize=(13, 2.6 * nrow), squeeze=False)
base = {t: df[y].mean() * 100 for t, y in TARGETS.items()}
for ax, c in zip(axes.flat, PLOT_FEATURES):
    d = curves[curves.feature == c]
    for t in TARGETS:
        ax.plot(d.x_mid, d[f"{t}_rate"] * 100, marker="o", ms=3, color=COL[t], label=t.title())
        ax.axhline(base[t], color=COL[t], lw=1, ls=":", alpha=0.6)
    ax.set_title(c); ax.set_ylabel("% at t+15")
for ax in list(axes.flat)[len(PLOT_FEATURES):]:
    ax.set_visible(False)
axes.flat[0].legend(loc="upper right")
fig.suptitle("Empty / full rate at t+15 across each feature's range (dotted = overall rate)", color=COL["ink"], fontsize=11)
fig.tight_layout()
save(fig, "03_feature_vs_target")

# COMMAND ----------

# MAGIC %md ## 4 · Univariate ranking
# MAGIC * **PR-AUC lift:** PR-AUC of the feature used alone as a score, divided by the event rate (1 = no skill).
# MAGIC   Direction is chosen per feature (e.g. fewer bikes → more likely empty).
# MAGIC * **Mutual information:** how much knowing the feature reduces uncertainty about the label (any shape of relationship).
# MAGIC * Computed for all rows and for the **onset** subset (station not empty/full now), where persistence has no skill and
# MAGIC   the model has to earn its keep.

# COMMAND ----------

timer.start("univariate")
Xf = df[FEATURES].astype(float)
Xf = Xf.fillna(Xf.median())
discrete = np.array([df[c].nunique() <= 24 for c in FEATURES])
mi_idx = np.random.default_rng(C.RANDOM_STATE).choice(len(df), size=min(MI_ROWS, len(df)), replace=False)

def univariate(mask: np.ndarray, y: np.ndarray, tag: str, target: str) -> pd.DataFrame:
    yy, XX = y[mask], Xf[mask]
    rate = yy.mean()
    out = []
    for c in FEATURES:
        x = XX[c].to_numpy()
        if yy.sum() == 0 or np.nanstd(x) == 0:
            ap, sign = np.nan, 0
        else:
            ap_pos, ap_neg = average_precision_score(yy, x), average_precision_score(yy, -x)
            ap, sign = (ap_pos, 1) if ap_pos >= ap_neg else (ap_neg, -1)
        out.append({"target": target, "subset": tag, "feature": c, "event_rate": rate,
                    "pr_auc": ap, "pr_auc_lift": ap / rate if rate else np.nan, "direction": "+" if sign > 0 else "-"})
    res = pd.DataFrame(out)
    m_idx = np.intersect1d(np.flatnonzero(mask), mi_idx)
    if len(m_idx) and y[m_idx].sum() > 0:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)      # sklearn warns about mixing discrete/continuous; expected
            res["mutual_info"] = mutual_info_classif(Xf.iloc[m_idx], y[m_idx], discrete_features=discrete,
                                                     random_state=C.RANDOM_STATE)
    else:
        res["mutual_info"] = np.nan
    return res

uni = []
for t, ycol in TARGETS.items():
    y = df[ycol].to_numpy()
    now = df["is_empty_now" if t == "empty" else "is_full_now"].to_numpy()
    uni.append(univariate(np.ones(len(df), bool), y, "all", t))
    uni.append(univariate(now == 0, y, "onset", t))
uni = pd.concat(uni, ignore_index=True)
write_table(uni, "univariate_ranking")
timer.stop(rows=len(df))

fig, axes = plt.subplots(1, 4, figsize=(15, 5.2), sharey=True)
order = (uni.groupby("feature")["pr_auc_lift"].mean().fillna(0).sort_values().index.tolist())   # strongest at top
for ax, (t, sub) in zip(axes, [(t, s) for t in TARGETS for s in ("all", "onset")]):
    d = uni[(uni.target == t) & (uni.subset == sub)].set_index("feature").reindex(order)
    ax.barh(range(len(order)), d.pr_auc_lift, color=COL[t], height=0.65)
    ax.axvline(1, color=COL["ref"], ls="--", lw=1.2)
    ax.set_yticks(range(len(order)), order)
    ax.set_title(f"{t.title()} · {'all rows' if sub == 'all' else 'onset (not ' + t + ' now)'}")
    ax.set_xlabel("PR-AUC ÷ event rate (1 = no skill)")
fig.tight_layout()
save(fig, "04_univariate_pr_auc_lift")

# COMMAND ----------

# MAGIC %md ## 5 · Correlation and redundancy
# MAGIC Spearman (rank) correlation, so monotonic but non-linear relationships count and outliers matter less.
# MAGIC Features are ordered by hierarchical clustering on 1 − |ρ|, so blocks of near-duplicates sit together.

# COMMAND ----------

from scipy.cluster.hierarchy import linkage, leaves_list, dendrogram
from scipy.spatial.distance import squareform

corr_feats = [c for c in FEATURES if Xf[c].std() > 0]
rho = Xf[corr_feats].corr(method="spearman")
dist = np.clip(1 - rho.abs().to_numpy(), 0, None)
np.fill_diagonal(dist, 0)
Z = linkage(squareform(dist, checks=False), method="average")
ordered = [corr_feats[i] for i in leaves_list(Z)]
rho_o = rho.loc[ordered, ordered]

fig, ax = plt.subplots(figsize=(10, 8.5))
im = ax.imshow(rho_o.values, cmap=DIV, vmin=-1, vmax=1)
ax.set_xticks(range(len(ordered)), ordered, rotation=90); ax.set_yticks(range(len(ordered)), ordered); ax.grid(False)
for i in range(len(ordered)):
    for j in range(len(ordered)):
        v = rho_o.values[i, j]
        if i != j and abs(v) >= 0.5:
            ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=6.5, color=COL["ink"])
fig.colorbar(im, ax=ax, fraction=0.035, pad=0.02, label="Spearman ρ")
ax.set_title("Feature correlation (Spearman, clustered); |ρ| ≥ 0.5 labelled")
fig.tight_layout()
save(fig, "05_correlation_heatmap")

fig, ax = plt.subplots(figsize=(10, 3.6))
dendrogram(Z, labels=corr_feats, ax=ax, leaf_rotation=90, color_threshold=1 - REDUNDANT_RHO,
           above_threshold_color=COL["ref"])
ax.axhline(1 - REDUNDANT_RHO, color=COL["full"], ls="--", lw=1.2, label=f"|ρ| = {REDUNDANT_RHO}")
ax.set_ylabel("1 − |ρ|"); ax.set_title("Feature clusters: branches joined below the line are near-duplicates"); ax.legend()
fig.tight_layout()
save(fig, "06_feature_dendrogram")

pairs = []
for i, a in enumerate(corr_feats):
    for b in corr_feats[i + 1:]:
        pairs.append({"feature_a": a, "feature_b": b, "spearman": rho.loc[a, b], "abs_spearman": abs(rho.loc[a, b])})
pairs = pd.DataFrame(pairs).sort_values("abs_spearman", ascending=False)
write_table(pairs, "correlation_pairs")
print(f"pairs with |rho| >= {REDUNDANT_RHO}:")
display(pairs[pairs.abs_spearman >= REDUNDANT_RHO].round(3))

# COMMAND ----------

# MAGIC %md ## 6 · Onset view: what precedes a station running empty or full?
# MAGIC Among rows where the station is **not** empty (or full) now, compare rows that become empty (full) within 15 minutes
# MAGIC with rows that do not. These are the events persistence can never catch.

# COMMAND ----------

ONSET_FEATURES = {"empty": ["num_bikes_available", "fill_ratio", "d_bikes_15", "d_bikes_60"],
                  "full": ["num_docks_available", "fill_ratio", "d_docks_15", "d_docks_60"]}
fig, axes = plt.subplots(2, 4, figsize=(14, 5.6))
onset_rows = []
for r, t in enumerate(TARGETS):
    now = "is_empty_now" if t == "empty" else "is_full_now"
    sub = df[df[now] == 0]
    for ax, c in zip(axes[r], ONSET_FEATURES[t]):
        a, b = sub.loc[sub[TARGETS[t]] == 1, c].dropna(), sub.loc[sub[TARGETS[t]] == 0, c].dropna()
        lo, hi = np.nanpercentile(sub[c], [1, 99])
        bins = np.linspace(lo, hi, 40) if hi > lo else 10
        ax.hist(b, bins=bins, density=True, color=COL["ref"], alpha=0.45, label=f"stays not {t}")
        ax.hist(a, bins=bins, density=True, histtype="step", lw=2, color=COL[t], label=f"becomes {t}")
        ax.set_title(f"{t.title()} onset · {c}"); ax.set_yticks([])
        onset_rows.append({"target": t, "feature": c, "median_onset": a.median(), "median_no_onset": b.median(),
                           "onset_rows": len(a), "other_rows": len(b)})
    axes[r, 0].legend(loc="upper right")
fig.suptitle("Distributions before an onset vs. no onset (density; not empty/full now)", color=COL["ink"], fontsize=11)
fig.tight_layout()
save(fig, "07_onset_distributions")
onset_tbl = pd.DataFrame(onset_rows)
write_table(onset_tbl, "onset_medians")
display(onset_tbl.round(3))

# COMMAND ----------

# MAGIC %md ## 7 · Feature-selection candidates
# MAGIC Combines §4 and §5. **Suggestions, not decisions:** correlated features can still both help a tree model, and a weak
# MAGIC feature alone can matter in combination. Each suggestion is tested in `11_model_comparison` by retraining without it
# MAGIC and comparing validation PR-AUC and Brier with a paired bootstrap.
# MAGIC
# MAGIC Rules: **drop candidate** = near-duplicate (|ρ| ≥ 0.90) of a feature with higher onset lift;
# MAGIC **weak alone** = onset lift < 1.1 for both targets; otherwise **keep**.

# COMMAND ----------

lift = (uni[uni.subset == "onset"].pivot(index="feature", columns="target", values="pr_auc_lift")
          .rename(columns=lambda t: f"onset_lift_{t}"))
lift_all = (uni[uni.subset == "all"].pivot(index="feature", columns="target", values="pr_auc_lift")
              .rename(columns=lambda t: f"all_lift_{t}"))
mi = (uni[uni.subset == "all"].pivot(index="feature", columns="target", values="mutual_info")
        .rename(columns=lambda t: f"mi_{t}"))
summary = lift_all.join(lift).join(mi)
summary["onset_lift_max"] = summary[["onset_lift_empty", "onset_lift_full"]].max(axis=1)

partner, partner_rho, suggestion = {}, {}, {}
for f in summary.index:
    if f not in rho.index:
        partner[f], partner_rho[f], suggestion[f] = None, np.nan, "constant: drop"
        continue
    r = rho.loc[f].drop(f).abs()
    p = r.idxmax()
    partner[f], partner_rho[f] = p, float(rho.loc[f, p])
    stronger = summary.loc[p, "onset_lift_max"] > summary.loc[f, "onset_lift_max"]
    if r.max() >= REDUNDANT_RHO and stronger:
        suggestion[f] = f"drop candidate (repeats {p})"
    elif summary.loc[f, "onset_lift_max"] < 1.1:
        suggestion[f] = "weak alone: test removal"
    else:
        suggestion[f] = "keep"
summary["most_correlated_with"] = pd.Series(partner)
summary["spearman_with_it"] = pd.Series(partner_rho)
summary["suggestion"] = pd.Series(suggestion)
summary["group"] = [DESCRIPTIONS[f][0] for f in summary.index]
summary = summary.reset_index().sort_values("onset_lift_max", ascending=False)
write_table(summary, "feature_selection_candidates")
display(summary.round(3))

# COMMAND ----------

# MAGIC %md ## Save figures and timings to MLflow

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/{C.EXPERIMENT_NAME}")
with mlflow.start_run(run_name="eda"):
    for name, fig in figures.items():
        mlflow.log_figure(fig, f"eda/{name}.png")
    for name, d in {"data_dictionary": dictionary, "univariate_ranking": uni, "correlation_pairs": pairs,
                    "feature_selection_candidates": summary}.items():
        mlflow.log_table(d, f"eda/{name}.json")
    mlflow.log_params({"scope": C.SCOPE, "n_train_rows": n_train, "eda_sample_rows": len(df), "redundant_rho": REDUNDANT_RHO})
display(timer.save(spark))
print("Figures:", FIG_DIR)
