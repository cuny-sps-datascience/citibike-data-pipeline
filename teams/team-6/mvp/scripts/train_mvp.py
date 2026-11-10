"""
Team 6 MVP — model 1: calendar-only LightGBM (Poisson) vs. station x hour x weekday baseline.
6 stations, horizons 1-3 (target hour = origin + h-1, matching Randy's panel convention).

Splits by TARGET local date:
    train       2024-01-01 .. 2025-12-31   (24 months, two winters)
    validation  2026-01-01 .. 2026-02-28   (early stopping / model selection)
    test        2026-03-01 .. 2026-08-31   (6-month forecast simulation — touched once)

Usage: python3 train_mvp.py            (reads trips_6stations_hourly.parquet, writes model1/)
"""
import json, sys
from pathlib import Path
import numpy as np, pandas as pd, lightgbm as lgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_poisson_deviance

OUT = Path("model1"); OUT.mkdir(exist_ok=True)
ZONE = "America/New_York"
SPLITS = {"train": ("2024-01-01", "2025-12-31"), "validation": ("2026-01-01", "2026-02-28"), "test": ("2026-03-01", "2026-08-31")}
# US federal holidays observed (NYC); 2024-2026
HOLIDAYS = pd.to_datetime("""2024-01-01 2024-01-15 2024-02-19 2024-05-27 2024-06-19 2024-07-04 2024-09-02 2024-10-14 2024-11-11 2024-11-28 2024-12-25
2025-01-01 2025-01-20 2025-02-17 2025-05-26 2025-06-19 2025-07-04 2025-09-01 2025-10-13 2025-11-11 2025-11-27 2025-12-25
2026-01-01 2026-01-19 2026-02-16 2026-05-25 2026-06-19 2026-07-03 2026-09-07""".split()).date

# ---- build the station x origin x horizon frame --------------------------
h = pd.read_parquet("trips_6stations_hourly.parquet")
h["hour_utc"] = pd.to_datetime(h.hour_utc, utc=True)
h = h.sort_values(["station_id", "hour_utc"])
first_full = h.hour_utc.min().tz_convert(ZONE).normalize() + pd.Timedelta(days=1)   # drop the partial first local day
last_full = h.hour_utc.max().tz_convert(ZONE).normalize()
h = h[(h.hour_utc >= first_full) & (h.hour_utc < last_full)]

rows = []
for hz in (1, 2, 3):
    f = h[["station_id", "station_name", "hour_utc"]].rename(columns={"hour_utc": "origin_utc"}).copy()
    f["horizon"] = hz
    f["target_hour_utc"] = f.origin_utc + pd.Timedelta(hours=hz - 1)
    rows.append(f)
df = pd.concat(rows, ignore_index=True)
lab = h[["station_id", "hour_utc", "departures"]].rename(columns={"hour_utc": "target_hour_utc"})
df = df.merge(lab, on=["station_id", "target_hour_utc"], how="inner")

# calendar features of the TARGET hour (what we're forecasting), local time
t = df.target_hour_utc.dt.tz_convert(ZONE)
df["t_hour"] = t.dt.hour; df["t_dow"] = t.dt.dayofweek; df["t_month"] = t.dt.month
df["t_doy"] = t.dt.dayofyear; df["t_weekend"] = (df.t_dow >= 5).astype(int)
df["t_holiday"] = t.dt.date.isin(set(HOLIDAYS)).astype(int)
df["t_date"] = t.dt.date.astype(str)
df["station_id"] = df.station_id.astype("category")

def split_of(d):
    for name, (lo, hi) in SPLITS.items():
        if lo <= d <= hi: return name
    return None
df["split"] = df.t_date.map(split_of)
df = df[df.split.notna()]

FEATURES = ["station_id", "horizon", "t_hour", "t_dow", "t_month", "t_doy", "t_weekend", "t_holiday"]
TARGET = "departures"
parts = {k: df[df.split == k] for k in SPLITS}
print({k: len(v) for k, v in parts.items()})

# ---- baseline: station x hour x weekday mean from TRAIN only --------------
base = parts["train"].groupby(["station_id", "t_hour", "t_dow"], observed=True)[TARGET].mean().rename("baseline").reset_index()
def baseline_pred(frame):
    m = frame[["station_id", "t_hour", "t_dow"]].merge(base, how="left", on=["station_id", "t_hour", "t_dow"])
    return m.baseline.fillna(parts["train"][TARGET].mean()).to_numpy()

# ---- LightGBM, Poisson objective ----------------------------------------------
model = lgb.LGBMRegressor(objective="poisson", n_estimators=3000, learning_rate=0.03, num_leaves=63,
                          min_child_samples=50, subsample=0.8, subsample_freq=1, colsample_bytree=0.9, verbose=-1)
model.fit(parts["train"][FEATURES], parts["train"][TARGET],
          eval_set=[(parts["validation"][FEATURES], parts["validation"][TARGET])], eval_metric="poisson",
          categorical_feature=["station_id"], callbacks=[lgb.early_stopping(150, verbose=False)])
print("best_iteration", model.best_iteration_)

def score(y, p):
    p = np.maximum(p, 1e-9)
    return {"mae": round(float(mean_absolute_error(y, p)), 3), "rmse": round(float(np.sqrt(mean_squared_error(y, p))), 3),
            "poisson_deviance": round(float(mean_poisson_deviance(y, p)), 4), "rows": int(len(y))}

report = {"splits": SPLITS, "features": FEATURES, "best_iteration": int(model.best_iteration_), "metrics": {}}
preds = []
for name in ("validation", "test"):
    fr = parts[name]; y = fr[TARGET].to_numpy()
    p_lgb = model.predict(fr[FEATURES]); p_base = baseline_pred(fr)
    report["metrics"][name] = {"lightgbm_calendar": score(y, p_lgb), "baseline_station_hour_dow": score(y, p_base),
        "by_horizon": {str(hz): {"lightgbm": score(y[fr.horizon == hz], p_lgb[fr.horizon == hz]),
                                 "baseline": score(y[fr.horizon == hz], p_base[fr.horizon == hz])} for hz in (1, 2, 3)},
        "by_station": {s: {"lightgbm": score(y[fr.station_name == s], p_lgb[fr.station_name == s]),
                           "baseline": score(y[fr.station_name == s], p_base[fr.station_name == s])} for s in sorted(fr.station_name.unique())}}
    preds.append(fr[["station_id", "station_name", "origin_utc", "target_hour_utc", "horizon", "split", TARGET]]
                 .assign(pred_lightgbm=p_lgb.round(3), pred_baseline=p_base.round(3)))
pd.concat(preds).to_parquet(OUT / "predictions.parquet", index=False)
model.booster_.save_model(str(OUT / "lgbm_calendar.txt"))
base.to_csv(OUT / "baseline_station_hour_dow.csv", index=False)
(OUT / "report.json").write_text(json.dumps(report, indent=2))

m = report["metrics"]
print("\n=== MAE (departures/hour) ===")
print(f"{'':12s} {'LightGBM':>10s} {'Baseline':>10s}")
for name in ("validation", "test"):
    print(f"{name:12s} {m[name]['lightgbm_calendar']['mae']:10.3f} {m[name]['baseline_station_hour_dow']['mae']:10.3f}")
print("\ntest by horizon:")
for hz in ("1", "2", "3"):
    print(f"  h{hz}: lgbm {m['test']['by_horizon'][hz]['lightgbm']['mae']:.3f}  base {m['test']['by_horizon'][hz]['baseline']['mae']:.3f}")
print("\ntest by station:")
for s, v in m["test"]["by_station"].items():
    print(f"  {s:28s} lgbm {v['lightgbm']['mae']:.3f}  base {v['baseline']['mae']:.3f}")
imp = dict(sorted(zip(FEATURES, model.feature_importances_.tolist()), key=lambda kv: -kv[1]))
print("\nfeature importance (splits):", imp)
