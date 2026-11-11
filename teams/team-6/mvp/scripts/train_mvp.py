"""
Team 6 MVP — model 1: calendar-only LightGBM (Poisson) vs. station x hour x weekday baseline.

Run from anywhere:   python3 teams/team-6/mvp/scripts/train_mvp.py
Reads data/trips_6stations_hourly.parquet, writes model1/. Rows, splits and features come from
scripts/mvp_data.py, which play_model1.ipynb also uses, so script and notebook score identical rows.
"""
import json, sys, warnings
from pathlib import Path
import numpy as np, pandas as pd, lightgbm as lgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_poisson_deviance

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mvp_data import MVP_ROOT, SPLITS, FEATURES, TARGET, HORIZONS, build_frame
warnings.filterwarnings("ignore")

OUT = MVP_ROOT / "model1"; OUT.mkdir(exist_ok=True)
SEED = 622

df = build_frame()
parts = {k: df[df.split == k] for k in SPLITS}
print({k: len(v) for k, v in parts.items()}, "| last test date:", parts["test"].t_date.max())

# ---- baseline: station x hour x weekday mean from TRAIN only --------------
base = parts["train"].groupby(["station_id", "t_hour", "t_dow"], observed=True)[TARGET].mean().rename("baseline").reset_index()
def baseline_pred(frame):
    m = frame[["station_id", "t_hour", "t_dow"]].merge(base, how="left", on=["station_id", "t_hour", "t_dow"])
    return m.baseline.fillna(parts["train"][TARGET].mean()).to_numpy()

# ---- LightGBM, Poisson objective; early stopping on VALIDATION -------------
model = lgb.LGBMRegressor(objective="poisson", n_estimators=3000, learning_rate=0.03, num_leaves=63,
                          min_child_samples=50, subsample=0.8, subsample_freq=1, colsample_bytree=0.9,
                          random_state=SEED, verbose=-1)
model.fit(parts["train"][FEATURES], parts["train"][TARGET],
          eval_set=[(parts["validation"][FEATURES], parts["validation"][TARGET])], eval_metric="poisson",
          categorical_feature=["station_id"], callbacks=[lgb.early_stopping(150, verbose=False)])
print("best_iteration", model.best_iteration_)

def score(y, p):
    p = np.maximum(p, 1e-9)
    return {"mae": round(float(mean_absolute_error(y, p)), 3), "rmse": round(float(np.sqrt(mean_squared_error(y, p))), 3),
            "poisson_deviance": round(float(mean_poisson_deviance(y, p)), 4), "rows": int(len(y))}

report = {"splits": SPLITS, "features": FEATURES, "seed": SEED, "best_iteration": int(model.best_iteration_),
          "rows": {k: int(len(v)) for k, v in parts.items()}, "metrics": {}}
preds = []
for name in ("validation", "test"):
    fr = parts[name]; y = fr[TARGET].to_numpy(dtype=float)
    p_lgb = model.predict(fr[FEATURES]); p_base = baseline_pred(fr)
    report["metrics"][name] = {
        "lightgbm_calendar": score(y, p_lgb), "baseline_station_hour_dow": score(y, p_base),
        "by_horizon": {str(hz): {"lightgbm": score(y[fr.horizon == hz], p_lgb[fr.horizon == hz]),
                                 "baseline": score(y[fr.horizon == hz], p_base[fr.horizon == hz])} for hz in HORIZONS},
        "by_station": {s: {"lightgbm": score(y[fr.station_name == s], p_lgb[fr.station_name == s]),
                           "baseline": score(y[fr.station_name == s], p_base[fr.station_name == s])}
                       for s in sorted(fr.station_name.unique())}}
    preds.append(fr[["station_id", "station_name", "origin_utc", "target_hour_utc", "horizon", "split", TARGET]]
                 .assign(pred_lightgbm=p_lgb.round(3), pred_baseline=p_base.round(3)))
pd.concat(preds).to_parquet(OUT / "predictions.parquet", index=False)
model.booster_.save_model(str(OUT / "lgbm_calendar.txt"))
base.to_csv(OUT / "baseline_station_hour_dow.csv", index=False)
(OUT / "report.json").write_text(json.dumps(report, indent=2))

m = report["metrics"]
print(f"\n{'MAE':12s} {'LightGBM':>10s} {'Baseline':>10s}")
for name in ("validation", "test"):
    print(f"{name:12s} {m[name]['lightgbm_calendar']['mae']:10.3f} {m[name]['baseline_station_hour_dow']['mae']:10.3f}")
print("\ntest by station:")
for s, v in m["test"]["by_station"].items():
    print(f"  {s:28s} lgbm {v['lightgbm']['mae']:.3f}  base {v['baseline']['mae']:.3f}")
