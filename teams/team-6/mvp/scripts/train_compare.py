"""
Team 6 MVP — model 1 (calendar) vs model 2 (calendar + forecast weather) on IDENTICAL rows.

    python3 teams/team-6/mvp/scripts/train_compare.py

Both models, and the station x hour x weekday baseline, are trained and scored on the rows returned by
mvp_data.build_common_frame(): common-window splits, known label, usable weather. The only difference between
model 1 and model 2 is the five weather columns. Each model is trained with several seeds so a difference
smaller than seed-to-seed noise isn't read as a weather effect. Model choice is made on VALIDATION; test is
reported once for the two final models. Writes model2/.
"""
import json, sys, warnings
from pathlib import Path
import numpy as np, pandas as pd, lightgbm as lgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_poisson_deviance

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mvp_data import MVP_ROOT, SPLITS_COMMON, FEATURES, WEATHER_FEATURES, TARGET, HORIZONS, build_common_frame
warnings.filterwarnings("ignore")

OUT = MVP_ROOT / "model2"; OUT.mkdir(exist_ok=True)
SEEDS = (622, 1, 2, 3, 4)                      # first seed is the saved model
MODELS = {"model1_calendar": FEATURES, "model2_calendar_weather": FEATURES + WEATHER_FEATURES}
WET = 0.005                                    # forecast precipitation (in) above which an hour counts as "wet"

df = build_common_frame()
parts = {k: df[df.split == k] for k in SPLITS_COMMON}
print({k: len(v) for k, v in parts.items()})


def score(y, p):
    p = np.maximum(p, 1e-9)
    return {"mae": round(float(mean_absolute_error(y, p)), 4), "rmse": round(float(np.sqrt(mean_squared_error(y, p))), 4),
            "poisson_deviance": round(float(mean_poisson_deviance(y, p)), 4), "rows": int(len(y))}


def fit(features, seed):
    m = lgb.LGBMRegressor(objective="poisson", n_estimators=3000, learning_rate=0.03, num_leaves=63,
                          min_child_samples=50, subsample=0.8, subsample_freq=1, colsample_bytree=0.9,
                          random_state=seed, verbose=-1)
    m.fit(parts["train"][features], parts["train"][TARGET],
          eval_set=[(parts["validation"][features], parts["validation"][TARGET])], eval_metric="poisson",
          categorical_feature=["station_id"], callbacks=[lgb.early_stopping(150, verbose=False)])
    return m


# ---- baseline -----------------------------------------------------------------
base = parts["train"].groupby(["station_id", "t_hour", "t_dow"], observed=True)[TARGET].mean().rename("baseline").reset_index()
def baseline_pred(frame):
    m = frame[["station_id", "t_hour", "t_dow"]].merge(base, how="left", on=["station_id", "t_hour", "t_dow"])
    return m.baseline.fillna(parts["train"][TARGET].mean()).to_numpy()

# ---- train every model with every seed ---------------------------------------
fitted, seed_scores = {}, {}
for name, feats in MODELS.items():
    seed_scores[name] = {"validation": [], "test": []}
    for seed in SEEDS:
        m = fit(feats, seed)
        if seed == SEEDS[0]: fitted[name] = m
        for split in ("validation", "test"):
            y = parts[split][TARGET].to_numpy(dtype=float)
            seed_scores[name][split].append(score(y, m.predict(parts[split][feats])))
    v = [s["mae"] for s in seed_scores[name]["validation"]]
    print(f"{name:26s} validation MAE over {len(SEEDS)} seeds: mean {np.mean(v):.3f}  min {min(v):.3f}  max {max(v):.3f}")

# ---- report --------------------------------------------------------------------
def slices(fr):
    wet = fr.forecast_precipitation_in.to_numpy() > WET
    out = {"all": np.ones(len(fr), bool), "wet_hours": wet, "dry_hours": ~wet}
    out.update({f"horizon_{h}": (fr.horizon == h).to_numpy() for h in HORIZONS})
    out.update({f"station::{s}": (fr.station_name == s).to_numpy() for s in sorted(fr.station_name.unique())})
    return out

report = {"splits": SPLITS_COMMON, "rows": {k: int(len(v)) for k, v in parts.items()}, "seeds": list(SEEDS),
          "features": MODELS, "wet_threshold_in": WET,
          "best_iteration": {k: int(m.best_iteration_) for k, m in fitted.items()}, "seed_summary": {}, "metrics": {}}
for name in MODELS:
    report["seed_summary"][name] = {
        split: {k: {"mean": round(float(np.mean([s[k] for s in seed_scores[name][split]])), 4),
                    "std": round(float(np.std([s[k] for s in seed_scores[name][split]])), 4)} for k in ("mae", "rmse")}
        for split in ("validation", "test")}

preds = []
for split in ("validation", "test"):
    fr = parts[split]; y = fr[TARGET].to_numpy(dtype=float)
    p = {"baseline": baseline_pred(fr)}
    p.update({name: fitted[name].predict(fr[feats]) for name, feats in MODELS.items()})
    report["metrics"][split] = {sl: {name: score(y[mask], pr[mask]) for name, pr in p.items()}
                                for sl, mask in slices(fr).items() if mask.sum() > 0}
    preds.append(fr[["station_id", "station_name", "origin_utc", "target_hour_utc", "horizon", "split", TARGET] + WEATHER_FEATURES]
                 .assign(pred_baseline=p["baseline"].round(3), pred_model1=p["model1_calendar"].round(3),
                         pred_model2=p["model2_calendar_weather"].round(3)))
pd.concat(preds).to_parquet(OUT / "predictions.parquet", index=False)
fitted["model1_calendar"].booster_.save_model(str(OUT / "lgbm_calendar_common.txt"))
fitted["model2_calendar_weather"].booster_.save_model(str(OUT / "lgbm_calendar_weather.txt"))
imp = fitted["model2_calendar_weather"].booster_.feature_importance(importance_type="gain")
report["model2_gain_importance"] = {f: round(float(g / imp.sum()), 4) for f, g in
                                    sorted(zip(MODELS["model2_calendar_weather"], imp), key=lambda kv: -kv[1])}
(OUT / "report.json").write_text(json.dumps(report, indent=2))

# ---- print ---------------------------------------------------------------------
for split in ("validation", "test"):
    print(f"\n=== {split.upper()} ({report['rows'][split]:,} rows) — MAE / RMSE ===")
    print(f"{'slice':34s} {'baseline':>14s} {'model 1':>14s} {'model 2':>14s} {'m2 vs m1':>9s}")
    for sl, d in report["metrics"][split].items():
        b, m1, m2 = d["baseline"], d["model1_calendar"], d["model2_calendar_weather"]
        print(f"{sl.replace('station::',''):34s} {b['mae']:6.3f}/{b['rmse']:6.3f}  {m1['mae']:6.3f}/{m1['rmse']:6.3f}  "
              f"{m2['mae']:6.3f}/{m2['rmse']:6.3f}  {100*(m2['mae']/m1['mae']-1):+7.1f}%   n={d['baseline']['rows']:,}")
print("\nmodel 2 share of total gain by feature:", report["model2_gain_importance"])
