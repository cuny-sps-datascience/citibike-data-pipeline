"""
Checks for the model 1 vs model 2 comparison. Prints PASS/FAIL per check and writes model2/checks.json.

    python3 teams/team-6/mvp/scripts/check_model2.py

1. Same rows       — models 1 and 2 are scored on the same station x origin x horizon keys, once each.
2. Target hour     — the weather attached to a row is the forecast for that row's departure hour.
3. Cutoff          — every weather run was available before origin - 5 min; no later run is used.
4. Unmatched rows  — how many common-window rows are dropped for weather, by split and reason.
5. Placebo         — replace each row's weather with the weather from 7 days later (wrong weather, same
                     season/hour). If model 2's gain is real weather signal, the gain should mostly vanish.
6. Date sensitivity— rerun with the month-boundary dates Michael suggested (train from Sep 1 2024, test to Aug 1 2026).
Placebo and sensitivity are judged on VALIDATION only.
"""
import json, sys, warnings
from pathlib import Path
import numpy as np, pandas as pd, lightgbm as lgb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mvp_data import (MVP_ROOT, SPLITS_COMMON, FEATURES, WEATHER_FEATURES, TARGET, ISSUE_CUTOFF,
                      build_frame, add_weather, build_common_frame)
warnings.filterwarnings("ignore")
KEY = ["station_id", "origin_utc", "horizon"]
out, ok = {}, True


def check(name, passed, detail):
    global ok
    ok &= bool(passed)
    out[name] = {"pass": bool(passed), **detail}
    print(f"[{'PASS' if passed else 'FAIL'}] {name}: {detail}")


def fit_mae(tr, va, te, feats, seed=622):
    m = lgb.LGBMRegressor(objective="poisson", n_estimators=3000, learning_rate=0.03, num_leaves=63, min_child_samples=50,
                          subsample=0.8, subsample_freq=1, colsample_bytree=0.9, random_state=seed, verbose=-1)
    m.fit(tr[feats], tr[TARGET], eval_set=[(va[feats], va[TARGET])], eval_metric="poisson",
          categorical_feature=["station_id"], callbacks=[lgb.early_stopping(150, verbose=False)])
    f = lambda d: round(float(np.abs(m.predict(d[feats]) - d[TARGET]).mean()), 3)
    return f(va), f(te)


full = add_weather(build_frame(splits=SPLITS_COMMON))          # every common-window row with a known label
df = full[full.weather_eligible].reset_index(drop=True)        # the rows both models use
assert len(df) == len(build_common_frame())

# 1 ---------------------------------------------------------------------------------------------------
pred = pd.read_parquet(MVP_ROOT / "model2" / "predictions.parquet")
scored = df[df.split.isin(["validation", "test"])]
same = (len(pred) == len(scored) and not pred.duplicated(KEY).any()
        and pred[["pred_model1", "pred_model2", "pred_baseline"]].notna().all().all()
        and set(map(tuple, pred[KEY].astype({"station_id": str}).itertuples(index=False)))
            == set(map(tuple, scored[KEY].astype({"station_id": str}).itertuples(index=False))))
check("same_rows", same, {"scored_rows": int(len(pred)), "train_rows_both_models": int((df.split == "train").sum())})

# 2 ---------------------------------------------------------------------------------------------------
mism = int((df.weather_target_hour_utc != df.target_hour_utc).sum())
check("weather_is_for_the_target_hour", mism == 0, {"rows_checked": int(len(df)), "mismatched": mism})

# 3 ---------------------------------------------------------------------------------------------------
lead = (df.origin_utc - df.selected_run_available_utc).dt.total_seconds() / 60
late = int((df.selected_run_available_utc >= df.origin_utc - ISSUE_CUTOFF).sum())
one_origin = bool(((df.origin_utc - df.weather_origin_utc) == pd.Timedelta(hours=1)).all())
check("weather_available_before_cutoff", late == 0 and one_origin,
      {"rows_after_cutoff": late, "min_minutes_available_before_origin": round(float(lead.min()), 1),
       "median_minutes": round(float(lead.median()), 1), "always_previous_hour_weather_origin": one_origin})

# 4 ---------------------------------------------------------------------------------------------------
reason = pd.Series("eligible", index=full.index)
no_row = full.weather_target_hour_utc.isna()
flagged = ~no_row & full.weather_forecast_has_missing.fillna(False).astype(bool)
null_f = ~no_row & ~flagged & full[WEATHER_FEATURES].isna().any(axis=1)
late_m = ~no_row & ~flagged & ~null_f & ~(full.selected_run_available_utc < full.origin_utc - ISSUE_CUTOFF)
reason[no_row] = "no_weather_row"; reason[flagged] = "forecast_flagged_missing"
reason[null_f] = "null_weather_value"; reason[late_m] = "run_not_available_before_cutoff"
tab = pd.crosstab(full.split, reason).reindex(["train", "validation", "test"]).fillna(0).astype(int)
tab["total"] = tab.sum(axis=1)
print(tab.to_string())
check("unmatched_rows_reported", int(tab.eligible.sum()) == len(df),
      {"by_split": {s: {c: int(v) for c, v in row.items()} for s, row in tab.iterrows()},
       "dropped_share": round(float(1 - len(df) / len(full)), 4)})

# 5 ---------------------------------------------------------------------------------------------------
tr, va, te = (df[df.split == s] for s in ("train", "validation", "test"))
m1 = fit_mae(tr, va, te, FEATURES); m2 = fit_mae(tr, va, te, FEATURES + WEATHER_FEATURES)
w = pd.read_parquet(MVP_ROOT / "data" / "weather_6stations.parquet").rename(
    columns={"forecast_origin_utc": "weather_origin_utc", "forecast_horizon": "horizon"})
w["weather_origin_utc"] = w.weather_origin_utc - pd.Timedelta(days=7)      # weather from 7 days LATER lands on this row
plc = df.drop(columns=WEATHER_FEATURES).merge(
    w[["weather_request_class", "weather_origin_utc", "horizon"] + WEATHER_FEATURES],
    on=["weather_request_class", "weather_origin_utc", "horizon"], how="inner").dropna(subset=WEATHER_FEATURES)
plc["station_id"] = plc.station_id.astype(str).astype("category")
ptr, pva, pte = (plc[plc.split == s] for s in ("train", "validation", "test"))
p1 = fit_mae(ptr, pva, pte, FEATURES); p2 = fit_mae(ptr, pva, pte, FEATURES + WEATHER_FEATURES)
real_gain = m1[0] - m2[0]; placebo_gain = p1[0] - p2[0]
check("placebo_wrong_week_weather", placebo_gain < 0.25 * real_gain,
      {"validation_mae_model1": m1[0], "validation_mae_model2_real_weather": m2[0], "real_gain": round(real_gain, 3),
       "placebo_rows_share": round(len(plc) / len(df), 3), "validation_mae_model1_on_placebo_rows": p1[0],
       "validation_mae_model2_wrong_weather": p2[0], "placebo_gain": round(placebo_gain, 3)})

# 6 ---------------------------------------------------------------------------------------------------
alt = df[((df.split == "train") & (df.t_date >= "2024-09-01")) | (df.split == "validation")
         | ((df.split == "test") & (df.t_date <= "2026-08-01"))]
atr, ava, ate = (alt[alt.split == s] for s in ("train", "validation", "test"))
a1 = fit_mae(atr, ava, ate, FEATURES); a2 = fit_mae(atr, ava, ate, FEATURES + WEATHER_FEATURES)
check("date_sensitivity_month_boundaries", a2[0] < a1[0] and abs((a1[0] - a2[0]) - real_gain) < 0.15,
      {"common_window": {"train_from": "2024-08-11", "test_to": "2026-08-10", "validation_mae_m1": m1[0],
                         "validation_mae_m2": m2[0], "test_mae_m1": m1[1], "test_mae_m2": m2[1]},
       "month_boundaries": {"train_from": "2024-09-01", "test_to": "2026-08-01", "validation_mae_m1": a1[0],
                            "validation_mae_m2": a2[0], "test_mae_m1": a1[1], "test_mae_m2": a2[1],
                            "rows": {"train": int(len(atr)), "test": int(len(ate))}}})

(MVP_ROOT / "model2" / "checks.json").write_text(json.dumps(out, indent=2, default=str))
print("\nALL CHECKS PASSED" if ok else "\nSOME CHECKS FAILED")
sys.exit(0 if ok else 1)
