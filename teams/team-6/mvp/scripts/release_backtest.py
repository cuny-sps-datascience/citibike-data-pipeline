"""Walk-forward comparisons with label maturity and a reusable scoring bundle."""

import argparse
import json
from pathlib import Path

import pandas as pd

from mvp_data import MVP_ROOT, SPLITS_COMMON, TARGET, ZONE
from next_hour import (
    MODEL_FEATURES,
    RATE_FLOOR,
    attach_curve,
    demand_curves,
    fit_lightgbm,
    issue_time,
    load_next_hour,
    load_station_map,
    metric_row,
    poisson_log_loss,
)
from poisson_challenger import ALPHAS, fit_glm, predict_glm, save_glm

FIT_BUFFER = pd.Timedelta(minutes=5)
MIN_TRAIN_DAYS = 30


def load_release_receipts(path):
    frame = pd.read_csv(path, dtype={"target_month": str})
    frame["label_available_at_utc"] = frame.label_available_at_utc.map(issue_time)
    if (frame.target_month.duplicated().any()
            or not frame.target_month.str.fullmatch(r"\d{4}-(0[1-9]|1[0-2])").fillna(False).all()):
        raise ValueError("Expected one valid release receipt per target month.")
    month_end = pd.to_datetime(frame.target_month, format="%Y-%m") + pd.offsets.MonthBegin(1)
    if frame.label_available_at_utc.lt(month_end.dt.tz_localize(ZONE).dt.tz_convert("UTC")).any():
        raise ValueError("A full month of outcomes cannot be available before that local month ends.")
    return frame.set_index("target_month").label_available_at_utc


def label_maturity(frame, receipts=None):
    month = frame.target_hour_utc.dt.tz_convert(ZONE).dt.strftime("%Y-%m")
    if receipts is None:
        # Reuse the earlier Team 6 retrospective assumption: last calendar
        # day of the nominal trip-file month, plus 45 days at 00:00 UTC.
        month_start = pd.to_datetime(month, format="%Y-%m", utc=True)
        released = month_start + pd.offsets.MonthEnd() + pd.Timedelta(days=45)
    else:
        released = pd.to_datetime(month.map(receipts.to_dict()), utc=True)
    return frame.assign(
        target_month=month, target_end_utc=frame.target_hour_utc + pd.Timedelta(hours=1),
        label_available_at_utc=released,
    )


def mature_mask(frame, cutoff):
    return frame.target_end_utc.le(cutoff) & frame.label_available_at_utc.lt(cutoff)


def month_cutoff(month):
    start = pd.Timestamp(month, tz=ZONE).tz_convert("UTC")
    return start - pd.Timedelta(hours=1) - FIT_BUFFER


def fold_inputs(frame, cutoff):
    mature = frame.loc[mature_mask(frame, cutoff)]
    for month, validation in reversed(list(mature.groupby("target_month", sort=True))):
        dates = pd.Period(month, freq="M")
        if validation.t_date.min() != str(dates.start_time.date()) or validation.t_date.max() != str(
            dates.end_time.date()
        ):
            continue
        earlier_cutoff = month_cutoff(month)
        train = frame.loc[mature_mask(frame, earlier_cutoff)]
        if train.t_date.nunique() >= MIN_TRAIN_DAYS:
            return train, validation, mature, month
    raise ValueError(f"No fully mature validation month with enough prior training at {cutoff}.")


def select_alpha(train, validation):
    losses = {}
    for alpha in ALPHAS:
        estimator = fit_glm(train, "both", alpha)
        rates = predict_glm(estimator, validation, "both")
        losses[str(alpha)] = float(poisson_log_loss(validation[TARGET], rates).mean())
    best = min(ALPHAS, key=lambda alpha: losses[str(alpha)])
    return best, losses


def run_backtest(data_dir, model_dir, as_of, receipts=None):
    as_of = issue_time(as_of)
    checked = load_next_hour(data_dir, {"archive": (
        SPLITS_COMMON["train"][0], as_of.tz_convert(ZONE).strftime("%Y-%m-%d"),
    )})
    stations = sorted(checked.station_id.astype(str).unique())
    frame = label_maturity(checked.loc[checked.weather_eligible], receipts)
    features = MODEL_FEATURES["both"]
    test = frame.loc[frame.t_date.between(*SPLITS_COMMON["test"])]
    fold_reports = []
    scored_folds = []
    for month, held_out in test.groupby("target_month", sort=True):
        cutoff = month_cutoff(month)
        if cutoff >= as_of or not mature_mask(held_out, as_of).all():
            continue
        train, validation, refit, validation_month = fold_inputs(frame, cutoff)
        alpha, losses = select_alpha(train, validation)
        tuned_booster = fit_lightgbm(train, features, validation)
        booster = fit_lightgbm(refit, features, iterations=tuned_booster.best_iteration)
        glm = fit_glm(refit, "both", alpha)
        glm_rates = predict_glm(glm, held_out, "both")
        booster_rates = booster.predict(held_out[features], num_threads=4)
        scored = attach_curve(held_out, demand_curves(refit))
        scored["pred_baseline"] = scored.typical_departures.clip(lower=RATE_FLOOR)
        scored["pred_glm"] = glm_rates.clip(min=RATE_FLOOR)
        scored["pred_lightgbm"] = booster_rates.clip(min=RATE_FLOOR)
        scored_folds.append(scored)
        fold_reports.append({
            "target_month": month, "fit_cutoff_utc": cutoff.isoformat(),
            "validation_month": validation_month, "train_rows": len(train),
            "validation_rows": len(validation), "refit_rows": len(refit),
            "latest_refit_label_utc": refit.label_available_at_utc.max().isoformat(),
            "glm_alpha": alpha, "glm_validation_log_loss": losses,
            "lightgbm_rounds": tuned_booster.best_iteration,
            "metrics": [metric_row(scored, name, month) for name in ("baseline", "glm", "lightgbm")],
        })
    if not scored_folds:
        raise ValueError("No held-out backtest months had mature outcomes.")
    scored = pd.concat(scored_folds, ignore_index=True)

    train, validation, refit, validation_month = fold_inputs(frame, as_of)
    alpha, losses = select_alpha(train, validation)
    model_dir.mkdir(parents=True, exist_ok=False)
    for mode in MODEL_FEATURES:
        save_glm(model_dir / f"{mode}.joblib", fit_glm(refit, mode, alpha))
    for filename, table in {
        "demand_curves.parquet": demand_curves(refit),
        "stations.parquet": load_station_map(data_dir, stations),
        "rolling_predictions.parquet": scored,
    }.items():
        (model_dir / filename).write_bytes(table.to_parquet(index=False))
    manifest = {
        "estimator_family": "poisson_glm", "bundle_version": model_dir.name,
        "target": "recorded_departures", "station_ids": stations, "fit_cutoff_utc": as_of.isoformat(),
        "release_basis": "supplied_receipts" if receipts is not None else "month_end_plus_45_days_assumed",
        "settings": {
            "fit_buffer_minutes": 5, "min_inner_train_days": MIN_TRAIN_DAYS, "glm_alphas": ALPHAS,
            "selection_metric": "poisson_log_loss", "evaluation_window": SPLITS_COMMON["test"],
        },
        "candidate_fit": {"validation_month": validation_month, "alpha": alpha,
                          "validation_log_loss": losses, "refit_rows": len(refit)},
        "folds": fold_reports,
        "metrics": [metric_row(scored, name, "pooled") for name in ("baseline", "glm", "lightgbm")],
    }
    (model_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("model_dir", type=Path)
    parser.add_argument("--data-dir", type=Path, default=MVP_ROOT / "data")
    parser.add_argument("--fit-cutoff", required=True)
    parser.add_argument("--release-receipts", type=Path)
    args = parser.parse_args()
    receipts = load_release_receipts(args.release_receipts) if args.release_receipts else None
    print(json.dumps(run_backtest(args.data_dir, args.model_dir, args.fit_cutoff, receipts), indent=2))
