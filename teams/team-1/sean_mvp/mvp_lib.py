"""Shared code for Team 1's 15-minute MVP notebooks.

Features and targets come from the professor's free_edition_models/station_features.py;
this file adds the split, baselines, metrics, and map status on top of it.
"""

import os
import sys

import numpy as np
import pandas as pd

import mvp_config as cfg


def use_station_features():
    """Make the professor's station_features.py importable (one copy, no fork)."""
    here = os.path.abspath(os.getcwd())
    for up in range(5):
        cand = os.path.join(here, *[".."] * up, "free_edition_models")
        if os.path.exists(os.path.join(cand, "station_features.py")):
            cand = os.path.abspath(cand)
            if cand not in sys.path:
                sys.path.append(cand)
            return cand
    raise FileNotFoundError("free_edition_models/station_features.py not found above this folder")


# =============================================================================
# Model inputs (same predictors as the professor's logistic regression)
# =============================================================================

SHARED = ["log_capacity", "weekend", "hour_sin_1", "hour_cos_1", "hour_sin_2", "hour_cos_2"]


def numeric_features(target):
    """target is 'bike' or 'dock'."""
    return [f"num_{target}s_available", f"{target}_available_fraction",
            f"{target}_change_30min_per_minute_fraction", *SHARED]


def gbm_features(target):
    """Gradient boosting uses location directly instead of one-hot cells."""
    return numeric_features(target) + ["lat", "lon"]


def all_input_columns():
    cols = []
    for t in ("bike", "dock"):
        for c in numeric_features(t) + ["lat", "lon"]:
            if c not in cols:
                cols.append(c)
    return cols + ["geographic_cell"]


# =============================================================================
# Chronological split (Spark)
# =============================================================================

def split_cutoffs(targets):
    """Validation and test start times, counted back from the newest labeled poll.

    Returns a dict of naive UTC timestamps (matching Spark's UTC session)."""
    from pyspark.sql import functions as F
    newest = (targets.filter(F.col("bike_target").isNotNull() | F.col("dock_target").isNotNull())
                     .agg(F.max("fetched_at")).first()[0])
    oldest = targets.agg(F.min("fetched_at")).first()[0]
    newest, oldest = pd.Timestamp(newest), pd.Timestamp(oldest)
    test_start = (newest - pd.Timedelta(days=cfg.TEST_DAYS)).ceil("D")
    val_start = test_start - pd.Timedelta(days=cfg.VAL_DAYS)
    return {"data_start": oldest, "val_start": val_start, "test_start": test_start, "data_end": newest}


def with_split(frame, cutoffs):
    """Add a `split` column. A row belongs to a period only if its whole label
    window (fetched_at to label_end) lies inside that period; rows straddling a
    cutoff are dropped, so no training label overlaps validation or test."""
    from pyspark.sql import functions as F
    start = F.col("fetched_at")
    end = F.timestamp_micros(F.col("label_end_microseconds"))
    v = F.lit(pd.Timestamp(cutoffs["val_start"]).to_pydatetime())
    t = F.lit(pd.Timestamp(cutoffs["test_start"]).to_pydatetime())
    split = (F.when(end < v, "train")
              .when((start >= v) & (end < t), "validation")
              .when(start >= t, "test"))
    return frame.withColumn("split", split).filter(F.col("split").isNotNull())


# =============================================================================
# Station x hour history baseline (Spark, computed on all training rows)
# =============================================================================

def station_hour_rates(train, target):
    """Smoothed historical onset rate for each station, local hour, and weekend flag."""
    from pyspark.sql import functions as F
    y = f"{target}_target"
    labeled = train.filter(F.col(y).isNotNull())
    overall = labeled.agg(F.avg(y)).first()[0]
    m = cfg.HISTORY_SMOOTHING
    rates = (labeled.groupBy("station_id", "hour", "weekend")
                    .agg(F.count("*").alias("n"), F.sum(y).alias("events"))
                    .withColumn(f"history_rate_{target}",
                                (F.col("events") + F.lit(m * overall)) / (F.col("n") + F.lit(m)))
                    .select("station_id", "hour", "weekend", f"history_rate_{target}"))
    return rates, overall


# =============================================================================
# Metrics
# =============================================================================

def clip(p):
    return np.clip(np.asarray(p, dtype=float), 1e-6, 1 - 1e-6)


def best_threshold(y, p):
    """Warning threshold that maximizes F1. Chosen on validation only."""
    from sklearn.metrics import precision_recall_curve
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec * rec / np.clip(prec + rec, 1e-12, None)
    return float(thr[np.nanargmax(f1[:-1])]) if len(thr) else 0.5


def score(y, p, threshold):
    from sklearn.metrics import (average_precision_score, brier_score_loss, f1_score,
                                 log_loss, precision_score, recall_score)
    y = np.asarray(y, dtype=int)
    p = clip(p)
    yhat = (p >= threshold).astype(int)
    two = len(np.unique(y)) > 1
    return {
        "n": int(len(y)), "events": int(y.sum()), "base_rate": float(y.mean()),
        "log_loss": float(log_loss(y, p, labels=[0, 1])),
        "brier": float(brier_score_loss(y, p)),
        "pr_auc": float(average_precision_score(y, p)) if two else np.nan,
        "precision": float(precision_score(y, yhat, zero_division=0)),
        "recall": float(recall_score(y, yhat, zero_division=0)),
        "f1": float(f1_score(y, yhat, zero_division=0)),
        "threshold": float(threshold),
    }


def calibration_rows(y, p, target, model, split, bins=10):
    """Observed rate vs mean prediction in quantile bins of the prediction."""
    df = pd.DataFrame({"y": np.asarray(y, dtype=float), "p": clip(p)})
    df["bin"] = pd.qcut(df["p"].rank(method="first"), bins, labels=False)
    out = df.groupby("bin").agg(mean_predicted=("p", "mean"), observed_rate=("y", "mean"),
                                n=("y", "size")).reset_index()
    return out.assign(target=target, model=model, split=split)


# =============================================================================
# Map status
# =============================================================================

def state_from_stockout(stockout):
    """Professor's convention: eligible (operating, not stocked out),
    stocked_out, or unknown (not operating / no data)."""
    s = pd.Series(stockout)
    out = pd.Series("unknown", index=s.index, dtype=object)
    out[s == False] = "eligible"   # noqa: E712 — nullable boolean column
    out[s == True] = "stocked_out"  # noqa: E712
    return out


def map_status(bike_state, dock_state, p_bike, p_dock, thr_bike, thr_dock,
               watch_fraction=cfg.WATCH_FRACTION):
    """One status per station for the map.

    offline     neither bikes nor docks can be scored (station not operating)
    empty_now   0 rentable bikes now          full_now   0 usable docks now
    empty_risk  p_bike >= threshold           full_risk  p_dock >= threshold
                (if both cross, the larger probability/threshold ratio wins)
    watch       either probability >= watch_fraction x its threshold
    ok          otherwise
    """
    bs, ds = np.asarray(bike_state, dtype=object), np.asarray(dock_state, dtype=object)
    rb = np.nan_to_num(np.asarray(p_bike, dtype=float), nan=0.0) / thr_bike
    rd = np.nan_to_num(np.asarray(p_dock, dtype=float), nan=0.0) / thr_dock
    status = np.full(len(bs), "ok", dtype=object)
    status[(rb >= watch_fraction) | (rd >= watch_fraction)] = "watch"
    status[(rd >= 1) & (rd > rb)] = "full_risk"
    status[(rb >= 1) & (rb >= rd)] = "empty_risk"
    status[ds == "stocked_out"] = "full_now"
    status[bs == "stocked_out"] = "empty_now"
    status[(bs == "unknown") & (ds == "unknown")] = "offline"
    return status


def score_stations(frame, models, thresholds):
    """Probabilities, states, and map status for a pandas frame of predictor rows.

    models: {"bike": loaded model, "dock": ...}; each needs the columns it was trained on.
    Only eligible stations (operating, not already stocked out) get a probability."""
    frame = frame.copy()
    for target in ("bike", "dock"):
        frame[f"{target}_state"] = state_from_stockout(frame[f"{target}_stockout"]).to_numpy()
        frame[f"p_{target}"] = np.nan
        mask = (frame[f"{target}_state"] == "eligible").to_numpy()
        if mask.any():
            X = frame.loc[mask, model_columns(models[target])]
            X = X.astype({c: "float64" for c in X.columns if c != "geographic_cell"})
            frame.loc[mask, f"p_{target}"] = predict_proba(models[target], X)
    frame["status"] = map_status(frame["bike_state"], frame["dock_state"], frame["p_bike"],
                                 frame["p_dock"], thresholds["bike"], thresholds["dock"])
    return frame


def model_columns(model):
    """Input columns a fitted sklearn model (or MLflow pyfunc) expects."""
    if hasattr(model, "feature_names_in_"):
        return list(model.feature_names_in_)
    schema = model.metadata.get_input_schema()
    return [c.name for c in schema.inputs]


def predict_proba(model, X):
    out = model.predict_proba(X) if hasattr(model, "predict_proba") else model.predict(X)
    out = np.asarray(out)
    return out[:, 1] if out.ndim == 2 else out
