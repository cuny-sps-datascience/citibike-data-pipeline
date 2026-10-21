"""Candidate comparison for a new stockout anywhere in the next 30 minutes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import hashlib
import json

import joblib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from threadpoolctl import threadpool_limits

FEATURES = [
    "station_index", "horizon_minutes", "current_bikes", "current_docks", "current_ebikes",
    "disabled_bikes", "disabled_docks", "capacity", "local_hour", "dow", "hour_sin", "hour_cos",
]
OUTCOMES = ("bike_onset", "dock_onset")
ZONE = ZoneInfo("America/New_York")
UTC = timezone.utc


def matrix(table):
    return np.column_stack([table[name].to_numpy(zero_copy_only=False) for name in FEATURES]).astype(float)


def utc_boundary(value):
    return datetime.fromisoformat(value).replace(tzinfo=ZONE).astimezone(UTC)


def partition(features, labels, protocol):
    if not np.array_equal(features["example_id"].to_numpy(), labels["example_id"].to_numpy()):
        raise ValueError("Label/predictor key mismatch")
    origins = features["origin_utc"].to_pylist()
    observed = labels["label_observed_at_utc"].to_pylist()
    targets = labels["target_utc"].to_pylist()
    for origin, input_at, info_at, target, label_at, horizon in zip(
        origins,
        features["input_observed_at_utc"].to_pylist(),
        features["information_available_at_utc"].to_pylist(),
        targets,
        observed,
        features["horizon_minutes"].to_pylist(),
    ):
        if horizon != protocol["horizon_minutes"] or target != origin + timedelta(minutes=horizon):
            raise ValueError("Invalid onset horizon")
        if not input_at < origin or (origin - input_at).total_seconds() > protocol["source_freshness_seconds"]:
            raise ValueError("Future/stale input")
        if not info_at < origin:
            raise ValueError("Future information")
        if not target <= label_at <= target + timedelta(seconds=protocol["endpoint_tolerance_seconds"]):
            raise ValueError("Invalid confirming endpoint")
    masks = {}
    for name in ("train", "validation", "refit", "calibration", "test"):
        first, last = map(utc_boundary, protocol[name + "_local"])
        masks[name] = np.asarray([first <= origin and label_at < last for origin, label_at in zip(origins, observed)])
        if not masks[name].any():
            raise ValueError("Empty " + name + " partition")
    return masks


def candidate_names():
    return ("persistence_no_onset", "regularized_station_logistic", "histogram_boosting_15_leaves")


def selection_key(name, score):
    order = {candidate: index for index, candidate in enumerate(candidate_names())}
    return round(float(score) / 1e-12) * 1e-12, order[name]


def make_model(name, protocol):
    if name == "regularized_station_logistic":
        numeric = list(range(1, len(FEATURES)))
        return Pipeline(
            [
                ("features", ColumnTransformer([
                    ("station", OneHotEncoder(handle_unknown="ignore"), [0]),
                    ("numeric", StandardScaler(), numeric),
                ])),
                ("model", LogisticRegression(**protocol["logistic"])),
            ]
        )
    if name == "histogram_boosting_15_leaves":
        settings = protocol["boosting"]
        return HistGradientBoostingClassifier(
            max_iter=settings["iterations"], max_leaf_nodes=settings["max_leaf_nodes"],
            min_samples_leaf=settings["min_samples_leaf"], l2_regularization=settings["l2_regularization"],
            categorical_features=[0], early_stopping=False, random_state=settings["random_state"],
        )
    raise ValueError("Persistence has no fitted model")


def fit_model(name, X, y, protocol):
    if len(np.unique(y)) < 2:
        return float((y.sum() + 1) / (len(y) + 2))
    model = make_model(name, protocol)
    model.fit(X, y)
    return model


def probability(model, X):
    if isinstance(model, float):
        return np.full(len(X), model)
    return model.predict_proba(X)[:, 1]


def logit(values):
    values = np.clip(values, 1e-6, 1 - 1e-6)
    return np.log(values / (1 - values)).reshape(-1, 1)


def calibrate(raw, y):
    if len(np.unique(y)) < 2:
        return float((y.sum() + 1) / (len(y) + 2))
    return LogisticRegression(C=1.0, max_iter=200, random_state=622).fit(logit(raw), y)


def calibrated(calibrator, raw):
    if isinstance(calibrator, float):
        return np.full(len(raw), calibrator)
    return calibrator.predict_proba(logit(raw))[:, 1]


def metrics(y, probability_values):
    clipped = np.clip(probability_values, 1e-6, 1 - 1e-6)
    return {
        "rows": int(len(y)),
        "events": int(y.sum()),
        "prevalence": float(y.mean()),
        "brier": float(brier_score_loss(y, probability_values)),
        "log_loss": float(log_loss(y, clipped, labels=[0, 1])),
        "average_precision": float(average_precision_score(y, probability_values)) if y.any() else None,
    }


def run(package: Path, output: Path):
    manifest = json.loads((package / "manifest.json").read_text())
    protocol = json.loads((package / "protocol.json").read_text())
    if manifest.get("schema_version") != "stockout-onset-package-v1" or manifest.get("status") != "built":
        raise ValueError("Wrong stockout-onset package")
    for artifact in manifest["artifacts"]:
        if hashlib.sha256((package / artifact["path"]).read_bytes()).hexdigest() != artifact["sha256"]:
            raise ValueError("Package checksum mismatch")
    features = pq.read_table(package / "features.parquet")
    labels = pq.read_table(package / "labels.parquet")
    masks = partition(features, labels, protocol)
    X = matrix(features)
    outcomes = {name: labels[name].to_numpy().astype(int) for name in OUTCOMES}
    output.mkdir(parents=True, exist_ok=False)
    validation, selected, train_models = {}, {}, {}
    with threadpool_limits(limits=4):
        for outcome in OUTCOMES:
            validation[outcome] = {}
            train_models[outcome] = {}
            y = outcomes[outcome]
            for candidate in candidate_names():
                if candidate == "persistence_no_onset":
                    prediction = np.zeros(masks["validation"].sum())
                else:
                    model = fit_model(candidate, X[masks["train"]], y[masks["train"]], protocol)
                    train_models[outcome][candidate] = model
                    prediction = probability(model, X[masks["validation"]])
                validation[outcome][candidate] = metrics(y[masks["validation"]], prediction)
            selected[outcome] = min(
                candidate_names(), key=lambda name: selection_key(name, validation[outcome][name]["brier"])
            )
        selection = {
            "schema_version": "stockout-onset-selection-v1",
            "selected": selected,
            "validation": validation,
            "frozen_at_utc": datetime.now(UTC).isoformat(),
            "test_metrics_viewed": False,
        }
        (output / "selection.json").write_text(json.dumps(selection, indent=2) + "\n")
        models, calibrators, test_metrics, predictions = {}, {}, {}, {}
        for outcome in OUTCOMES:
            models[outcome], calibrators[outcome], test_metrics[outcome], predictions[outcome] = {}, {}, {}, {}
            y = outcomes[outcome]
            for candidate in candidate_names():
                if candidate == "persistence_no_onset":
                    test_probability = np.zeros(masks["test"].sum())
                else:
                    model = fit_model(candidate, X[masks["refit"]], y[masks["refit"]], protocol)
                    models[outcome][candidate] = model
                    raw_calibration = probability(model, X[masks["calibration"]])
                    calibrator = calibrate(raw_calibration, y[masks["calibration"]])
                    calibrators[outcome][candidate] = calibrator
                    test_probability = calibrated(calibrator, probability(model, X[masks["test"]]))
                predictions[outcome][candidate] = test_probability
                test_metrics[outcome][candidate] = metrics(y[masks["test"]], test_probability)
        bundle = {
            "features": FEATURES,
            "models": models,
            "calibrators": calibrators,
            "selected": selected,
            "protocol": protocol,
            "scope": "conditional 30-minute new-stockout probability",
        }
        joblib.dump(bundle, output / "models.joblib")
        test_indices = np.flatnonzero(masks["test"])
        result_table = pa.table(
            {
                "example_id": features["example_id"].take(pa.array(test_indices)),
                "station_id": features["station_id"].take(pa.array(test_indices)),
                "origin_utc": features["origin_utc"].take(pa.array(test_indices)),
                "bike_onset": labels["bike_onset"].take(pa.array(test_indices)),
                "dock_onset": labels["dock_onset"].take(pa.array(test_indices)),
            }
        )
        for outcome in OUTCOMES:
            for candidate in candidate_names():
                result_table = result_table.append_column(
                    outcome + "_" + candidate, pa.array(predictions[outcome][candidate])
                )
        pq.write_table(result_table, output / "predictions.parquet", compression="zstd")
        report = {
            "schema_version": "stockout-onset-result-v1",
            **selection,
            "split_rows": {name: int(mask.sum()) for name, mask in masks.items()},
            "test": test_metrics,
            "test_rows": len(test_indices),
            "package_root": str(package),
            "output_root": str(output),
            "production_activation": False,
            "limitations": protocol["limitations"],
        }
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"selected": selected, "validation": validation, "test": test_metrics}, indent=2))
    return report
