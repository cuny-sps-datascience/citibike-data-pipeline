"""Horizon-routed inventory ensemble comparison with separate calibration."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from zoneinfo import ZoneInfo
import hashlib
import json
import math
import resource
import sys

import joblib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from threadpoolctl import threadpool_limits

FEATURES = [
    "station_index", "horizon_minutes", "current_bikes", "current_docks", "current_ebikes",
    "disabled_bikes", "disabled_docks", "capacity", "local_hour", "dow", "hour_sin", "hour_cos",
]
TARGETS = ("bikes", "docks")
ZONE = ZoneInfo("America/New_York")
UTC = timezone.utc


def peak_rss_mb():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value / (1024 * 1024) if sys.platform == "darwin" else value / 1024


def matrix(table):
    return np.column_stack([table[name].to_numpy(zero_copy_only=False) for name in FEATURES]).astype(float)


def utc_boundary(value):
    return datetime.fromisoformat(value).replace(tzinfo=ZONE).astimezone(UTC)


def partition(features, labels, protocol):
    if not np.array_equal(features["example_id"].to_numpy(), labels["example_id"].to_numpy()):
        raise ValueError("Label/predictor key mismatch")
    origins = features["origin_utc"].to_pylist()
    observed = labels["label_observed_at_utc"].to_pylist()
    for origin, input_at, info_at, target, label_at, horizon in zip(
        origins, features["input_observed_at_utc"].to_pylist(),
        features["information_available_at_utc"].to_pylist(), labels["target_utc"].to_pylist(),
        observed, features["horizon_minutes"].to_pylist(),
    ):
        if not input_at < origin or (origin - input_at).total_seconds() > 120:
            raise ValueError("Future/stale input")
        if not info_at < origin:
            raise ValueError("Future information")
        if target != origin + timedelta(minutes=horizon) or not 0 <= (label_at - target).total_seconds() <= 120:
            raise ValueError("Invalid future endpoint label")
    masks = {}
    for name in ("train", "validation", "refit", "calibration", "test"):
        first, last = map(utc_boundary, protocol[name + "_local"])
        masks[name] = np.asarray([
            first <= origin and origin + timedelta(minutes=182) < last and label_at < last
            for origin, label_at in zip(origins, observed)
        ])
        if not masks[name].any():
            raise ValueError("Empty " + name + " partition")
    return masks


def finite_quantile(scores, coverage=0.9):
    scores = np.asarray(scores, dtype=float)
    if not len(scores) or not np.isfinite(scores).all():
        raise ValueError("Invalid calibration residuals")
    return float(np.sort(scores)[min(math.ceil((len(scores) + 1) * coverage), len(scores)) - 1])


def logit(probability):
    probability = np.clip(probability, 1e-6, 1 - 1e-6)
    return np.log(probability / (1 - probability)).reshape(-1, 1)


def probability(model, X):
    return np.full(len(X), model) if isinstance(model, float) else model.predict_proba(X)[:, 1]


def calibrate(raw, y):
    if len(np.unique(y)) < 2:
        return float((sum(y) + 1) / (len(y) + 2))
    return LogisticRegression(C=1.0, max_iter=200).fit(logit(raw), y)


def calibrated(calibrator, probability_values):
    if isinstance(calibrator, float):
        return np.full(len(probability_values), calibrator)
    return calibrator.predict_proba(logit(probability_values))[:, 1]


def count_metrics(y, prediction):
    return {
        "mae": float(mean_absolute_error(y, prediction)),
        "rmse": float(np.sqrt(mean_squared_error(y, prediction))),
    }


def candidate_specs(protocol):
    specs = [{"id": "boosting_" + str(leaves), "family": "boosting", "leaves": leaves}
             for leaves in protocol["boosting_leaf_candidates"]]
    specs.extend({"id": "forest_depth_" + str(depth), "family": "forest", "max_depth": depth}
                 for depth in protocol["random_forest"]["max_depths"])
    return specs


def make_model(spec, protocol):
    if spec["family"] == "boosting":
        return HistGradientBoostingRegressor(
            max_iter=protocol["boosting_iterations"], max_leaf_nodes=spec["leaves"],
            min_samples_leaf=80, l2_regularization=1.0, early_stopping=False,
            categorical_features=[0], random_state=622,
        )
    settings = protocol["random_forest"]
    numeric = list(range(1, len(FEATURES)))
    return Pipeline([
        ("features", ColumnTransformer([
            ("station", OneHotEncoder(handle_unknown="ignore"), [0]),
            ("numeric", "passthrough", numeric),
        ])),
        ("model", RandomForestRegressor(
            n_estimators=settings["n_estimators"], max_depth=spec["max_depth"],
            max_leaf_nodes=settings["max_leaf_nodes"], min_samples_leaf=settings["min_samples_leaf"],
            max_features=settings["max_features"], max_samples=settings["max_samples"],
            criterion=settings["criterion"], bootstrap=True, n_jobs=settings["n_jobs"],
            random_state=settings["random_state"],
        )),
    ])


def fit_model(X, y, spec, protocol):
    model = make_model(spec, protocol)
    started = perf_counter()
    model.fit(X, y)
    return model, {
        "fit_seconds": perf_counter() - started,
        "peak_rss_mb": peak_rss_mb(),
    }


def model_prediction(model, X):
    return np.maximum(model.predict(X), 0)


def routed_count_prediction(bundle, target, X):
    horizons = X[:, FEATURES.index("horizon_minutes")].astype(int)
    current = X[:, FEATURES.index("current_" + target)]
    result = np.empty(len(X))
    for horizon in np.unique(horizons):
        mask = horizons == horizon
        candidate = bundle["count_routing"][target][str(horizon)]
        result[mask] = current[mask] if candidate == "persistence" else model_prediction(
            bundle["count_models"][target][candidate], X[mask]
        )
    return result


def forecast(bundle, X):
    result = {}
    horizons = X[:, FEATURES.index("horizon_minutes")].astype(int)
    for target in TARGETS:
        point = routed_count_prediction(bundle, target, X)
        raw = probability(bundle["risk_models"][target], X)
        calibrated_probability = np.empty(len(X))
        radius = np.empty(len(X))
        for horizon in np.unique(horizons):
            mask = horizons == horizon
            key = target + "_" + str(horizon)
            calibrated_probability[mask] = calibrated(bundle["calibrators"][key], raw[mask])
            radius[mask] = bundle["interval_radius"][key]
        result[target] = {
            "prediction": point,
            "lower": np.maximum(0, point - radius),
            "upper": point + radius,
            "p_zero": calibrated_probability,
        }
    return result


def selection_key(name, score):
    if name == "persistence":
        preference = 0
    elif name.startswith("boosting_"):
        preference = 1 + int(name.split("_")[1]) / 1000
    else:
        preference = 2 + int(name.rsplit("_", 1)[1]) / 1000
    return round(score / 1e-9) * 1e-9, preference, name


def run(package: Path, output: Path):
    manifest = json.loads((package / "manifest.json").read_text())
    protocol = json.loads((package / "protocol.json").read_text())
    if manifest["schema_version"] != "availability-ensemble-package-v1" or manifest["status"] != "built":
        raise ValueError("Wrong package")
    for artifact in manifest["artifacts"]:
        path = package / artifact["path"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != artifact["sha256"]:
            raise ValueError("Package checksum mismatch")
    features = pq.read_table(package / "features.parquet")
    labels = pq.read_table(package / "labels.parquet")
    masks = partition(features, labels, protocol)
    X = matrix(features)
    outcomes = {target: labels[target].to_numpy().astype(float) for target in TARGETS}
    horizons = X[:, FEATURES.index("horizon_minutes")].astype(int)
    specs = candidate_specs(protocol)
    spec_by_id = {spec["id"]: spec for spec in specs}
    output.mkdir(parents=True, exist_ok=False)
    trained = {target: {} for target in TARGETS}
    validation = {target: {} for target in TARGETS}
    fit_quality = {target: {} for target in TARGETS}
    routing = {target: {} for target in TARGETS}
    with threadpool_limits(limits=protocol["resource_limits"]["threads"]):
        for target in TARGETS:
            train = masks["train"]
            validation_mask = masks["validation"]
            current = X[:, FEATURES.index("current_" + target)]
            for horizon in protocol["horizons_minutes"]:
                mask = validation_mask & (horizons == horizon)
                validation[target].setdefault("persistence", {})[str(horizon)] = count_metrics(outcomes[target][mask], current[mask])
            for spec in specs:
                model, quality = fit_model(X[train], outcomes[target][train], spec, protocol)
                trained[target][spec["id"]] = model
                fit_quality[target][spec["id"]] = quality
                prediction = model_prediction(model, X[validation_mask])
                validation[target][spec["id"]] = {}
                validation_horizons = horizons[validation_mask]
                for horizon in protocol["horizons_minutes"]:
                    mask = validation_horizons == horizon
                    validation[target][spec["id"]][str(horizon)] = count_metrics(outcomes[target][validation_mask][mask], prediction[mask])
                print("Availability ensemble validation", target, spec["id"], flush=True)
            for horizon in protocol["horizons_minutes"]:
                eligible = list(validation[target])
                winner = min(eligible, key=lambda name: selection_key(name, validation[target][name][str(horizon)]["mae"]))
                routing[target][str(horizon)] = winner
        selection = {
            "schema_version": "availability-ensemble-selection-v1",
            "selected_counts_by_horizon": routing,
            "validation": validation,
            "fit_quality": fit_quality,
            "frozen_at_utc": datetime.now(UTC).isoformat(),
            "protocol_sha256": manifest["protocol_sha256"],
            "test_metrics_viewed": False,
        }
        (output / "selection.json").write_text(json.dumps(selection, indent=2) + "\n")
        refit = masks["refit"]
        calibration_mask = masks["calibration"]
        count_models = {target: {} for target in TARGETS}
        refit_quality = {target: {} for target in TARGETS}
        for target in TARGETS:
            for candidate in sorted(set(routing[target].values()) - {"persistence"}):
                model, quality = fit_model(X[refit], outcomes[target][refit], spec_by_id[candidate], protocol)
                count_models[target][candidate] = model
                refit_quality[target][candidate] = quality
        bundle = {
            "schema_version": "availability-ensemble-model-v1",
            "count_models": count_models,
            "count_routing": routing,
            "risk_models": {}, "calibrators": {}, "interval_radius": {},
            "features": FEATURES,
            "cohort": json.loads((package / "cohort.json").read_text()),
            "protocol": protocol,
            "selection": selection,
            "scope": protocol["scope"],
        }
        calibration = {}
        prevalence = {}
        for target in TARGETS:
            zero = (outcomes[target] == 0).astype(int)
            if len(np.unique(zero[refit])) < 2:
                risk = float((zero[refit].sum() + 1) / (refit.sum() + 2))
            else:
                risk = HistGradientBoostingClassifier(
                    max_iter=protocol["boosting_iterations"], max_leaf_nodes=15,
                    min_samples_leaf=80, l2_regularization=1.0, early_stopping=False,
                    categorical_features=[0], random_state=622,
                ).fit(X[refit], zero[refit])
            bundle["risk_models"][target] = risk
            point = routed_count_prediction(bundle, target, X[calibration_mask])
            raw = probability(risk, X[calibration_mask])
            calibration_horizons = horizons[calibration_mask]
            for horizon in protocol["horizons_minutes"]:
                mask = calibration_horizons == horizon
                key = target + "_" + str(horizon)
                residual = np.abs(outcomes[target][calibration_mask][mask] - point[mask])
                radius = finite_quantile(residual)
                bundle["interval_radius"][key] = radius
                bundle["calibrators"][key] = calibrate(raw[mask], zero[calibration_mask][mask])
                calibration[key] = {"rows": int(mask.sum()), "zero_events": int(zero[calibration_mask][mask].sum()), "interval_radius": radius}
                fit_horizon = refit & (horizons == horizon)
                prevalence[key] = float((zero[fit_horizon].sum() + 1) / (fit_horizon.sum() + 2))
        joblib.dump(bundle, output / "models.joblib")
        (output / "calibration.json").write_text(json.dumps(calibration, indent=2) + "\n")
        test = masks["test"]
        predictions = forecast(bundle, X[test])
        report = {
            **selection,
            "schema_version": "availability-ensemble-result-v1",
            "calibration": calibration,
            "refit_quality": refit_quality,
            "split_rows": {name: int(mask.sum()) for name, mask in masks.items()},
            "feature_names": FEATURES,
            "test_by_horizon": {},
            "risk_reliability": {},
            "nominal_interval_coverage": 0.9,
            "cohort_stations": len(bundle["cohort"]),
            "test_stations": len(set(np.asarray(features["station_id"].to_pylist())[test])),
            "scope": bundle["scope"],
            "development_evaluation": True,
            "confirmation_status": protocol["confirmation"]["status"],
            "active_departure_model_changed": False,
            "production_activation": False,
            "limitations": protocol["limitations"],
        }
        test_horizons = horizons[test]
        for horizon in protocol["horizons_minutes"]:
            horizon_mask = test_horizons == horizon
            record = {"rows": int(horizon_mask.sum())}
            for target in TARGETS:
                y = outcomes[target][test][horizon_mask]
                prediction = predictions[target]
                zero = (y == 0).astype(int)
                baseline = X[test][horizon_mask, FEATURES.index("current_" + target)]
                probability_values = prediction["p_zero"][horizon_mask]
                low = prediction["lower"][horizon_mask]
                high = prediction["upper"][horizon_mask]
                record[target] = {
                    "selected_candidate": routing[target][str(horizon)],
                    "model": count_metrics(y, prediction["prediction"][horizon_mask]),
                    "persistence": count_metrics(y, baseline),
                    "interval_coverage": float(np.mean((low <= y) & (y <= high))),
                    "mean_interval_width": float(np.mean(high - low)),
                    "zero_events": int(zero.sum()),
                    "brier": float(brier_score_loss(zero, probability_values)),
                    "persistence_brier": float(brier_score_loss(zero, (baseline == 0).astype(float))),
                    "prevalence_brier": float(brier_score_loss(zero, np.full(len(zero), prevalence[target + "_" + str(horizon)]))),
                    "average_precision": float(average_precision_score(zero, probability_values)) if zero.any() else None,
                }
                bins = []
                for index in range(10):
                    mask = (probability_values >= index / 10) & (probability_values < ((index + 1) / 10) if index < 9 else probability_values <= 1)
                    if mask.any():
                        bins.append({"lower": index / 10, "rows": int(mask.sum()),
                                     "mean_probability": float(probability_values[mask].mean()),
                                     "observed_rate": float(zero[mask].mean())})
                report["risk_reliability"][target + "_" + str(horizon)] = bins
            report["test_by_horizon"][str(horizon)] = record
        indices = np.flatnonzero(test)
        selected_features = features.take(pa.array(indices))
        selected_labels = labels.take(pa.array(indices))
        prediction_table = pa.table({name: selected_features[name] for name in (
            "example_id", "station_id", "origin_utc", "horizon_minutes", "current_bikes", "current_docks")})
        for name in ("target_utc", "label_observed_at_utc", "bikes", "docks"):
            prediction_table = prediction_table.append_column(name, selected_labels[name])
        for target in TARGETS:
            for name, values_array in predictions[target].items():
                prediction_table = prediction_table.append_column(target + "_" + name, pa.array(values_array))
        pq.write_table(prediction_table, output / "predictions.parquet", compression="zstd")
        report["test_forecast_rows"] = len(indices)
        report["package_root"] = str(package)
        report["output_root"] = str(output)
        reloaded = joblib.load(output / "models.joblib")
        sample = min(100, len(indices))
        check = forecast(reloaded, X[test][:sample])
        report["save_reload_checked_rows"] = sample
        report["save_reload_max_difference"] = max(
            float(np.max(np.abs(check[target][name] - predictions[target][name][:sample])))
            for target in TARGETS for name in predictions[target]
        )
        report["model_artifact_bytes"] = (output / "models.joblib").stat().st_size
        if report["save_reload_max_difference"] != 0:
            raise ValueError("Save/reload parity failed")
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"selected": routing, "test_by_horizon": report["test_by_horizon"]}, indent=2), flush=True)
    return report
