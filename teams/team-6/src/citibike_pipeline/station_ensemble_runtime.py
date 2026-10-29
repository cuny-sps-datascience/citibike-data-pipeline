"""Bounded ensemble comparison for the conditional station departure pilot."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from zoneinfo import ZoneInfo
import hashlib
import json
import resource
import sys
import warnings

import joblib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import mean_absolute_error, mean_poisson_deviance, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler
from threadpoolctl import threadpool_limits

ZONE = ZoneInfo("America/New_York")
UTC = timezone.utc
POISSON_CATEGORIES = ["station_hour", "dow_hour", "horizon"]
WEATHER = ["temperature", "temperature_squared", "precipitation", "wind", "wet"]
TREE_CATEGORIES = ["station_id", "target_local_hour", "target_local_dow", "horizon"]
FAMILY_ORDER = {"poisson": 0, "histogram_boosting": 1, "random_forest": 2}


def peak_rss_mb():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value / (1024 * 1024) if sys.platform == "darwin" else value / 1024


def metrics(y, prediction):
    prediction = np.maximum(np.asarray(prediction, dtype=float), 1e-9)
    y = np.asarray(y, dtype=float)
    if not len(y) or not np.isfinite(prediction).all():
        raise ValueError("Empty or nonfinite evaluation")
    return {
        "rows": len(y),
        "mae": float(mean_absolute_error(y, prediction)),
        "rmse": float(np.sqrt(mean_squared_error(y, prediction))),
        "poisson_deviance": float(mean_poisson_deviance(y, prediction)),
        "bias": float(np.mean(prediction - y)),
        "actual_mean": float(np.mean(y)),
        "zero_labels": int(np.sum(y == 0)),
    }


def local(value):
    return datetime.fromisoformat(value).replace(tzinfo=ZONE).astimezone(UTC)


def in_partition(row, bounds):
    first, last = map(local, bounds)
    return first <= row["origin_utc"] and row["origin_utc"] + timedelta(hours=3) <= last


def partition(rows, protocol):
    validation_fit = datetime.fromisoformat(protocol["validation_fit_origin_local"]).astimezone(UTC) - timedelta(minutes=5)
    test_fit = datetime.fromisoformat(protocol["test_fit_origin_local"]).astimezone(UTC) - timedelta(minutes=5)
    seen = set()
    for row in rows:
        key = row["station_id"], row["origin_utc"], row["horizon"]
        if key in seen:
            raise ValueError("Duplicate station/origin/horizon key")
        seen.add(key)
        cutoff = row["issue_cutoff_utc"]
        if cutoff != row["origin_utc"] - timedelta(minutes=5):
            raise ValueError("Wrong origin cutoff")
        for field in ("weather_assumed_available_at_utc", "information_assumed_available_at_utc"):
            if not row[field] < cutoff:
                raise ValueError("Future predictor availability: " + field)
        if row["target_start_utc"] != row["origin_utc"] + timedelta(hours=row["horizon"]):
            raise ValueError("Target/horizon mismatch")
        if row["target_end_utc"] != row["target_start_utc"] + timedelta(hours=1):
            raise ValueError("Target interval mismatch")
        if row["departures"] < 0:
            raise ValueError("Negative departure count")
    result = {
        "train": [r for r in rows if in_partition(r, protocol["train_target_local"]) and r["label_available_at_utc"] < validation_fit],
        "validation": [r for r in rows if in_partition(r, protocol["validation_target_local"]) and r["label_available_at_utc"] < test_fit],
        "refit": [r for r in rows if in_partition(r, protocol["refit_target_local"]) and r["label_available_at_utc"] < test_fit],
        "test": [r for r in rows if in_partition(r, protocol["test_target_local"])],
    }
    if any(not value for value in result.values()):
        raise ValueError("Empty release-eligible partition")
    return result, validation_fit, test_fit


def values(rows, estimator, arm):
    if estimator == "poisson":
        names = POISSON_CATEGORIES + ([] if arm == "calendar" else WEATHER)
        return np.asarray([[row[name] for name in names] for row in rows], dtype=object)
    result = []
    for row in rows:
        target = row["target_start_utc"].astimezone(ZONE)
        item = [row["station_id"], target.hour, target.weekday(), row["horizon"]]
        if arm == "weather":
            item.extend(row[name] for name in WEATHER)
        result.append(item)
    return np.asarray(result, dtype=object)


def candidate_specs(protocol):
    specs = []
    for arm in protocol["feature_arms"]:
        for alpha in protocol["poisson_alphas"]:
            specs.append({"id": f"{arm}__poisson__alpha_{alpha}", "arm": arm, "family": "poisson", "alpha": alpha})
        specs.append({"id": f"{arm}__histogram_boosting__leaves_15", "arm": arm, "family": "histogram_boosting"})
        for depth in protocol["random_forest"]["max_depths"]:
            specs.append({"id": f"{arm}__random_forest__depth_{depth}", "arm": arm, "family": "random_forest", "max_depth": depth})
    return specs


def make_model(spec, protocol):
    arm = spec["arm"]
    family = spec["family"]
    if family == "poisson":
        numeric = list(range(3, 3 + (len(WEATHER) if arm == "weather" else 0)))
        transforms = [("categories", OneHotEncoder(handle_unknown="ignore"), [0, 1, 2])]
        if numeric:
            transforms.append(("numeric", StandardScaler(), numeric))
        return Pipeline([
            ("features", ColumnTransformer(transforms)),
            ("model", PoissonRegressor(alpha=spec["alpha"], max_iter=500, tol=1e-6)),
        ])
    numeric = list(range(4, 4 + (len(WEATHER) if arm == "weather" else 0)))
    if family == "random_forest":
        transforms = [("categories", OneHotEncoder(handle_unknown="ignore"), [0, 1, 2, 3])]
        if numeric:
            transforms.append(("numeric", "passthrough", numeric))
        settings = protocol["random_forest"]
        return Pipeline([
            ("features", ColumnTransformer(transforms)),
            ("model", RandomForestRegressor(
                n_estimators=settings["n_estimators"], max_depth=spec["max_depth"],
                max_leaf_nodes=settings["max_leaf_nodes"], min_samples_leaf=settings["min_samples_leaf"],
                max_features=settings["max_features"], max_samples=settings["max_samples"],
                criterion=settings["criterion"], bootstrap=True, n_jobs=settings["n_jobs"],
                random_state=settings["random_state"],
            )),
        ])
    settings = protocol["histogram_boosting"]
    transforms = [("categories", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), [0, 1, 2, 3])]
    if numeric:
        transforms.append(("numeric", "passthrough", numeric))
    return Pipeline([
        ("features", ColumnTransformer(transforms, sparse_threshold=0)),
        ("model", HistGradientBoostingRegressor(
            loss=settings["loss"], max_iter=settings["max_iter"], max_leaf_nodes=settings["max_leaf_nodes"],
            min_samples_leaf=settings["min_samples_leaf"], l2_regularization=settings["l2_regularization"],
            learning_rate=settings["learning_rate"], early_stopping=settings["early_stopping"],
            categorical_features=[0, 1, 2, 3], random_state=settings["random_state"],
        )),
    ])


def fit_candidate(rows, spec, protocol):
    X = values(rows, spec["family"], spec["arm"])
    y = np.asarray([row["departures"] for row in rows], dtype=float)
    if np.any(y < 0) or y.sum() <= 0:
        raise ValueError("Poisson-loss candidates require nonnegative labels with positive total")
    model = make_model(spec, protocol)
    started = perf_counter()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ConvergenceWarning)
        model.fit(X, y)
    quality = {
        "converged": not any(issubclass(w.category, ConvergenceWarning) for w in caught),
        "fit_seconds": perf_counter() - started,
        "peak_rss_mb": peak_rss_mb(),
    }
    final = model[-1]
    if hasattr(final, "n_iter_"):
        quality["iterations"] = int(final.n_iter_)
    return model, quality


def predict(model, rows, spec):
    return np.maximum(model.predict(values(rows, spec["family"], spec["arm"])), 1e-9)


def selection_key(spec, score):
    if spec["family"] == "poisson":
        size = -float(spec["alpha"])
    elif spec["family"] == "random_forest":
        size = int(spec["max_depth"])
    else:
        size = 0
    return round(score / 1e-9) * 1e-9, FAMILY_ORDER[spec["family"]], size, spec["id"]


def block_bootstrap(rows, outcomes, predictions, reference, seed=622, replicates=1000):
    days = sorted({row["origin_utc"].astimezone(ZONE).date() for row in rows})
    blocks = [days[index:index + 7] for index in range(0, len(days), 7)]
    day_index = {day: np.asarray([i for i, row in enumerate(rows) if row["origin_utc"].astimezone(ZONE).date() == day]) for day in days}
    rng = np.random.default_rng(seed)
    result = {}
    for name, prediction in predictions.items():
        if name == reference:
            continue
        block_values = []
        for block in blocks:
            indices = np.concatenate([day_index[day] for day in block])
            block_values.append((
                float(np.abs(prediction[indices] - outcomes[indices]).sum()),
                float(np.abs(predictions[reference][indices] - outcomes[indices]).sum()),
                len(indices),
            ))
        draws = []
        for _ in range(replicates):
            chosen = rng.integers(0, len(blocks), len(blocks))
            model_sum = sum(block_values[index][0] for index in chosen)
            reference_sum = sum(block_values[index][1] for index in chosen)
            rows_count = sum(block_values[index][2] for index in chosen)
            draws.append((model_sum - reference_sum) / rows_count)
        result[name] = {
            "reference": reference,
            "replicates": replicates,
            "block_days": 7,
            "blocks": len(blocks),
            "mae_difference_interval_2_5_97_5": [float(v) for v in np.quantile(draws, [0.025, 0.975])],
        }
    return result


def run(package: Path, output: Path):
    manifest = json.loads((package / "manifest.json").read_text())
    protocol = json.loads((package / "protocol.json").read_text())
    if manifest["schema_version"] != "station-ensemble-package-v1" or manifest["status"] != "built":
        raise ValueError("Wrong package")
    for artifact in manifest["artifacts"]:
        path = package / artifact["path"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != artifact["sha256"]:
            raise ValueError("Package checksum mismatch")
    rows = pq.read_table(package / "features.parquet").to_pylist()
    parts, validation_fit, test_fit = partition(rows, protocol)
    arrays_y = {name: np.asarray([row["departures"] for row in part], dtype=float) for name, part in parts.items()}
    specs = candidate_specs(protocol)
    output.mkdir(parents=True, exist_ok=False)
    fitted = {}
    validation = {}
    quality = {}
    failures = {}
    with threadpool_limits(limits=protocol["resource_limits"]["threads"]):
        for spec in specs:
            try:
                model, fit_quality = fit_candidate(parts["train"], spec, protocol)
                prediction = predict(model, parts["validation"], spec)
                fitted[spec["id"]] = model
                quality[spec["id"]] = fit_quality
                validation[spec["id"]] = metrics(arrays_y["validation"], prediction)
                print("Station ensemble validation", spec["id"], validation[spec["id"]]["mae"], flush=True)
            except Exception as error:
                failures[spec["id"]] = {"type": type(error).__name__, "message": str(error)}
                print("Station ensemble candidate failed", spec["id"], failures[spec["id"]], flush=True)
        selected = {}
        by_id = {spec["id"]: spec for spec in specs}
        for arm in protocol["feature_arms"]:
            for family in FAMILY_ORDER:
                eligible = [spec for spec in specs if spec["arm"] == arm and spec["family"] == family and spec["id"] in validation]
                if not eligible:
                    raise ValueError(f"No eligible candidate for {arm}/{family}")
                winner = min(eligible, key=lambda spec: selection_key(spec, validation[spec["id"]]["mae"]))
                selected[f"{arm}__{family}"] = winner["id"]
        overall_id = min(selected.values(), key=lambda name: selection_key(by_id[name], validation[name]["mae"]))
        selection = {
            "schema_version": "station-ensemble-selection-v1",
            "selected_by_family_arm": selected,
            "overall_validation_winner": overall_id,
            "validation": validation,
            "fit_quality": quality,
            "candidate_failures": failures,
            "validation_fit_cutoff_utc": str(validation_fit),
            "test_fit_cutoff_utc": str(test_fit),
            "frozen_at_utc": datetime.now(UTC).isoformat(),
            "protocol_sha256": manifest["protocol_sha256"],
            "test_metrics_viewed": False,
        }
        (output / "selection.json").write_text(json.dumps(selection, indent=2) + "\n")
        refits = {}
        refit_quality = {}
        selected_specs = {name: by_id[candidate] for name, candidate in selected.items()}
        for name, spec in selected_specs.items():
            model, fit_quality = fit_candidate(parts["refit"], spec, protocol)
            if not fit_quality["converged"]:
                raise ValueError("Selected refit did not converge: " + name)
            refits[name] = model
            refit_quality[name] = fit_quality
        bundle = {
            "schema_version": "station-ensemble-model-v1",
            "models": refits,
            "specs": selected_specs,
            "protocol": protocol,
            "selection": selection,
            "scope": protocol["scope"],
        }
        joblib.dump(bundle, output / "models.joblib")
        test_rows = parts["test"]
        predictions = {name: predict(model, test_rows, selected_specs[name]) for name, model in refits.items()}
        report = {
            **selection,
            "schema_version": "station-ensemble-result-v1",
            "package_root": str(package),
            "output_root": str(output),
            "split_rows": {name: len(part) for name, part in parts.items()},
            "split_stations": {name: len({row["station_id"] for row in part}) for name, part in parts.items()},
            "test_rows_with_station_absent_from_refit": sum(row["station_id"] not in {r["station_id"] for r in parts["refit"]} for row in test_rows),
            "refit_quality": refit_quality,
            "test": {},
            "test_by_horizon": {},
            "daily_paired_comparison": [],
            "development_evaluation": True,
            "confirmation_status": protocol["confirmation"]["status"],
            "active_model_changed": False,
            "production_activation": False,
            "limitations": protocol["limitations"],
        }
        for name, prediction in predictions.items():
            report["test"][name] = metrics(arrays_y["test"], prediction)
            report["test_by_horizon"][name] = {}
            for horizon in protocol["horizons"]:
                mask = np.asarray([row["horizon"] == horizon for row in test_rows])
                report["test_by_horizon"][name][str(horizon)] = metrics(arrays_y["test"][mask], prediction[mask])
        for day in sorted({row["origin_utc"].astimezone(ZONE).date() for row in test_rows}):
            mask = np.asarray([row["origin_utc"].astimezone(ZONE).date() == day for row in test_rows])
            report["daily_paired_comparison"].append({
                "day": str(day), "rows": int(mask.sum()),
                **{name: float(np.mean(np.abs(prediction[mask] - arrays_y["test"][mask]))) for name, prediction in predictions.items()},
            })
        reference = "calendar__poisson"
        report["block_bootstrap"] = block_bootstrap(test_rows, arrays_y["test"], predictions, reference)
        records = []
        for name, prediction in predictions.items():
            for row, value in zip(test_rows, prediction):
                records.append({
                    **{key: row[key] for key in ("station_id", "origin_utc", "target_start_utc", "target_end_utc", "horizon", "departures")},
                    "model": name, "candidate_id": selected[name], "prediction": float(value), "fit_cutoff_utc": test_fit,
                })
        pq.write_table(pa.Table.from_pylist(records), output / "predictions.parquet", compression="zstd")
        reloaded = joblib.load(output / "models.joblib")
        sample = test_rows[:min(100, len(test_rows))]
        report["save_reload_checked_rows"] = len(sample)
        report["save_reload_max_prediction_difference"] = max(
            float(np.max(np.abs(predict(reloaded["models"][name], sample, reloaded["specs"][name]) -
                                predict(refits[name], sample, selected_specs[name])))) for name in refits
        )
        report["model_artifact_bytes"] = (output / "models.joblib").stat().st_size
        if report["save_reload_max_prediction_difference"] > 1e-12:
            raise ValueError("Save/reload parity failed")
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"selected": selected, "overall": overall_id, "test": report["test"]}, indent=2), flush=True)
    return report
