"""Train and score the six-station next-hour departure model."""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.special import gammaln

from mvp_data import (
    FEATURES,
    MVP_ROOT,
    SPLITS_COMMON,
    TARGET,
    WEATHER_FEATURES,
    add_weather,
    build_frame,
    calendar_features,
    load_hourly,
)

NEXT_HOUR = 2  # In the Colab convention, horizon 1 denotes the origin hour.
CURVE_KEY = ["station_id", "t_dow", "t_hour"]
MODEL_FEATURES = {
    "calendar": [f for f in FEATURES if f != "horizon"],
    "weather": ["station_id", *WEATHER_FEATURES],
    "both": [f for f in FEATURES if f != "horizon"] + WEATHER_FEATURES,
}
RATE_FLOOR = 0.1
WEIGHT_FLOOR = 2.0
MAX_WEATHER_AGE = pd.Timedelta(hours=3)


def demand_curves(train: pd.DataFrame) -> pd.DataFrame:
    """Summarize historical departures by station, New York weekday and hour."""
    curve = (train.groupby(CURVE_KEY, observed=True)[TARGET]
             .agg(typical_departures="mean", observed_hours="size").reset_index())
    station_mean = train.groupby("station_id", observed=True)[TARGET].mean()
    curve["station_average"] = curve.station_id.map(station_mean).astype(float)
    curve["demand_index"] = 100 * curve.typical_departures / curve.station_average.replace(0, np.nan)
    day_total = curve.groupby(["station_id", "t_dow"], observed=True).typical_departures.transform("sum")
    curve["share_of_typical_day"] = curve.typical_departures.div(day_total.replace(0, np.nan))
    weights = 1 / curve.typical_departures.clip(lower=WEIGHT_FLOOR)
    curve["comparison_weight"] = weights / weights.mean()
    curve["station_id"] = curve.station_id.astype(str)
    return curve


def attach_curve(frame: pd.DataFrame, curve: pd.DataFrame) -> pd.DataFrame:
    out = frame.assign(station_id=frame.station_id.astype(str)).merge(
        curve, on=CURVE_KEY, how="left", validate="many_to_one"
    )
    if out.typical_departures.isna().any():
        raise ValueError("Station, weekday and hour must exist in the fitted demand curve.")
    return out


def poisson_log_loss(counts, rates) -> np.ndarray:
    """Negative log probability of the observed count under the model's Poisson mean."""
    y = np.asarray(counts, dtype=float)
    rate = np.maximum(np.asarray(rates, dtype=float), RATE_FLOOR)
    return rate - y * np.log(rate) + gammaln(y + 1)


def fit_and_save(data_dir: Path, model_dir: Path) -> dict:
    trip_hours = load_hourly(data_dir / "trips_6stations_hourly.parquet")
    stations = sorted(trip_hours.station_id.astype(str).unique())
    frame = build_frame(trip_hours, splits=SPLITS_COMMON)
    next_hour = frame.loc[frame.horizon.eq(NEXT_HOUR)].copy()
    next_hour = add_weather(next_hour, data_dir)
    if next_hour.duplicated(["station_id", "target_hour_utc"]).any():
        raise ValueError("Expected one next-hour label per station and target hour.")
    curve = demand_curves(next_hour.loc[next_hour.split.eq("train")])
    eligible = attach_curve(next_hour.loc[next_hour.weather_eligible], curve)
    eligible["station_id"] = pd.Categorical(eligible.station_id, categories=stations)
    train, validation = (eligible.loc[eligible.split.eq(split)] for split in ("train", "validation"))
    if train.empty or validation.empty:
        raise ValueError("Training and validation both require weather-eligible observations.")

    fitted = {}
    for name, features in MODEL_FEATURES.items():
        model = lgb.LGBMRegressor(
            objective="poisson", n_estimators=3000, learning_rate=0.03, num_leaves=63,
            min_child_samples=50, subsample=0.8, subsample_freq=1, colsample_bytree=0.9,
            random_state=622, n_jobs=4, verbosity=-1,
        )
        model.fit(
            train[features], train[TARGET],
            eval_set=[(validation[features], validation[TARGET])],
            eval_metric="poisson", categorical_feature=["station_id"],
            callbacks=[lgb.early_stopping(150, verbose=False)],
        )
        fitted[name] = model.booster_

    scored = eligible.loc[eligible.split.isin(["validation", "test"])].copy()
    if not scored.split.eq("test").any():
        raise ValueError("Test requires weather-eligible observations.")
    scored["pred_baseline"] = scored.typical_departures.clip(lower=RATE_FLOOR)
    for name, model in fitted.items():
        scored[f"pred_{name}"] = np.maximum(model.predict(scored[MODEL_FEATURES[name]]), RATE_FLOOR)
    metrics = [
        metric_row(group, model_name, split)
        for split, group in scored.groupby("split")
        for model_name in ("baseline", *MODEL_FEATURES)
    ]
    by_station_hour = [
        {"station_id": station, "local_hour": int(hour), **metric_row(group, model_name, "test")}
        for (station, hour), group in scored.loc[scored.split.eq("test")].groupby(
            ["station_id", "t_hour"], observed=True
        )
        for model_name in ("baseline", *MODEL_FEATURES)
    ]
    station_map = pd.read_csv(data_dir / "station_map.csv", dtype={"station_id": str})
    station_map = station_map[["station_id", "station_name", "weather_request_class"]]
    if set(station_map.station_id) != set(stations) or station_map.station_id.duplicated().any():
        raise ValueError("Expected one weather mapping for each trained station.")
    manifest = {
        "trained_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "target": "recorded_departures",
        "station_ids": stations,
        "next_hour_colab_horizon": NEXT_HOUR,
        "splits": SPLITS_COMMON,
        "eligible_rows": {k: int(v) for k, v in eligible.split.value_counts().items()},
        "metrics": metrics,
    }

    model_dir.mkdir(parents=True, exist_ok=False)
    # Unity Catalog volumes require sequential writes. save_model() opens the
    # file twice and uses append mode for LightGBM's pandas category metadata.
    for name, model in fitted.items():
        (model_dir / f"{name}.txt").write_text(model.model_to_string(), encoding="utf-8")
    for filename, table in {
        "demand_curves.parquet": curve,
        "backtest_predictions.parquet": scored,
        "metrics_by_station_hour.parquet": pd.DataFrame(by_station_hour),
        "stations.parquet": station_map,
    }.items():
        (model_dir / filename).write_bytes(table.to_parquet(index=False))
    (model_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def metric_row(group: pd.DataFrame, model: str, split: str) -> dict:
    loss = poisson_log_loss(group[TARGET], group[f"pred_{model}"])
    return {
        "split": split,
        "model": model,
        "rows": int(len(group)),
        "poisson_log_loss": float(loss.mean()),
        "weighted_poisson_log_loss": float(np.average(loss, weights=group.comparison_weight)),
        "mae": float(np.abs(group[TARGET] - group[f"pred_{model}"]).mean()),
    }


def issue_time(as_of) -> pd.Timestamp:
    time = pd.Timestamp(as_of)
    if pd.isna(time) or time.tzinfo is None:
        raise ValueError("Forecast issue time must include a time zone.")
    return time.tz_convert("UTC")


def latest_forecasts(forecasts: pd.DataFrame, as_of: pd.Timestamp, target: pd.Timestamp) -> pd.DataFrame:
    cols = ["weather_request_class", "received_at_utc", "weather_source", *WEATHER_FEATURES]
    forecasts = forecasts.assign(
        received_at_utc=pd.to_datetime(forecasts.received_at_utc, utc=True),
        target_hour_utc=pd.to_datetime(forecasts.target_hour_utc, utc=True),
    )
    eligible = forecasts.loc[
        forecasts.target_hour_utc.eq(target)
        & forecasts.received_at_utc.between(as_of - MAX_WEATHER_AGE, as_of)
        & np.isfinite(forecasts[WEATHER_FEATURES]).all(axis=1),
        cols,
    ]
    return eligible.sort_values("received_at_utc").drop_duplicates("weather_request_class", keep="last")


def score_next_hour(model_dir: Path, as_of, mode="both", station_id="", forecasts=None) -> pd.DataFrame:
    if mode not in MODEL_FEATURES:
        raise ValueError(f"Choose one of {', '.join(MODEL_FEATURES)}.")
    manifest = json.loads((model_dir / "manifest.json").read_text())
    stations = pd.read_parquet(model_dir / "stations.parquet")
    if station_id:
        stations = stations.loc[stations.station_id.eq(station_id)]
    if stations.empty:
        raise ValueError("Choose a station ID from the six-station training cohort.")
    as_of = issue_time(as_of)
    target = as_of.floor("h") + pd.Timedelta(hours=1)
    frame = calendar_features(stations.assign(target_hour_utc=target, issued_at_utc=as_of))
    frame = attach_curve(frame, pd.read_parquet(model_dir / "demand_curves.parquet"))
    frame["station_id"] = pd.Categorical(frame.station_id, categories=manifest["station_ids"])
    frame["model_used"] = "calendar"
    if mode != "calendar" and forecasts is not None and not forecasts.empty:
        frame = frame.merge(latest_forecasts(forecasts, as_of, target), on="weather_request_class",
                            how="left", validate="many_to_one")
        frame.loc[frame.received_at_utc.notna(), "model_used"] = mode
    else:
        frame["received_at_utc"] = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
        frame["weather_source"] = pd.Series(pd.NA, index=frame.index, dtype="string")
        frame[WEATHER_FEATURES] = np.nan
    for name, rows in frame.groupby("model_used").groups.items():
        model = lgb.Booster(model_str=(model_dir / f"{name}.txt").read_text(encoding="utf-8"))
        frame.loc[rows, "predicted_departures"] = np.maximum(
            model.predict(frame.loc[rows, MODEL_FEATURES[name]], num_threads=4), RATE_FLOOR
        )
    frame["relative_to_typical"] = frame.predicted_departures.div(frame.typical_departures.replace(0, np.nan))
    frame["target_new_york"] = frame.target_hour_utc.dt.tz_convert("America/New_York").astype(str)
    frame["requested_model"] = mode
    return frame


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("model_dir", type=Path)
    parser.add_argument("--data-dir", type=Path, default=MVP_ROOT / "data")
    args = parser.parse_args()
    print(json.dumps(fit_and_save(args.data_dir, args.model_dir), indent=2))
