"""Regularized count regression with station-specific daily cycles."""

from io import BytesIO

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer, make_column_selector
from sklearn.linear_model import PoissonRegressor
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from threadpoolctl import threadpool_limits

from mvp_data import TARGET, WEATHER_FEATURES

ALPHAS = (0.01, 0.1, 1.0)


def glm_features(frame, mode):
    features = frame[["station_id"]].astype(str)
    if mode != "weather":
        features["weekday"] = frame.t_dow.astype(str)
        features["holiday"] = frame.t_holiday.astype(float)
        features["annual_sin"] = np.sin(2 * np.pi * frame.t_doy / 365.25)
        features["annual_cos"] = np.cos(2 * np.pi * frame.t_doy / 365.25)
        # Interaction columns give each station its own daily and half-day curve.
        # The weekend terms let each station's weekday and weekend peaks differ.
        categories = frame.station_id.cat.categories
        stations = pd.get_dummies(frame.station_id).reindex(columns=categories, fill_value=False)
        for frequency in (1, 2):
            angle = 2 * np.pi * frequency * frame.t_hour / 24
            for wave in (np.sin, np.cos):
                base = stations.mul(wave(angle), axis=0)
                suffix = f"_{wave.__name__}_{frequency}"
                features = pd.concat([
                    features,
                    base.add_suffix(suffix),
                    base.mul(frame.t_weekend, axis=0).add_suffix(suffix + "_weekend"),
                ], axis=1)
    if mode != "calendar":
        features[WEATHER_FEATURES] = frame[WEATHER_FEATURES]
    return features


def fit_glm(frame, mode, alpha):
    preprocess = ColumnTransformer([
        ("numeric", StandardScaler(), make_column_selector(dtype_include=np.number)),
        ("categorical", OneHotEncoder(handle_unknown="ignore"), ["station_id"] if mode == "weather"
         else ["station_id", "weekday"]),
    ])
    model = make_pipeline(preprocess, PoissonRegressor(alpha=alpha, max_iter=500, tol=1e-6))
    with threadpool_limits(limits=4):
        model.fit(glm_features(frame, mode), frame[TARGET])
    return model


def predict_glm(model, frame, mode):
    with threadpool_limits(limits=4):
        return model.predict(glm_features(frame, mode))


def save_glm(path, model):
    buffer = BytesIO()
    joblib.dump(model, buffer)
    path.write_bytes(buffer.getvalue())


def predict_saved_glm(path, frame, mode):
    return predict_glm(joblib.load(BytesIO(path.read_bytes())), frame, mode)
