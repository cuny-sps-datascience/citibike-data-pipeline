"""
Shared data + feature definitions for the Team 6 MVP. Used by train_mvp.py, train_compare.py AND the
notebook, so every script and notebook evaluates on exactly the same rows.
"""
from pathlib import Path
import pandas as pd

MVP_ROOT = Path(__file__).resolve().parent.parent          # teams/team-6/mvp
HOURLY_PATH = MVP_ROOT / "data" / "trips_6stations_hourly.parquet"
ZONE = "America/New_York"

# Splits by TARGET local date (inclusive). Validation = model selection / feature experiments.
# Test is only used to report the final model of each stage.
SPLITS = {"train": ("2024-01-01", "2025-12-31"),
          "validation": ("2026-01-01", "2026-02-28"),
          "test": ("2026-03-01", "2026-08-31")}

# Common window for comparing models 1-3 on identical rows: the weather archive starts 2024-08-10 (UTC)
# and the subway archive ends 2026-08-10 (New York date). Same validation period as above.
SPLITS_COMMON = {"train": ("2024-08-11", "2025-12-31"),
                 "validation": ("2026-01-01", "2026-02-28"),
                 "test": ("2026-03-01", "2026-08-10")}

HORIZONS = (1, 2, 3)          # target hour = origin + (h - 1)  — same convention as the 689-station panel
FEATURES = ["station_id", "horizon", "t_hour", "t_dow", "t_month", "t_doy", "t_weekend", "t_holiday"]
WEATHER_FEATURES = ["forecast_temperature_f", "forecast_precipitation_in", "forecast_rain_in",
                    "forecast_snowfall_in", "forecast_wind_speed_mph"]
TARGET = "departures"
ISSUE_CUTOFF = pd.Timedelta(minutes=5)      # a forecast made at `origin` may only use information from before origin - 5 min

# US federal holidays (observed), 2024–2026
HOLIDAYS = set(pd.to_datetime("""
2024-01-01 2024-01-15 2024-02-19 2024-05-27 2024-06-19 2024-07-04 2024-09-02 2024-10-14 2024-11-11 2024-11-28 2024-12-25
2025-01-01 2025-01-20 2025-02-17 2025-05-26 2025-06-19 2025-07-04 2025-09-01 2025-10-13 2025-11-11 2025-11-27 2025-12-25
2026-01-01 2026-01-19 2026-02-16 2026-05-25 2026-06-19 2026-07-03 2026-09-07 2026-10-12 2026-11-11 2026-11-26 2026-12-25
""".split()).date)


def load_hourly(path=HOURLY_PATH) -> pd.DataFrame:
    h = pd.read_parquet(path)
    h["hour_utc"] = pd.to_datetime(h.hour_utc, utc=True)
    return h.sort_values(["station_id", "hour_utc"]).reset_index(drop=True)


def calendar_features(frame: pd.DataFrame, target_col: str = "target_hour_utc") -> pd.DataFrame:
    t = frame[target_col].dt.tz_convert(ZONE)
    frame["t_hour"] = t.dt.hour
    frame["t_dow"] = t.dt.dayofweek
    frame["t_month"] = t.dt.month
    frame["t_doy"] = t.dt.dayofyear
    frame["t_weekend"] = (frame.t_dow >= 5).astype(int)
    frame["t_holiday"] = t.dt.date.isin(HOLIDAYS).astype(int)
    frame["t_date"] = t.dt.strftime("%Y-%m-%d")
    return frame


def build_frame(h: pd.DataFrame | None = None, splits: dict = SPLITS) -> pd.DataFrame:
    """station x origin x horizon rows with calendar features of the TARGET hour and a split label.
    Rows whose label is unknown (DST-ambiguous hours, stored as NaN) are dropped, never treated as zero."""
    if h is None:
        h = load_hourly()
    lab = h[["station_id", "hour_utc", TARGET]].rename(columns={"hour_utc": "target_hour_utc"})
    rows = []
    for hz in HORIZONS:
        f = h[["station_id", "station_name", "hour_utc"]].rename(columns={"hour_utc": "origin_utc"})
        f = f.assign(horizon=hz, target_hour_utc=f.origin_utc + pd.Timedelta(hours=hz - 1))
        rows.append(f)
    df = pd.concat(rows, ignore_index=True).merge(lab, on=["station_id", "target_hour_utc"], how="inner")
    df = df[df[TARGET].notna()]
    df = calendar_features(df)
    df["station_id"] = df.station_id.astype(str).astype("category")
    df["split"] = None
    for name, (lo, hi) in splits.items():
        df.loc[(df.t_date >= lo) & (df.t_date <= hi), "split"] = name
    return df[df.split.notna()].reset_index(drop=True)


def add_weather(df: pd.DataFrame, data_dir: Path = MVP_ROOT / "data") -> pd.DataFrame:
    """Attach forecast weather for each row's TARGET hour and flag rows where it is usable.

    The two sources number horizons differently:
        MVP / 689-station panel:  target = origin + (h - 1)
        weather archive:          target = forecast_origin + h
    so joining on the horizon number alone would attach the NEXT hour's weather. We use the weather forecast
    issued one hour before our origin (weather origin = origin - 1 h) with the same h, which lands on the same
    target hour, and we assert that the two target timestamps are equal on every joined row. Using the previous
    weather origin also guarantees the selected model run was available before the 5-minute issue cutoff.
    """
    smap = pd.read_csv(data_dir / "station_map.csv", dtype={"station_id": str})
    w = pd.read_parquet(data_dir / "weather_6stations.parquet")
    w = w.rename(columns={"forecast_origin_utc": "weather_origin_utc", "forecast_horizon": "horizon",
                          "target_hour_utc": "weather_target_hour_utc"})
    out = df.merge(smap[["station_id", "weather_request_class"]].astype({"station_id": str}),
                   left_on=df.station_id.astype(str), right_on="station_id", how="left",
                   suffixes=("", "_map"), validate="many_to_one")
    out = out.drop(columns=[c for c in ("key_0", "station_id_map") if c in out.columns])
    out["weather_origin_utc"] = out.origin_utc - pd.Timedelta(hours=1)
    out = out.merge(w, on=["weather_request_class", "weather_origin_utc", "horizon"], how="left",
                    validate="many_to_one")
    joined = out.weather_target_hour_utc.notna()
    if not (out.loc[joined, "weather_target_hour_utc"] == out.loc[joined, "target_hour_utc"]).all():
        raise ValueError("weather target hour does not match the departure target hour")
    out["weather_eligible"] = (joined
                               & ~out.weather_forecast_has_missing.fillna(True).astype(bool)
                               & out[WEATHER_FEATURES].notna().all(axis=1)
                               & (out.selected_run_available_utc < out.origin_utc - ISSUE_CUTOFF))
    out["station_id"] = out.station_id.astype(str).astype("category")
    return out


def build_common_frame(h: pd.DataFrame | None = None) -> pd.DataFrame:
    """Rows every model in the 1-vs-2 comparison is trained and scored on: common-window splits,
    known label, usable weather. Models that don't use weather still get exactly these rows."""
    df = add_weather(build_frame(h, splits=SPLITS_COMMON))
    return df[df.weather_eligible].reset_index(drop=True)
