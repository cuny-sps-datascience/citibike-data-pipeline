"""
Shared data + feature definitions for the Team 6 MVP. Used by train_mvp.py AND play_model1.ipynb,
so the script and the notebook always evaluate on exactly the same rows.
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

HORIZONS = (1, 2, 3)          # target hour = origin + (h - 1)  — same convention as the 689-station panel
FEATURES = ["station_id", "horizon", "t_hour", "t_dow", "t_month", "t_doy", "t_weekend", "t_holiday"]
TARGET = "departures"

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


def build_frame(h: pd.DataFrame | None = None) -> pd.DataFrame:
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
    for name, (lo, hi) in SPLITS.items():
        df.loc[(df.t_date >= lo) & (df.t_date <= hi), "split"] = name
    return df[df.split.notna()].reset_index(drop=True)
