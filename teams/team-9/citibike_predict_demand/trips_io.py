"""Read Citi Bike trip-history zips into monthly trips per start station.

Handles both file schemas (before and after 2021) and zips nested inside zips
(the yearly files contain monthly zips). Only the columns we need are read, in
chunks, so a multi-million row month fits in a small cluster's memory.
"""
import io
import zipfile
import numpy as np
import pandas as pd

RENAMES = {
    "starttime": "started_at", "start station id": "start_station_id",
    "start station name": "start_station_name",
    "start station latitude": "start_lat", "start station longitude": "start_lng",
}
KEEP = ["started_at", "start_station_id", "start_station_name", "start_lat", "start_lng"]


WANTED = set(RENAMES) | set(KEEP)


def normalize_station_id(ids):
    """Trip files sometimes drop a trailing zero ("3834.10" appears as "3834.1").
    Restore it so one station is not split into two ids. Works on a pandas Series."""
    return ids.astype(str).str.strip().str.replace(r"^(\d+\.\d)$", r"\g<1>0", regex=True)


def _csv_to_monthly(fileobj, chunksize=250_000):
    """Read only the 5 columns we need, as text, in small chunks (keeps memory low)."""
    parts = []
    for chunk in pd.read_csv(fileobj, chunksize=chunksize, dtype=str,
                             usecols=lambda c: c.strip().lower() in WANTED):
        chunk = chunk.rename(columns=lambda c: RENAMES.get(c.strip().lower(), c.strip().lower()))
        if not set(KEEP).issubset(chunk.columns):
            return pd.DataFrame()
        chunk = chunk[KEEP].dropna(subset=["start_station_id", "started_at"])
        chunk["start_station_id"] = normalize_station_id(chunk["start_station_id"])
        chunk["start_lat"] = pd.to_numeric(chunk["start_lat"], errors="coerce")
        chunk["start_lng"] = pd.to_numeric(chunk["start_lng"], errors="coerce")
        chunk["month"] = pd.to_datetime(chunk["started_at"], errors="coerce").dt.to_period("M").dt.to_timestamp()
        parts.append(chunk.groupby(["start_station_id", "month"]).agg(
            trips=("started_at", "size"), start_station_name=("start_station_name", "first"),
            lat=("start_lat", "mean"), lng=("start_lng", "mean")).reset_index())
        del chunk
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def zip_to_monthly(zf):
    """zf: an open zipfile.ZipFile (or a path). Returns monthly trips per station."""
    if not isinstance(zf, zipfile.ZipFile):
        with zipfile.ZipFile(zf) as z:
            return zip_to_monthly(z)
    parts = []
    for name in zf.namelist():
        if name.startswith("__MACOSX") or name.endswith("/"):
            continue
        if name.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(zf.read(name))) as inner:
                parts.append(zip_to_monthly(inner))
        elif name.lower().endswith(".csv"):
            with zf.open(name) as f:
                parts.append(_csv_to_monthly(f))
    parts = [p for p in parts if len(p)]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def combine_monthly(frames):
    """Re-aggregate (a station-month can be split across files or chunks)."""
    df = pd.concat([f for f in frames if len(f)], ignore_index=True)
    df["w"] = df["trips"]
    df["lat_w"], df["lng_w"] = df["lat"] * df["w"], df["lng"] * df["w"]
    g = df.groupby(["start_station_id", "month"]).agg(
        trips=("trips", "sum"), start_station_name=("start_station_name", "first"),
        lat_w=("lat_w", "sum"), lng_w=("lng_w", "sum"), w=("w", "sum")).reset_index()
    g["lat"], g["lng"] = g["lat_w"] / g["w"], g["lng_w"] / g["w"]
    return g[["start_station_id", "month", "trips", "start_station_name", "lat", "lng"]]


def build_targets(monthly, history_start, full_months=12, min_trips=30):
    """One row per station: opening month, first-year average monthly trips.

    The opening month is usually partial, so the target uses the months AFTER it
    (up to 12). Stations already active in the first month of the history are
    marked is_pre_history: they are used as neighbors but never as test stations.
    """
    m = monthly[monthly["trips"] >= min_trips].sort_values("month")
    rows = []
    for sid, g in m.groupby("start_station_id"):
        first = g["month"].iloc[0]
        window = g[(g["month"] > first) &
                   (g["month"] <= first + pd.DateOffset(months=full_months))]
        if window.empty:
            continue
        rows.append({"start_station_id": sid, "first_month": first,
                     "target": window["trips"].mean(), "n_months": len(window),
                     "is_pre_history": first <= pd.Timestamp(history_start)})
    return pd.DataFrame(rows)


def seasonal_share(monthly):
    """Share of annual trips by calendar month (sums to 1).

    Uses the average of each calendar month across years. Calendar months with no data
    are filled by circular interpolation (an approximation; a full year of history avoids it).
    """
    totals = monthly.groupby("month")["trips"].sum()
    by_cal = totals.groupby(totals.index.month).mean().reindex(range(1, 13))
    if by_cal.isna().any():
        have = by_cal.dropna()
        print("seasonal_share: no data for months", list(by_cal.index[by_cal.isna()]), "- interpolated")
        by_cal = pd.Series(np.interp(range(1, 13), have.index, have.values, period=12),
                           index=range(1, 13))
    return (by_cal / by_cal.sum()).rename_axis("month_of_year")
