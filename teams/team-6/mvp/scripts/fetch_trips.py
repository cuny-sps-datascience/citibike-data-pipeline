"""
Download Citi Bike monthly trip files (Jan 2024 -> Aug 2026), keep only trips that
start or end at the 6 MVP stations, and write:
    trips_6stations_raw.parquet     every kept trip (ride_id, started_at, ended_at, start/end station, member_casual)
    trips_6stations_hourly.parquet  one row per station x UTC hour: departures, returns  (zeros filled)

Run in Terminal:   cd ~/Downloads/team6_panel && python3 fetch_trips.py
Needs: pip3 install pandas pyarrow      Downloads ~7-8 GB total but keeps only one zip on disk at a time.
Re-runnable: months already done are skipped.
"""
import io, os, sys, zipfile, urllib.request, time
from pathlib import Path
import pandas as pd

STATIONS = {"5137.11", "5105.01", "5001.08", "6140.05", "5905.12", "7260.09"}
START, END = "2024-01", "2026-08"          # inclusive
OUT = Path("trips_raw_by_month"); OUT.mkdir(exist_ok=True)
BASE = "https://s3.amazonaws.com/tripdata/"
COLS = ["ride_id", "rideable_type", "started_at", "ended_at",
        "start_station_name", "start_station_id", "end_station_name", "end_station_id", "member_casual"]

def candidates(ym):
    return [f"{ym}-citibike-tripdata.zip", f"{ym}-citibike-tripdata.csv.zip"]

def download(name, dest):
    url = BASE + name
    try:
        with urllib.request.urlopen(url, timeout=60) as r, open(dest, "wb") as f:
            total = int(r.headers.get("Content-Length", 0)); done = 0
            while True:
                chunk = r.read(1 << 22)
                if not chunk: break
                f.write(chunk); done += len(chunk)
                if total: print(f"\r  {name}: {done/1e6:,.0f} / {total/1e6:,.0f} MB", end="", flush=True)
        print()
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404: return False
        raise

def filter_zip(path):
    """Yield filtered DataFrames from every CSV inside the zip (zips may nest zips/CSVs)."""
    with zipfile.ZipFile(path) as z:
        for info in z.infolist():
            n = info.filename
            if "__MACOSX" in n or info.is_dir(): continue
            if n.lower().endswith(".zip"):
                inner = io.BytesIO(z.read(info))
                yield from filter_zip(inner)
            elif n.lower().endswith(".csv"):
                with z.open(info) as fh:
                    for chunk in pd.read_csv(fh, usecols=lambda c: c in COLS, dtype=str, chunksize=500_000):
                        m = chunk.start_station_id.isin(STATIONS) | chunk.end_station_id.isin(STATIONS)
                        if m.any(): yield chunk[m]

months = pd.period_range(START, END, freq="M").strftime("%Y%m")
for ym in months:
    out = OUT / f"{ym}.parquet"
    if out.exists(): print(f"{ym}: done already"); continue
    got = None
    for name in candidates(ym):
        tmp = Path(name)
        if download(name, tmp): got = tmp; break
    if got is None: print(f"{ym}: not published yet, stopping here"); break
    t0 = time.time()
    parts = list(filter_zip(got))
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=COLS)
    df.to_parquet(out, index=False)
    got.unlink()
    print(f"{ym}: kept {len(df):,} trips in {time.time()-t0:.0f}s")

# ---- combine + hourly aggregation ---------------------------------------
raw = pd.concat([pd.read_parquet(p) for p in sorted(OUT.glob("*.parquet"))], ignore_index=True)
raw.to_parquet("trips_6stations_raw.parquet", index=False)
print(f"\ntotal kept trips: {len(raw):,}")

def to_utc(s):
    t = pd.to_datetime(s, errors="coerce", format="mixed")
    # Trip files are New York local time. Drop the ~2 ambiguous/nonexistent DST hours a year rather than guess.
    return t.dt.tz_localize("America/New_York", ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")

dep = raw[raw.start_station_id.isin(STATIONS)].assign(hour_utc=lambda d: to_utc(d.started_at).dt.floor("h"))
ret = raw[raw.end_station_id.isin(STATIONS)].assign(hour_utc=lambda d: to_utc(d.ended_at).dt.floor("h"))
d = dep.dropna(subset=["hour_utc"]).groupby(["start_station_id", "hour_utc"]).size().rename("departures")
r = ret.dropna(subset=["hour_utc"]).groupby(["end_station_id", "hour_utc"]).size().rename("returns")
d.index.names = r.index.names = ["station_id", "hour_utc"]

lo = min(d.index.get_level_values(1).min(), r.index.get_level_values(1).min())
hi = max(d.index.get_level_values(1).max(), r.index.get_level_values(1).max())
grid = pd.MultiIndex.from_product([sorted(STATIONS), pd.date_range(lo, hi, freq="h", tz="UTC")], names=["station_id", "hour_utc"])
hourly = pd.DataFrame(index=grid).join(d).join(r).fillna(0).astype(int).reset_index()
names = raw.drop_duplicates("start_station_id").set_index("start_station_id").start_station_name
hourly["station_name"] = hourly.station_id.map(names)
hourly.to_parquet("trips_6stations_hourly.parquet", index=False)
print(f"hourly rows: {len(hourly):,}  ({lo} -> {hi})")
print(hourly.groupby("station_name").departures.agg(["sum", "mean"]).round(2))
