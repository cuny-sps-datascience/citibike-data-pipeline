"""
Build the MVP weather inputs from the Team 6 weather/subway release.

    python3 teams/team-6/mvp/scripts/fetch_weather.py                 # downloads the release zip (~69 MB)
    python3 teams/team-6/mvp/scripts/fetch_weather.py path/to/zip     # or use a copy you already have

Writes:
    data/station_map.csv            MVP short station id -> GBFS uuid -> weather grid class (+ subway complex if mapped)
    data/weather_6stations.parquet  forecast weather for the 3 grid classes our 6 stations use, forecast horizons 1-3

Nothing is recomputed here: rows are copied from the release tables `station_weather_mapping`,
`weather_classes` and `subway_bridge`, filtered to our stations.
"""
import sys, zipfile, urllib.request
from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mvp_data import MVP_ROOT, load_hourly

RELEASE = ("https://github.com/cuny-sps-datascience/citibike-data-pipeline/releases/download/"
           "team-6-weather-subway-2026-10-23/citibike_weather_subway_handoff_20261023T174914038037Z.zip")
WORK = MVP_ROOT / "raw" / "weather_release"          # git-ignored
WEATHER_COLS = ["weather_request_class", "forecast_origin_utc", "forecast_horizon", "target_hour_utc",
                "forecast_temperature_f", "forecast_precipitation_in", "forecast_rain_in", "forecast_snowfall_in",
                "forecast_wind_speed_mph", "weather_forecast_has_missing", "selected_run_available_utc",
                "is_fallback_weather_run", "weather_lead_hours"]


def main():
    WORK.mkdir(parents=True, exist_ok=True)
    zpath = Path(sys.argv[1]) if len(sys.argv) > 1 else WORK / "handoff.zip"
    if not zpath.exists():
        print("downloading release zip ..."); urllib.request.urlretrieve(RELEASE, zpath)
    tables = WORK / "tables"
    if not tables.exists():
        with zipfile.ZipFile(zpath) as z: z.extractall(WORK)

    # --- station map: our ids are trip-file short names; the release keys stations by GBFS uuid. Bridge by name.
    ours = load_hourly()[["station_id", "station_name"]].drop_duplicates()
    mapping = ds.dataset(tables / "station_weather_mapping", format="parquet").to_table().to_pandas()
    bridge = ds.dataset(tables / "subway_bridge", format="parquet").to_table().to_pandas()
    smap = (ours.merge(mapping[["station_id", "station_name", "weather_request_class", "weather_mapping_distance_km"]]
                       .rename(columns={"station_id": "station_uuid"}), on="station_name", how="left")
                .merge(bridge[["station_id", "station_complex_id", "minimum_subway_bike_distance_m"]]
                       .rename(columns={"station_id": "station_uuid", "station_complex_id": "subway_complex_id"}),
                       on="station_uuid", how="left"))
    if smap.weather_request_class.isna().any() or smap.station_uuid.duplicated().any() or len(smap) != len(ours):
        raise ValueError(f"station mapping is not one-to-one:\n{smap}")
    smap["weather_request_class"] = smap.weather_request_class.astype(int)
    smap.to_csv(MVP_ROOT / "data" / "station_map.csv", index=False)
    print(smap.to_string(index=False))

    # --- weather: only our grid classes, only the horizons the MVP forecasts
    w = ds.dataset(tables / "weather_classes", format="parquet").to_table(
        columns=WEATHER_COLS,
        filter=ds.field("weather_request_class").isin(sorted(smap.weather_request_class.unique().tolist()))
               & (ds.field("forecast_horizon") <= 3)).to_pandas()
    for c in ("forecast_origin_utc", "target_hour_utc", "selected_run_available_utc"):
        w[c] = pd.to_datetime(w[c]).dt.tz_localize("UTC")
    # the archive's own convention: target = forecast origin + horizon
    assert ((w.target_hour_utc - w.forecast_origin_utc) == pd.to_timedelta(w.forecast_horizon, unit="h")).all()
    assert not w.duplicated(["weather_request_class", "forecast_origin_utc", "forecast_horizon"]).any()
    w = w.sort_values(["weather_request_class", "forecast_origin_utc", "forecast_horizon"]).reset_index(drop=True)
    w.to_parquet(MVP_ROOT / "data" / "weather_6stations.parquet", index=False)
    print(f"\nweather rows: {len(w):,}  origins {w.forecast_origin_utc.min()} -> {w.forecast_origin_utc.max()}"
          f"  | rows flagged missing: {int(w.weather_forecast_has_missing.sum()):,}")


if __name__ == "__main__":
    main()
