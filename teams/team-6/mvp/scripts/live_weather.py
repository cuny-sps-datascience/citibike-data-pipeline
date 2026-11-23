"""Capture same-grid ECMWF forecasts in the format used by the next-hour scorer."""

import argparse
import json
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

ENDPOINT = "https://api.open-meteo.com/v1/forecast"
SOURCE = "open_meteo_ecmwf_ifs"
# Request coordinates from the Team 6 archived weather classes.
LOCATIONS = {
    19: (40.702945821438455, -74.01556349990837),
    21: (40.7237520845199, -73.97696827423096),
    24: (40.80197051562125, -73.96547935661117),
}
FIELDS = {
    "temperature_2m": ("forecast_temperature_f", "°F"),
    "precipitation": ("forecast_precipitation_in", "inch"),
    "rain": ("forecast_rain_in", "inch"),
    "snowfall": ("forecast_snowfall_in", "inch"),
    "wind_speed_10m": ("forecast_wind_speed_mph", "mp/h"),
}


def collect_forecasts() -> tuple[pd.DataFrame, list[str]]:
    rows = []
    errors = []
    for class_id, (latitude, longitude) in LOCATIONS.items():
        query = urlencode({
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(FIELDS),
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "wind_speed_unit": "mph",
            "timezone": "UTC",
            "models": "ecmwf_ifs",
            "forecast_hours": 4,
        })
        requested_at = pd.Timestamp.now(tz="UTC")
        try:
            with urlopen(f"{ENDPOINT}?{query}", timeout=8) as response:
                payload = json.load(response)
            received_at = pd.Timestamp.now(tz="UTC")
            units = payload["hourly_units"]
            if payload["utc_offset_seconds"] != 0 or any(
                units[field] != unit for field, (_, unit) in FIELDS.items()
            ):
                raise ValueError("Unexpected forecast time zone or units.")
            hourly = payload["hourly"]
            for i, timestamp in enumerate(hourly["time"]):
                values = {name: hourly[field][i] for field, (name, _) in FIELDS.items()}
                if all(value is not None for value in values.values()):
                    rows.append({
                        "weather_request_class": class_id,
                        "target_hour_utc": pd.Timestamp(timestamp, tz="UTC"),
                        "weather_source": SOURCE,
                        "requested_at_utc": requested_at,
                        "received_at_utc": received_at,
                        **values,
                    })
        except (URLError, OSError, ValueError, KeyError, TypeError, IndexError) as exc:
            errors.append(f"Weather class {class_id}: {exc}")
    return pd.DataFrame(rows), errors


def save_forecasts(forecasts: pd.DataFrame, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    stamp = forecasts.received_at_utc.max().strftime("%Y%m%dT%H%M%S%fZ")
    path = directory / f"open_meteo_{stamp}.parquet"
    with path.open("xb") as output:
        output.write(forecasts.to_parquet(index=False))
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_directory", type=Path)
    args = parser.parse_args()
    forecasts, errors = collect_forecasts()
    for error in errors:
        print(error)
    if forecasts.empty:
        raise SystemExit("No usable forecasts returned.")
    print(save_forecasts(forecasts, args.output_directory))
