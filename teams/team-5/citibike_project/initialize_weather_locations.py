"""Map Citi Bike stations to Open-Meteo HRRR forecast grid cells.

This local version of the Databricks initializer reads the newest archived
station-information snapshot and creates the two reference files used to join
station records to weather forecasts.
"""

from __future__ import annotations

import argparse
import gzip
import json
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


API_URL = "https://api.open-meteo.com/v1/forecast"
MODEL = "gfs_hrrr"
GRID_OPTIONS = {
    "cell_selection": "land",
    "elevation": "nan",
}
USER_AGENT = "CUNY-SPS-DataScience-Weather-Mapper/1.0"
PROJECT_DIRECTORY = Path(__file__).parent
DEFAULT_STATION_ROOT = PROJECT_DIRECTORY / "data" / "raw" / "station_information"
DEFAULT_REFERENCE_DIRECTORY = (
    PROJECT_DIRECTORY / "data" / "raw" / "weather_forecast" / "reference"
)


def read_latest_station_information(root: Path) -> tuple[Path, pd.DataFrame]:
    """Read station IDs and coordinates from the newest local archive."""
    paths = sorted(root.rglob("*.json.gz"))
    if not paths:
        raise FileNotFoundError(
            f"No station-information snapshots found under {root}. "
            "Run download_station_info.py first."
        )

    path = paths[-1]
    with gzip.open(path, "rt", encoding="utf-8") as file:
        archive = json.load(file)

    stations = pd.DataFrame(archive["payload"]["data"]["stations"])
    required = {"station_id", "lat", "lon"}
    missing = required.difference(stations.columns)
    if missing:
        raise ValueError(f"Station information is missing columns: {sorted(missing)}")

    locations = (
        stations[["station_id", "lat", "lon"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["station_id", "lat", "lon"])
        .reset_index(drop=True)
    )
    return path, locations


def request_grid_batch(location_batch: pd.DataFrame) -> list[dict[str, Any]]:
    """Return Open-Meteo response metadata for one coordinate batch."""
    request_parameters = {
        "latitude": ",".join(location_batch["lat"].astype(str)),
        "longitude": ",".join(location_batch["lon"].astype(str)),
        "models": MODEL,
        **GRID_OPTIONS,
        "elevation": ",".join(["nan"] * len(location_batch)),
    }
    url = f"{API_URL}?{urlencode(request_parameters)}"
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=60) as response:
        forecasts = json.load(response)

    if isinstance(forecasts, dict):
        forecasts = [forecasts]
    if len(forecasts) != len(location_batch):
        raise ValueError(
            "Open-Meteo returned "
            f"{len(forecasts)} locations for {len(location_batch)} requests"
        )
    return forecasts


def map_weather_cells(
    station_locations: pd.DataFrame,
    batch_size: int = 50,
    pause_seconds: float = 6.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Assign each distinct station coordinate to an HRRR grid cell."""
    request_locations = (
        station_locations[["lat", "lon"]]
        .drop_duplicates()
        .sort_values(["lat", "lon"])
        .reset_index(drop=True)
    )
    grid_assignments: list[dict[str, Any]] = []

    for batch_start in range(0, len(request_locations), batch_size):
        location_batch = request_locations.iloc[batch_start : batch_start + batch_size]
        forecasts = request_grid_batch(location_batch)

        for location, forecast in zip(
            location_batch.itertuples(index=False), forecasts
        ):
            grid_assignments.append(
                {
                    "lat": location.lat,
                    "lon": location.lon,
                    "location_id": (
                        f"hrrr_{forecast['latitude']:.6f}_"
                        f"{forecast['longitude']:.6f}"
                    ),
                }
            )

        print(
            f"Mapped {len(grid_assignments):,}/{len(request_locations):,}",
            flush=True,
        )
        if batch_start + batch_size < len(request_locations):
            time.sleep(pause_seconds)

    assignments = pd.DataFrame(grid_assignments)
    station_weather_lookup = (
        station_locations.merge(
            assignments,
            on=["lat", "lon"],
            validate="many_to_one",
        )
        .sort_values(["station_id", "lat", "lon"])
        .reset_index(drop=True)
    )
    weather_locations = (
        assignments.drop_duplicates("location_id")
        .rename(columns={"lat": "latitude", "lon": "longitude"})
        [["location_id", "latitude", "longitude"]]
        .sort_values("location_id")
        .reset_index(drop=True)
    )
    return station_weather_lookup, weather_locations


def write_reference_files(
    reference_directory: Path,
    station_weather_lookup: pd.DataFrame,
    weather_locations: pd.DataFrame,
    source_snapshot: Path,
    overwrite: bool = False,
) -> tuple[Path, Path]:
    """Write the reproducible weather configuration and station lookup."""
    json_path = reference_directory / "hrrr_locations.json"
    csv_path = reference_directory / "station_weather_lookup.csv"
    existing = [path for path in (json_path, csv_path) if path.exists()]
    if existing and not overwrite:
        names = ", ".join(str(path) for path in existing)
        raise FileExistsError(
            f"Reference files already exist: {names}. Use --overwrite to replace them."
        )

    reference_directory.mkdir(parents=True, exist_ok=True)
    weather_configuration = {
        "model": MODEL,
        "grid_options": GRID_OPTIONS,
        "source_station_snapshot": str(source_snapshot),
        "locations": weather_locations.to_dict(orient="records"),
    }
    json_path.write_text(
        json.dumps(weather_configuration, indent=2),
        encoding="utf-8",
    )
    station_weather_lookup.to_csv(csv_path, index=False)
    return json_path, csv_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--station-root",
        type=Path,
        default=DEFAULT_STATION_ROOT,
        help=f"Station-information archive root (default: {DEFAULT_STATION_ROOT})",
    )
    parser.add_argument(
        "--reference-directory",
        type=Path,
        default=DEFAULT_REFERENCE_DIRECTORY,
        help=f"Reference output directory (default: {DEFAULT_REFERENCE_DIRECTORY})",
    )
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--pause-seconds", type=float, default=6.0)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    source_path, station_locations = read_latest_station_information(
        arguments.station_root
    )
    station_lookup, weather_locations = map_weather_cells(
        station_locations,
        batch_size=arguments.batch_size,
        pause_seconds=arguments.pause_seconds,
    )
    json_path, csv_path = write_reference_files(
        arguments.reference_directory,
        station_lookup,
        weather_locations,
        source_path,
        overwrite=arguments.overwrite,
    )
    print(f"Saved {len(weather_locations):,} weather locations to {json_path}")
    print(f"Saved {len(station_lookup):,} station mappings to {csv_path}")
