"""Refresh all live source data used by the Citi Bike analysis.

The script downloads current station information and station status from the
official Citi Bike GBFS feed. It also verifies the station-to-HRRR lookup and
creates or rebuilds it when station coordinates have changed. The complete
local ``data`` directory hierarchy is generated automatically and is not
intended to be committed to source control.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd

from download_station_info import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_INFO_ROOT,
    poll_station_info,
)
from download_station_status import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_STATUS_ROOT,
    poll_station_status,
)
from initialize_weather_locations import (
    DEFAULT_REFERENCE_DIRECTORY,
    map_weather_cells,
    read_latest_station_information,
    write_reference_files,
)


def create_data_directories(*directories: Path) -> None:
    """Create generated-data directories for a new project checkout."""
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


def coordinate_keys(frame: pd.DataFrame) -> set[tuple[str, float, float]]:
    """Return stable station/coordinate keys for mapping comparisons."""
    return {
        (str(row.station_id), round(float(row.lat), 7), round(float(row.lon), 7))
        for row in frame[["station_id", "lat", "lon"]].itertuples(index=False)
    }


def weather_mapping_is_current(
    station_locations: pd.DataFrame,
    reference_directory: Path,
) -> bool:
    """Check that both reference files exist and cover current coordinates."""
    lookup_path = reference_directory / "station_weather_lookup.csv"
    configuration_path = reference_directory / "hrrr_locations.json"
    if not lookup_path.exists() or not configuration_path.exists():
        return False

    lookup = pd.read_csv(
        lookup_path,
        dtype={"station_id": "string"},
    )
    required = {"station_id", "lat", "lon", "location_id"}
    if not required.issubset(lookup.columns):
        return False

    return coordinate_keys(station_locations) == coordinate_keys(lookup)


def refresh_weather_mapping(
    station_locations: pd.DataFrame,
    station_snapshot: Path,
    reference_directory: Path,
    force: bool,
    batch_size: int,
    pause_seconds: float,
) -> tuple[Path, Path]:
    """Verify the mapping and rebuild it only when necessary."""
    configuration_path = reference_directory / "hrrr_locations.json"
    lookup_path = reference_directory / "station_weather_lookup.csv"

    if not force and weather_mapping_is_current(
        station_locations,
        reference_directory,
    ):
        print("Weather mapping already covers all current station coordinates.")
        return configuration_path, lookup_path

    reason = "forced refresh" if force else "new or changed station coordinates"
    print(f"Rebuilding weather mapping: {reason}.")
    station_lookup, weather_locations = map_weather_cells(
        station_locations,
        batch_size=batch_size,
        pause_seconds=pause_seconds,
    )
    return write_reference_files(
        reference_directory,
        station_lookup,
        weather_locations,
        station_snapshot,
        overwrite=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--git-sha",
        default=os.environ.get("GIT_SHA", "manual_run"),
        help="Source revision to store in GBFS archives",
    )
    parser.add_argument(
        "--info-root",
        type=Path,
        default=DEFAULT_INFO_ROOT,
        help=f"Station-information archive root (default: {DEFAULT_INFO_ROOT})",
    )
    parser.add_argument(
        "--status-root",
        type=Path,
        default=DEFAULT_STATUS_ROOT,
        help=f"Station-status archive root (default: {DEFAULT_STATUS_ROOT})",
    )
    parser.add_argument(
        "--reference-directory",
        type=Path,
        default=DEFAULT_REFERENCE_DIRECTORY,
        help=f"Weather reference directory (default: {DEFAULT_REFERENCE_DIRECTORY})",
    )
    parser.add_argument(
        "--force-weather-map",
        action="store_true",
        help="Rebuild the HRRR mapping even when current station coordinates match",
    )
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--pause-seconds", type=float, default=6.0)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()

    create_data_directories(
        arguments.info_root,
        arguments.status_root,
        arguments.reference_directory,
    )
    print("Generated-data directories are ready.")

    print("Downloading live Citi Bike station information...")
    info_path = poll_station_info(arguments.info_root, arguments.git_sha)
    print(f"Saved: {info_path}")

    print("Downloading live Citi Bike station status...")
    status_path = poll_station_status(arguments.status_root, arguments.git_sha)
    print(f"Saved: {status_path}")

    station_snapshot, station_locations = read_latest_station_information(
        arguments.info_root
    )
    configuration_path, lookup_path = refresh_weather_mapping(
        station_locations,
        station_snapshot,
        arguments.reference_directory,
        force=arguments.force_weather_map,
        batch_size=arguments.batch_size,
        pause_seconds=arguments.pause_seconds,
    )

    print("Live-data refresh complete.")
    print(f"Station coordinates: {len(station_locations):,}")
    print(f"Weather configuration: {configuration_path}")
    print(f"Station/weather lookup: {lookup_path}")
