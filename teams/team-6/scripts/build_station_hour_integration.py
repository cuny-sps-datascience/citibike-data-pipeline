#!/usr/bin/env python3
"""Build the bounded station-hour feature/label integration package."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from citibike_pipeline.config import load_config
from citibike_pipeline.forecast_sources import stamp
from citibike_pipeline.station_hour_integration import (
    build_station_hour_integration,
    validate_station_hour_integration,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--station-package", type=Path)
    parser.add_argument("--availability-package", type=Path)
    parser.add_argument("--weather-root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--validate", type=Path)
    arguments = parser.parse_args()
    if arguments.validate:
        report = validate_station_hour_integration(arguments.validate)
        print(f"Validated {report['rows']:,} station-hours at {arguments.validate}")
        return 0
    if not all((arguments.station_package, arguments.availability_package, arguments.weather_root)):
        parser.error("build requires --station-package, --availability-package and --weather-root")
    cfg = load_config()
    output = arguments.output or (
        cfg.processed_root
        / "modeling_exports"
        / "three_hour"
        / "station_hour_integrations"
        / stamp()
    )
    result = build_station_hour_integration(
        cfg,
        arguments.station_package,
        arguments.availability_package,
        arguments.weather_root,
        output,
    )
    report = validate_station_hour_integration(result)
    pointer = result.parent / "CURRENT_STATION_HOUR"
    temporary = pointer.with_suffix(".tmp")
    temporary.write_text(str(result) + "\n")
    temporary.replace(pointer)
    print(f"Built {report['rows']:,} station-hours at {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
