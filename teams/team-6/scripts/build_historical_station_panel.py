#!/usr/bin/env python3
"""Build or validate the separate longer-history Manhattan station panel."""

import argparse
from pathlib import Path

from citibike_pipeline.config import load_config
from citibike_pipeline.forecast_sources import stamp
from citibike_pipeline.historical_station_panel import build_historical_panel, validate_historical_panel


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observed-root", type=Path)
    parser.add_argument("--transfer-root", type=Path)
    parser.add_argument("--reference-root", type=Path)
    parser.add_argument("--pilot-root", type=Path)
    parser.add_argument("--protocol", type=Path, default=Path("docs/historical_station_panel_protocol.json"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--validate", type=Path)
    args = parser.parse_args()
    if args.validate:
        report = validate_historical_panel(args.validate)
    else:
        base = load_config().processed_root / "modeling_exports/three_hour"
        observed = args.observed_root or base / "builds" / (base / "CURRENT_OBSERVED").read_text().strip()
        report = build_historical_panel(
            observed,
            args.transfer_root or base / "weather_handoffs/20261023T174914038037Z/extracted",
            args.reference_root or base / "sources/20261015T234334Z-d29974e2",
            args.pilot_root or base / "station_hour_integrations/20261021T-team6-v2",
            args.protocol,
            args.output or base / "historical_station_panels" / stamp(),
        )
    print(f"Validated {report['rows']:,} rows across {report['stations']} stations; main evaluation remains gated.")


if __name__ == "__main__":
    main()
