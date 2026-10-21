"""Fetch fresh public references and build a prospective 250m/500m crosswalk."""
import argparse
from pathlib import Path
from citibike_pipeline.station_subway import build_mapping

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/station_subway_mapping'))
    args = parser.parse_args()
    print(build_mapping(args.root))
