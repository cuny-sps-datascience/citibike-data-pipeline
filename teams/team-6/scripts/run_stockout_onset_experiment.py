"""Run the frozen logistic/boosting/persistence stockout-onset comparison."""
from __future__ import annotations

import argparse
from pathlib import Path

from citibike_pipeline.stockout_onset_runtime import run


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    run(args.package, args.output)
