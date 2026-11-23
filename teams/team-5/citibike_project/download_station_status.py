"""Download and archive the current Citi Bike station-status feed.

This is the local-project counterpart to the Databricks status poller. Each
run writes one timestamped, gzip-compressed snapshot by default.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from download_station_info import POLLER_VERSION, get_json, resolve_feed


DEFAULT_OUTPUT_ROOT = Path(__file__).parent / "data" / "raw" / "station_status"


def poll_station_status(
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    git_sha: str = "manual_run",
) -> Path:
    """Fetch station status and save one timestamped gzip archive."""
    output_root.mkdir(parents=True, exist_ok=True)
    payload = get_json(resolve_feed("station_status"))
    now = datetime.now(timezone.utc)
    destination = output_root / f"year={now:%Y}" / f"month={now:%m}" / f"day={now:%d}"
    destination.mkdir(parents=True, exist_ok=True)
    path = destination / f"status_{now:%Y%m%dT%H%M%S}Z.json.gz"

    archive = {
        "fetched_at": now.isoformat(),
        "feed_last_updated": payload.get("last_updated"),
        "version": POLLER_VERSION,
        "git_sha": git_sha,
        "payload": payload,
    }

    fd, temporary_name = tempfile.mkstemp(dir=destination, suffix=".tmp")
    os.close(fd)
    temporary_path = Path(temporary_name)
    try:
        with gzip.open(temporary_path, "wt", encoding="utf-8") as archive_file:
            json.dump(archive, archive_file)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)

    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help=f"Archive root (default: {DEFAULT_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "--git-sha",
        default=os.environ.get("GIT_SHA", "manual_run"),
        help="Source revision to record (default: GIT_SHA or manual_run)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    print(poll_station_status(arguments.output_root, arguments.git_sha))
