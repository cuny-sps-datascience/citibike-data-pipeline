"""Download and archive the current Citi Bike station-information feed.

The archive layout mirrors the Databricks poller used as the reference for
this project, but stores snapshots beneath this local project's ``data``
directory by default.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


USER_AGENT = {
    "User-Agent": "CUNY-SPS-DataScience-Archiver/1.0 (george.hagstrom@cuny.edu)"
}
GBFS_ROOT = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"
POLLER_VERSION = "1.2"
DEFAULT_OUTPUT_ROOT = Path(__file__).parent / "data" / "raw" / "station_information"


def get_json(url: str) -> dict[str, Any]:
    """Fetch a JSON document and raise a clear error for a bad response."""
    response = requests.get(url, headers=USER_AGENT, timeout=30)
    response.raise_for_status()
    return response.json()


def resolve_feed(name: str = "station_information") -> str:
    """Resolve a feed URL from either the GBFS v1/v2 or v3 root shape."""
    root = get_json(GBFS_ROOT)
    data = root["data"]
    feeds = data["en"]["feeds"] if "en" in data else data["feeds"]
    feed_urls = {feed["name"]: feed["url"] for feed in feeds}

    try:
        return feed_urls[name]
    except KeyError as exc:
        available = ", ".join(sorted(feed_urls))
        raise KeyError(f"Unknown GBFS feed {name!r}; available feeds: {available}") from exc


def poll_station_info(
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    git_sha: str = "manual_run",
) -> Path:
    """Fetch station information and save one timestamped gzip archive."""
    output_root.mkdir(parents=True, exist_ok=True)
    payload = get_json(resolve_feed("station_information"))
    now = datetime.now(timezone.utc)
    destination = output_root / f"year={now:%Y}" / f"month={now:%m}" / f"day={now:%d}"
    destination.mkdir(parents=True, exist_ok=True)
    path = destination / f"info_{now:%Y%m%dT%H%M%S}Z.json.gz"

    archive = {
        "fetched_at": now.isoformat(),
        "feed_last_updated": payload.get("last_updated"),
        "version": POLLER_VERSION,
        "git_sha": git_sha,
        "payload": payload,
    }

    # Write in the destination directory, then rename, so an interrupted fetch
    # never leaves a partial snapshot with a valid archive filename.
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
    print(poll_station_info(arguments.output_root, arguments.git_sha))
