"""Header families observed in the archive. Classification is by exact header, never by year."""

from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Sequence

LEGACY_LOWER = (
    "tripduration", "starttime", "stoptime", "start station id", "start station name",
    "start station latitude", "start station longitude", "end station id", "end station name",
    "end station latitude", "end station longitude", "bikeid", "usertype", "birth year", "gender",
)
LEGACY_TITLE = (
    "Trip Duration", "Start Time", "Stop Time", "Start Station ID", "Start Station Name",
    "Start Station Latitude", "Start Station Longitude", "End Station ID", "End Station Name",
    "End Station Latitude", "End Station Longitude", "Bike ID", "User Type", "Birth Year", "Gender",
)
MODERN = (
    "ride_id", "rideable_type", "started_at", "ended_at", "start_station_name", "start_station_id",
    "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
)
FAMILIES = {LEGACY_LOWER: "legacy_lower", LEGACY_TITLE: "legacy_title", MODERN: "modern"}

# Positional staging names per family; normalize.transform_sql reads these.
_LEGACY_RAW = (
    "src_duration", "start_ts", "end_ts", "start_station_id", "start_station_name", "start_lat",
    "start_lng", "end_station_id", "end_station_name", "end_lat", "end_lng", "bike_id",
    "membership", "birth_year", "gender",
)
_MODERN_RAW = (
    "ride_id", "rideable_type", "start_ts", "end_ts", "start_station_name", "start_station_id",
    "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "membership",
)
RAW_NAMES = {"legacy_lower": _LEGACY_RAW, "legacy_title": _LEGACY_RAW, "modern": _MODERN_RAW}
START_COLUMN = {"legacy_lower": 1, "legacy_title": 1, "modern": 2}
END_COLUMN = {"legacy_lower": 2, "legacy_title": 2, "modern": 3}


def parse_header(first_line: bytes) -> tuple[str, ...]:
    text = first_line.decode("utf-8-sig").rstrip("\r\n")
    if not text:
        return ()
    return tuple(c.strip() for c in next(csv.reader([text])))


def fingerprint(header: Sequence[str]) -> str:
    return hashlib.sha256(json.dumps(list(header)).encode()).hexdigest()[:16]


def classify(header: Sequence[str]) -> str:
    return FAMILIES.get(tuple(header), "unknown")
