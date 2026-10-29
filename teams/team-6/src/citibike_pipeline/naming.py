"""Filename-derived nominal release (YYYY-MM) and part number. Never proof of content dates."""

from __future__ import annotations

import re
from dataclasses import dataclass

_RELEASE = re.compile(
    r"(?P<year>20\d{2})(?P<month>\d{2})-citibike-tripdata(?:\.csv)?(?:(?:_|-part)(?P<part>\d+))?\.(?:csv|zip)"
)


@dataclass(frozen=True)
class NominalRelease:
    release: str
    part: int | None


def parse_nominal(name: str) -> NominalRelease | None:
    base = name.rsplit("/", 1)[-1]
    m = _RELEASE.fullmatch(base)
    if not m or not 1 <= int(m["month"]) <= 12:
        return None
    return NominalRelease(f"{m['year']}-{m['month']}", int(m["part"]) if m["part"] else None)
