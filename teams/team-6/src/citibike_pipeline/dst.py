"""Local wall-clock hours that are ambiguous (fall back) or nonexistent (spring forward)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo


def dst_hours(years, tz_name: str) -> dict[tuple[date, int], str]:
    tz = ZoneInfo(tz_name)
    out: dict[tuple[date, int], str] = {}
    for year in years:
        day = date(year, 1, 1)
        while day.year == year:
            for hour in range(24):
                wall = datetime(day.year, day.month, day.day, hour, 30)
                early, late = wall.replace(tzinfo=tz, fold=0), wall.replace(tzinfo=tz, fold=1)
                if early.utcoffset() != late.utcoffset():
                    round_trip = early.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None)
                    out[(day, hour)] = "ambiguous" if round_trip == wall else "nonexistent"
            day += timedelta(days=1)
    return out


def crosses_dst(start: datetime, end: datetime, tz: tzinfo) -> bool:
    """True when the UTC offset at the local end differs from the one at the local start (fold=0)."""
    return start.replace(tzinfo=tz, fold=0).utcoffset() != end.replace(tzinfo=tz, fold=0).utcoffset()
