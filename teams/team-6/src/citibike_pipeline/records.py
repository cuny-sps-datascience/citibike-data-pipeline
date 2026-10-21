"""Streaming CSV records with stable source_row numbers, and the one local-timestamp parser."""

from __future__ import annotations

import csv
import io
from datetime import datetime

NULL_TOKENS = frozenset({"", "\\N", "NULL"})
_FALLBACK_FORMATS = ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M")


def parse_local_ts(value: str) -> datetime | None:
    """Parses source wall-clock time. Offsets are rejected rather than silently converted.

    A value with no time part is rejected too: fromisoformat would read `2018-04-01` as midnight.
    A time part is a ':' or a date/time separator (' ' or 'T'), so `2018-04-01 00` is still accepted
    as midnight while `20180401`, `2018-W13-7` and `2018-091` are rejected.
    """
    s = value.strip()
    if s in NULL_TOKENS:
        return None
    ts = None
    try:
        ts = datetime.fromisoformat(s)
        if ":" not in s and " " not in s and "T" not in s:
            return None  # date only
    except ValueError:
        for fmt in _FALLBACK_FORMATS:
            try:
                ts = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
    if ts is None or ts.tzinfo is not None:
        return None
    return ts


def open_records(binary):
    text = io.TextIOWrapper(binary, encoding="utf-8-sig", newline="")
    reader = csv.reader(text)
    header = tuple(h.strip() for h in next(reader, []))

    def records():
        n = 0
        for fields in reader:
            if not fields:
                continue
            n += 1
            yield n, fields

    return header, records()
