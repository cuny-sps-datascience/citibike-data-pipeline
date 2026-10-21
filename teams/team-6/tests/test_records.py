import io
from datetime import datetime

import pytest

from citibike_pipeline.records import open_records, parse_local_ts


@pytest.mark.parametrize("text, expected", [
    ("2018-04-01 03:07:39.0700", datetime(2018, 4, 1, 3, 7, 39, 70000)),
    ("2025-01-10 14:31:22.100", datetime(2025, 1, 10, 14, 31, 22, 100000)),
    ("2016-12-01 00:00:04", datetime(2016, 12, 1, 0, 0, 4)),
    ("9/1/2014 00:00:25", datetime(2014, 9, 1, 0, 0, 25)),
    ("10/12/2015 13:05:09", datetime(2015, 10, 12, 13, 5, 9)),
    ("1/1/2015 0:01", datetime(2015, 1, 1, 0, 1)),
    (" 2016-12-01 00:00:04 ", datetime(2016, 12, 1, 0, 0, 4)),
    ("2018-04-01 00", datetime(2018, 4, 1, 0)),  # an hour-only time part is still a time part
    ("20180401T03", datetime(2018, 4, 1, 3)),
])
def test_parse_local_ts_formats(text, expected):
    assert parse_local_ts(text) == expected


@pytest.mark.parametrize("text", [
    "", "\\N", "NULL", "bad-date", "2025-01-10T14:31:22+00:00",
    "2018-04-01", "20180401", " 2018-04-01 ",  # a date with no time part is not a trip timestamp
    "2018-W13-7", "2018-091",  # ISO week and ordinal dates are dates too
])
def test_parse_local_ts_rejects(text):
    assert parse_local_ts(text) is None


def test_open_records_skips_blank_lines_and_numbers_records():
    header, records = open_records(io.BytesIO(b"a,b\n1,2\n\n3,4\n"))
    assert header == ("a", "b")
    assert list(records) == [(1, ["1", "2"]), (2, ["3", "4"])]


def test_open_records_strips_bom_and_unquotes():
    header, records = open_records(io.BytesIO(b'\xef\xbb\xbf"a","b"\n"1",""\n'))
    assert header == ("a", "b") and list(records) == [(1, ["1", ""])]


def test_parse_local_ts_does_not_raise_on_the_common_path(monkeypatch):
    import citibike_pipeline.records as records_mod

    calls = []
    real = records_mod.datetime

    class Spy(real):
        @classmethod
        def strptime(cls, *args):
            calls.append(args)
            return real.strptime(*args)

    monkeypatch.setattr(records_mod, "datetime", Spy)
    assert parse_local_ts("2025-01-10 14:31:22.100") == datetime(2025, 1, 10, 14, 31, 22, 100000)
    assert calls == [] and not hasattr(records_mod, "date")
