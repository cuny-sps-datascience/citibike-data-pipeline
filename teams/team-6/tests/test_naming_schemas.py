import pytest

from citibike_pipeline.naming import NominalRelease, parse_nominal
from citibike_pipeline.schemas import (
    LEGACY_LOWER, LEGACY_TITLE, MODERN, RAW_NAMES, classify, fingerprint, parse_header,
)


@pytest.mark.parametrize("name, expected", [
    ("201306-citibike-tripdata.csv", NominalRelease("2013-06", None)),
    ("2018-citibike-tripdata/4_April/201804-citibike-tripdata_2.csv", NominalRelease("2018-04", 2)),
    ("2017-citibike-tripdata/4_April/201704-citibike-tripdata.csv_1.csv", NominalRelease("2017-04", 1)),
    ("202608-citibike-tripdata.zip", NominalRelease("2026-08", None)),
    ("202604-citibike-tripdata-part1.csv", NominalRelease("2026-04", 1)),
    ("202604-citibike-tripdata-part12.csv", NominalRelease("2026-04", 12)),
    ("202604-citibike-tripdata-partx.csv", None),
    ("2013-citibike-tripdata.zip", None),
    ("201313-citibike-tripdata.csv", None),
    ("notes.csv", None),
])
def test_parse_nominal(name, expected):
    assert parse_nominal(name) == expected


def test_parse_header_handles_quotes_bom_and_crlf():
    assert parse_header(b'\xef\xbb\xbf"tripduration","starttime"\r\n') == ("tripduration", "starttime")
    assert parse_header(b"ride_id, rideable_type \n") == ("ride_id", "rideable_type")
    assert parse_header(b"") == ()


def test_classify_known_families_exactly():
    assert classify(LEGACY_LOWER) == "legacy_lower"
    assert classify(LEGACY_TITLE) == "legacy_title"
    assert classify(MODERN) == "modern"
    assert classify(("foo", "bar")) == "unknown"
    assert classify(MODERN[:-1]) == "unknown"


def test_fingerprint_is_stable_and_order_sensitive():
    assert fingerprint(MODERN) == fingerprint(tuple(MODERN))
    assert fingerprint(("a", "b")) != fingerprint(("b", "a"))


def test_raw_names_match_header_widths():
    assert len(RAW_NAMES["legacy_lower"]) == len(LEGACY_LOWER)
    assert len(RAW_NAMES["legacy_title"]) == len(LEGACY_TITLE)
    assert len(RAW_NAMES["modern"]) == len(MODERN)


def test_header_parameters_are_typed_as_string_sequences():
    import typing
    from collections.abc import Sequence

    for fn in (fingerprint, classify):
        assert typing.get_type_hints(fn)["header"] == Sequence[str]
