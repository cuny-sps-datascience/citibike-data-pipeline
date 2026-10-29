from datetime import date

from citibike_pipeline.dst import dst_hours


def test_dst_hours_2025():
    assert dst_hours([2025], "America/New_York") == {
        (date(2025, 3, 9), 2): "nonexistent",
        (date(2025, 11, 2), 1): "ambiguous",
    }


def test_dst_hours_2023_fall_back():
    assert dst_hours([2023], "America/New_York")[(date(2023, 11, 5), 1)] == "ambiguous"


def test_crosses_dst():
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from citibike_pipeline.dst import crosses_dst

    tz = ZoneInfo("America/New_York")
    assert crosses_dst(datetime(2025, 3, 9, 1, 55), datetime(2025, 3, 9, 3, 5), tz) is True
    assert crosses_dst(datetime(2025, 11, 2, 0, 50), datetime(2025, 11, 2, 2, 10), tz) is True
    assert crosses_dst(datetime(2025, 6, 1, 10, 0), datetime(2025, 6, 1, 10, 10), tz) is False
