"""Builds a miniature Citi Bike archive that mirrors the layouts found on the RAID."""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

from citibike_pipeline.schemas import LEGACY_LOWER, LEGACY_TITLE, MODERN

GOOD_201804_A = ["840", "2018-04-01 03:07:39.0700", "2018-04-01 03:21:39.1280", "72", "W 52 St & 11 Ave",
                 "40.76727216", "-73.99392888", "379", "W 31 St & 7 Ave", "40.749156", "-73.9916",
                 "31977", "Subscriber", "1973", "1"]
BAD_201804 = ["120", "bad-date", "2018-04-03 10:02:00.0000", "72", "W 52 St & 11 Ave", "40.76727216",
              "-73.99392888", "379", "W 31 St & 7 Ave", "40.749156", "-73.9916", "31979", "Subscriber",
              "1990", "1"]
GOOD_201804_B = ["600", "2018-04-02 10:00:00.0000", "2018-04-02 10:10:00.0000", "\\N", "", "0.0", "0.0",
                 "379", "W 31 St & 7 Ave", "40.749156", "-73.9916", "31978", "Customer", "1980", "2"]
ROWS_201804 = [GOOD_201804_A, BAD_201804, GOOD_201804_B]

ROWS_201501 = [
    ["1346", "1/1/2015 0:01", "1/1/2015 0:24", "455", "1 Ave & E 44 St", "40.75001986", "-73.96905301",
     "265", "Stanton St & Chrystie St", "40.72229346", "-73.99147535", "18660", "Subscriber", "1960", "2"],
    ["363", "1/1/2015 00:02:00", "1/1/2015 00:08:03", "434", "9 Ave & W 18 St", "40.74317449",
     "-74.00366443", "482", "W 15 St & 7 Ave", "40.73935542", "-73.99931783", "16085", "Customer", "", "0"],
]
ROWS_201612 = [
    ["528", "2016-12-01 00:00:04", "2016-12-01 00:08:52", "499", "Broadway & W 60 St", "40.76915505",
     "-73.98191841", "228", "E 48 St & 3 Ave", "40.7546011026", "-73.971878855", "26931", "Subscriber",
     "1964.0", "1"],
]
_D4 = ["D4", "classic_bike", "2023-11-05 01:30:00.000", "2023-11-05 01:45:00.000", "E 33 St & 5 Ave",
       "6322.01", "W 24 St & 7 Ave", "6257.03", "40.74765947", "-73.98490707", "40.74487634",
       "-73.99529885", "member"]
_D4_CONFLICT = _D4[:6] + ["E 33 St & 5 Ave", "6322.01"] + _D4[8:10] + ["40.74765947", "-73.98490707", "member"]
_E5 = ["E5", "classic_bike", "2023-11-06 08:00:00", "2023-11-06 08:10:00", "E 33 St & 5 Ave", "6322.01",
       "W 24 St & 7 Ave", "6257.03", "40.74765947", "-73.98490707", "40.74487634", "-73.99529885", "member"]
ROWS_202311 = [_D4, _D4_CONFLICT, _E5, list(_E5), ["F6", "classic_bike", "2023-11-07 08:00:00"]]
ROWS_202501 = [
    ["E4A8132DACF670C3", "electric_bike", "2025-01-10 14:31:22.100", "2025-01-10 14:37:37.034",
     "E 33 St & 5 Ave", "6322.01", "W 24 St & 7 Ave", "6257.03", "40.74765947", "-73.98490707",
     "40.74487634", "-73.99529885", "member"],
    ["B2", "classic_bike", "2025-01-31 23:55:00.000", "2025-02-01 00:05:00.000", "W 24 St & 7 Ave",
     "6257.03", "", "", "40.74487634", "-73.99529885", "", "", "casual"],
    ["C3", "classic_bike", "2024-12-31 23:50:00.000", "2025-01-01 00:10:00.000", "E 33 St & 5 Ave",
     "6322.01", "W 24 St & 7 Ave", "6257.03", "40.74765947", "-73.98490707", "40.74487634",
     "-73.99529885", "member"],
]

KEY_2015 = "2015-citibike-tripdata.zip::2015-citibike-tripdata/1_January/201501-citibike-tripdata_1.csv"
KEY_2016 = "2016-citibike-tripdata.zip::2016-citibike-tripdata/12_December/201612-citibike-tripdata_1.csv"
KEY_2018_WHOLE = "2018-citibike-tripdata.zip::2018-citibike-tripdata/201804-citibike-tripdata.csv"
KEY_2018_P1 = "2018-citibike-tripdata.zip::2018-citibike-tripdata/4_April/201804-citibike-tripdata_1.csv"
KEY_2018_P2 = "2018-citibike-tripdata.zip::2018-citibike-tripdata/4_April/201804-citibike-tripdata_2.csv"
KEY_UNKNOWN = "201902-citibike-tripdata.csv"
KEY_2023_NESTED = ("2023-citibike-tripdata.zip::2023-citibike-tripdata/202311-citibike-tripdata.zip"
                   "::202311-citibike-tripdata_1.csv")
KEY_2025_ZIP = "202501-citibike-tripdata.zip::202501-citibike-tripdata_1.csv"
KEY_2025_CSV = "202501-citibike-tripdata/202501-citibike-tripdata_1.csv"
EXPECTED_KEYS = {KEY_2015, KEY_2016, KEY_2018_WHOLE, KEY_2018_P1, KEY_2018_P2, KEY_UNKNOWN,
                 KEY_2023_NESTED, KEY_2025_ZIP, KEY_2025_CSV}
FIXTURE_RELEASES = ["2015-01", "2016-12", "2018-04", "2023-11", "2025-01"]


def QUIET(*_args, **_kwargs):
    pass


def csv_bytes(header, rows, quoting=csv.QUOTE_MINIMAL) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=quoting, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode()


def zip_bytes(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def build_archive(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "2015-citibike-tripdata.zip").write_bytes(zip_bytes({
        "2015-citibike-tripdata/1_January/201501-citibike-tripdata_1.csv": csv_bytes(LEGACY_LOWER, ROWS_201501),
    }))
    (root / "2016-citibike-tripdata.zip").write_bytes(zip_bytes({
        "2016-citibike-tripdata/12_December/201612-citibike-tripdata_1.csv": csv_bytes(LEGACY_TITLE, ROWS_201612),
    }))
    (root / "2018-citibike-tripdata.zip").write_bytes(zip_bytes({
        "2018-citibike-tripdata/201804-citibike-tripdata.csv": csv_bytes(LEGACY_LOWER, ROWS_201804, csv.QUOTE_ALL),
        "2018-citibike-tripdata/4_April/201804-citibike-tripdata_1.csv": csv_bytes(LEGACY_LOWER, ROWS_201804[:2]),
        "2018-citibike-tripdata/4_April/201804-citibike-tripdata_2.csv": csv_bytes(LEGACY_LOWER, ROWS_201804[2:]),
        "2018-citibike-tripdata/.DS_Store": b"junk",
        "__MACOSX/2018-citibike-tripdata/._201804-citibike-tripdata.csv": b"junk",
    }))
    (root / "201902-citibike-tripdata.csv").write_bytes(csv_bytes(["foo", "bar"], [["1", "2"]]))
    inner = zip_bytes({"202311-citibike-tripdata_1.csv": csv_bytes(MODERN, ROWS_202311)})
    (root / "2023-citibike-tripdata.zip").write_bytes(zip_bytes({
        "2023-citibike-tripdata/202311-citibike-tripdata.zip": inner,
        "__MACOSX/2023-citibike-tripdata/._202311-citibike-tripdata.zip": b"junk",
    }))
    modern_2501 = csv_bytes(MODERN, ROWS_202501)
    (root / "202501-citibike-tripdata.zip").write_bytes(zip_bytes({"202501-citibike-tripdata_1.csv": modern_2501}))
    (root / "202501-citibike-tripdata").mkdir()
    (root / "202501-citibike-tripdata" / "202501-citibike-tripdata_1.csv").write_bytes(modern_2501)
    for excluded in ("citibike-production", ".hidden"):
        (root / excluded).mkdir()
        (root / excluded / "202501-citibike-tripdata_1.csv").write_bytes(modern_2501)
    (root / ".DS_Store").write_bytes(b"junk")
    return root


# Cross-release spillover (D2): a trip starting on the last day of 2025-05 that the 2025-06 release
# publishes again. Added only by the tests that need it, so the base archive's numbers are unchanged.
def _modern_trip(ride_id, start, end, start_id, end_id):
    return [ride_id, "classic_bike", start, end, "Broadway & W 58 St", start_id, "W 24 St & 7 Ave", end_id,
            "40.76", "-73.98", "40.74", "-73.99", "member"]


# 2025-05 comes as two -partN files, like the real 2026-04 release; S1 is in part 2.
ROWS_202505_P1 = [
    _modern_trip("A1", "2025-05-10 08:00:00", "2025-05-10 08:10:00", "6948.10", "6257.03"),
    _modern_trip("S2", "2025-05-31 23:40:00", "2025-05-31 23:59:00", "6948.10", "6257.03"),
]
ROWS_202505_P2 = [
    _modern_trip("S1", "2025-05-31 23:50:00", "2025-06-01 00:10:00", "6948.10", "6322.01"),
]
ROWS_202506 = [
    _modern_trip("B1", "2025-06-05 08:00:00", "2025-06-05 08:10:00", "6322.01", "6257.03"),
    _modern_trip("S1", "2025-05-31 23:50:00", "2025-06-01 00:10:00", "6948.1", "6322.01"),  # same trip after D1
    _modern_trip("S2", "2025-05-31 23:40:00", "2025-05-31 23:59:00", "6948.10", "6322.01"),  # conflicting copy
    _modern_trip("S3", "2025-05-31 23:30:00", "2025-05-31 23:45:00", "6948.10", "6257.03"),  # not in 2025-05
]
ROWS_201502 = [  # legacy: starts in 2015-01, so a spillover row; legacy rows are never removed
    ["1380", "1/31/2015 23:50", "2/1/2015 0:13", "455", "1 Ave & E 44 St", "40.75001986", "-73.96905301",
     "265", "Stanton St & Chrystie St", "40.72229346", "-73.99147535", "18660", "Subscriber", "1960", "2"],
]
KEY_2025_05_P1 = "202505-citibike-tripdata-part1.csv"
KEY_2025_05_P2 = "202505-citibike-tripdata-part2.csv"
# A different whole-file 2025-05 copy without S1, only added by tests that re-select 2025-05.
KEY_2025_05_ALT = "alt/202505-citibike-tripdata.csv"
SET_2025_05_PARTS, SET_2025_05_ALT = "|2025-05|parts", "alt|2025-05|whole"
KEY_2025_06 = "202506-citibike-tripdata_1.csv"
KEY_2015_02 = "201502-citibike-tripdata.csv"
SPILLOVER_RELEASES = ["2015-02", "2025-05", "2025-06"]


def add_spillover_releases(root: Path) -> None:
    (root / KEY_2025_05_P1).write_bytes(csv_bytes(MODERN, ROWS_202505_P1))
    (root / KEY_2025_05_P2).write_bytes(csv_bytes(MODERN, ROWS_202505_P2))
    (root / KEY_2025_06).write_bytes(csv_bytes(MODERN, ROWS_202506))
    (root / KEY_2015_02).write_bytes(csv_bytes(LEGACY_LOWER, ROWS_201502))


def add_alternative_2025_05(root: Path) -> None:
    (root / "alt").mkdir()
    (root / KEY_2025_05_ALT).write_bytes(csv_bytes(MODERN, ROWS_202505_P1))
