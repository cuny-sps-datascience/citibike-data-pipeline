import dataclasses
from datetime import datetime

import pyarrow.parquet as pq
import pytest

from archive_fixture import KEY_2018_P1
from citibike_pipeline.db import connect
from citibike_pipeline.dst import dst_hours
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.normalize import NormalizeError, normalize_source, slug_for, transform_sql


def _normalize(cfg, release, run_id="run-test"):
    with Manifest(cfg) as m:
        recs = [r for r in m.all() if r["nominal_release"] == release and r["status"] == "selected"]
    con = connect(cfg)
    dst = dst_hours(range(2013, 2028), cfg.timezone)
    results = [normalize_source(cfg, con, r, run_id, cfg.staging_dir / run_id, dst) for r in recs]
    files = [str(f) for res in results for f in (res.staged_dir / "trips").rglob("*.parquet")]
    rel = con.sql(
        "SELECT * FROM read_parquet(?, hive_partitioning = true, "
        "hive_types = {'trip_year': INTEGER, 'trip_month': INTEGER})",
        params=[files],
    ).order("source_key, source_row")
    rows = [dict(zip(rel.columns, row)) for row in rel.fetchall()]
    return results, rows


def _quarantine(results):
    out = []
    for res in results:
        path = res.staged_dir / "quarantine.parquet"
        if path.exists():
            out += pq.read_table(path).to_pylist()
    return out


def test_legacy_parts_reconcile_and_quarantine(selected_cfg):
    results, rows = _normalize(selected_cfg, "2018-04")
    assert sum(r.source_rows for r in results) == 3
    assert sum(r.staged_rows for r in results) == 2 == len(rows)
    q = _quarantine(results)
    assert [(x["source_row"], x["reason"]) for x in q] == [(2, "start_ts_invalid")]
    assert q[0]["source_key"] == KEY_2018_P1


def test_legacy_field_mapping(selected_cfg):
    _, rows = _normalize(selected_cfg, "2018-04")
    first = next(r for r in rows if r["source_key"] == KEY_2018_P1)
    assert first["started_at_local"] == datetime(2018, 4, 1, 3, 7, 39, 70000)
    assert first["duration_seconds"] == pytest.approx(840.058)
    assert first["source_duration_seconds"] == 840
    assert (first["membership_group"], first["original_membership"]) == ("member", "Subscriber")
    assert first["start_station_id"] == "72" and first["quality_flags"] == []
    assert (first["trip_year"], first["trip_month"]) == (2018, 4)
    assert first["ride_id"] is None and first["schema_family"] == "legacy_lower"
    second = next(r for r in rows if r["source_key"] != KEY_2018_P1)
    assert second["start_station_id"] is None and second["start_station_name"] is None
    assert {"start_station_missing", "start_coords_invalid"} <= set(second["quality_flags"])
    assert second["membership_group"] == "casual" and second["eligible_departure"] is False


def test_modern_ids_strings_and_month_by_timestamp(selected_cfg):
    _, rows = _normalize(selected_cfg, "2025-01")
    by_id = {r["ride_id"]: r for r in rows}
    assert by_id["E4A8132DACF670C3"]["start_station_id"] == "6322.01"
    assert by_id["E4A8132DACF670C3"]["rideable_type"] == "electric_bike"
    assert (by_id["C3"]["trip_year"], by_id["C3"]["trip_month"]) == (2024, 12)
    assert {"end_station_missing", "end_coords_invalid"} <= set(by_id["B2"]["quality_flags"])
    assert by_id["B2"]["eligible_arrival"] is False and by_id["B2"]["eligible_departure"] is True


def test_dst_flags_and_field_count_quarantine(selected_cfg):
    results, rows = _normalize(selected_cfg, "2023-11")
    d4 = [r for r in rows if r["ride_id"] == "D4"]
    assert len(d4) == 2
    assert {"start_dst_ambiguous", "end_dst_ambiguous"} <= set(d4[0]["quality_flags"])
    assert [x["reason"] for x in _quarantine(results)] == ["field_count"]
    assert len(rows) == 4


def test_month_day_year_formats(selected_cfg):
    _, rows = _normalize(selected_cfg, "2015-01")
    assert rows[0]["started_at_local"] == datetime(2015, 1, 1, 0, 1)
    assert "duration_mismatch" in rows[0]["quality_flags"]
    assert rows[1]["membership_group"] == "casual" and "duration_mismatch" not in rows[1]["quality_flags"]


def test_title_case_family(selected_cfg):
    _, rows = _normalize(selected_cfg, "2016-12")
    assert rows[0]["started_at_local"] == datetime(2016, 12, 1, 0, 0, 4)
    assert rows[0]["schema_family"] == "legacy_title" and rows[0]["source_duration_seconds"] == 528


def test_unknown_family_has_no_adapter():
    with pytest.raises(NormalizeError):
        transform_sql("unknown")


def test_quarantine_guard_blocks_source(selected_cfg):
    strict = dataclasses.replace(selected_cfg, max_quarantine_fraction=0.1)
    with pytest.raises(NormalizeError, match="quarantine"):
        _normalize(strict, "2018-04")


def test_slug_is_short_and_unique():
    a = slug_for("a.zip::x/202501-citibike-tripdata_1.csv")
    b = slug_for("b.zip::x/202501-citibike-tripdata_1.csv")
    assert a.startswith("202501-citibike-tripdata_1-") and a != b


def test_dst_crossing_and_nonexistent_start_are_flagged(selected_cfg):
    from archive_fixture import KEY_2025_CSV, csv_bytes
    from citibike_pipeline.schemas import MODERN

    def trip(ride_id, start, end):
        return [ride_id, "classic_bike", start, end, "E 33 St & 5 Ave", "6322.01", "W 24 St & 7 Ave", "6257.03",
                "40.74765947", "-73.98490707", "40.74487634", "-73.99529885", "member"]

    rel = "dst-test/202503-citibike-tripdata_1.csv"
    (selected_cfg.source_root / "dst-test").mkdir()
    (selected_cfg.source_root / rel).write_bytes(csv_bytes(MODERN, [
        trip("X1", "2025-03-09 01:55:00", "2025-03-09 03:05:00"),
        trip("X2", "2025-03-09 02:30:00", "2025-03-09 03:10:00"),
        trip("X3", "2025-06-01 10:00:00", "2025-06-01 10:10:00"),
    ]))
    with Manifest(selected_cfg) as m:
        rec = dict(m.get(KEY_2025_CSV), source_key=rel, path=rel, rows=3)
    con = connect(selected_cfg)
    res = normalize_source(selected_cfg, con, rec, "run-dst", selected_cfg.staging_dir / "run-dst",
                           dst_hours(range(2013, 2028), selected_cfg.timezone))
    files = [str(f) for f in (res.staged_dir / "trips").rglob("*.parquet")]
    flags = dict(con.execute("SELECT ride_id, quality_flags FROM read_parquet(?)", [files]).fetchall())
    assert "crosses_dst" in flags["X1"]
    assert "start_dst_nonexistent" in flags["X2"]
    assert flags["X3"] == []


def test_legacy_reexported_integer_station_ids_are_canonical(tmp_path):
    import yaml

    from archive_fixture import QUIET, csv_bytes, zip_bytes
    from citibike_pipeline.config import load_config
    from citibike_pipeline.inventory import run_discover
    from citibike_pipeline.profile import run_profile
    from citibike_pipeline.schemas import LEGACY_LOWER
    from citibike_pipeline.sets import apply_selection

    # 2019-01/02/03/07/08 exist only as re-exported parts that write integer IDs as "72.0".
    row = ["840", "2019-01-01 03:07:39.0700", "2019-01-01 03:21:39.1280", "72.0", "W 52 St & 11 Ave",
           "40.76727216", "-73.99392888", "379.0", "W 31 St & 7 Ave", "40.749156", "-73.9916",
           "31977", "Subscriber", "1973", "1"]
    keep = list(row)
    keep[3], keep[7] = "3255.01", "NULL"  # not a pure .0 integer, and a null token
    source = tmp_path / "archive"
    source.mkdir()
    (source / "2019-citibike-tripdata.zip").write_bytes(zip_bytes({
        "2019-citibike-tripdata/1_January/201901-citibike-tripdata_1.csv": csv_bytes(LEGACY_LOWER, [row, keep]),
    }))
    path = tmp_path / "local.yaml"
    path.write_text(yaml.safe_dump({
        "schema_version": 1, "parser_version": "test",
        "paths": {"source_root": str(source), "processed_root": str(tmp_path / "processed")},
        "duckdb": {"memory_limit": "1GB", "threads": 2},
    }))
    cfg = load_config(path)
    with Manifest(cfg) as m:
        run_discover(cfg, m, "d", QUIET)
        run_profile(cfg, m, "p", None, False, 1, QUIET)
        assert apply_selection(m, ["2019-01"], {}, False, QUIET) == 0
    _, rows = _normalize(cfg, "2019-01")
    assert (rows[0]["start_station_id"], rows[0]["end_station_id"]) == ("72", "379")
    assert (rows[1]["start_station_id"], rows[1]["end_station_id"]) == ("3255.01", None)


def _selected(cfg, key):
    with Manifest(cfg) as m:
        return m.get(key)


def _run_one(cfg, rec, run_id="run-one"):
    con = connect(cfg)
    return con, normalize_source(cfg, con, rec, run_id, cfg.staging_dir / run_id,
                                 dst_hours(range(2024, 2027), cfg.timezone))


def test_header_changed_since_discovery_is_an_error(selected_cfg):
    import json

    from archive_fixture import KEY_2025_CSV

    rec = _selected(selected_cfg, KEY_2025_CSV)
    rec["header_json"] = json.dumps(json.loads(rec["header_json"])[:-1] + ["member_type"])
    with pytest.raises(NormalizeError, match="header differs from discovery"):
        _run_one(selected_cfg, rec)


def test_row_count_differing_from_profile_is_an_error(selected_cfg):
    from archive_fixture import KEY_2025_CSV

    rec = dict(_selected(selected_cfg, KEY_2025_CSV))
    rec["rows"] += 1
    with pytest.raises(NormalizeError, match="profiling saw"):
        _run_one(selected_cfg, rec)


def test_existing_staging_dir_is_a_normalize_error(selected_cfg):
    from archive_fixture import KEY_2025_CSV

    rec = _selected(selected_cfg, KEY_2025_CSV)
    (selected_cfg.staging_dir / "run-one" / slug_for(KEY_2025_CSV)).mkdir(parents=True)
    with pytest.raises(NormalizeError, match="staging"):
        _run_one(selected_cfg, rec)


def test_end_ts_invalid_and_multi_day_dst_crossing(selected_cfg):
    from archive_fixture import KEY_2025_CSV, csv_bytes
    from citibike_pipeline.schemas import MODERN

    def trip(ride_id, start, end):
        return [ride_id, "classic_bike", start, end, "E 33 St & 5 Ave", "6322.01", "W 24 St & 7 Ave", "6257.03",
                "40.74765947", "-73.98490707", "40.74487634", "-73.99529885", "member"]

    rel = "end-test/202503-citibike-tripdata_1.csv"
    (selected_cfg.source_root / "end-test").mkdir()
    (selected_cfg.source_root / rel).write_bytes(csv_bytes(MODERN, [
        trip("Y1", "2025-03-05 10:00:00", "2025-03-05"),
        trip("Y2", "2025-03-08 23:00:00", "2025-03-10 01:00:00"),
        trip("Y3", "2025-03-05 10:00:00", "2025-03-05 10:10:00"),
    ]))
    rec = dict(_selected(selected_cfg, KEY_2025_CSV), source_key=rel, path=rel, rows=3)
    con, res = _run_one(selected_cfg, rec)
    files = [str(f) for f in (res.staged_dir / "trips").rglob("*.parquet")]
    rows = {r[0]: r[1:] for r in con.execute(
        "SELECT ride_id, quality_flags, ended_at_local, eligible_arrival FROM read_parquet(?)", [files]).fetchall()}
    assert "end_ts_invalid" in rows["Y1"][0] and rows["Y1"][1] is None and rows["Y1"][2] is False
    assert "crosses_dst" in rows["Y2"][0]
    assert rows["Y3"][0] == []


def test_modern_one_decimal_station_ids_get_their_trailing_zero(selected_cfg):
    from archive_fixture import KEY_2025_CSV, csv_bytes
    from citibike_pipeline.schemas import MODERN

    def trip(ride_id, start_id, end_id):
        return [ride_id, "classic_bike", "2025-03-05 10:00:00", "2025-03-05 10:10:00", "Broadway & W 58 St",
                start_id, "Somewhere", end_id, "40.76", "-73.98", "40.74", "-73.99", "member"]

    rel = "pad-test/202503-citibike-tripdata_1.csv"
    (selected_cfg.source_root / "pad-test").mkdir()
    (selected_cfg.source_root / rel).write_bytes(csv_bytes(MODERN, [
        trip("P1", "6948.1", "6948.10"),
        trip("P2", "JC032", "6322.01"),
        trip("P3", "6432.10_Pillar", "6948.1"),
        trip("P4", "HB503", "SYS033"),
    ]))
    rec = dict(_selected(selected_cfg, KEY_2025_CSV), source_key=rel, path=rel, rows=4)
    con, res = _run_one(selected_cfg, rec)
    files = [str(f) for f in (res.staged_dir / "trips").rglob("*.parquet")]
    ids = {r[0]: r[1:] for r in con.execute(
        "SELECT ride_id, start_station_id, end_station_id FROM read_parquet(?)", [files]).fetchall()}
    assert ids == {"P1": ("6948.10", "6948.10"), "P2": ("JC032", "6322.01"),
                   "P3": ("6432.10_Pillar", "6948.10"), "P4": ("HB503", "SYS033")}
