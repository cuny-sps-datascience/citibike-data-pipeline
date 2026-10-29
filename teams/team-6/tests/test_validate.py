from archive_fixture import QUIET
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.pipeline import run_normalize
from citibike_pipeline.validate import run_validate, validate_releases


def test_validate_reconciles_and_reports_ride_id_duplicates(selected_cfg):
    with Manifest(selected_cfg) as m:
        assert run_normalize(selected_cfg, m, ["2023-11", "2025-01"], out=QUIET) == 0
        res = validate_releases(selected_cfg, m, ["2023-11"])
    assert res["reconciled"] and res["schema_consistent"]
    assert (res["source_rows"], res["published_rows"], res["quarantined_rows"]) == (5, 4, 1)
    assert (res["ride_id_identical"], res["ride_id_conflicting"]) == (1, 1)
    assert res["months"] == [(2023, 11, 4)]
    assert dict(res["flags"])["start_dst_ambiguous"] == 2


def test_month_spillover_is_visible(selected_cfg):
    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2025-01"], out=QUIET)
        res = validate_releases(selected_cfg, m, ["2025-01"])
    assert res["months"] == [(2024, 12, 1), (2025, 1, 2)]


def test_unpublished_release_does_not_reconcile(selected_cfg):
    with Manifest(selected_cfg) as m:
        res = validate_releases(selected_cfg, m, ["2018-04"])
    assert res["reconciled"] is False and res["published_rows"] == 0


def test_run_validate_writes_reports(selected_cfg):
    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2018-04", "2025-01"], out=QUIET)
    assert run_validate(selected_cfg, ["2018-04", "2025-01"], QUIET) == 0
    names = sorted(p.name for p in selected_cfg.reports_dir.glob("validate_*.md"))
    assert names == ["validate_2018-04.md", "validate_2018-04_2025-01.md", "validate_2025-01.md"]
    assert "reconciled: True" in (selected_cfg.reports_dir / "validate_2018-04.md").read_text()


def test_run_validate_fails_when_unpublished(selected_cfg):
    assert run_validate(selected_cfg, ["2018-04"], QUIET) == 1


def test_stray_parquet_under_trips_does_not_reconcile(selected_cfg):
    import duckdb

    from citibike_pipeline.validate import write_validation_report

    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2025-01"], out=QUIET)
        assert validate_releases(selected_cfg, m, ["2025-01"])["unlisted_files"] == 0
        stray = selected_cfg.trips_dir / "trip_year=2025" / "trip_month=01" / "stray__x__0.parquet"
        duckdb.connect().execute(f"COPY (SELECT 1 AS a) TO '{stray}' (FORMAT parquet)")
        res = validate_releases(selected_cfg, m, ["2025-01"])
    assert res["unlisted_files"] == 1 and res["reconciled"] is False
    assert "unlisted files under trips/, quarantine/ and removed/: 1" in write_validation_report(selected_cfg, res).read_text()


def test_published_source_no_longer_selected_is_stale(selected_cfg):
    from archive_fixture import KEY_2025_CSV, KEY_2025_ZIP

    from citibike_pipeline.validate import write_validation_report

    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2025-01"], out=QUIET)
        assert validate_releases(selected_cfg, m, ["2025-01"])["stale_ledger_sources"] == []
        for key, status in ((KEY_2025_CSV, "excluded"), (KEY_2025_ZIP, "selected")):
            rec = m.get(key)
            rec["status"] = status
            m.upsert(rec)
        res = validate_releases(selected_cfg, m, ["2025-01"])
    assert res["stale_ledger_sources"] == [KEY_2025_CSV] and res["reconciled"] is False
    assert KEY_2025_CSV in write_validation_report(selected_cfg, res).read_text()


def test_validate_connection_is_bounded(selected_cfg):
    import dataclasses

    from citibike_pipeline.validate import validate_connection

    big = dataclasses.replace(selected_cfg, memory_limit="6GB", threads=6)
    con = validate_connection(big)
    try:
        threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
        limit = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    finally:
        con.close()
    assert int(threads) == 4
    assert limit == "2.7 GiB"  # 3 GiB minus DuckDB's display rounding of the 3GB setting
    small = validate_connection(selected_cfg)  # fixture asks for 1GB and 2 threads
    try:
        assert int(small.execute("SELECT current_setting('threads')").fetchone()[0]) == 2
        assert small.execute("SELECT current_setting('memory_limit')").fetchone()[0] == \
            _duckdb_display_of(selected_cfg.memory_limit)
    finally:
        small.close()


def _duckdb_display_of(limit):
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{limit}'")
    return con.execute("SELECT current_setting('memory_limit')").fetchone()[0]


def test_shared_connection_drops_views_between_units(selected_cfg):
    from citibike_pipeline.validate import validate_connection

    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2023-11", "2025-01"], out=QUIET)
        con = validate_connection(selected_cfg)
        try:
            first = validate_releases(selected_cfg, m, ["2023-11"], con=con)
            assert con.execute("SELECT count(*) FROM duckdb_views() WHERE NOT internal").fetchone()[0] == 0
            second = validate_releases(selected_cfg, m, ["2025-01"], con=con)
        finally:
            con.close()
    assert first["reconciled"] and second["reconciled"]
    assert second["months"] == [(2024, 12, 1), (2025, 1, 2)]


def test_run_validate_twice_in_one_process(selected_cfg):
    from archive_fixture import FIXTURE_RELEASES

    with Manifest(selected_cfg) as m:
        assert run_normalize(selected_cfg, m, FIXTURE_RELEASES, out=QUIET) == 0
    lines = []
    for _ in range(2):
        assert run_validate(selected_cfg, FIXTURE_RELEASES, lines.append) == 0
    assert len(lines) == 2 * (len(FIXTURE_RELEASES) + 1)


def test_validation_report_escapes_pipes_in_cells(selected_cfg):
    from citibike_pipeline.validate import write_validation_report

    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2025-01"], out=QUIET)
        res = validate_releases(selected_cfg, m, ["2025-01"])
    res["rideable"] = [("odd|type", 1)]
    text = write_validation_report(selected_cfg, res).read_text()
    assert "| odd\\|type | 1 |" in text.splitlines()


def test_differing_trip_file_columns_are_not_schema_consistent(selected_cfg):
    import duckdb

    from archive_fixture import KEY_2025_CSV

    from citibike_pipeline.validate import write_validation_report

    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2025-01"], out=QUIET)
        entries = [(e["dataset"], e["path"], e["rows"]) for e in m.published_for_sources([KEY_2025_CSV])]
        odd = selected_cfg.trips_dir / "trip_year=2025" / "trip_month=01" / "odd__x__0.parquet"
        duckdb.connect().execute(f"COPY (SELECT 1 AS a, 'x' AS b) TO '{odd}' (FORMAT parquet)")
        m.replace_published(KEY_2025_CSV, "odd", entries + [("trips", odd.relative_to(selected_cfg.processed_root)
                                                               .as_posix(), 1)])
        res = validate_releases(selected_cfg, m, ["2025-01"])
    assert res["schema_consistent"] is False and res["reconciled"] is False
    assert "schema_consistent: False" in write_validation_report(selected_cfg, res).read_text()
    assert run_validate(selected_cfg, ["2025-01"], QUIET) == 1


def test_per_source_table_and_quarantine_cross_check(selected_cfg):
    from archive_fixture import KEY_2023_NESTED

    from citibike_pipeline.validate import write_validation_report

    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2023-11"], out=QUIET)
        res = validate_releases(selected_cfg, m, ["2023-11"])
    assert res["per_source"] == [(KEY_2023_NESTED, 5, 4, 1, 0, 1)]
    assert res["quarantine_mismatches"] == [] and res["record_removals"] == 0 and res["reconciled"]
    text = write_validation_report(selected_cfg, res).read_text()
    assert f"| {KEY_2023_NESTED} | 5 | 4 | 1 | 0 | 1 |" in text.splitlines()
    assert "- record-level removals: 0" in text
    assert "- quarantine cross-check mismatches: 0" in text


def test_quarantine_mismatch_and_removals_block_reconciliation(selected_cfg):
    from archive_fixture import KEY_2023_NESTED

    from citibike_pipeline.validate import write_validation_report

    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2023-11"], out=QUIET)
        rec = m.get(KEY_2023_NESTED)
        rec.update(malformed_rows=0, rows=6)
        m.upsert(rec)
        res = validate_releases(selected_cfg, m, ["2023-11"])
    assert res["quarantine_mismatches"] == [KEY_2023_NESTED]
    assert res["record_removals"] == 1 and res["reconciled"] is False
    text = write_validation_report(selected_cfg, res).read_text()
    assert f"  - {KEY_2023_NESTED}: quarantined 1, profile malformed + invalid start 0" in text
    assert "- record-level removals: 1" in text


def test_report_filename_uses_sorted_releases(selected_cfg):
    from citibike_pipeline.validate import write_validation_report

    with Manifest(selected_cfg) as m:
        res = validate_releases(selected_cfg, m, ["2025-01", "2018-04"])
    assert write_validation_report(selected_cfg, res).name == "validate_2018-04_2025-01.md"


def test_drop_view_error_does_not_mask_the_original_error(selected_cfg, monkeypatch):
    import pytest

    import citibike_pipeline.validate as validate_mod

    class Con:
        def execute(self, sql, *args):
            if sql.startswith("DROP VIEW"):
                raise RuntimeError("cleanup failed too")
            raise ValueError("original failure")

    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2025-01"], out=QUIET)
        with pytest.raises(ValueError, match="original failure"):
            validate_mod.validate_releases(selected_cfg, m, ["2025-01"], con=Con())


def test_limit_bytes_accepts_sizes_and_percentages():
    import os

    from citibike_pipeline.validate import _limit_bytes

    assert _limit_bytes("6GB") == 6 * 1000**3
    ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    assert _limit_bytes("50%") == ram // 2
    assert _limit_bytes("lots") is None


def test_unparseable_memory_limit_is_left_uncapped(selected_cfg, monkeypatch):
    import dataclasses

    import citibike_pipeline.validate as validate_mod

    monkeypatch.setattr(validate_mod, "connect", lambda cfg: _RecordingCon())
    con = validate_mod.validate_connection(dataclasses.replace(selected_cfg, memory_limit="lots", threads=6))
    assert con.sql == ["SET threads = 4"]
    con = validate_mod.validate_connection(dataclasses.replace(selected_cfg, memory_limit="50%", threads=2))
    assert con.sql[0].startswith("SET memory_limit = '") and con.sql[1] == "SET threads = 2"


class _RecordingCon:
    def __init__(self):
        self.sql = []

    def execute(self, sql, *args):
        self.sql.append(sql)


def test_ride_id_variants_treat_null_fields_as_values(tmp_path):
    """Same answers as count(DISTINCT (start, end, start id, end id)) per ride_id, NULL fields included."""
    import duckdb

    from citibike_pipeline.validate import _profile_trips

    d = tmp_path / "trip_year=2025" / "trip_month=01"
    d.mkdir(parents=True)
    f = d / "x.parquet"
    con = duckdb.connect()
    con.execute(f"""COPY (SELECT *, 600.0 AS duration_seconds, []::VARCHAR[] AS quality_flags,
        'member' AS membership_group, 'member' AS original_membership, 'classic_bike' AS rideable_type
      FROM (VALUES
        ('A', TIMESTAMP '2025-01-01 10:00', TIMESTAMP '2025-01-01 10:10', 's1', NULL),
        ('A', TIMESTAMP '2025-01-01 10:00', TIMESTAMP '2025-01-01 10:10', 's1', NULL),
        ('B', TIMESTAMP '2025-01-01 10:00', NULL, 's1', 's2'),
        ('B', TIMESTAMP '2025-01-01 10:00', TIMESTAMP '2025-01-01 10:10', 's1', 's2'),
        ('C', TIMESTAMP '2025-01-01 10:00', TIMESTAMP '2025-01-01 10:10', NULL, 's2'),
        (NULL, TIMESTAMP '2025-01-01 10:00', TIMESTAMP '2025-01-01 10:10', 's1', 's2'),
        (NULL, TIMESTAMP '2025-01-01 10:00', TIMESTAMP '2025-01-01 10:20', 's1', 's2')
      ) t(ride_id, started_at_local, ended_at_local, start_station_id, end_station_id)) TO '{f}' (FORMAT parquet)""")
    res = {}
    _profile_trips(con, res, [str(f)])
    assert (res["ride_id_identical"], res["ride_id_conflicting"]) == (1, 1)  # A identical, B conflicting
    assert res["start_stations"] == 1  # NULL is not a station


def test_ride_id_check_in_several_hash_buckets_matches_one_bucket(selected_cfg, monkeypatch, tmp_path):
    import citibike_pipeline.validate as validate_mod

    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2023-11"], out=QUIET)
        single = validate_releases(selected_cfg, m, ["2023-11"])
        buckets = []
        real = validate_mod._ride_id_bucket

        def spy(con, b, n):
            buckets.append((b, n))
            return real(con, b, n)

        monkeypatch.setattr(validate_mod, "_ride_id_bucket", spy)
        monkeypatch.setattr(validate_mod, "RIDE_IDS_PER_BUCKET", 1)
        bucketed = validate_releases(selected_cfg, m, ["2023-11"])
    assert {n for _, n in buckets} == {4} and [b for b, _ in buckets] == [0, 1, 2, 3]
    assert (bucketed["ride_id_identical"], bucketed["ride_id_conflicting"]) == \
        (single["ride_id_identical"], single["ride_id_conflicting"]) == (1, 1)
    test_ride_id_variants_treat_null_fields_as_values(tmp_path)  # 5 non-null ride_ids -> 5 buckets
