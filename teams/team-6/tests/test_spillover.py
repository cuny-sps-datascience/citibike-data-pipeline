"""Cross-release duplicate trips (D2): a later release's identical copy of a published trip is removed."""

from datetime import datetime

import duckdb
import pyarrow.parquet as pq
import pytest

from archive_fixture import (
    FIXTURE_RELEASES, KEY_2015_02, KEY_2025_05_ALT, KEY_2025_05_P1, KEY_2025_05_P2, KEY_2025_06, QUIET,
    SET_2025_05_ALT, SPILLOVER_RELEASES, add_alternative_2025_05, add_spillover_releases,
)
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.pipeline import run_normalize
from citibike_pipeline.validate import validate_releases, write_validation_report


@pytest.fixture
def spill_cfg(cfg):
    from citibike_pipeline.inventory import run_discover
    from citibike_pipeline.profile import run_profile
    from citibike_pipeline.sets import apply_selection

    add_spillover_releases(cfg.source_root)
    with Manifest(cfg) as m:
        run_discover(cfg, m, "t-discover", QUIET)
        run_profile(cfg, m, "t-profile", None, False, 1, QUIET)
        assert apply_selection(m, FIXTURE_RELEASES + SPILLOVER_RELEASES, {}, False, QUIET) == 0
    return cfg


def _normalize(cfg, releases):
    lines = []
    with Manifest(cfg) as m:
        rc = run_normalize(cfg, m, releases, out=lines.append)
    assert rc == 0, lines
    return lines


def _trips(cfg):
    files = [str(p) for p in cfg.trips_dir.rglob("*.parquet")]
    return duckdb.connect().execute(
        "SELECT ride_id, source_key, start_station_id, end_station_id FROM read_parquet(?, hive_partitioning = false) "
        "ORDER BY ride_id, source_key", [files]).fetchall()


def _removed(cfg):
    files = sorted(cfg.removed_dir.glob("*.parquet")) if cfg.removed_dir.exists() else []
    return [row for f in files for row in pq.read_table(f).to_pylist()]


def _ledger(cfg, dataset):
    with Manifest(cfg) as m:
        return m.con.execute("SELECT source_key, rows FROM published_files WHERE dataset = ? ORDER BY 1",
                             [dataset]).fetchall()


EXPECTED_REMOVED = [{"source_key": KEY_2025_06, "source_row": 2, "ride_id": "S1",
                     "reason": "duplicate_of_release:2025-05", "kept_source_key": KEY_2025_05_P2,
                     "started_at_local": datetime(2025, 5, 31, 23, 50), "ended_at_local": datetime(2025, 6, 1, 0, 10),
                     "start_station_id": "6948.10", "end_station_id": "6322.01"}]


@pytest.mark.parametrize("order", [["2025-05", "2025-06"], ["2025-06", "2025-05"]])
def test_identical_spillover_copy_is_removed_in_chronological_order(spill_cfg, order):
    lines = _normalize(spill_cfg, order)
    trips = _trips(spill_cfg)
    assert [t for t in trips if t[0] == "S1"] == [("S1", KEY_2025_05_P2, "6948.10", "6322.01")]
    assert _removed(spill_cfg) == EXPECTED_REMOVED
    assert _ledger(spill_cfg, "removed") == [(KEY_2025_06, 1)]
    assert any(KEY_2025_06 in line and "removed=1" in line and "conflicts=1" in line for line in lines), lines
    with Manifest(spill_cfg) as m:
        runs = dict(m.con.execute("SELECT source_key, removed_rows FROM source_runs").fetchall())
    assert runs == {KEY_2025_05_P1: 0, KEY_2025_05_P2: 0, KEY_2025_06: 1}


def test_conflicting_and_unpublished_spillover_copies_are_kept(spill_cfg):
    _normalize(spill_cfg, ["2025-05", "2025-06"])
    trips = _trips(spill_cfg)
    assert [t[1] for t in trips if t[0] == "S2"] == [KEY_2025_05_P1, KEY_2025_06]
    assert [t[1] for t in trips if t[0] == "S3"] == [KEY_2025_06]
    assert len(trips) == 3 + 4 - 1


def test_spillover_without_published_start_release_is_kept(spill_cfg):
    _normalize(spill_cfg, ["2025-06"])
    assert _removed(spill_cfg) == []
    assert len(_trips(spill_cfg)) == 4


def test_validate_counts_removed_rows(spill_cfg):
    _normalize(spill_cfg, ["2025-05", "2025-06"])
    with Manifest(spill_cfg) as m:
        res = validate_releases(spill_cfg, m, ["2025-06"])
    assert res["reconciled"] is True
    assert (res["source_rows"], res["published_rows"], res["quarantined_rows"], res["removed_rows"]) == (4, 3, 0, 1)
    assert res["record_removals"] == 1
    assert res["per_source"] == [(KEY_2025_06, 4, 3, 0, 1, 0)]
    text = write_validation_report(spill_cfg, res).read_text()
    assert "- removed rows: 1" in text.splitlines()
    assert f"| {KEY_2025_06} | 4 | 3 | 0 | 1 | 0 |" in text.splitlines()
    with Manifest(spill_cfg) as m:
        both = validate_releases(spill_cfg, m, ["2025-05", "2025-06"])
    assert both["reconciled"] and (both["ride_id_identical"], both["ride_id_conflicting"]) == (0, 1)


def test_removed_rows_missing_from_the_ledger_do_not_reconcile(spill_cfg):
    _normalize(spill_cfg, ["2025-05", "2025-06"])
    with Manifest(spill_cfg) as m:
        m.con.execute("DELETE FROM published_files WHERE dataset = 'removed'")
        res = validate_releases(spill_cfg, m, ["2025-06"])
    assert res["record_removals"] == 1 and res["removed_rows"] == 0
    assert res["unlisted_files"] == 1 and res["reconciled"] is False


def test_legacy_sources_are_never_removed(spill_cfg):
    _normalize(spill_cfg, ["2015-01", "2015-02"])
    assert _removed(spill_cfg) == [] and _ledger(spill_cfg, "removed") == []
    files = [str(p) for p in spill_cfg.trips_dir.rglob("*.parquet")]
    keys = duckdb.connect().execute("SELECT source_key FROM read_parquet(?)", [files]).fetchall()
    assert (KEY_2015_02,) in keys and len(keys) == 3


def test_legacy_source_does_not_look_up_published_trips(spill_cfg):
    from citibike_pipeline.db import connect
    from citibike_pipeline.dst import dst_hours
    from citibike_pipeline.normalize import normalize_source

    def lookup(release):
        raise AssertionError(f"looked up {release}")

    with Manifest(spill_cfg) as m:
        rec = m.get(KEY_2015_02)
    res = normalize_source(spill_cfg, connect(spill_cfg), rec, "r", spill_cfg.staging_dir / "r",
                           dst_hours(range(2013, 2028), spill_cfg.timezone), lookup)
    assert (res.staged_rows, res.removed_rows) == (1, 0)


def test_rerun_is_idempotent_and_retires_removed_files(spill_cfg):
    _normalize(spill_cfg, ["2025-05", "2025-06"])
    first_trips, first_removed = _trips(spill_cfg), _removed(spill_cfg)
    first_files = sorted(p.name for p in spill_cfg.removed_dir.glob("*.parquet"))
    _normalize(spill_cfg, ["2025-05", "2025-06"])
    assert _trips(spill_cfg) == first_trips and _removed(spill_cfg) == first_removed == EXPECTED_REMOVED
    assert _ledger(spill_cfg, "removed") == [(KEY_2025_06, 1)]
    retired = sorted(p.name for p in spill_cfg.retired_dir.rglob("*.parquet") if "removed" in p.parts)
    assert retired == first_files


def test_manifest_adds_removed_rows_to_an_existing_source_runs_table(cfg):
    cfg.manifest_db.parent.mkdir(parents=True)
    con = duckdb.connect(str(cfg.manifest_db))
    con.execute("""CREATE TABLE source_runs (
      run_id VARCHAR, source_key VARCHAR, release VARCHAR, source_rows BIGINT, published_rows BIGINT,
      quarantined_rows BIGINT, status VARCHAR, message VARCHAR, seconds DOUBLE, finished_at TIMESTAMP)""")
    con.execute("INSERT INTO source_runs VALUES ('old', 'k', '2025-01', 1, 1, 0, 'ok', NULL, 1.0, now())")
    con.close()
    with Manifest(cfg) as m:
        m.record_source_run("new", {"source_key": "k", "nominal_release": "2025-01", "rows": 1}, None,
                            "failed", "x", 1.0)
        rows = m.con.execute("SELECT run_id, removed_rows FROM source_runs ORDER BY run_id").fetchall()
    assert rows == [("new", None), ("old", None)]


def _reselect_2025_05(cfg):
    """Adds a 2025-05 copy without S1 and selects it in place of the published parts."""
    from citibike_pipeline.inventory import run_discover
    from citibike_pipeline.profile import run_profile
    from citibike_pipeline.sets import apply_selection

    add_alternative_2025_05(cfg.source_root)
    with Manifest(cfg) as m:
        run_discover(cfg, m, "t-discover-2", QUIET)
        run_profile(cfg, m, "t-profile-2", None, False, 1, QUIET)
        assert apply_selection(m, ["2025-05"], {"2025-05": SET_2025_05_ALT}, False, QUIET) == 0
        assert m.get(KEY_2025_05_ALT)["status"] == "selected"


def _published_copies(cfg, ride_id):
    """Copies of a trip in the trips files the ledger lists for selected sources (what validate reads)."""
    with Manifest(cfg) as m:
        paths = [r[0] for r in m.con.execute("""
            SELECT p.path FROM published_files p JOIN sources s USING (source_key)
            WHERE p.dataset = 'trips' AND s.status = 'selected'""").fetchall()]
    files = [str(cfg.processed_root / p) for p in paths]
    return duckdb.connect().execute("SELECT count(*) FROM read_parquet(?) WHERE ride_id = ?",
                                    [files, ride_id]).fetchone()[0]


def test_reselected_start_month_and_later_release_in_one_run_keep_the_trip_once(spill_cfg):
    _normalize(spill_cfg, ["2025-05", "2025-06"])
    _reselect_2025_05(spill_cfg)
    _normalize(spill_cfg, ["2025-05", "2025-06"])
    assert _published_copies(spill_cfg, "S1") == 1
    assert [t[1] for t in _trips(spill_cfg) if t[0] == "S1"] == [KEY_2025_06]
    assert _ledger(spill_cfg, "removed") == []
    with Manifest(spill_cfg) as m:
        assert validate_releases(spill_cfg, m, ["2025-05", "2025-06"])["reconciled"] is True


def test_later_release_alone_ignores_a_deselected_start_month_source(spill_cfg):
    _normalize(spill_cfg, ["2025-05", "2025-06"])
    _reselect_2025_05(spill_cfg)
    _normalize(spill_cfg, ["2025-06"])
    assert _published_copies(spill_cfg, "S1") == 1
    assert _ledger(spill_cfg, "removed") == []


def test_removal_whose_kept_copy_was_unpublished_is_orphaned(spill_cfg):
    _normalize(spill_cfg, ["2025-05", "2025-06"])
    _reselect_2025_05(spill_cfg)
    _normalize(spill_cfg, ["2025-05"])  # 2025-06 still points at the retired copy in part 2
    with Manifest(spill_cfg) as m:
        res = validate_releases(spill_cfg, m, ["2025-06"])
    assert res["orphaned_removals"] == 1 and res["orphaned_ride_ids"] == ["S1"]
    assert res["reconciled"] is False
    text = write_validation_report(spill_cfg, res).read_text()
    assert "- orphaned removals (kept copy no longer published): 1" in text.splitlines()
    assert "  - S1" in text.splitlines()


def test_removal_whose_kept_file_left_the_ledger_is_orphaned(spill_cfg, tmp_path):
    import os

    _normalize(spill_cfg, ["2025-05", "2025-06"])
    with Manifest(spill_cfg) as m:
        path = m.published_for_sources([KEY_2025_05_P2])[0]["path"]
        m.con.execute("DELETE FROM published_files WHERE source_key = ?", [KEY_2025_05_P2])
        os.replace(spill_cfg.processed_root / path, tmp_path / "aside.parquet")  # moved, not deleted
        res = validate_releases(spill_cfg, m, ["2025-06"])
        ok = validate_releases(spill_cfg, m, ["2015-01"])
    assert res["orphaned_removals"] == 1 and res["reconciled"] is False
    assert ok["orphaned_removals"] == 0


def test_validated_removals_with_live_kept_copy_are_not_orphaned(spill_cfg):
    _normalize(spill_cfg, ["2025-05", "2025-06"])
    with Manifest(spill_cfg) as m:
        res = validate_releases(spill_cfg, m, ["2025-06"])
    assert res["orphaned_removals"] == 0 and res["reconciled"] is True
    assert "- orphaned removals (kept copy no longer published): 0" in \
        write_validation_report(spill_cfg, res).read_text().splitlines()


def test_per_source_row_mismatch_blocks_reconciliation(spill_cfg):
    _normalize(spill_cfg, ["2025-05"])
    with Manifest(spill_cfg) as m:
        # Totals still balance (+1 on one part, -1 on the other), so only the per-source check sees it.
        for key, delta in ((KEY_2025_05_P1, 1), (KEY_2025_05_P2, -1)):
            rec = m.get(key)
            rec["rows"] += delta
            m.upsert(rec)
        res = validate_releases(spill_cfg, m, ["2025-05"])
    assert res["record_removals"] == res["removed_rows"] == 0
    assert res["row_mismatches"] == [KEY_2025_05_P1, KEY_2025_05_P2]
    assert res["reconciled"] is False
    text = write_validation_report(spill_cfg, res).read_text()
    assert "- per-source row mismatches (profiled != published + quarantined + removed): 2" in text.splitlines()
