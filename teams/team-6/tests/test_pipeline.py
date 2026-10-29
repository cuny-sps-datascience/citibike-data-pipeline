import duckdb

from archive_fixture import KEY_2018_P2, KEY_2025_CSV, KEY_2025_ZIP, QUIET
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.pipeline import run_normalize


def _trip_count(cfg):
    files = [str(p) for p in cfg.trips_dir.rglob("*.parquet")]
    if not files:
        return 0
    return duckdb.connect().execute("SELECT count(*) FROM read_parquet(?)", [files]).fetchone()[0]


def _normalize(cfg, releases, **kw):
    with Manifest(cfg) as m:
        return run_normalize(cfg, m, releases, out=QUIET, **kw)


def test_normalize_publishes_and_reconciles(selected_cfg):
    assert _normalize(selected_cfg, ["2018-04", "2025-01"]) == 0
    assert _trip_count(selected_cfg) == 5
    assert len(list(selected_cfg.quarantine_dir.glob("*.parquet"))) == 1
    with Manifest(selected_cfg) as m:
        runs = m.con.execute("SELECT status, count(*) FROM source_runs GROUP BY ALL").fetchall()
    assert runs == [("ok", 3)]


def test_rerun_is_idempotent(selected_cfg):
    _normalize(selected_cfg, ["2018-04", "2025-01"])
    first = sorted(p.name for p in selected_cfg.trips_dir.rglob("*.parquet"))
    assert _normalize(selected_cfg, ["2018-04", "2025-01"]) == 0
    assert _trip_count(selected_cfg) == 5
    second = sorted(p.name for p in selected_cfg.trips_dir.rglob("*.parquet"))
    assert len(second) == len(first) and set(second).isdisjoint(first)
    assert sorted(p.name for p in selected_cfg.retired_dir.rglob("*.parquet") if "trips" in p.parts) == first


def test_dry_run_writes_nothing(selected_cfg):
    assert _normalize(selected_cfg, ["2025-01"], dry_run=True) == 0
    assert not selected_cfg.trips_dir.exists()


def test_release_without_selection_fails(selected_cfg):
    assert _normalize(selected_cfg, ["2025-01", "2019-02"]) == 1
    assert not selected_cfg.trips_dir.exists()


def test_selection_change_retires_previous_source(selected_cfg):
    _normalize(selected_cfg, ["2025-01"])
    with Manifest(selected_cfg) as m:
        for key, status in ((KEY_2025_CSV, "excluded"), (KEY_2025_ZIP, "selected")):
            rec = m.get(key)
            rec["status"] = status
            m.upsert(rec)
    assert _normalize(selected_cfg, ["2025-01"]) == 0
    assert _trip_count(selected_cfg) == 3
    with Manifest(selected_cfg) as m:
        assert m.published_keys_for_releases(["2025-01"]) == [KEY_2025_ZIP]
    retired = [p.name for p in selected_cfg.retired_dir.rglob("*.parquet") if "trips" in p.parts]
    assert retired and all(p.startswith("202501-citibike-tripdata_1-") for p in retired)
    assert all(p.name not in retired for p in selected_cfg.trips_dir.rglob("*.parquet"))


def test_failed_reselection_keeps_previous_publication(selected_cfg, monkeypatch):
    import citibike_pipeline.pipeline as pipeline_mod
    from citibike_pipeline.normalize import NormalizeError

    _normalize(selected_cfg, ["2025-01"])
    with Manifest(selected_cfg) as m:
        ledger_before = m.con.execute("SELECT * FROM published_files ORDER BY path").fetchall()
        for key, status in ((KEY_2025_CSV, "excluded"), (KEY_2025_ZIP, "selected")):
            rec = m.get(key)
            rec["status"] = status
            m.upsert(rec)
    trips_before = sorted(p.name for p in selected_cfg.trips_dir.rglob("*.parquet"))

    def boom(*_args, **_kwargs):
        raise NormalizeError("simulated failure of the newly selected source")

    monkeypatch.setattr(pipeline_mod, "normalize_source", boom)
    assert _normalize(selected_cfg, ["2025-01"]) == 1
    with Manifest(selected_cfg) as m:
        assert m.published_keys_for_releases(["2025-01"]) == [KEY_2025_CSV]
        assert m.con.execute("SELECT * FROM published_files ORDER BY path").fetchall() == ledger_before
    assert sorted(p.name for p in selected_cfg.trips_dir.rglob("*.parquet")) == trips_before


def test_interrupted_publish_is_rolled_back(selected_cfg):
    stray = selected_cfg.trips_dir / "trip_year=2025" / "trip_month=01" / "stray__x__0.parquet"
    stray.parent.mkdir(parents=True)
    duckdb.connect().execute(f"COPY (SELECT 1 AS a) TO '{stray}' (FORMAT parquet)")
    assert _normalize(selected_cfg, ["2025-01"]) == 0
    assert not stray.exists() and _trip_count(selected_cfg) == 3
    assert any(p.name == stray.name for p in selected_cfg.retired_dir.rglob("*.parquet"))


def test_failed_source_is_not_published(selected_cfg):
    import dataclasses

    strict = dataclasses.replace(selected_cfg, max_quarantine_fraction=0.1)
    assert _normalize(strict, ["2018-04"]) == 1
    with Manifest(strict) as m:
        statuses = dict(m.con.execute("SELECT source_key, status FROM source_runs").fetchall())
    assert "failed" in statuses.values()
    trips = list(strict.trips_dir.rglob("*.parquet")) if strict.trips_dir.exists() else []
    assert all("201804-citibike-tripdata_1" not in p.name for p in trips)


def test_partial_publish_failure_retires_moved_files(selected_cfg, monkeypatch):
    import os

    calls = {"n": 0}
    real_replace = os.replace

    def flaky_replace(src, dst):
        calls["n"] += 1
        if calls["n"] == 2:
            raise OSError("simulated failure on 2nd move")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", flaky_replace)
    assert _normalize(selected_cfg, ["2025-01"]) == 1
    with Manifest(selected_cfg) as m:
        statuses = [r[0] for r in m.con.execute("SELECT status FROM source_runs").fetchall()]
    assert statuses == ["failed"]
    trips = list(selected_cfg.trips_dir.rglob("*.parquet")) if selected_cfg.trips_dir.exists() else []
    assert trips == []


def test_cleanup_failure_does_not_change_source_status(selected_cfg, monkeypatch):
    import citibike_pipeline.pipeline as pipeline_mod

    assert _normalize(selected_cfg, ["2025-01"]) == 0

    def boom(cfg, manifest, run_id):
        raise OSError("simulated retire failure")

    monkeypatch.setattr(pipeline_mod, "retire_unlisted", boom)
    lines = []
    with Manifest(selected_cfg) as m:
        assert run_normalize(selected_cfg, m, ["2025-01"], out=lines.append) == 1
    assert lines[-1].endswith(f": 1 ok, 0 failed, {sum('cleanup failed' in x for x in lines)} cleanup failures")
    assert "cleanup failed" in lines[0]
    with Manifest(selected_cfg) as m:
        run_id = m.con.execute("SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()[0]
        run_status = m.con.execute("SELECT status FROM runs WHERE run_id = ?", [run_id]).fetchone()[0]
        source_status = m.con.execute(
            "SELECT status FROM source_runs ORDER BY finished_at DESC LIMIT 1"
        ).fetchone()[0]
        run_ids = {r[0] for r in m.con.execute("SELECT DISTINCT run_id FROM published_files").fetchall()}
    assert source_status == "ok"
    assert run_status == "cleanup_failed"
    assert run_ids == {run_id}


def test_partly_selected_set_is_not_normalized_or_validated(selected_cfg):
    from citibike_pipeline.validate import validate_releases, write_validation_report

    assert _normalize(selected_cfg, ["2018-04"]) == 0
    with Manifest(selected_cfg) as m:
        ledger_before = m.con.execute("SELECT * FROM published_files ORDER BY path").fetchall()
        rec = m.get(KEY_2018_P2)
        rec["status"] = "discovered"
        m.upsert(rec)
    trips_before = sorted(p.name for p in selected_cfg.trips_dir.rglob("*.parquet"))
    lines = []
    with Manifest(selected_cfg) as m:
        assert run_normalize(selected_cfg, m, ["2018-04"], out=lines.append) == 1
        assert m.con.execute("SELECT * FROM published_files ORDER BY path").fetchall() == ledger_before
        assert m.con.execute("SELECT count(*) FROM runs WHERE command = 'normalize'").fetchone()[0] == 1
        res = validate_releases(selected_cfg, m, ["2018-04"])
    assert sorted(p.name for p in selected_cfg.trips_dir.rglob("*.parquet")) == trips_before
    assert any("2018-04" in line for line in lines)
    assert res["reconciled"] is False and res["selection_complete"] is False
    assert res["selection_problems"]
    report = write_validation_report(selected_cfg, res).read_text()
    assert "selection_complete: False" in report and res["selection_problems"][0] in report


def test_failed_source_staging_is_moved_to_retired(selected_cfg):
    import dataclasses

    from archive_fixture import KEY_2018_P1
    from citibike_pipeline.normalize import slug_for

    strict = dataclasses.replace(selected_cfg, max_quarantine_fraction=0.1)
    assert _normalize(strict, ["2018-04"]) == 1
    with Manifest(strict) as m:
        run_id = m.con.execute("SELECT run_id FROM runs WHERE command = 'normalize'").fetchone()[0]
    slug = slug_for(KEY_2018_P1)
    assert not (strict.staging_dir / run_id / slug).exists()
    assert (strict.retired_dir / run_id / "staging" / slug / "quarantine.parquet").exists()


def test_dst_years_follow_the_releases(selected_cfg, monkeypatch):
    import citibike_pipeline.pipeline as pipeline_mod

    seen = []
    real = pipeline_mod.dst_hours

    def spy(years, tz):
        seen.append(list(years))
        return real(years, tz)

    monkeypatch.setattr(pipeline_mod, "dst_hours", spy)
    assert _normalize(selected_cfg, ["2015-01", "2025-01"]) == 0
    assert seen == [list(range(2013, 2028))]  # never narrower than the published run's range


def test_second_process_cannot_open_the_manifest(selected_cfg):
    import subprocess
    import sys

    probe = (
        "import sys, duckdb\n"
        "from citibike_pipeline.config import load_config\n"
        "from citibike_pipeline.manifest import Manifest\n"
        "try:\n"
        "    Manifest(load_config(sys.argv[1]))\n"
        "except duckdb.IOException:\n"
        "    print('IOException')\n"
        "else:\n"
        "    print('opened')\n"
    )
    with Manifest(selected_cfg):
        result = subprocess.run([sys.executable, "-c", probe, str(selected_cfg.processed_root.parent / "local.yaml")],
                                capture_output=True, text=True, timeout=60)
    if result.stdout.strip() == "opened":
        import pytest

        pytest.skip("DuckDB on this platform let a second process open the manifest")
    assert result.stdout.strip() == "IOException", result.stderr


def test_dst_years_extend_beyond_the_default_range(selected_cfg, monkeypatch):
    import citibike_pipeline.pipeline as pipeline_mod

    seen = []
    monkeypatch.setattr(pipeline_mod, "dst_hours", lambda years, tz: seen.append(list(years)) or {})
    monkeypatch.setattr(pipeline_mod, "selection_problems", lambda records, release: [])
    with Manifest(selected_cfg) as m:
        rec = m.get(KEY_2025_CSV)
        rec["nominal_release"] = "2030-01"
        m.upsert(rec)
    _normalize(selected_cfg, ["2030-01"])
    assert seen == [list(range(2013, 2032))]


def test_changed_set_with_a_failed_member_does_not_reconcile(selected_cfg, monkeypatch):
    import citibike_pipeline.pipeline as pipeline_mod
    from archive_fixture import KEY_2018_P1, KEY_2018_WHOLE
    from citibike_pipeline.normalize import NormalizeError
    from citibike_pipeline.validate import validate_releases

    def set_statuses(statuses):
        with Manifest(selected_cfg) as m:
            for key, status in statuses:
                rec = m.get(key)
                rec.update(status=status, status_reason="manual choice" if status == "selected" else "x")
                m.upsert(rec)

    set_statuses([(KEY_2018_WHOLE, "selected"), (KEY_2018_P1, "excluded"), (KEY_2018_P2, "excluded")])
    assert _normalize(selected_cfg, ["2018-04"]) == 0
    set_statuses([(KEY_2018_WHOLE, "excluded"), (KEY_2018_P1, "selected"), (KEY_2018_P2, "selected")])
    real = pipeline_mod.normalize_source

    def fail_p2(cfg, con, rec, *args):
        if rec["source_key"] == KEY_2018_P2:
            raise NormalizeError("simulated failure of one member")
        return real(cfg, con, rec, *args)

    monkeypatch.setattr(pipeline_mod, "normalize_source", fail_p2)
    assert _normalize(selected_cfg, ["2018-04"]) == 1
    with Manifest(selected_cfg) as m:
        res = validate_releases(selected_cfg, m, ["2018-04"])
    assert res["reconciled"] is False
    assert KEY_2018_WHOLE in res["stale_ledger_sources"]
