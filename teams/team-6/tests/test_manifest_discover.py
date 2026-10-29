from archive_fixture import EXPECTED_KEYS, KEY_2016, KEY_2018_WHOLE, KEY_UNKNOWN, QUIET
from citibike_pipeline.inventory import run_discover
from citibike_pipeline.manifest import Manifest


def test_discover_populates_manifest(cfg):
    with Manifest(cfg) as m:
        stats = run_discover(cfg, m, "r1", QUIET)
        recs = {r["source_key"]: r for r in m.all()}
    assert stats["new"] == len(EXPECTED_KEYS) and set(recs) == EXPECTED_KEYS
    whole = recs[KEY_2018_WHOLE]
    assert whole["schema_family"] == "legacy_lower"
    assert (whole["nominal_release"], whole["nominal_part"]) == ("2018-04", None)
    assert whole["status"] == "discovered" and whole["discovered_run"] == "r1"
    assert recs[KEY_2016]["schema_family"] == "legacy_title"
    assert recs[KEY_UNKNOWN]["schema_family"] == "unknown"


def test_rediscovery_is_resumable(cfg):
    with Manifest(cfg) as m:
        run_discover(cfg, m, "r1", QUIET)
        stats = run_discover(cfg, m, "r2", QUIET)
    assert stats["unchanged"] == len(EXPECTED_KEYS) and stats.get("new", 0) == 0


def test_vanished_source_is_marked_missing(cfg):
    with Manifest(cfg) as m:
        run_discover(cfg, m, "r1", QUIET)
        (cfg.source_root / KEY_UNKNOWN).rename(cfg.source_root / "201902-renamed.txt")
        stats = run_discover(cfg, m, "r2", QUIET)
        assert stats["missing"] == 1
        assert m.get(KEY_UNKNOWN)["status"] == "missing"


def test_runs_are_recorded(cfg):
    with Manifest(cfg) as m:
        run_id = m.start_run("inventory discover", {"x": 1})
        m.finish_run(run_id, "ok")
        row = m.con.execute("SELECT status, finished_at IS NOT NULL FROM runs WHERE run_id = ?", [run_id]).fetchone()
    assert row == ("ok", True)


def test_rediscovery_refreshes_nominal_release_of_unchanged_source(cfg):
    with Manifest(cfg) as m:
        run_discover(cfg, m, "r1", QUIET)
        rec = m.get(KEY_2018_WHOLE)
        rec.update(nominal_release=None, nominal_part=None, status_reason="unrecognized file name")
        m.upsert(rec)
        stats = run_discover(cfg, m, "r2", QUIET)
        refreshed = m.get(KEY_2018_WHOLE)
    assert stats["renamed"] == 1
    assert (refreshed["nominal_release"], refreshed["nominal_part"]) == ("2018-04", None)
    assert refreshed["status_reason"] is None and refreshed["status"] == "discovered"
