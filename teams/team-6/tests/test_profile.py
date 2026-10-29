from archive_fixture import (
    EXPECTED_KEYS, KEY_2018_P1, KEY_2018_P2, KEY_2018_WHOLE, KEY_2023_NESTED, KEY_2025_CSV,
    KEY_2025_ZIP, KEY_UNKNOWN, QUIET,
)
from citibike_pipeline.inventory import run_discover
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.profile import MASK, run_profile


def _profile(cfg, **kw):
    with Manifest(cfg) as m:
        run_discover(cfg, m, "d", QUIET)
        counts = run_profile(cfg, m, "p", releases=None, force=False, workers=kw.get("workers", 1), out=QUIET)
        return counts, {r["source_key"]: r for r in m.all()}


def test_profile_counts(cfg):
    counts, recs = _profile(cfg)
    assert counts == {"profiled": len(EXPECTED_KEYS)}
    whole = recs[KEY_2018_WHOLE]
    assert (whole["rows"], whole["malformed_rows"], whole["invalid_start_rows"]) == (3, 0, 1)
    assert (whole["min_start_date"], whole["max_start_date"]) == ("2018-04-01", "2018-04-02")
    nested = recs[KEY_2023_NESTED]
    assert (nested["rows"], nested["malformed_rows"]) == (5, 1)


def test_whole_file_digest_equals_sum_of_parts(cfg):
    _, recs = _profile(cfg)
    parts = (int(recs[KEY_2018_P1]["row_digest"], 16) + int(recs[KEY_2018_P2]["row_digest"], 16)) & MASK
    assert int(recs[KEY_2018_WHOLE]["row_digest"], 16) == parts
    assert recs[KEY_2018_WHOLE]["sha256"] != recs[KEY_2018_P1]["sha256"]


def test_identical_copies_share_sha_and_duplicate_group(cfg):
    _, recs = _profile(cfg)
    assert recs[KEY_2025_CSV]["sha256"] == recs[KEY_2025_ZIP]["sha256"]
    assert recs[KEY_2025_CSV]["duplicate_group"] == recs[KEY_2025_CSV]["sha256"][:12]
    assert recs[KEY_2018_WHOLE]["duplicate_group"] is None


def test_unknown_family_is_counted_without_dates(cfg):
    _, recs = _profile(cfg)
    assert recs[KEY_UNKNOWN]["rows"] == 1 and recs[KEY_UNKNOWN]["min_start_date"] is None


def test_profile_resumes_and_force_reprofiles(cfg):
    with Manifest(cfg) as m:
        run_discover(cfg, m, "d", QUIET)
        run_profile(cfg, m, "p1", None, False, 1, QUIET)
        assert run_profile(cfg, m, "p2", None, False, 1, QUIET) == {}
        assert run_profile(cfg, m, "p3", ["2018-04"], True, 1, QUIET) == {"profiled": 3}


def test_parallel_profile_matches(cfg, tmp_path):
    import dataclasses

    from citibike_pipeline.profile import PROFILE_FIELDS

    counts, parallel = _profile(cfg, workers=2)
    assert counts == {"profiled": len(EXPECTED_KEYS)}
    _, serial = _profile(dataclasses.replace(cfg, processed_root=tmp_path / "serial"), workers=1)
    assert set(parallel) == set(serial) == EXPECTED_KEYS
    for key in EXPECTED_KEYS:
        assert {f: parallel[key][f] for f in PROFILE_FIELDS} == {f: serial[key][f] for f in PROFILE_FIELDS}, key
    assert parallel[KEY_2018_WHOLE]["rows"] == 3


def test_missing_sources_are_left_out_of_duplicate_groups(cfg):
    with Manifest(cfg) as m:
        run_discover(cfg, m, "d", QUIET)
        run_profile(cfg, m, "p", None, False, 1, QUIET)
        rec = m.get(KEY_2025_ZIP)
        rec.update(status="missing")
        m.upsert(rec)
        m.assign_duplicate_groups()
        assert m.get(KEY_2025_ZIP)["duplicate_group"] is None
        assert m.get(KEY_2025_CSV)["duplicate_group"] is None


def test_profile_failure_keeps_short_reason_and_logs_traceback(cfg, monkeypatch):
    import citibike_pipeline.profile as profile_mod
    from citibike_pipeline.normalize import slug_for

    def boom(*_args, **_kwargs):
        raise OSError("simulated read failure")

    monkeypatch.setattr(profile_mod, "profile_candidate", boom)
    with Manifest(cfg) as m:
        run_discover(cfg, m, "d", QUIET)
        run_profile(cfg, m, "p-fail", ["2018-04"], False, 1, QUIET)
        rec = m.get(KEY_2018_P1)
    log = cfg.reports_dir / "profile_failures" / "p-fail" / f"{slug_for(KEY_2018_P1)}.txt"
    assert rec["status"] == "failed"
    assert rec["status_reason"] == f"OSError: simulated read failure (traceback: {log})"
    text = log.read_text()
    assert "Traceback (most recent call last)" in text and "simulated read failure" in text


def test_profile_failure_log_write_error_falls_back_to_short_reason(cfg, monkeypatch):
    from pathlib import Path

    import citibike_pipeline.profile as profile_mod

    def boom(*_args, **_kwargs):
        raise OSError("simulated read failure")

    real_write = Path.write_text

    def no_write(self, *args, **kwargs):
        if "profile_failures" in self.parts:
            raise OSError("disk full")
        return real_write(self, *args, **kwargs)

    monkeypatch.setattr(profile_mod, "profile_candidate", boom)
    monkeypatch.setattr(Path, "write_text", no_write)
    with Manifest(cfg) as m:
        run_discover(cfg, m, "d", QUIET)
        run_profile(cfg, m, "p-nolog", ["2018-04"], False, 1, QUIET)
        rec = m.get(KEY_2018_P1)
    assert rec["status"] == "failed"
    assert rec["status_reason"] == "OSError: simulated read failure (traceback not saved: disk full)"
