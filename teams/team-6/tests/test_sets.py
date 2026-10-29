import copy

from archive_fixture import (
    KEY_2018_P1, KEY_2018_P2, KEY_2018_WHOLE, KEY_2025_CSV, QUIET,
)
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.profile import run_profile
from citibike_pipeline.sets import apply_selection, build_sets, decide_release, selection_problems


def _records(cfg):
    with Manifest(cfg) as m:
        return m.all()


def _decide(records, release):
    families = {r["source_key"]: r["schema_family"] for r in records}
    return decide_release(release, build_sets(records)[release], families)


def test_2018_parts_set_is_chosen_over_whole_copy(profiled_cfg):
    d = _decide(_records(profiled_cfg), "2018-04")
    assert d.status == "selectable"
    assert d.chosen.members == (KEY_2018_P1, KEY_2018_P2)
    assert [s.members for s in d.equivalent] == [(KEY_2018_WHOLE,)]


def test_plain_csv_preferred_over_identical_zip_member(profiled_cfg):
    assert _decide(_records(profiled_cfg), "2025-01").chosen.members == (KEY_2025_CSV,)


def test_unknown_schema_is_not_selectable(profiled_cfg):
    assert _decide(_records(profiled_cfg), "2019-02").status == "unknown_schema"


def test_differing_content_is_a_conflict(profiled_cfg):
    records = copy.deepcopy(_records(profiled_cfg))
    next(r for r in records if r["source_key"] == KEY_2018_WHOLE)["row_digest"] = "0" * 16
    assert _decide(records, "2018-04").status == "conflict"


def test_unprofiled_release(cfg):
    from citibike_pipeline.inventory import run_discover

    with Manifest(cfg) as m:
        run_discover(cfg, m, "d", QUIET)
        assert _decide(m.all(), "2018-04").status == "unprofiled"


def test_parts_gap_detected():
    base = {"kind": "csv", "container": None, "member": None, "status": "profiled",
            "nominal_release": "2020-01", "rows": 1, "row_digest": "0" * 16}
    recs = [dict(base, source_key=f"d/202001-citibike-tripdata_{p}.csv", path=f"d/202001-citibike-tripdata_{p}.csv",
                 nominal_part=p) for p in (1, 3)]
    assert build_sets(recs)["2020-01"][0].parts_gap is True


def test_apply_selection_marks_statuses(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        assert apply_selection(m, ["2018-04"], {}, False, QUIET) == 0
        assert m.get(KEY_2018_P1)["status"] == "selected"
        assert m.get(KEY_2018_P2)["status"] == "selected"
        whole = m.get(KEY_2018_WHOLE)
        assert whole["status"] == "excluded" and whole["status_reason"].startswith("equivalent_to:")


def test_dry_run_changes_nothing(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        before = m.all()
        assert apply_selection(m, ["2018-04"], {}, True, QUIET) == 0
        assert m.all() == before


def test_unselectable_release_returns_1(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        assert apply_selection(m, ["2019-02", "2030-01"], {}, False, QUIET) == 1


def test_manual_override_on_conflict(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        whole = m.get(KEY_2018_WHOLE)
        whole["row_digest"] = "0" * 16
        m.upsert(whole)
        set_id = "2018-citibike-tripdata.zip::2018-citibike-tripdata|2018-04|whole"
        assert apply_selection(m, ["2018-04"], {"2018-04": set_id}, False, QUIET) == 0
        assert m.get(KEY_2018_WHOLE)["status"] == "selected"
        assert m.get(KEY_2018_P1)["status_reason"] == f"manual_override:{set_id}"


def test_selection_survives_reprofile(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        apply_selection(m, ["2018-04"], {}, False, QUIET)
        run_profile(profiled_cfg, m, "again", ["2018-04"], True, 1, QUIET)
        assert m.get(KEY_2018_P1)["status"] == "selected"


def test_missing_part_creates_gap():
    """A set with a missing part record has parts_gap=True and excludes the missing key."""
    base = {"kind": "csv", "container": None, "member": None,
            "nominal_release": "2020-01", "rows": 1, "row_digest": "0" * 16,
            "source_key": "d/202001-citibike-tripdata_1.csv", "path": "d/202001-citibike-tripdata_1.csv",
            "nominal_part": 1, "status": "profiled"}
    missing = dict(base, source_key="d/202001-citibike-tripdata_2.csv",
                   path="d/202001-citibike-tripdata_2.csv", nominal_part=2, status="missing")
    recs = [base, missing]
    sets = build_sets(recs)["2020-01"]
    assert len(sets) == 1
    assert sets[0].parts_gap is True
    assert sets[0].members == ("d/202001-citibike-tripdata_1.csv",)


def test_incomplete_parts_not_selectable():
    """A release whose only set has a parts gap is not selectable."""
    base = {"kind": "csv", "container": None, "member": None,
            "nominal_release": "2020-01", "rows": 1, "row_digest": "0" * 16,
            "source_key": "d/202001-citibike-tripdata_1.csv", "path": "d/202001-citibike-tripdata_1.csv",
            "nominal_part": 1, "status": "profiled", "schema_family": "legacy"}
    missing = dict(base, source_key="d/202001-citibike-tripdata_2.csv",
                   path="d/202001-citibike-tripdata_2.csv", nominal_part=2, status="missing")
    recs = [base, missing]
    families = {r["source_key"]: r["schema_family"] for r in recs if r["status"] != "missing"}
    decision = decide_release("2020-01", build_sets(recs)["2020-01"], families)
    assert decision.status == "incomplete_parts"
    assert decision.chosen is None


def _with_status(records, key, status, reason=None):
    out = copy.deepcopy(records)
    rec = next(r for r in out if r["source_key"] == key)
    rec.update(status=status, status_reason=reason)
    return out


def test_selection_problems_complete_set_is_ok(selected_cfg):
    assert selection_problems(_records(selected_cfg), "2018-04") == []


def test_selection_problems_partial_set(selected_cfg):
    records = _with_status(_records(selected_cfg), KEY_2018_P2, "discovered")
    problems = selection_problems(records, "2018-04")
    assert len(problems) == 1 and "2018-04" in problems[0]


def test_selection_problems_mixed_sets(selected_cfg):
    records = _with_status(_records(selected_cfg), KEY_2018_WHOLE, "selected", "manual choice")
    assert selection_problems(records, "2018-04")


def test_selection_problems_nothing_selected(profiled_cfg):
    assert selection_problems(_records(profiled_cfg), "2018-04")


def test_selection_problems_parts_gap():
    base = {"kind": "csv", "container": None, "member": None, "nominal_release": "2020-01", "rows": 1,
            "row_digest": "0" * 16, "source_key": "d/202001-citibike-tripdata_1.csv",
            "path": "d/202001-citibike-tripdata_1.csv", "nominal_part": 1, "status": "selected",
            "status_reason": "representative of 1 equivalent set(s)"}
    missing = dict(base, source_key="d/202001-citibike-tripdata_2.csv", path="d/202001-citibike-tripdata_2.csv",
                   nominal_part=2, status="missing", status_reason="not found during discovery")
    assert selection_problems([base, missing], "2020-01")
    manual = dict(base, status_reason="manual choice")
    assert selection_problems([manual, missing], "2020-01") == []


def test_discovered_or_failed_member_makes_release_unprofiled(profiled_cfg):
    for status in ("discovered", "failed"):
        records = _with_status(_records(profiled_cfg), KEY_2018_P2, status)
        assert _decide(records, "2018-04").status == "unprofiled"


def test_failed_reprofile_clears_results_and_blocks_selection(profiled_cfg, monkeypatch):
    import citibike_pipeline.profile as profile_mod

    def boom(*_args, **_kwargs):
        raise OSError("simulated read failure")

    monkeypatch.setattr(profile_mod, "profile_candidate", boom)
    with Manifest(profiled_cfg) as m:
        run_profile(profiled_cfg, m, "again", ["2018-04"], True, 1, QUIET)
        rec = m.get(KEY_2018_P1)
        assert rec["status"] == "failed"
        cleared = ("sha256", "row_digest", "rows", "malformed_rows", "invalid_start_rows",
                   "min_start_date", "max_start_date", "day_counts_json")
        assert {c: rec[c] for c in cleared} == dict.fromkeys(cleared)
        assert _decide(m.all(), "2018-04").status == "unprofiled"
        assert apply_selection(m, ["2018-04"], {}, False, QUIET) == 1
        assert [r for r in m.all() if r["nominal_release"] == "2018-04" and r["status"] == "selected"] == []


def test_selection_problems_unprofiled_member_even_when_manual(cfg):
    from citibike_pipeline.inventory import run_discover

    set_id = "2018-citibike-tripdata.zip::2018-citibike-tripdata/4_April|2018-04|parts"
    with Manifest(cfg) as m:
        run_discover(cfg, m, "d", QUIET)
        assert apply_selection(m, ["2018-04"], {"2018-04": set_id}, False, QUIET) == 0
        records = m.all()
    assert next(r for r in records if r["source_key"] == KEY_2018_P1)["status_reason"] == "manual choice"
    problems = selection_problems(records, "2018-04")
    assert len(problems) == 1 and "not fully profiled" in problems[0]
