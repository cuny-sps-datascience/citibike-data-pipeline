from archive_fixture import EXPECTED_KEYS, KEY_2025_CSV, KEY_2025_ZIP
from citibike_pipeline.discover import discover, open_candidate


def _archive_state(root):
    return {p: (p.stat().st_size, p.stat().st_mtime) for p in root.rglob("*") if p.is_file()}


def test_discovers_expected_candidates(cfg):
    assert {c.source_key for c in discover(cfg)} == EXPECTED_KEYS


def test_skips_excluded_hidden_and_junk(cfg):
    for key in (c.source_key for c in discover(cfg)):
        assert "__MACOSX" not in key and ".DS_Store" not in key
        assert "citibike-production" not in key and ".hidden" not in key


def test_nested_member_is_readable(cfg):
    nested = next(c for c in discover(cfg) if c.kind == "nested_zip_member")
    assert nested.container == "2023-citibike-tripdata.zip::2023-citibike-tripdata/202311-citibike-tripdata.zip"
    with open_candidate(cfg, nested) as fh:
        assert fh.readline().startswith(b"ride_id")


def test_nested_extraction_is_cached_and_archive_untouched(cfg):
    before = _archive_state(cfg.source_root)
    list(discover(cfg))
    cached = list(cfg.nested_dir.rglob("*.zip"))
    list(discover(cfg))
    assert _archive_state(cfg.source_root) == before
    assert len(cached) == 1 and cached[0].with_suffix(".zip.done").exists()
    assert list(cfg.nested_dir.rglob("*.zip")) == cached


def test_sizes(cfg):
    by_key = {c.source_key: c for c in discover(cfg)}
    assert by_key[KEY_2025_CSV].size == by_key[KEY_2025_ZIP].size
    zip_path = cfg.source_root / "202501-citibike-tripdata.zip"
    assert by_key[KEY_2025_ZIP].file_size == zip_path.stat().st_size


def _nested_cache(cfg):
    (cached,) = cfg.nested_dir.rglob("*.zip")
    return cached


def test_changed_outer_zip_is_re_extracted(cfg):
    import json
    import os

    list(discover(cfg))
    cached = _nested_cache(cfg)
    inode = cached.stat().st_ino
    outer = cfg.source_root / "2023-citibike-tripdata.zip"
    os.utime(outer, (outer.stat().st_atime, outer.stat().st_mtime + 60))
    before = _archive_state(cfg.source_root)
    list(discover(cfg))
    assert _archive_state(cfg.source_root) == before
    assert _nested_cache(cfg).stat().st_ino != inode
    marker = json.loads(cached.with_suffix(".zip.done").read_text())
    assert marker == {"outer_size": outer.stat().st_size, "outer_mtime": outer.stat().st_mtime}


def test_unchanged_outer_zip_is_not_re_extracted(cfg):
    list(discover(cfg))
    inode = _nested_cache(cfg).stat().st_ino
    list(discover(cfg))
    assert _nested_cache(cfg).stat().st_ino == inode


def test_legacy_done_marker_is_upgraded_without_re_extracting(cfg):
    import json

    list(discover(cfg))
    cached = _nested_cache(cfg)
    inode = cached.stat().st_ino
    done = cached.with_suffix(".zip.done")
    done.write_text("ok\n")
    list(discover(cfg))
    assert cached.stat().st_ino == inode
    assert set(json.loads(done.read_text())) == {"outer_size", "outer_mtime"}


def test_same_named_inner_zips_in_different_outer_zips_get_distinct_caches(cfg):
    from archive_fixture import csv_bytes, zip_bytes
    from citibike_pipeline.schemas import MODERN

    row = ["Z9", "classic_bike", "2023-11-08 08:00:00", "2023-11-08 08:10:00", "E 33 St & 5 Ave", "6322.01",
           "W 24 St & 7 Ave", "6257.03", "40.74765947", "-73.98490707", "40.74487634", "-73.99529885", "member"]
    inner = zip_bytes({"202311-citibike-tripdata_1.csv": csv_bytes(MODERN, [row])})
    (cfg.source_root / "copy").mkdir()
    (cfg.source_root / "copy" / "2023-citibike-tripdata.zip").write_bytes(
        zip_bytes({"2023-citibike-tripdata/202311-citibike-tripdata.zip": inner}))
    nested = [c for c in discover(cfg) if c.kind == "nested_zip_member"]
    assert len(nested) == 2 and len(set(cfg.nested_dir.rglob("*.zip"))) == 2
    contents = set()
    for cand in nested:
        with open_candidate(cfg, cand) as fh:
            contents.add(fh.read())
    assert len(contents) == 2
