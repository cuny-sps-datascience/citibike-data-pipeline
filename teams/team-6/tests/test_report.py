import json

from citibike_pipeline.manifest import Manifest
from citibike_pipeline.report import build_report, completeness, parse_listing, write_report
from citibike_pipeline.sets import ReleaseDecision, SourceSet

LISTING = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>tripdata</Name>
  <Contents><Key>202502-citibike-tripdata.zip</Key><Size>100</Size></Contents>
  <Contents><Key>2018-citibike-tripdata.zip</Key><Size>5</Size></Contents>
</ListBucketResult>"""


def _feb(file_size):
    days = {f"2025-02-{d:02d}": 1 for d in range(1, 29)}
    rec = {"source_key": "k", "path": "202502-citibike-tripdata.zip", "file_size": file_size,
           "day_counts_json": json.dumps(days)}
    chosen = SourceSet("x|2025-02|parts", "2025-02", "parts", 1, ("k",), 28, "0" * 16, False, True)
    return ReleaseDecision("2025-02", "selectable", chosen, (), ""), [rec]


def test_parse_listing():
    assert parse_listing(LISTING) == ({"202502-citibike-tripdata.zip": 100, "2018-citibike-tripdata.zip": 5}, 0)


def test_parse_listing_without_namespace():
    plain = LISTING.replace(' xmlns="http://s3.amazonaws.com/doc/2006-03-01/"', "")
    assert parse_listing(plain) == parse_listing(LISTING)


def test_parse_listing_skips_and_counts_incomplete_entries():
    xml = """<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Contents><Key>a.zip</Key><Size>1</Size></Contents>
      <Contents><Key>no-size.zip</Key></Contents>
      <Contents><Size>7</Size></Contents>
      <Contents><Key>bad-size.zip</Key><Size>n/a</Size></Contents>
    </ListBucketResult>"""
    assert parse_listing(xml) == ({"a.zip": 1}, 3)


def test_completeness_states():
    listing, _ = parse_listing(LISTING)
    decision, recs = _feb(100)
    assert completeness("2025-02", decision, recs, listing)[0] == "confirmed"
    assert completeness("2025-02", decision, recs, None)[0] == "unverified"
    decision, recs = _feb(99)
    assert completeness("2025-02", decision, recs, listing)[0] == "incomplete"
    none = ReleaseDecision("2025-02", "conflict", None, (), "")
    assert completeness("2025-02", none, recs, listing)[0] == "unverified"


def test_report_sections(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        text = build_report(profiled_cfg, m.all())
    assert "## Releases" in text and "## Exact duplicate copies" in text
    assert "| 2018-04 | 2 | selectable" in text
    assert "unknown_schema" in text
    assert "`202501-citibike-tripdata/202501-citibike-tripdata_1.csv`" in text
    assert "2015-02" in text.split("## Releases missing between first and last")[1]
    row_2018 = next(line for line in text.splitlines() if line.startswith("| 2018-04 |"))
    assert "incomplete" in row_2018


def test_write_report_outputs(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        path = write_report(profiled_cfg, m)
    assert path.read_text().startswith("# Citi Bike archive inventory")
    assert (profiled_cfg.reports_dir / "manifest_sources.csv").read_text().startswith("source_key,")


def _cells(line):
    import re

    return re.split(r"(?<!\\)\|", line.strip().strip("|"))


def test_set_id_pipes_are_escaped_in_release_table(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        text = build_report(profiled_cfg, m.all())
    row = next(line for line in text.splitlines() if line.startswith("| 2018-04 |"))
    assert len(_cells(row)) == 7
    assert "4_April\\|2018-04\\|parts" in row


def test_table_row_escapes_every_cell():
    from citibike_pipeline.report import table_row

    assert table_row("a|b", 3, None) == "| a\\|b | 3 | None |"


def test_report_shows_incomplete_parts_release(profiled_cfg):
    from archive_fixture import KEY_2018_P2, KEY_2018_WHOLE

    with Manifest(profiled_cfg) as m:
        records = m.all()
    for rec in records:
        if rec["source_key"] in (KEY_2018_P2, KEY_2018_WHOLE):
            rec["status"] = "missing"
    text = build_report(profiled_cfg, records)
    row = next(line for line in text.splitlines() if line.startswith("| 2018-04 |"))
    cells = [c.strip() for c in _cells(row)]
    assert cells[1] == "1" and cells[2].startswith("incomplete_parts:")
    assert cells[3:6] == ["-", "-", "unverified"] and "incomplete_parts" in cells[6]


def test_write_report_counts_skipped_listing_entries(profiled_cfg):
    profiled_cfg.reports_dir.mkdir(parents=True, exist_ok=True)
    (profiled_cfg.reports_dir / "tripdata_listing.xml").write_text(
        LISTING.replace("<Size>5</Size>", ""))
    with Manifest(profiled_cfg) as m:
        text = write_report(profiled_cfg, m).read_text()
    assert "- Published listing compared: yes (1 entries without Key or Size skipped)" in text
