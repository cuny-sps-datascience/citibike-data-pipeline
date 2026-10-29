"""Human-readable inventory report plus a CSV export of the manifest."""

from __future__ import annotations

import calendar
import json
import posixpath
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path

from .sets import ReleaseDecision, build_sets, decide_release

# The S3 listing namespace; parse_listing also accepts listings without it.
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def table_row(*cells) -> str:
    """One markdown table row; a literal `|` inside a cell (set IDs contain them) is escaped."""
    return "| " + " | ".join(str(c).replace("|", "\\|") for c in cells) + " |"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child_text(elem, name: str) -> str | None:
    return next((c.text for c in elem if _local(c.tag) == name), None)


def parse_listing(xml_text: str) -> tuple[dict[str, int], int]:
    """S3 bucket listing as {key: size}, with or without the S3 namespace.

    Returns the entries and the number of Contents entries skipped for a missing Key or a
    missing or non-integer Size.
    """
    entries: dict[str, int] = {}
    skipped = 0
    for contents in ET.fromstring(xml_text).iter():
        if _local(contents.tag) != "Contents":
            continue
        key, size = _child_text(contents, "Key"), _child_text(contents, "Size")
        if not key or size is None or not size.strip().isdigit():
            skipped += 1
            continue
        entries[key] = int(size)
    return entries, skipped


def _listing_state(release_records: list[dict], listing: dict[str, int] | None) -> bool | None:
    if not listing:
        return None
    states = [listing[name] == rec["file_size"] for rec in release_records
              if (name := posixpath.basename(rec["path"])) in listing]
    return all(states) if states else None


def completeness(release: str, decision: ReleaseDecision, release_records: list[dict],
                 listing: dict[str, int] | None) -> tuple[str, str]:
    if decision.chosen is None:
        return "unverified", f"no selectable set ({decision.status})"
    by_key = {r["source_key"]: r for r in release_records}
    year, month = map(int, release.split("-"))
    expected = calendar.monthrange(year, month)[1]
    days = {d for key in decision.chosen.members
            for d in json.loads(by_key[key]["day_counts_json"] or "{}") if d.startswith(release)}
    listed = _listing_state(release_records, listing)
    if len(days) < expected:
        return "incomplete", f"{len(days)}/{expected} days have trips"
    if listed is False:
        return "incomplete", "on-disk size differs from the published listing"
    if listed:
        return "confirmed", f"{expected}/{expected} days; size matches the published listing"
    return "unverified", f"{expected}/{expected} days; no published listing to compare"


def _table(title: str, counter: Counter) -> list[str]:
    lines = [f"## {title}", "", "| value | sources |", "|---|---|"]
    lines += [table_row(k, v) for k, v in sorted(counter.items(), key=lambda kv: str(kv[0]))]
    return lines + [""]


def _months_between(first: str, last: str) -> list[str]:
    year, month = map(int, first.split("-"))
    out = []
    while f"{year:04d}-{month:02d}" <= last:
        out.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def build_report(cfg, records: list[dict], listing: dict[str, int] | None = None,
                 listing_skipped: int = 0) -> str:
    compared = "no"
    if listing:
        compared = f"yes ({listing_skipped} entries without Key or Size skipped)" if listing_skipped else "yes"
    lines = [
        "# Citi Bike archive inventory", "",
        f"- Source root: `{cfg.source_root}`",
        f"- Parser version: `{cfg.parser_version}`",
        f"- Candidates: {len(records)}",
        f"- Published listing compared: {compared}", "",
    ]
    lines += _table("Sources by status", Counter(r["status"] for r in records))
    lines += _table("Sources by kind", Counter(r["kind"] for r in records))
    lines += _table("Sources by schema family", Counter(r["schema_family"] for r in records))

    lines += ["## Header fingerprints", "", "| family | fingerprint | sources | header |", "|---|---|---|---|"]
    fingerprints: dict[tuple, list[dict]] = defaultdict(list)
    for rec in records:
        fingerprints[(rec["schema_family"], rec["schema_fingerprint"])].append(rec)
    for (family, fp), recs in sorted(fingerprints.items()):
        lines.append(table_row(family, f"`{fp}`", len(recs), f"`{', '.join(json.loads(recs[0]['header_json']))}`"))
    lines.append("")

    attention = [r for r in records if r["status"] in ("failed", "missing")
                 or r["nominal_release"] is None or r["schema_family"] == "unknown"]
    lines += ["## Sources needing attention", ""]
    lines += [f"- `{r['source_key']}`: status={r['status']}, family={r['schema_family']}, "
              f"reason={r['status_reason']}" for r in attention] or ["- none"]
    lines.append("")

    by_sha: dict[str, list[str]] = defaultdict(list)
    for rec in records:
        if rec["sha256"]:
            by_sha[rec["sha256"]].append(rec["source_key"])
    dupes = {sha: keys for sha, keys in by_sha.items() if len(keys) > 1}
    lines += ["## Exact duplicate copies (identical bytes)", ""]
    lines += [f"- `{sha[:12]}`: " + ", ".join(f"`{k}`" for k in keys) for sha, keys in sorted(dupes.items())] \
        or ["- none"]
    lines.append("")

    families = {r["source_key"]: r["schema_family"] for r in records}
    by_release: dict[str, list[dict]] = defaultdict(list)
    for rec in records:
        if rec["nominal_release"]:
            by_release[rec["nominal_release"]].append(rec)
    sets = build_sets(records)
    decisions = {}
    lines += ["## Releases", "", "| release | sets | decision | chosen set | rows | completeness | basis |",
              "|---|---|---|---|---|---|---|"]
    for release in sorted(sets):
        decision = decide_release(release, sets[release], families)
        decisions[release] = decision
        state, basis = completeness(release, decision, by_release[release], listing)
        chosen = f"`{decision.chosen.set_id}`" if decision.chosen else "-"
        rows = decision.chosen.rows if decision.chosen else "-"
        lines.append(table_row(release, len(sets[release]), f"{decision.status}: {decision.reason}",
                               chosen, rows, state, basis))
    lines.append("")

    if sets:
        present = sorted(sets)
        missing = [m for m in _months_between(present[0], present[-1]) if m not in sets]
        lines += ["## Releases missing between first and last", "", ", ".join(missing) or "none", ""]

    lines += ["## Pilot candidates", ""]
    for release in cfg.pilot_releases:
        decision = decisions.get(release)
        lines.append(f"- {release}: " + (f"{decision.status} ({decision.reason})" if decision else "not in archive"))
    return "\n".join(lines) + "\n"


def write_report(cfg, manifest) -> Path:
    cfg.reports_dir.mkdir(parents=True, exist_ok=True)
    listing_file = cfg.reports_dir / "tripdata_listing.xml"
    listing, skipped = parse_listing(listing_file.read_text()) if listing_file.exists() else (None, 0)
    path = cfg.reports_dir / "inventory_report.md"
    path.write_text(build_report(cfg, manifest.all(), listing, skipped))
    manifest.export_sources_csv(cfg.reports_dir / "manifest_sources.csv")
    return path
