"""Groups candidates into source sets per release and picks one representative set.

Exact byte copies and re-quoted copies both show up as sets with equal (rows, record digest).
Nothing is deleted; non-chosen sets are marked excluded with the reason.
"""

from __future__ import annotations

import posixpath
from collections import defaultdict
from dataclasses import dataclass

from .profile import MASK

KIND_RANK = {"csv": 0, "zip_member": 1, "nested_zip_member": 2}
UNPROFILED_STATUSES = ("discovered", "failed")


@dataclass(frozen=True)
class SourceSet:
    set_id: str
    release: str
    layout: str
    kind_rank: int
    members: tuple[str, ...]
    rows: int | None
    digest: str | None
    parts_gap: bool
    all_profiled: bool  # no member is still "discovered" or left "failed" by profiling


@dataclass(frozen=True)
class ReleaseDecision:
    release: str
    status: str
    chosen: SourceSet | None
    equivalent: tuple[SourceSet, ...]
    reason: str


def _location(rec: dict) -> str:
    parent = posixpath.dirname(rec["member"] or rec["path"])
    return f"{rec['container']}::{parent}" if rec["container"] else parent


def build_sets(records) -> dict[str, list[SourceSet]]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for rec in records:
        if rec["nominal_release"] is None:
            continue
        layout = "whole" if rec["nominal_part"] is None else "parts"
        groups[(rec["nominal_release"], _location(rec), layout)].append(rec)
    out: dict[str, list[SourceSet]] = defaultdict(list)
    for (release, location, layout), recs in sorted(groups.items()):
        recs.sort(key=lambda r: (r["nominal_part"] or 0, r["source_key"]))

        # Skip group if all records are missing
        if all(r["status"] == "missing" for r in recs):
            continue

        # Separate available and missing records
        available = [r for r in recs if r["status"] != "missing"]

        profiled = all(r["row_digest"] is not None for r in available) if available else False

        # Collect all part numbers (including from missing records) to detect gaps
        all_parts = [r["nominal_part"] for r in recs if r["nominal_part"] is not None]
        has_missing = any(r["status"] == "missing" for r in recs)
        parts_gap = has_missing or (bool(all_parts) and sorted(all_parts) != list(range(1, len(all_parts) + 1)))

        out[release].append(SourceSet(
            set_id=f"{location}|{release}|{layout}",
            release=release,
            layout=layout,
            kind_rank=min(KIND_RANK[r["kind"]] for r in available) if available else 0,
            members=tuple(r["source_key"] for r in available),
            rows=sum(r["rows"] for r in available) if profiled else None,
            digest=f"{sum(int(r['row_digest'], 16) for r in available) & MASK:016x}" if profiled else None,
            parts_gap=parts_gap,
            all_profiled=all(r["status"] not in UNPROFILED_STATUSES for r in available),
        ))
    return dict(out)


def decide_release(release: str, sets: list[SourceSet], families: dict[str, str]) -> ReleaseDecision:
    if any(s.digest is None or not s.all_profiled for s in sets):
        return ReleaseDecision(release, "unprofiled", None, (), "at least one candidate set is not profiled")
    if any(families[k] == "unknown" for s in sets for k in s.members):
        return ReleaseDecision(release, "unknown_schema", None, (), "a candidate has an unrecognized header")
    contents = {(s.rows, s.digest) for s in sets}
    if len(contents) > 1:
        return ReleaseDecision(release, "conflict", None, (),
                               f"{len(contents)} distinct contents across {len(sets)} sets; needs review")
    # Tiebreak among content-equivalent sets only (rows and digest already match): sets without a
    # parts gap first, then plain CSV over ZIP member over nested member, then set_id string order.
    # set_id is "location|release|layout", so at an equal location "parts" sorts before "whole"; at
    # different locations the location string decides. Either way the rows chosen are the same.
    ordered = sorted(sets, key=lambda s: (s.parts_gap, s.kind_rank, s.set_id))
    chosen = ordered[0]

    # Check for incomplete parts after choosing
    if chosen.parts_gap:
        return ReleaseDecision(release, "incomplete_parts", None, (),
                               "chosen set has a parts gap or a missing part; needs review")

    return ReleaseDecision(release, "selectable", chosen, tuple(ordered[1:]),
                           f"{len(sets)} equivalent set(s)")


def selection_problems(records, release: str) -> list[str]:
    """Why the `selected` sources of a release are not one complete chosen set (empty list: they are)."""
    records = list(records)
    selected = {r["source_key"] for r in records if r["nominal_release"] == release and r["status"] == "selected"}
    if not selected:
        return [f"{release}: no selected sources"]
    by_key = {r["source_key"]: r for r in records}
    matches = [s for s in build_sets(records).get(release, []) if set(s.members) == selected]
    if len(matches) != 1:
        return [f"{release}: selected sources {sorted(selected)} are not exactly one complete source set; "
                "re-run `citibike inventory select`"]
    chosen = matches[0]
    unprofiled = [k for k in chosen.members
                  if by_key[k]["status"] in UNPROFILED_STATUSES or by_key[k]["row_digest"] is None]
    if unprofiled or not chosen.all_profiled:
        return [f"{release}: selected set {chosen.set_id} is not fully profiled ({sorted(unprofiled)}); "
                "run `citibike inventory profile` and re-run `citibike inventory select`"]
    if chosen.parts_gap and any(by_key[k]["status_reason"] != "manual choice" for k in chosen.members):
        return [f"{release}: selected set {chosen.set_id} has a parts gap or a missing part; "
                "select it manually or re-run `citibike inventory select`"]
    return []


def _set_status(manifest, rec: dict, status: str, reason: str) -> None:
    rec.update(status=status, status_reason=reason)
    manifest.upsert(rec)


def apply_selection(manifest, releases, overrides, dry_run, out) -> int:
    records = manifest.all()
    by_key = {r["source_key"]: r for r in records}
    families = {k: r["schema_family"] for k, r in by_key.items()}
    sets = build_sets(records)
    rc = 0
    for release in releases:
        candidates = sets.get(release, [])
        if not candidates:
            out(f"{release}: no candidates in the archive")
            rc = 1
            continue
        if release in overrides:
            chosen = next((s for s in candidates if s.set_id == overrides[release]), None)
            if chosen is None:
                out(f"{release}: no set named {overrides[release]!r}")
                rc = 1
                continue
            others = tuple(s for s in candidates if s is not chosen)
            chosen_reason, other_reason = "manual choice", f"manual_override:{chosen.set_id}"
        else:
            decision = decide_release(release, candidates, families)
            if decision.status != "selectable":
                out(f"{release}: {decision.status} ({decision.reason}); nothing selected")
                rc = 1
                continue
            chosen, others = decision.chosen, decision.equivalent
            chosen_reason = f"representative of {len(others) + 1} equivalent set(s)"
            other_reason = f"equivalent_to:{chosen.set_id}"
        out(f"{release}: select {chosen.set_id} ({chosen.rows} rows), exclude {len(others)} set(s)")
        if dry_run:
            continue
        for key in chosen.members:
            _set_status(manifest, by_key[key], "selected", chosen_reason)
        for other in others:
            for key in other.members:
                _set_status(manifest, by_key[key], "excluded", other_reason)
    return rc
