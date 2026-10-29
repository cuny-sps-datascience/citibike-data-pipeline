"""`citibike inventory ...` subcommands."""

from __future__ import annotations

import json
from collections import Counter

from .discover import discover, open_candidate
from .manifest import Manifest
from .naming import parse_nominal
from .profile import run_profile
from .report import write_report
from .schemas import classify, fingerprint, parse_header
from .sets import apply_selection


def _refresh_nominal(manifest: Manifest, prev: dict, cand) -> bool:
    """Re-applies the filename rules to an unchanged source; returns True if its release changed."""
    nominal = parse_nominal(cand.member or cand.path)
    release, part = (nominal.release, nominal.part) if nominal else (None, None)
    if (prev["nominal_release"], prev["nominal_part"]) == (release, part):
        return False
    prev.update(nominal_release=release, nominal_part=part)
    if nominal is None:
        prev["status_reason"] = "unrecognized file name"
    elif prev["status_reason"] == "unrecognized file name":
        prev["status_reason"] = None
    manifest.upsert(prev)
    return True


def run_discover(cfg, manifest: Manifest, run_id: str, out, additional_only: bool = False) -> dict:
    stats: Counter = Counter()
    # Scoped discovery may mark absent external inputs missing, never primary inputs.
    seen: set[str] = {r['source_key'] for r in manifest.all()
        if not r['source_key'].startswith('external/')} if additional_only else set()
    for cand in discover(cfg, additional_only):
        seen.add(cand.source_key)
        stats["candidates"] += 1
        prev = manifest.get(cand.source_key)
        if prev and prev["status"] != "missing" and \
                (prev["size"], prev["file_size"], prev["modified"]) == (cand.size, cand.file_size, cand.modified):
            stats["unchanged"] += 1
            if _refresh_nominal(manifest, prev, cand):
                stats["renamed"] += 1
                out(f"release name refreshed {cand.source_key}")
            continue
        with open_candidate(cfg, cand) as fh:
            header = parse_header(fh.readline())
        nominal = parse_nominal(cand.member or cand.path)
        manifest.upsert({
            "source_key": cand.source_key, "kind": cand.kind, "path": cand.path,
            "container": cand.container, "member": cand.member, "size": cand.size,
            "file_size": cand.file_size, "modified": cand.modified,
            "header_json": json.dumps(list(header)), "schema_fingerprint": fingerprint(header),
            "schema_family": classify(header),
            "nominal_release": nominal.release if nominal else None,
            "nominal_part": nominal.part if nominal else None,
            "status": "discovered", "status_reason": None if nominal else "unrecognized file name",
            "discovered_run": run_id,
        })
        stats["changed" if prev else "new"] += 1
        out(f"discovered {cand.source_key}")
    stats["missing"] = manifest.mark_missing(seen)
    return dict(stats)


def run_inventory(cfg, args, out) -> int:
    with Manifest(cfg) as manifest:
        if args.action == "discover":
            run_id = manifest.start_run("inventory discover", {'additional_only':args.additional_only})
            stats = run_discover(cfg, manifest, run_id, out, args.additional_only)
            manifest.finish_run(run_id, "ok")
            out(f"discover {run_id}: {stats}")
            return 0
        if args.action == "profile":
            if not args.all and not args.releases:
                out("profile needs --release or --all")
                return 2
            run_id = manifest.start_run("inventory profile", {"releases": args.releases, "force": args.force})
            counts = run_profile(cfg, manifest, run_id, None if args.all else args.releases,
                                 args.force, args.workers, out)
            manifest.finish_run(run_id, "ok" if not counts.get("failed") else "failed")
            out(f"profile {run_id}: {counts}")
            return 1 if counts.get("failed") else 0
        if args.action == "select":
            overrides = dict(item.split("=", 1) for item in args.set)
            return apply_selection(manifest, args.releases, overrides, args.dry_run, out)
        if args.action == "report":
            out(f"wrote {write_report(cfg, manifest)}")
            return 0
    out(f"inventory {args.action}: not available yet")
    return 2
