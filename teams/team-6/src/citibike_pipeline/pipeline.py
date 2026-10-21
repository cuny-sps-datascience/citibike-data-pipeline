"""`citibike normalize`: stage, reconcile, and publish every selected source of the given releases."""

from __future__ import annotations

import csv
import os
import time
import zipfile

import duckdb

from .db import connect
from .dst import dst_hours
from .normalize import NormalizeError, normalize_source, slug_for
from .publish import publish_source, retire_unlisted
from .sets import selection_problems

_SOURCE_ERRORS = (NormalizeError, OSError, UnicodeDecodeError, csv.Error, zipfile.BadZipFile, duckdb.Error)


def run_normalize(cfg, manifest, releases, dry_run=False, out=print) -> int:
    all_records = manifest.all()
    records = [r for r in all_records if r["nominal_release"] in releases]
    # Chronological, so a release's spillover check sees the earlier releases published in this run.
    selected = sorted((r for r in records if r["status"] == "selected"),
                      key=lambda r: (r["nominal_release"], r["source_key"]))
    missing = sorted(set(releases) - {r["nominal_release"] for r in selected})
    if missing:
        out(f"no selected sources for: {', '.join(missing)}; run `citibike inventory select` first")
        return 1
    problems = [p for release in releases for p in selection_problems(all_records, release)]
    if problems:
        for problem in problems:
            out(problem)
        return 1
    if dry_run:
        for rec in selected:
            out(f"would normalize {rec['source_key']} ({rec['rows']} rows, {rec['schema_family']})")
        return 0

    run_id = manifest.start_run("normalize", {"releases": releases})
    failures = 0
    cleanup_failures = 0
    failed_releases: set[str] = set()

    def _cleanup() -> None:
        nonlocal cleanup_failures
        try:
            retire_unlisted(cfg, manifest, run_id)
        except OSError as exc:
            cleanup_failures += 1
            out(f"cleanup failed: {exc}")

    def _retire_staging(rec) -> None:
        """Moves a failed source's partial staging dir aside (never deleted) so a retry starts clean."""
        nonlocal cleanup_failures
        staged = cfg.staging_dir / run_id / slug_for(rec["source_key"])
        if not staged.exists():
            return
        dest = cfg.retired_dir / run_id / "staging" / staged.name
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, dest)
        except OSError as exc:
            cleanup_failures += 1
            out(f"cleanup failed: {exc}")

    try:
        _cleanup()  # rolls back files of an interrupted earlier publish to the committed ledger

        con = connect(cfg)
        years = [int(release[:4]) for release in releases]
        # Never narrower than 2013..2027, the range the published archive was normalized with, so
        # DST flags of out-of-range timestamps stay the same; wider when the releases need it.
        dst = dst_hours(range(min(min(years) - 1, 2013), max(max(years) + 2, 2028)), cfg.timezone)
        staging_root = cfg.staging_dir / run_id

        def published_trips(release: str) -> list[str]:
            return [str(cfg.processed_root / p) for p in manifest.published_trip_paths(release)]

        try:
            for rec in selected:
                started = time.monotonic()
                try:
                    res = normalize_source(cfg, con, rec, run_id, staging_root, dst, published_trips)
                    publish_source(cfg, manifest, run_id, res)
                except _SOURCE_ERRORS as exc:
                    failures += 1
                    seconds = time.monotonic() - started
                    manifest.record_source_run(run_id, rec, None, "failed", f"{type(exc).__name__}: {exc}", seconds)
                    out(f"FAILED {rec['source_key']}: {exc}")
                    _retire_staging(rec)
                    failed_releases.add(rec["nominal_release"])
                else:
                    seconds = time.monotonic() - started
                    manifest.record_source_run(run_id, rec, res, "ok", None, seconds)
                    out(f"ok {seconds:7.1f}s {res.source_rows / max(seconds, 1e-9):10,.0f} rows/s "
                        f"published={res.staged_rows} quarantined={res.quarantined_rows} removed={res.removed_rows} "
                        f"conflicts={res.spillover_conflicts} {rec['source_key']}")
                _cleanup()
        finally:
            con.close()

        # Sources no longer selected stay published until every selected source of their release has
        # published; if any failed, the release keeps its previous publication.
        selected_keys = {r["source_key"] for r in selected}
        for release in releases:
            if release in failed_releases:
                continue
            stale = [k for k in manifest.published_keys_for_releases([release]) if k not in selected_keys]
            if stale:
                manifest.unpublish(stale)
                _cleanup()
    except Exception:
        manifest.finish_run(run_id, "failed")
        raise

    status = "failed" if failures else ("cleanup_failed" if cleanup_failures else "ok")
    manifest.finish_run(run_id, status)
    out(f"normalize {run_id}: {len(selected) - failures} ok, {failures} failed, {cleanup_failures} cleanup failures")
    return 1 if (failures or cleanup_failures) else 0
