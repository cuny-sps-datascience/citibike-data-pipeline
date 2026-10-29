"""Full profiling pass: byte hash, record digest, row counts, and start-date coverage."""

from __future__ import annotations

import hashlib
import io
import json
import time
import traceback
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

from .discover import candidate_from_record, open_candidate
from .normalize import slug_for
from .records import open_records, parse_local_ts
from .schemas import START_COLUMN

MASK = (1 << 64) - 1
PROFILE_FIELDS = ("sha256", "row_digest", "rows", "malformed_rows", "invalid_start_rows",
                  "min_start_date", "max_start_date", "day_counts_json")


class _HashingReader(io.RawIOBase):
    """Pass-through reader that SHA-256 hashes every byte it delivers."""

    def __init__(self, raw):
        self._raw = raw
        self.sha256 = hashlib.sha256()

    def readable(self):
        return True

    def readinto(self, buffer):
        n = self._raw.readinto(buffer)
        if n:
            self.sha256.update(memoryview(buffer)[:n])
        return n


def row_hash(fields: list[str]) -> int:
    digest = hashlib.blake2b("\x1f".join(fields).encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def profile_candidate(cfg, cand, family: str) -> dict:
    with open_candidate(cfg, cand) as fh:
        hasher = _HashingReader(fh)
        header, records = open_records(io.BufferedReader(hasher, 1 << 20))
        width = len(header)
        start_idx = START_COLUMN.get(family)
        rows = malformed = invalid = digest = 0
        days: Counter = Counter()
        for _, fields in records:
            rows += 1
            digest = (digest + row_hash(fields)) & MASK
            if len(fields) != width:
                malformed += 1
                continue
            if start_idx is None:
                continue
            ts = parse_local_ts(fields[start_idx])
            if ts is None:
                invalid += 1
                continue
            days[ts.date()] += 1
    ordered = sorted(days)
    return {
        "sha256": hasher.sha256.hexdigest(),
        "row_digest": f"{digest:016x}",
        "rows": rows,
        "malformed_rows": malformed,
        "invalid_start_rows": invalid,
        "min_start_date": ordered[0].isoformat() if ordered else None,
        "max_start_date": ordered[-1].isoformat() if ordered else None,
        "day_counts_json": json.dumps({d.isoformat(): days[d] for d in ordered}),
    }


def _profile_job(cfg, cand, family):
    started = time.monotonic()
    try:
        return "ok", profile_candidate(cfg, cand, family), time.monotonic() - started
    except Exception as exc:  # worker boundary: recorded as a failed source, the run continues
        reason = f"{type(exc).__name__}: {exc}"
        return "failed", (reason, traceback.format_exc()), time.monotonic() - started


def _failure_reason(cfg, run_id: str, source_key: str, payload: tuple[str, str]) -> str:
    """Short status_reason (type and message); the full traceback goes to a per-run log file."""
    reason, trace = payload
    log = cfg.reports_dir / "profile_failures" / run_id / f"{slug_for(source_key)}.txt"
    try:
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(f"{source_key}\n\n{trace}")
    except OSError as exc:
        return f"{reason[:300]} (traceback not saved: {exc})"
    return f"{reason[:300]} (traceback: {log})"


def run_profile(cfg, manifest, run_id, releases, force, workers, out) -> dict:
    todo = []
    for rec in manifest.all():
        if rec["status"] == "missing":
            continue
        if releases is not None and rec["nominal_release"] not in releases:
            continue
        fresh = rec["profiled_run"] and rec["parser_version"] == cfg.parser_version and rec["status"] != "failed"
        if fresh and not force:
            continue
        todo.append(rec)
    counts: Counter = Counter()

    def finish(rec, result):
        state, payload, seconds = result
        if state == "ok":
            keep = rec["status"] in ("selected", "excluded") and payload["row_digest"] == rec["row_digest"]
            rec.update(payload, status=rec["status"] if keep else "profiled",
                       status_reason=rec["status_reason"] if keep else None)
        else:
            rec.update(dict.fromkeys(PROFILE_FIELDS), status="failed",
                       status_reason=_failure_reason(cfg, run_id, rec["source_key"], payload))
        rec.update(profiled_run=run_id, parser_version=cfg.parser_version, profile_seconds=seconds)
        manifest.upsert(rec)
        counts[rec["status"]] += 1
        out(f"{rec['status']:9} {seconds:8.1f}s {rec['source_key']}")

    if workers <= 1:
        for rec in todo:
            finish(rec, _profile_job(cfg, candidate_from_record(rec), rec["schema_family"]))
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_profile_job, cfg, candidate_from_record(r), r["schema_family"]): r for r in todo}
            for fut in as_completed(futures):
                finish(futures[fut], fut.result())
    manifest.assign_duplicate_groups()
    return dict(counts)
