"""Moves staged files into the published layout. Superseded files are moved, never deleted."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow.parquet as pq


def retire_unlisted(cfg, manifest, run_id: str) -> list[Path]:
    listed = {cfg.processed_root / p for p in manifest.published_paths()}
    retired = []
    for root in (cfg.trips_dir, cfg.quarantine_dir, cfg.removed_dir):
        if not root.exists():
            continue
        for f in sorted(root.rglob("*.parquet")):
            if f not in listed:
                dest = cfg.retired_dir / run_id / f.relative_to(cfg.processed_root)
                dest.parent.mkdir(parents=True, exist_ok=True)
                os.replace(f, dest)
                retired.append(dest)
    return retired


def publish_source(cfg, manifest, run_id: str, res) -> list[tuple[str, str, int]]:
    moves = []
    trips_root = res.staged_dir / "trips"
    staged = sorted(trips_root.rglob("*.parquet")) if trips_root.exists() else []
    for n, f in enumerate(staged):
        dest = cfg.trips_dir / f.parent.relative_to(trips_root) / f"{res.slug}__{run_id}__{n}.parquet"
        moves.append(("trips", f, dest))
    quarantine = res.staged_dir / "quarantine.parquet"
    if quarantine.exists():
        moves.append(("quarantine", quarantine, cfg.quarantine_dir / f"{res.slug}__{run_id}.parquet"))
    removed = res.staged_dir / "removed.parquet"
    if removed.exists():
        moves.append(("removed", removed, cfg.removed_dir / f"{res.slug}__{run_id}.parquet"))
    entries = []
    for dataset, src, dest in moves:
        rows = pq.ParquetFile(src).metadata.num_rows
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(src, dest)
        entries.append((dataset, dest.relative_to(cfg.processed_root).as_posix(), rows))
    manifest.replace_published(res.source_key, run_id, entries)
    return entries
