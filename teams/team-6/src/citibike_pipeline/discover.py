"""Enumerates candidate CSVs: plain files, ZIP members, and members of ZIPs nested in ZIPs.

The archive is opened read-only. Nested ZIPs are copied once into processed staging.
"""

from __future__ import annotations

import json
import hashlib
import os
import shutil
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from .config import Config

SEP = "::"


@dataclass(frozen=True)
class Candidate:
    source_key: str
    kind: str              # "csv" | "zip_member" | "nested_zip_member"
    path: str              # on-disk file relative to source_root (the CSV or the outer ZIP)
    container: str | None  # None, "outer.zip", or "outer.zip::inner.zip"
    member: str | None     # CSV name inside the innermost ZIP
    size: int              # uncompressed CSV bytes
    file_size: int         # bytes of the on-disk file at `path`
    modified: float        # mtime of the on-disk file


def _skip_member(name: str) -> bool:
    if name.endswith("/"):
        return True
    return any(part == "__MACOSX" or part.startswith(".") for part in name.split("/"))


def _walk_files(cfg: Config, additional_only: bool = False) -> Iterator[Path]:
    excluded = set(cfg.exclude_dir_names)
    roots=cfg.additional_source_roots if additional_only else (cfg.source_root,*cfg.additional_source_roots)
    if not roots:raise ValueError('No additional source roots configured')
    # Fail before discovery can mark sources missing merely because a mount disappeared.
    for root in roots:
        if not root.is_dir():raise FileNotFoundError('Configured source root is unavailable: '+str(root))
    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = sorted(d for d in dirnames if d not in excluded and not d.startswith("."))
            for name in sorted(filenames):
                if not name.startswith("."):
                    yield Path(dirpath) / name


def source_path(cfg: Config, value: str) -> Path:
    """Resolve a ledger path only inside an explicitly configured raw-input root."""
    path=(cfg.source_root/value).resolve()
    if not any(path.is_relative_to(root.resolve()) for root in (cfg.source_root,*cfg.additional_source_roots)):
        raise ValueError('Source path escapes configured raw-input roots')
    return path


def _nested_cache_path(cfg: Config, outer_rel: str, inner_name: str) -> Path:
    return cfg.nested_dir / (outer_rel.replace("/", "__") + "__" + inner_name.replace("/", "__"))


def _outer_state(cfg: Config, outer_rel: str) -> dict:
    st = source_path(cfg,outer_rel).stat()
    return {"outer_size": st.st_size, "outer_mtime": st.st_mtime}


def materialize_nested(cfg: Config, outer_rel: str, inner_name: str) -> Path:
    """Copies one nested ZIP out of its container; CRC is verified by zipfile at EOF.

    The `.done` marker records the outer ZIP's size and mtime. If either changed, the nested ZIP is
    copied again (the archive itself is only ever read). A marker from before this rule ("ok") is
    taken as valid, as it was when written, and rewritten with the current outer state.
    """
    target = _nested_cache_path(cfg, outer_rel, inner_name)
    done = target.with_suffix(target.suffix + ".done")
    state = _outer_state(cfg, outer_rel)
    if done.exists() and target.exists():
        marker = done.read_text()
        if marker.strip() == "ok":
            done.write_text(json.dumps(state) + "\n")
            return target
        try:
            if json.loads(marker) == state:
                return target
        except json.JSONDecodeError:
            pass
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(target.suffix + ".partial")
    with zipfile.ZipFile(source_path(cfg,outer_rel)) as outer, outer.open(inner_name) as src, \
            open(partial, "wb") as dst:
        shutil.copyfileobj(src, dst, 1 << 20)
    os.replace(partial, target)
    done.write_text(json.dumps(state) + "\n")
    return target


def discover(cfg: Config, additional_only: bool = False) -> Iterator[Candidate]:
    for f in _walk_files(cfg, additional_only):
        if f.is_relative_to(cfg.source_root):
            rel=f.relative_to(cfg.source_root).as_posix();key=rel
        else:
            root=next(r for r in cfg.additional_source_roots if f.is_relative_to(r))
            namespace=hashlib.sha256(str(root).encode()).hexdigest()[:12]
            key='external/'+namespace+'/'+f.relative_to(root).as_posix()
            # Absolute external paths remain resolvable without changing old ledger records.
            rel=str(f.resolve())
        st = f.stat()
        lower = f.name.lower()
        if lower.endswith(".csv"):
            yield Candidate(key, "csv", rel, None, None, st.st_size, st.st_size, st.st_mtime)
        elif lower.endswith(".zip"):
            with zipfile.ZipFile(f) as zf:
                for info in zf.infolist():
                    name = info.filename
                    if _skip_member(name):
                        continue
                    if name.lower().endswith(".csv"):
                        yield Candidate(f"{key}{SEP}{name}", "zip_member", rel, rel, name,
                                        info.file_size, st.st_size, st.st_mtime)
                    elif name.lower().endswith(".zip"):
                        container = f"{rel}{SEP}{name}"
                        with zipfile.ZipFile(materialize_nested(cfg, rel, name)) as inner:
                            for ii in inner.infolist():
                                if _skip_member(ii.filename) or not ii.filename.lower().endswith(".csv"):
                                    continue
                                yield Candidate(f"{key}{SEP}{name}{SEP}{ii.filename}", "nested_zip_member", rel,
                                                container, ii.filename, ii.file_size, st.st_size, st.st_mtime)


@contextmanager
def open_candidate(cfg: Config, cand: Candidate):
    if cand.kind == "csv":
        with open(source_path(cfg,cand.path), "rb") as fh:
            yield fh
    elif cand.kind == "zip_member":
        with zipfile.ZipFile(source_path(cfg,cand.path)) as zf, zf.open(cand.member) as fh:
            yield fh
    else:
        outer_rel, inner_name = cand.container.split(SEP)
        with zipfile.ZipFile(materialize_nested(cfg, outer_rel, inner_name)) as zf, zf.open(cand.member) as fh:
            yield fh


def candidate_from_record(rec: dict) -> Candidate:
    return Candidate(rec["source_key"], rec["kind"], rec["path"], rec["container"], rec["member"],
                     rec["size"], rec["file_size"], rec["modified"])
