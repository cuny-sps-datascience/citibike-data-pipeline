"""Loads config/local.yaml and enforces the code/source/processed separation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "local.yaml"


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Config:
    source_root: Path
    processed_root: Path
    exclude_dir_names: tuple[str, ...]
    memory_limit: str
    threads: int
    timezone: str
    schema_version: int
    parser_version: str
    batch_rows: int
    max_quarantine_fraction: float
    pilot_releases: tuple[str, ...]
    additional_source_roots: tuple[Path, ...] = ()

    @property
    def manifest_db(self) -> Path:
        return self.processed_root / "manifest" / "manifest.duckdb"

    @property
    def staging_dir(self) -> Path:
        return self.processed_root / "staging"

    @property
    def nested_dir(self) -> Path:
        return self.processed_root / "staging" / "nested"

    @property
    def tmp_dir(self) -> Path:
        return self.processed_root / "tmp"

    @property
    def trips_dir(self) -> Path:
        return self.processed_root / "trips"

    @property
    def quarantine_dir(self) -> Path:
        return self.processed_root / "quarantine"

    @property
    def removed_dir(self) -> Path:
        return self.processed_root / "removed"

    @property
    def retired_dir(self) -> Path:
        return self.processed_root / "retired"

    @property
    def reports_dir(self) -> Path:
        return self.processed_root / "reports"


def _is_within(child: Path, parent: Path) -> bool:
    return child == parent or parent in child.parents


def load_config(path: Path = DEFAULT_CONFIG, project_root: Path = PROJECT_ROOT) -> Config:
    raw = yaml.safe_load(Path(path).read_text())
    paths = raw["paths"]
    source_root = Path(paths["source_root"]).expanduser().resolve()
    processed_root = Path(paths["processed_root"]).expanduser().resolve()
    additional_roots=tuple(Path(p).expanduser().resolve() for p in paths.get('additional_source_roots',[]))
    if _is_within(processed_root, source_root) or _is_within(source_root, processed_root):
        raise ConfigError("processed_root and source_root must not contain one another")
    if _is_within(Path(project_root).resolve(), source_root):
        raise ConfigError("the code project must not live inside source_root")
    roots=(source_root,*additional_roots)
    for index,extra in enumerate(additional_roots,1):
        if _is_within(extra,processed_root) or _is_within(processed_root,extra):
            raise ConfigError('additional source roots and processed_root must not contain one another')
        if _is_within(extra,Path(project_root).resolve()) or _is_within(Path(project_root).resolve(),extra):
            raise ConfigError('additional source roots and code project must be separate')
        if any(_is_within(extra,other) or _is_within(other,extra) for other in roots[:index]):
            raise ConfigError('source roots must not overlap')
    duck = raw.get("duckdb", {})
    return Config(
        source_root=source_root,
        processed_root=processed_root,
        exclude_dir_names=tuple(raw.get("exclude_dir_names", [])),
        memory_limit=str(duck.get("memory_limit", "4GB")),
        threads=int(duck.get("threads", 4)),
        timezone=raw.get("timezone", "America/New_York"),
        schema_version=int(raw["schema_version"]),
        parser_version=str(raw["parser_version"]),
        batch_rows=int(raw.get("batch_rows", 250_000)),
        max_quarantine_fraction=float(raw.get("max_quarantine_fraction", 0.01)),
        pilot_releases=tuple(raw.get("pilot", {}).get("releases", [])),
        additional_source_roots=additional_roots,
    )
