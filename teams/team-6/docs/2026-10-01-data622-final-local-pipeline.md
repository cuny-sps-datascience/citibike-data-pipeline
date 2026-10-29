# DATA 622 Final Project: Local Setup and Historical Archive Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Final Project's local code project and Python environment in `~/Sites/CUNY/citibike-data-pipeline/teams/team-6`, then build a resumable, reconciled pipeline that inventories the Citi Bike archive on the RAID and normalizes approved pilot releases into Parquet that stays on the RAID (Stages 0 to 2 of `docs/spec/Citi_Bike_Local_to_Databricks_Codex_Workflow.pdf`).

**Architecture:** A small `citibike_pipeline` package with one CLI (`citibike`). Python's `csv` module streams every source record (plain CSV, ZIP member, or member of a ZIP nested in a ZIP) so row identity and counts are exact; DuckDB does the typing, flagging and partitioned Parquet writes. A DuckDB manifest on the RAID records every candidate, its profile, its selection decision and a ledger of published files, which is the commit point for idempotent reruns.

**Tech Stack:** Python 3.12 (uv-managed `.venv` inside FinalProject), DuckDB >= 1.3, PyArrow, PyYAML, pytest.

## Global Constraints

- Code, config, tests, docs and the Python environment live in `/Users/randyhowk/Sites/CUNY/citibike-data-pipeline/teams/team-6`. No data files are committed or stored there.
- Source archive (read-only, never modified, never unzipped in place): `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject`.
- All generated data (manifest, staging, nested-zip cache, DuckDB spill, Parquet, quarantine, reports, retired files): `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed`.
- Snapshot data already collected (`station_status.json`): `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_snapshots`.
- DuckDB `temp_directory` must point at the RAID. The internal disk has 14 GiB free (97% used).
- Nothing is ever deleted by the pipeline. Superseded published files are moved to `FinalProject_processed/retired/<run_id>/`.
- No full-archive pandas load. No cloud upload in this plan.
- Do not invent schemas, table names, or completeness claims. Unknown headers are reported, not guessed.
- No em dashes in any file or output.
- Commits use the repo-local identity (Vaene). No co-author trailers. Never push without asking.
- Run all commands from `/Users/randyhowk/Sites/CUNY/citibike-data-pipeline/teams/team-6` on branch `team-6` unless stated.

---

## Decisions (deviations from the workflow PDF, with reasons)

| # | Decision | Reason |
|---|---|---|
| D1 | Code root is `~/Sites/CUNY/citibike-data-pipeline/teams/team-6` inside the shared course repository, not `.../FinalProject/citibike-production` on the RAID. | User instruction: code and env stay in the Team 6 project; data stays on the RAID. It also keeps code out of the scanner's input by construction. `config.py` refuses a code root inside the source root. |
| D2 | uv project with `pyproject.toml` + `uv.lock`, `.venv` in FinalProject, Python 3.12. | Matches the lab convention (lab3 uses uv). 3.12 matches current Databricks runtimes, so pipeline code stays portable. Databricks Connect gets its own `.venv-dbconnect` later (Stage 4), per the PDF. |
| D3 | `pandas` is not installed yet. | Not needed for Stages 0 to 2 (YAGNI). Added with modeling. |
| D4 | Nested ZIPs (2020 to 2023 year ZIPs hold deflated monthly ZIPs) are extracted once to `processed/staging/nested/` and read from there. | A deflated inner ZIP is not seekable without re-decompressing from the start for every seek. Extraction writes only to the processed root. |
| D5 | Records are streamed with Python `csv`, typed with DuckDB. | Exact `source_row` provenance and identical row accounting in profiling and normalization. One timestamp parser for both. |
| D6 | Canonical trips drop `birth year`, `gender`, `bikeid`. | Not needed for station demand; birth year and gender are sensitive rider attributes. Re-addable from sources if a question needs them. |
| D7 | Equivalent copies are detected by an order-independent record digest, not only byte hashes. | The 2018 ZIP holds whole-month CSVs (quoted) and split parts (unquoted) with the same records but different bytes. |
| D8 | Workflow PDFs move to `docs/spec/` and are committed. | They are input specifications, not deliverables produced here, and the workflow PDF says to keep it in `docs/`. |

## Archive facts observed on 2026-10-01 (read-only survey)

These were seen directly; the inventory in Task 9 replaces them with complete evidence.

- 32 GiB total. Year ZIPs `2013` to `2023`, monthly ZIPs `202401` to `202608`, plus two extracted directories (`2013-citibike-tripdata/`, `202608-citibike-tripdata/`) that duplicate their ZIPs.
- Year ZIPs 2013 to 2019 contain CSVs under month folders (`4_April/201804-citibike-tripdata_1.csv`). Year ZIPs 2020 to 2023 contain monthly ZIPs (Deflate, about 80% ratio). Monthly ZIPs 2024+ contain split parts `_1.._N`.
- 2018 ZIP: each month appears as a whole quoted CSV at top level and as unquoted split parts in the month folder; April also has top-level split parts. First records match.
- 2013 extracted dir: whole-month CSVs at top level plus split parts in month folders.
- 2017 member names look like `201704-citibike-tripdata.csv_1.csv`.
- ZIPs contain `__MACOSX/` and `.DS_Store` members.
- Header families seen: legacy lowercase (`tripduration,starttime,...,gender`) 2013 to 2019, legacy Title Case (`Trip Duration,Start Time,...,Gender`) in some 2016 and 2017 files, modern (`ride_id,rideable_type,started_at,...,member_casual`) in the sampled 2020 nested ZIP onward.
- Start timestamp shapes: `YYYY-MM-DD HH:MM:SS`, `YYYY-MM-DD HH:MM:SS.ffff` (2018, 2019), `M/D/YYYY HH:MM:SS` (2014 to 2016), `M/D/YYYY H:MM` with no seconds (2015). Modern: `YYYY-MM-DD HH:MM:SS.fff`.
- Modern station IDs look decimal (`6322.01`); legacy are integers. Both stay strings.
- Machine: Apple M4, 10 cores, 16 GiB RAM. RAID: 5.6 TiB free. Internal disk: 14 GiB free.

## Module map and shared interfaces

Every task's implementer sees only their task; these names and types are the contract.

| Module | Public names |
|---|---|
| `config.py` | `PROJECT_ROOT: Path`, `DEFAULT_CONFIG: Path`, `ConfigError`, `Config` (frozen dataclass: `source_root`, `processed_root`, `exclude_dir_names: tuple[str,...]`, `memory_limit: str`, `threads: int`, `timezone: str`, `schema_version: int`, `parser_version: str`, `batch_rows: int`, `max_quarantine_fraction: float`, `pilot_releases: tuple[str,...]`; properties `manifest_db`, `staging_dir`, `nested_dir`, `tmp_dir`, `trips_dir`, `quarantine_dir`, `removed_dir`, `retired_dir`, `reports_dir`), `load_config(path=DEFAULT_CONFIG, project_root=PROJECT_ROOT) -> Config` |
| `db.py` | `sql_literal(s: str) -> str`, `connect(cfg, database=":memory:") -> duckdb.DuckDBPyConnection` |
| `cli.py` | `expand_releases(values: list[str]) -> list[str]`, `build_parser()`, `main(argv=None) -> int` |
| `naming.py` | `NominalRelease(release: str, part: int | None)`, `parse_nominal(name: str) -> NominalRelease | None` |
| `schemas.py` | `LEGACY_LOWER`, `LEGACY_TITLE`, `MODERN` (header tuples), `RAW_NAMES: dict[str, tuple[str,...]]`, `START_COLUMN`, `END_COLUMN: dict[str,int]`, `parse_header(line: bytes) -> tuple[str,...]`, `fingerprint(header) -> str`, `classify(header) -> str` (`legacy_lower` / `legacy_title` / `modern` / `unknown`) |
| `discover.py` | `SEP = "::"`, `Candidate` (frozen: `source_key, kind, path, container, member, size, file_size, modified`), `discover(cfg) -> Iterator[Candidate]`, `materialize_nested(cfg, outer_rel, inner_name) -> Path`, `open_candidate(cfg, cand)` (context manager yielding a binary stream), `candidate_from_record(rec: dict) -> Candidate` |
| `manifest.py` | `SOURCE_COLUMNS`, `Manifest(cfg)` (context manager) with `get`, `all`, `upsert`, `mark_missing`, `assign_duplicate_groups`, `start_run`, `finish_run`, `export_sources_csv`, `replace_published`, `unpublish`, `published_paths`, `published_for_sources`, `published_keys_for_releases`, `published_trip_paths(release)`, `record_source_run` |
| `inventory.py` | `run_discover(cfg, manifest, run_id, out) -> dict`, `run_inventory(cfg, args, out) -> int` |
| `records.py` | `NULL_TOKENS`, `parse_local_ts(value: str) -> datetime | None`, `open_records(binary) -> (header, iterator of (source_row, fields))` |
| `profile.py` | `MASK`, `row_hash(fields) -> int`, `profile_candidate(cfg, cand, family) -> dict`, `run_profile(cfg, manifest, run_id, releases, force, workers, out) -> dict` |
| `sets.py` | `SourceSet`, `ReleaseDecision`, `build_sets(records) -> dict[str, list[SourceSet]]`, `decide_release(release, sets, families) -> ReleaseDecision`, `apply_selection(manifest, releases, overrides, dry_run, out) -> int` |
| `report.py` | `parse_listing(xml_text) -> dict[str,int]`, `completeness(release, decision, release_records, listing) -> (str, str)`, `build_report(cfg, records, listing=None) -> str`, `write_report(cfg, manifest) -> Path` |
| `dst.py` | `dst_hours(years, tz_name) -> dict[(date, int), str]` |
| `normalize.py` | `NormalizeError`, `TRIPS_DDL`, `QUARANTINE_SCHEMA`, `REMOVED_SCHEMA`, `slug_for(source_key) -> str`, `transform_sql(family) -> str`, `SourceResult` (adds `removed_rows`, `spillover_conflicts`), `normalize_source(cfg, con, rec, run_id, staging_root, dst, published_trips=None) -> SourceResult` |
| `publish.py` | `retire_unlisted(cfg, manifest, run_id) -> list[Path]`, `publish_source(cfg, manifest, run_id, res) -> list[tuple]` |
| `pipeline.py` | `run_normalize(cfg, manifest, releases, dry_run=False, out=print) -> int` |
| `validate.py` | `validate_releases(cfg, manifest, releases) -> dict`, `write_validation_report(cfg, result) -> Path`, `run_validate(cfg, releases, out) -> int` |

Manifest `status` values: `discovered`, `profiled`, `selected`, `excluded`, `failed`, `missing`.

Final file layout:

```
FinalProject/
  pyproject.toml  uv.lock  .python-version  .gitignore
  config/local.yaml
  src/citibike_pipeline/{__init__,config,db,cli,naming,schemas,discover,manifest,inventory,records,profile,sets,report,dst,normalize,publish,pipeline,validate}.py
  tests/{archive_fixture.py,conftest.py,test_*.py}
  docs/spec/*.pdf  docs/implementation_status.md  docs/data_dictionary.md  docs/runbook.md
  docs/archive_inventory.md  docs/validation/*.md  docs/2026-10-01-data622-final-local-pipeline.md

/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/
  manifest/manifest.duckdb
  staging/<run_id>/<slug>/...   staging/nested/<outer>/<inner>.zip
  tmp/                          (DuckDB spill)
  trips/trip_year=YYYY/trip_month=MM/<slug>__<run_id>__<n>.parquet
  quarantine/<slug>__<run_id>.parquet
  retired/<run_id>/...
  reports/inventory_report.md  reports/manifest_sources.csv  reports/validate_*.md
```

---
## Stage 0: Project setup

### Task 1: Scaffold, environment, and data relocation

**Files:**
- Create: `pyproject.toml`, `.python-version`, `.gitignore`, `src/citibike_pipeline/__init__.py`, `tests/test_smoke.py`, `docs/implementation_status.md`
- Move: the four `Citi_Bike_*.pdf` files to `docs/spec/`; `station_status.json` to the RAID snapshots folder

**Interfaces:**
- Produces: an installable package `citibike_pipeline` with `__version__ = "0.1.0"`; `uv run` works from FinalProject.

- [ ] **Step 1: Relocate inputs (moves only, nothing deleted)**

```bash
cd /Users/randyhowk/Sites/CUNY/citibike-data-pipeline/teams/team-6
mkdir -p docs/spec config src/citibike_pipeline tests
mv Citi_Bike_*.pdf docs/spec/
mkdir -p /Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_snapshots
mv station_status.json /Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_snapshots/
ls docs/spec /Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_snapshots
```

Expected: four PDFs in `docs/spec/`, `station_status.json` on the RAID.

- [ ] **Step 2: Write `pyproject.toml`**

```toml
[project]
name = "citibike-pipeline"
version = "0.1.0"
description = "DATA 622 Final Project: Citi Bike archive inventory, normalization, and station-demand features"
requires-python = ">=3.12,<3.13"
dependencies = [
    "duckdb>=1.3",
    "pyarrow>=18",
    "pyyaml>=6.0",
]

[project.scripts]
citibike = "citibike_pipeline.cli:main"

[dependency-groups]
dev = ["pytest>=8.0"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/citibike_pipeline"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["tests"]
```

- [ ] **Step 3: Write `.gitignore` (FinalProject-level; the repo root already ignores `.venv`, `.env`, caches)**

```gitignore
# Environments
.venv/
.venv-*/
# Data never lives in the code project
*.parquet
*.duckdb
*.duckdb.wal
*.csv
*.zip
station_status*.json
data/
# Logs and local tooling
logs/
*.log
.databricks/
.databrickscfg
```

- [ ] **Step 4: Write the package marker and the smoke test**

`src/citibike_pipeline/__init__.py`:

```python
"""Citi Bike archive pipeline for the DATA 622 Final Project."""

__version__ = "0.1.0"
```

`tests/test_smoke.py`:

```python
import duckdb
import pyarrow
import yaml

import citibike_pipeline


def test_stack_imports():
    assert citibike_pipeline.__version__ == "0.1.0"
    major, minor = (int(x) for x in duckdb.__version__.split(".")[:2])
    assert (major, minor) >= (1, 3)
    assert pyarrow.__version__
    assert yaml.safe_load("a: 1") == {"a": 1}
```

- [ ] **Step 5: Create the environment and run the smoke test**

```bash
uv python pin 3.12
uv sync
uv run python --version
uv run pytest -q
```

Expected: `Python 3.12.x`, `1 passed`. `.venv/` exists inside FinalProject.

- [ ] **Step 6: Write `docs/implementation_status.md`**

```markdown
# Implementation status

Spec: `docs/spec/Citi_Bike_Local_to_Databricks_Codex_Workflow.pdf`.
Plan: `docs/2026-10-01-data622-final-local-pipeline.md`.

## Locations

- Code and env: `~/Sites/CUNY/citibike-data-pipeline/teams/team-6` (`.venv`, Python 3.12 via uv)
- Source archive (read-only): `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject`
- Processed data: `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed`
- Snapshots: `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_snapshots`

## Decisions

See the Decisions table in the plan (D1 to D8). New decisions are appended here with a date.

## Progress

- [ ] Stage 0: setup, CLI, config
- [ ] Stage 1: inventory (discover, profile, report, select)
- [ ] Stage 2: pilot normalization (2018-04, 2025-03), rerun check, validation
- [ ] Stage 2: recent year 2025
- [ ] Stage 3: aggregates and as-of features (separate plan)
- [ ] Stage 4: Databricks publication (separate plan; needs workspace details)

## Log

- 2026-10-01: Plan written. Archive surveyed read-only (facts in the plan).
```

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock .python-version .gitignore src/citibike_pipeline/__init__.py tests/test_smoke.py docs/spec docs/implementation_status.md
git commit -m "Scaffold DATA 622 final project pipeline environment"
```

### Task 2: Config, DuckDB connection, and CLI skeleton

**Files:**
- Create: `config/local.yaml`, `src/citibike_pipeline/config.py`, `src/citibike_pipeline/db.py`, `src/citibike_pipeline/cli.py`
- Test: `tests/test_config.py`, `tests/test_cli.py`

**Interfaces:**
- Produces: `Config`, `load_config`, `ConfigError`, `connect`, `sql_literal`, `main`, `expand_releases`.
- The CLI lazily imports handlers that later tasks create: `inventory.run_inventory(cfg, args, out) -> int` (Tasks 5 to 8), `pipeline.run_normalize(cfg, manifest, releases, dry_run, out) -> int` (Task 11), `validate.run_validate(cfg, releases, out) -> int` (Task 12).

- [ ] **Step 1: Write the failing tests**

`tests/test_config.py`:

```python
from pathlib import Path

import pytest
import yaml

from citibike_pipeline.config import ConfigError, load_config


def write_cfg(tmp_path, source, processed, **extra):
    data = {
        "schema_version": 1,
        "parser_version": "test",
        "paths": {"source_root": str(source), "processed_root": str(processed)},
        "exclude_dir_names": ["citibike-production", "__MACOSX"],
        "duckdb": {"memory_limit": "1GB", "threads": 2},
    }
    data.update(extra)
    path = tmp_path / "local.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def test_load_config_resolves_paths(tmp_path):
    cfg = load_config(write_cfg(tmp_path, tmp_path / "src", tmp_path / "out"))
    assert cfg.source_root == (tmp_path / "src").resolve()
    assert cfg.processed_root == (tmp_path / "out").resolve()
    assert cfg.exclude_dir_names == ("citibike-production", "__MACOSX")
    assert cfg.threads == 2 and cfg.memory_limit == "1GB"
    assert cfg.batch_rows == 250_000
    assert cfg.max_quarantine_fraction == 0.01
    assert cfg.pilot_releases == ()


def test_derived_paths_live_under_processed_root(tmp_path):
    cfg = load_config(write_cfg(tmp_path, tmp_path / "src", tmp_path / "out"))
    out = (tmp_path / "out").resolve()
    assert cfg.manifest_db == out / "manifest" / "manifest.duckdb"
    assert cfg.tmp_dir == out / "tmp"
    assert cfg.nested_dir == out / "staging" / "nested"
    assert cfg.trips_dir == out / "trips"
    assert cfg.retired_dir == out / "retired"


def test_rejects_processed_inside_source(tmp_path):
    with pytest.raises(ConfigError):
        load_config(write_cfg(tmp_path, tmp_path / "src", tmp_path / "src" / "out"))


def test_rejects_source_inside_processed(tmp_path):
    with pytest.raises(ConfigError):
        load_config(write_cfg(tmp_path, tmp_path / "out" / "src", tmp_path / "out"))


def test_rejects_project_inside_source(tmp_path):
    path = write_cfg(tmp_path, tmp_path / "src", tmp_path / "out")
    with pytest.raises(ConfigError):
        load_config(path, project_root=tmp_path / "src" / "code")


def test_real_config_parses():
    cfg = load_config()
    assert cfg.source_root == Path("/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject")
    assert cfg.processed_root == Path("/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed")
    assert len(cfg.pilot_releases) == 2  # exact months may change in Task 9
```

`tests/test_cli.py`:

```python
import argparse

import pytest
import yaml

from citibike_pipeline.cli import expand_releases, main


@pytest.fixture
def cfg_file(tmp_path):
    (tmp_path / "archive").mkdir()
    path = tmp_path / "local.yaml"
    path.write_text(yaml.safe_dump({
        "schema_version": 1,
        "parser_version": "test",
        "paths": {"source_root": str(tmp_path / "archive"), "processed_root": str(tmp_path / "processed")},
    }))
    return path


def test_help_lists_subcommands(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    for name in ("doctor", "inventory", "normalize", "validate", "aggregate", "publish-plan"):
        assert name in out


def test_doctor_reports_paths(cfg_file, capsys):
    assert main(["--config", str(cfg_file), "doctor"]) == 0
    out = capsys.readouterr().out
    assert "source_root" in out and "processed_root" in out and "python 3.12" in out


def test_later_stage_commands_are_explicitly_unavailable(cfg_file, capsys):
    assert main(["--config", str(cfg_file), "aggregate"]) == 2
    assert main(["--config", str(cfg_file), "publish-plan"]) == 2
    assert "not implemented" in capsys.readouterr().err


def test_bad_config_returns_2(tmp_path, capsys):
    assert main(["--config", str(tmp_path / "nope.yaml"), "doctor"]) == 2


def test_expand_releases():
    assert expand_releases(["2025"]) == [f"2025-{m:02d}" for m in range(1, 13)]
    assert expand_releases(["2018-04", "2018-04", "2015-01"]) == ["2015-01", "2018-04"]
    for bad in ("2025-13", "25-01", "2025-1"):
        with pytest.raises(argparse.ArgumentTypeError):
            expand_releases([bad])
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `uv run pytest tests/test_config.py tests/test_cli.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'citibike_pipeline.config'`.

- [ ] **Step 3: Write `config/local.yaml`**

```yaml
# Local pipeline configuration. Data never lives in the code project.
schema_version: 1
parser_version: "0.1.0"
paths:
  source_root: /Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject
  processed_root: /Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed
# Directory names never scanned inside source_root (dot-directories are always skipped).
exclude_dir_names:
  - citibike-production
  - __MACOSX
duckdb:
  memory_limit: 6GB   # of 16 GiB RAM; spill goes to processed_root/tmp on the RAID
  threads: 6
timezone: America/New_York
batch_rows: 250000
# A source whose quarantined share exceeds this is not published.
max_quarantine_fraction: 0.01
pilot:
  # 2018-04: three copies in the 2018 ZIP, fractional legacy timestamps.
  # 2025-03: modern schema, DST spring-forward, inside the recent full year.
  releases: ["2018-04", "2025-03"]
```

- [ ] **Step 4: Write `src/citibike_pipeline/config.py`**

```python
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
    if _is_within(processed_root, source_root) or _is_within(source_root, processed_root):
        raise ConfigError("processed_root and source_root must not contain one another")
    if _is_within(Path(project_root).resolve(), source_root):
        raise ConfigError("the code project must not live inside source_root")
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
    )
```

- [ ] **Step 5: Write `src/citibike_pipeline/db.py`**

```python
"""DuckDB connections with the configured memory, thread, and spill settings."""

from __future__ import annotations

import duckdb

from .config import Config


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(cfg: Config, database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    cfg.tmp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database)
    con.execute(f"SET memory_limit = {sql_literal(cfg.memory_limit)}")
    con.execute(f"SET threads = {int(cfg.threads)}")
    con.execute(f"SET temp_directory = {sql_literal(str(cfg.tmp_dir))}")
    con.execute("SET preserve_insertion_order = true")
    return con
```

- [ ] **Step 6: Write `src/citibike_pipeline/cli.py`**

```python
"""`citibike` command line entry point."""

from __future__ import annotations

import argparse
import os
import platform
import re
import shutil
import sys
from pathlib import Path

from .config import DEFAULT_CONFIG, ConfigError, load_config

_RELEASE_ARG = re.compile(r"^(\d{4})(?:-(\d{2}))?$")


def expand_releases(values: list[str]) -> list[str]:
    """Accepts YYYY (all twelve months) or YYYY-MM; returns sorted unique YYYY-MM."""
    out: list[str] = []
    for value in values:
        m = _RELEASE_ARG.match(value)
        if not m or (m[2] and not 1 <= int(m[2]) <= 12):
            raise argparse.ArgumentTypeError(f"release must be YYYY or YYYY-MM, got {value!r}")
        out.extend([value] if m[2] else [f"{m[1]}-{mm:02d}" for mm in range(1, 13)])
    return sorted(set(out))


def _doctor(cfg, args) -> int:
    import duckdb
    import pyarrow

    print(f"python {platform.python_version()} ({platform.machine()})")
    print(f"duckdb {duckdb.__version__}, pyarrow {pyarrow.__version__}")
    ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    print(f"cpus {os.cpu_count()}, ram {ram / 2**30:.1f} GiB")
    ok = True
    for label, path in (("source_root", cfg.source_root), ("processed_root", cfg.processed_root)):
        exists = path.exists()
        print(f"{label}: {path} ({'ok' if exists else 'not present'})")
        probe = path if exists else path.parent
        if probe.exists():
            print(f"  free {shutil.disk_usage(probe).free / 2**30:.0f} GiB on {probe}")
        if label == "source_root" and not exists:
            ok = False
    print(f"duckdb memory_limit {cfg.memory_limit}, threads {cfg.threads}, temp_directory {cfg.tmp_dir}")
    return 0 if ok else 1


def _later_stage(cfg, args) -> int:
    print(f"`{args.command}` is not implemented in this phase (see docs/implementation_status.md)", file=sys.stderr)
    return 2


def _inventory(cfg, args) -> int:
    from .inventory import run_inventory

    return run_inventory(cfg, args, print)


def _normalize(cfg, args) -> int:
    from .manifest import Manifest
    from .pipeline import run_normalize

    with Manifest(cfg) as manifest:
        return run_normalize(cfg, manifest, args.releases, dry_run=args.dry_run, out=print)


def _validate(cfg, args) -> int:
    from .validate import run_validate

    return run_validate(cfg, args.releases, print)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="citibike", description="Citi Bike archive pipeline (DATA 622)")
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="path to local.yaml")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="report environment, resources, and resolved paths").set_defaults(handler=_doctor)

    inv = sub.add_parser("inventory", help="discover, profile, report, and select archive sources")
    inv.set_defaults(handler=_inventory)
    isub = inv.add_subparsers(dest="action", required=True)
    isub.add_parser("discover", help="list CSV and ZIP-member candidates and their headers")
    prof = isub.add_parser("profile", help="stream candidates for hashes, row counts, and date coverage")
    prof.add_argument("--release", action="append", help="YYYY or YYYY-MM (repeatable)")
    prof.add_argument("--all", action="store_true", help="profile every candidate")
    prof.add_argument("--force", action="store_true", help="re-profile even if already profiled")
    prof.add_argument("--workers", type=int, default=1)
    isub.add_parser("report", help="write the inventory report and CSV manifest export")
    sel = isub.add_parser("select", help="choose one representative source set per release")
    sel.add_argument("--release", action="append", required=True)
    sel.add_argument("--set", action="append", default=[], metavar="RELEASE=SET_ID",
                     help="manual choice for a release whose copies conflict")
    sel.add_argument("--dry-run", action="store_true")

    norm = sub.add_parser("normalize", help="normalize selected releases to Parquet and publish")
    norm.add_argument("--release", action="append", required=True)
    norm.add_argument("--dry-run", action="store_true")
    norm.set_defaults(handler=_normalize)

    val = sub.add_parser("validate", help="reconcile and profile published releases")
    val.add_argument("--release", action="append", required=True)
    val.set_defaults(handler=_validate)

    sub.add_parser("aggregate", help="Stage 3 station aggregates (later plan)").set_defaults(handler=_later_stage)
    sub.add_parser("publish-plan", help="Stage 4 Databricks publish plan (later plan)").set_defaults(handler=_later_stage)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.releases = expand_releases(args.release) if getattr(args, "release", None) else None
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    try:
        cfg = load_config(args.config)
    except (ConfigError, OSError, KeyError) as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    return args.handler(cfg, args)


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 7: Run the tests**

Run: `uv run pytest -q`
Expected: all pass (smoke, config, cli).

- [ ] **Step 8: Check the real paths**

Run: `uv run citibike doctor`
Expected: `source_root: /Volumes/BigRaid/.../FinalProject (ok)`, `processed_root: ... (not present)` with free space of the RAID shown, exit code 0. Paste the output into the Log section of `docs/implementation_status.md`.

- [ ] **Step 9: Commit**

```bash
git add config/local.yaml src/citibike_pipeline/config.py src/citibike_pipeline/db.py src/citibike_pipeline/cli.py tests/test_config.py tests/test_cli.py docs/implementation_status.md
git commit -m "Add pipeline config, DuckDB connection, and CLI skeleton"
```

## Stage 1: Inventory the archive before conversion

### Task 3: Release names and header families

**Files:**
- Create: `src/citibike_pipeline/naming.py`, `src/citibike_pipeline/schemas.py`
- Test: `tests/test_naming_schemas.py`

**Interfaces:**
- Produces: `parse_nominal`, `NominalRelease`, `parse_header`, `fingerprint`, `classify`, `RAW_NAMES`, `START_COLUMN`, `END_COLUMN`, `LEGACY_LOWER`, `LEGACY_TITLE`, `MODERN`.

- [ ] **Step 1: Write the failing tests**

```python
import pytest

from citibike_pipeline.naming import NominalRelease, parse_nominal
from citibike_pipeline.schemas import (
    LEGACY_LOWER, LEGACY_TITLE, MODERN, RAW_NAMES, classify, fingerprint, parse_header,
)


@pytest.mark.parametrize("name, expected", [
    ("201306-citibike-tripdata.csv", NominalRelease("2013-06", None)),
    ("2018-citibike-tripdata/4_April/201804-citibike-tripdata_2.csv", NominalRelease("2018-04", 2)),
    ("2017-citibike-tripdata/4_April/201704-citibike-tripdata.csv_1.csv", NominalRelease("2017-04", 1)),
    ("202608-citibike-tripdata.zip", NominalRelease("2026-08", None)),
    ("2013-citibike-tripdata.zip", None),
    ("201313-citibike-tripdata.csv", None),
    ("notes.csv", None),
])
def test_parse_nominal(name, expected):
    assert parse_nominal(name) == expected


def test_parse_header_handles_quotes_bom_and_crlf():
    assert parse_header(b'\xef\xbb\xbf"tripduration","starttime"\r\n') == ("tripduration", "starttime")
    assert parse_header(b"ride_id, rideable_type \n") == ("ride_id", "rideable_type")
    assert parse_header(b"") == ()


def test_classify_known_families_exactly():
    assert classify(LEGACY_LOWER) == "legacy_lower"
    assert classify(LEGACY_TITLE) == "legacy_title"
    assert classify(MODERN) == "modern"
    assert classify(("foo", "bar")) == "unknown"
    assert classify(MODERN[:-1]) == "unknown"


def test_fingerprint_is_stable_and_order_sensitive():
    assert fingerprint(MODERN) == fingerprint(tuple(MODERN))
    assert fingerprint(("a", "b")) != fingerprint(("b", "a"))


def test_raw_names_match_header_widths():
    assert len(RAW_NAMES["legacy_lower"]) == len(LEGACY_LOWER)
    assert len(RAW_NAMES["legacy_title"]) == len(LEGACY_TITLE)
    assert len(RAW_NAMES["modern"]) == len(MODERN)
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_naming_schemas.py -q`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write `src/citibike_pipeline/naming.py`**

```python
"""Filename-derived nominal release (YYYY-MM) and part number. Never proof of content dates."""

from __future__ import annotations

import re
from dataclasses import dataclass

_RELEASE = re.compile(
    r"(?P<year>20\d{2})(?P<month>\d{2})-citibike-tripdata(?:\.csv)?(?:_(?P<part>\d+))?\.(?:csv|zip)"
)


@dataclass(frozen=True)
class NominalRelease:
    release: str
    part: int | None


def parse_nominal(name: str) -> NominalRelease | None:
    base = name.rsplit("/", 1)[-1]
    m = _RELEASE.fullmatch(base)
    if not m or not 1 <= int(m["month"]) <= 12:
        return None
    return NominalRelease(f"{m['year']}-{m['month']}", int(m["part"]) if m["part"] else None)
```

- [ ] **Step 4: Write `src/citibike_pipeline/schemas.py`**

```python
"""Header families observed in the archive. Classification is by exact header, never by year."""

from __future__ import annotations

import csv
import hashlib
import json

LEGACY_LOWER = (
    "tripduration", "starttime", "stoptime", "start station id", "start station name",
    "start station latitude", "start station longitude", "end station id", "end station name",
    "end station latitude", "end station longitude", "bikeid", "usertype", "birth year", "gender",
)
LEGACY_TITLE = (
    "Trip Duration", "Start Time", "Stop Time", "Start Station ID", "Start Station Name",
    "Start Station Latitude", "Start Station Longitude", "End Station ID", "End Station Name",
    "End Station Latitude", "End Station Longitude", "Bike ID", "User Type", "Birth Year", "Gender",
)
MODERN = (
    "ride_id", "rideable_type", "started_at", "ended_at", "start_station_name", "start_station_id",
    "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
)
FAMILIES = {LEGACY_LOWER: "legacy_lower", LEGACY_TITLE: "legacy_title", MODERN: "modern"}

# Positional staging names per family; normalize.transform_sql reads these.
_LEGACY_RAW = (
    "src_duration", "start_ts", "end_ts", "start_station_id", "start_station_name", "start_lat",
    "start_lng", "end_station_id", "end_station_name", "end_lat", "end_lng", "bike_id",
    "membership", "birth_year", "gender",
)
_MODERN_RAW = (
    "ride_id", "rideable_type", "start_ts", "end_ts", "start_station_name", "start_station_id",
    "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "membership",
)
RAW_NAMES = {"legacy_lower": _LEGACY_RAW, "legacy_title": _LEGACY_RAW, "modern": _MODERN_RAW}
START_COLUMN = {"legacy_lower": 1, "legacy_title": 1, "modern": 2}
END_COLUMN = {"legacy_lower": 2, "legacy_title": 2, "modern": 3}


def parse_header(first_line: bytes) -> tuple[str, ...]:
    text = first_line.decode("utf-8-sig").rstrip("\r\n")
    if not text:
        return ()
    return tuple(c.strip() for c in next(csv.reader([text])))


def fingerprint(header) -> str:
    return hashlib.sha256(json.dumps(list(header)).encode()).hexdigest()[:16]


def classify(header) -> str:
    return FAMILIES.get(tuple(header), "unknown")
```

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests/test_naming_schemas.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/citibike_pipeline/naming.py src/citibike_pipeline/schemas.py tests/test_naming_schemas.py
git commit -m "Add nominal release parsing and header family classification"
```

### Task 4: Miniature archive fixture and source discovery

**Files:**
- Create: `tests/archive_fixture.py`, `tests/conftest.py`, `src/citibike_pipeline/discover.py`
- Test: `tests/test_discover.py`

**Interfaces:**
- Consumes: `Config`, `load_config` (Task 2).
- Produces: `Candidate`, `discover`, `open_candidate`, `materialize_nested`, `candidate_from_record`, `SEP`. Test helpers: `build_archive(root) -> Path`, key constants, `EXPECTED_KEYS`, `FIXTURE_RELEASES`, `QUIET`, and the `cfg` fixture.

- [ ] **Step 1: Write `tests/archive_fixture.py`** (mirrors the real layouts listed under "Archive facts")

```python
"""Builds a miniature Citi Bike archive that mirrors the layouts found on the RAID."""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

from citibike_pipeline.schemas import LEGACY_LOWER, LEGACY_TITLE, MODERN

GOOD_201804_A = ["840", "2018-04-01 03:07:39.0700", "2018-04-01 03:21:39.1280", "72", "W 52 St & 11 Ave",
                 "40.76727216", "-73.99392888", "379", "W 31 St & 7 Ave", "40.749156", "-73.9916",
                 "31977", "Subscriber", "1973", "1"]
BAD_201804 = ["120", "bad-date", "2018-04-03 10:02:00.0000", "72", "W 52 St & 11 Ave", "40.76727216",
              "-73.99392888", "379", "W 31 St & 7 Ave", "40.749156", "-73.9916", "31979", "Subscriber",
              "1990", "1"]
GOOD_201804_B = ["600", "2018-04-02 10:00:00.0000", "2018-04-02 10:10:00.0000", "\\N", "", "0.0", "0.0",
                 "379", "W 31 St & 7 Ave", "40.749156", "-73.9916", "31978", "Customer", "1980", "2"]
ROWS_201804 = [GOOD_201804_A, BAD_201804, GOOD_201804_B]

ROWS_201501 = [
    ["1346", "1/1/2015 0:01", "1/1/2015 0:24", "455", "1 Ave & E 44 St", "40.75001986", "-73.96905301",
     "265", "Stanton St & Chrystie St", "40.72229346", "-73.99147535", "18660", "Subscriber", "1960", "2"],
    ["363", "1/1/2015 00:02:00", "1/1/2015 00:08:03", "434", "9 Ave & W 18 St", "40.74317449",
     "-74.00366443", "482", "W 15 St & 7 Ave", "40.73935542", "-73.99931783", "16085", "Customer", "", "0"],
]
ROWS_201612 = [
    ["528", "2016-12-01 00:00:04", "2016-12-01 00:08:52", "499", "Broadway & W 60 St", "40.76915505",
     "-73.98191841", "228", "E 48 St & 3 Ave", "40.7546011026", "-73.971878855", "26931", "Subscriber",
     "1964.0", "1"],
]
_D4 = ["D4", "classic_bike", "2023-11-05 01:30:00.000", "2023-11-05 01:45:00.000", "E 33 St & 5 Ave",
       "6322.01", "W 24 St & 7 Ave", "6257.03", "40.74765947", "-73.98490707", "40.74487634",
       "-73.99529885", "member"]
_D4_CONFLICT = _D4[:6] + ["E 33 St & 5 Ave", "6322.01"] + _D4[8:10] + ["40.74765947", "-73.98490707", "member"]
_E5 = ["E5", "classic_bike", "2023-11-06 08:00:00", "2023-11-06 08:10:00", "E 33 St & 5 Ave", "6322.01",
       "W 24 St & 7 Ave", "6257.03", "40.74765947", "-73.98490707", "40.74487634", "-73.99529885", "member"]
ROWS_202311 = [_D4, _D4_CONFLICT, _E5, list(_E5), ["F6", "classic_bike", "2023-11-07 08:00:00"]]
ROWS_202501 = [
    ["E4A8132DACF670C3", "electric_bike", "2025-01-10 14:31:22.100", "2025-01-10 14:37:37.034",
     "E 33 St & 5 Ave", "6322.01", "W 24 St & 7 Ave", "6257.03", "40.74765947", "-73.98490707",
     "40.74487634", "-73.99529885", "member"],
    ["B2", "classic_bike", "2025-01-31 23:55:00.000", "2025-02-01 00:05:00.000", "W 24 St & 7 Ave",
     "6257.03", "", "", "40.74487634", "-73.99529885", "", "", "casual"],
    ["C3", "classic_bike", "2024-12-31 23:50:00.000", "2025-01-01 00:10:00.000", "E 33 St & 5 Ave",
     "6322.01", "W 24 St & 7 Ave", "6257.03", "40.74765947", "-73.98490707", "40.74487634",
     "-73.99529885", "member"],
]

KEY_2015 = "2015-citibike-tripdata.zip::2015-citibike-tripdata/1_January/201501-citibike-tripdata_1.csv"
KEY_2016 = "2016-citibike-tripdata.zip::2016-citibike-tripdata/12_December/201612-citibike-tripdata_1.csv"
KEY_2018_WHOLE = "2018-citibike-tripdata.zip::2018-citibike-tripdata/201804-citibike-tripdata.csv"
KEY_2018_P1 = "2018-citibike-tripdata.zip::2018-citibike-tripdata/4_April/201804-citibike-tripdata_1.csv"
KEY_2018_P2 = "2018-citibike-tripdata.zip::2018-citibike-tripdata/4_April/201804-citibike-tripdata_2.csv"
KEY_UNKNOWN = "201902-citibike-tripdata.csv"
KEY_2023_NESTED = ("2023-citibike-tripdata.zip::2023-citibike-tripdata/202311-citibike-tripdata.zip"
                   "::202311-citibike-tripdata_1.csv")
KEY_2025_ZIP = "202501-citibike-tripdata.zip::202501-citibike-tripdata_1.csv"
KEY_2025_CSV = "202501-citibike-tripdata/202501-citibike-tripdata_1.csv"
EXPECTED_KEYS = {KEY_2015, KEY_2016, KEY_2018_WHOLE, KEY_2018_P1, KEY_2018_P2, KEY_UNKNOWN,
                 KEY_2023_NESTED, KEY_2025_ZIP, KEY_2025_CSV}
FIXTURE_RELEASES = ["2015-01", "2016-12", "2018-04", "2023-11", "2025-01"]


def QUIET(*_args, **_kwargs):
    pass


def csv_bytes(header, rows, quoting=csv.QUOTE_MINIMAL) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=quoting, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode()


def zip_bytes(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def build_archive(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "2015-citibike-tripdata.zip").write_bytes(zip_bytes({
        "2015-citibike-tripdata/1_January/201501-citibike-tripdata_1.csv": csv_bytes(LEGACY_LOWER, ROWS_201501),
    }))
    (root / "2016-citibike-tripdata.zip").write_bytes(zip_bytes({
        "2016-citibike-tripdata/12_December/201612-citibike-tripdata_1.csv": csv_bytes(LEGACY_TITLE, ROWS_201612),
    }))
    (root / "2018-citibike-tripdata.zip").write_bytes(zip_bytes({
        "2018-citibike-tripdata/201804-citibike-tripdata.csv": csv_bytes(LEGACY_LOWER, ROWS_201804, csv.QUOTE_ALL),
        "2018-citibike-tripdata/4_April/201804-citibike-tripdata_1.csv": csv_bytes(LEGACY_LOWER, ROWS_201804[:2]),
        "2018-citibike-tripdata/4_April/201804-citibike-tripdata_2.csv": csv_bytes(LEGACY_LOWER, ROWS_201804[2:]),
        "2018-citibike-tripdata/.DS_Store": b"junk",
        "__MACOSX/2018-citibike-tripdata/._201804-citibike-tripdata.csv": b"junk",
    }))
    (root / "201902-citibike-tripdata.csv").write_bytes(csv_bytes(["foo", "bar"], [["1", "2"]]))
    inner = zip_bytes({"202311-citibike-tripdata_1.csv": csv_bytes(MODERN, ROWS_202311)})
    (root / "2023-citibike-tripdata.zip").write_bytes(zip_bytes({
        "2023-citibike-tripdata/202311-citibike-tripdata.zip": inner,
        "__MACOSX/2023-citibike-tripdata/._202311-citibike-tripdata.zip": b"junk",
    }))
    modern_2501 = csv_bytes(MODERN, ROWS_202501)
    (root / "202501-citibike-tripdata.zip").write_bytes(zip_bytes({"202501-citibike-tripdata_1.csv": modern_2501}))
    (root / "202501-citibike-tripdata").mkdir()
    (root / "202501-citibike-tripdata" / "202501-citibike-tripdata_1.csv").write_bytes(modern_2501)
    for excluded in ("citibike-production", ".hidden"):
        (root / excluded).mkdir()
        (root / excluded / "202501-citibike-tripdata_1.csv").write_bytes(modern_2501)
    (root / ".DS_Store").write_bytes(b"junk")
    return root
```

- [ ] **Step 2: Write `tests/conftest.py`**

```python
import pytest
import yaml

from archive_fixture import build_archive
from citibike_pipeline.config import load_config


@pytest.fixture
def cfg(tmp_path):
    source = build_archive(tmp_path / "archive")
    path = tmp_path / "local.yaml"
    path.write_text(yaml.safe_dump({
        "schema_version": 1,
        "parser_version": "test",
        "paths": {"source_root": str(source), "processed_root": str(tmp_path / "processed")},
        "exclude_dir_names": ["citibike-production", "__MACOSX"],
        "duckdb": {"memory_limit": "1GB", "threads": 2},
        "timezone": "America/New_York",
        "batch_rows": 2,
        "max_quarantine_fraction": 0.5,
    }))
    return load_config(path)
```

- [ ] **Step 3: Write the failing tests in `tests/test_discover.py`**

```python
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
```

- [ ] **Step 4: Run to see them fail**

Run: `uv run pytest tests/test_discover.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'citibike_pipeline.discover'`.

- [ ] **Step 5: Write `src/citibike_pipeline/discover.py`**

```python
"""Enumerates candidate CSVs: plain files, ZIP members, and members of ZIPs nested in ZIPs.

The archive is opened read-only. Nested ZIPs are copied once into processed staging.
"""

from __future__ import annotations

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


def _walk_files(cfg: Config) -> Iterator[Path]:
    excluded = set(cfg.exclude_dir_names)
    for dirpath, dirnames, filenames in os.walk(cfg.source_root):
        dirnames[:] = sorted(d for d in dirnames if d not in excluded and not d.startswith("."))
        for name in sorted(filenames):
            if not name.startswith("."):
                yield Path(dirpath) / name


def _nested_cache_path(cfg: Config, outer_rel: str, inner_name: str) -> Path:
    return cfg.nested_dir / outer_rel.replace("/", "__") / inner_name.replace("/", "__")


def materialize_nested(cfg: Config, outer_rel: str, inner_name: str) -> Path:
    """Copies one nested ZIP out of its container once; CRC is verified by zipfile at EOF."""
    target = _nested_cache_path(cfg, outer_rel, inner_name)
    done = target.with_suffix(target.suffix + ".done")
    if done.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(target.suffix + ".partial")
    with zipfile.ZipFile(cfg.source_root / outer_rel) as outer, outer.open(inner_name) as src, \
            open(partial, "wb") as dst:
        shutil.copyfileobj(src, dst, 1 << 20)
    os.replace(partial, target)
    done.write_text("ok\n")
    return target


def discover(cfg: Config) -> Iterator[Candidate]:
    for f in _walk_files(cfg):
        rel = f.relative_to(cfg.source_root).as_posix()
        st = f.stat()
        lower = f.name.lower()
        if lower.endswith(".csv"):
            yield Candidate(rel, "csv", rel, None, None, st.st_size, st.st_size, st.st_mtime)
        elif lower.endswith(".zip"):
            with zipfile.ZipFile(f) as zf:
                for info in zf.infolist():
                    name = info.filename
                    if _skip_member(name):
                        continue
                    if name.lower().endswith(".csv"):
                        yield Candidate(f"{rel}{SEP}{name}", "zip_member", rel, rel, name,
                                        info.file_size, st.st_size, st.st_mtime)
                    elif name.lower().endswith(".zip"):
                        container = f"{rel}{SEP}{name}"
                        with zipfile.ZipFile(materialize_nested(cfg, rel, name)) as inner:
                            for ii in inner.infolist():
                                if _skip_member(ii.filename) or not ii.filename.lower().endswith(".csv"):
                                    continue
                                yield Candidate(f"{container}{SEP}{ii.filename}", "nested_zip_member", rel,
                                                container, ii.filename, ii.file_size, st.st_size, st.st_mtime)


@contextmanager
def open_candidate(cfg: Config, cand: Candidate):
    if cand.kind == "csv":
        with open(cfg.source_root / cand.path, "rb") as fh:
            yield fh
    elif cand.kind == "zip_member":
        with zipfile.ZipFile(cfg.source_root / cand.path) as zf, zf.open(cand.member) as fh:
            yield fh
    else:
        outer_rel, inner_name = cand.container.split(SEP)
        with zipfile.ZipFile(materialize_nested(cfg, outer_rel, inner_name)) as zf, zf.open(cand.member) as fh:
            yield fh


def candidate_from_record(rec: dict) -> Candidate:
    return Candidate(rec["source_key"], rec["kind"], rec["path"], rec["container"], rec["member"],
                     rec["size"], rec["file_size"], rec["modified"])
```

- [ ] **Step 6: Run the tests**

Run: `uv run pytest tests/test_discover.py -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add tests/archive_fixture.py tests/conftest.py tests/test_discover.py src/citibike_pipeline/discover.py
git commit -m "Discover archive candidates across CSVs, ZIP members, and nested ZIPs"
```

### Task 5: Manifest store and `inventory discover`

**Files:**
- Create: `src/citibike_pipeline/manifest.py`, `src/citibike_pipeline/inventory.py`
- Test: `tests/test_manifest_discover.py`

**Interfaces:**
- Consumes: `connect`, `sql_literal` (Task 2); `discover`, `open_candidate` (Task 4); `parse_nominal`, `parse_header`, `fingerprint`, `classify` (Task 3).
- Produces: `Manifest` (all methods listed in the module map), `SOURCE_COLUMNS`, `run_discover(cfg, manifest, run_id, out) -> dict` with keys `candidates`, `new`, `changed`, `unchanged`, `missing`; `run_inventory(cfg, args, out) -> int` handling `discover` (Tasks 6 to 8 add `profile`, `select`, `report`).

- [ ] **Step 1: Write the failing tests**

```python
from archive_fixture import EXPECTED_KEYS, KEY_2016, KEY_2018_WHOLE, KEY_UNKNOWN, QUIET
from citibike_pipeline.inventory import run_discover
from citibike_pipeline.manifest import Manifest


def test_discover_populates_manifest(cfg):
    with Manifest(cfg) as m:
        stats = run_discover(cfg, m, "r1", QUIET)
        recs = {r["source_key"]: r for r in m.all()}
    assert stats["new"] == len(EXPECTED_KEYS) and set(recs) == EXPECTED_KEYS
    whole = recs[KEY_2018_WHOLE]
    assert whole["schema_family"] == "legacy_lower"
    assert (whole["nominal_release"], whole["nominal_part"]) == ("2018-04", None)
    assert whole["status"] == "discovered" and whole["discovered_run"] == "r1"
    assert recs[KEY_2016]["schema_family"] == "legacy_title"
    assert recs[KEY_UNKNOWN]["schema_family"] == "unknown"


def test_rediscovery_is_resumable(cfg):
    with Manifest(cfg) as m:
        run_discover(cfg, m, "r1", QUIET)
        stats = run_discover(cfg, m, "r2", QUIET)
    assert stats["unchanged"] == len(EXPECTED_KEYS) and stats.get("new", 0) == 0


def test_vanished_source_is_marked_missing(cfg):
    with Manifest(cfg) as m:
        run_discover(cfg, m, "r1", QUIET)
        (cfg.source_root / KEY_UNKNOWN).rename(cfg.source_root / "201902-renamed.txt")
        stats = run_discover(cfg, m, "r2", QUIET)
        assert stats["missing"] == 1
        assert m.get(KEY_UNKNOWN)["status"] == "missing"


def test_runs_are_recorded(cfg):
    with Manifest(cfg) as m:
        run_id = m.start_run("inventory discover", {"x": 1})
        m.finish_run(run_id, "ok")
        row = m.con.execute("SELECT status, finished_at IS NOT NULL FROM runs WHERE run_id = ?", [run_id]).fetchone()
    assert row == ("ok", True)
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_manifest_discover.py -q`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write `src/citibike_pipeline/manifest.py`**

```python
"""Resumable manifest of sources, runs, and the published-file ledger (DuckDB on the RAID)."""

from __future__ import annotations

import json
import secrets
from datetime import datetime
from pathlib import Path

from .config import Config
from .db import connect, sql_literal

SOURCE_COLUMNS = (
    "source_key", "kind", "path", "container", "member", "size", "file_size", "modified",
    "header_json", "schema_fingerprint", "schema_family", "nominal_release", "nominal_part",
    "sha256", "row_digest", "rows", "malformed_rows", "invalid_start_rows",
    "min_start_date", "max_start_date", "day_counts_json",
    "status", "status_reason", "duplicate_group",
    "discovered_run", "profiled_run", "parser_version", "profile_seconds",
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
  source_key VARCHAR PRIMARY KEY, kind VARCHAR NOT NULL, path VARCHAR NOT NULL,
  container VARCHAR, member VARCHAR, size BIGINT, file_size BIGINT, modified DOUBLE,
  header_json VARCHAR, schema_fingerprint VARCHAR, schema_family VARCHAR,
  nominal_release VARCHAR, nominal_part INTEGER,
  sha256 VARCHAR, row_digest VARCHAR, rows BIGINT, malformed_rows BIGINT, invalid_start_rows BIGINT,
  min_start_date VARCHAR, max_start_date VARCHAR, day_counts_json VARCHAR,
  status VARCHAR NOT NULL, status_reason VARCHAR, duplicate_group VARCHAR,
  discovered_run VARCHAR, profiled_run VARCHAR, parser_version VARCHAR, profile_seconds DOUBLE
);
CREATE TABLE IF NOT EXISTS runs (
  run_id VARCHAR PRIMARY KEY, command VARCHAR, args_json VARCHAR,
  started_at TIMESTAMP, finished_at TIMESTAMP, status VARCHAR
);
CREATE TABLE IF NOT EXISTS published_files (
  source_key VARCHAR, run_id VARCHAR, dataset VARCHAR, path VARCHAR, rows BIGINT, published_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS source_runs (
  run_id VARCHAR, source_key VARCHAR, release VARCHAR, source_rows BIGINT, published_rows BIGINT,
  quarantined_rows BIGINT, status VARCHAR, message VARCHAR, seconds DOUBLE, finished_at TIMESTAMP
);
"""


class Manifest:
    """One process at a time: DuckDB holds a write lock on the manifest file."""

    def __init__(self, cfg: Config):
        cfg.manifest_db.parent.mkdir(parents=True, exist_ok=True)
        self.con = connect(cfg, str(cfg.manifest_db))
        self.con.execute(SCHEMA)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.con.close()

    def _dicts(self, sql: str, params=()) -> list[dict]:
        cur = self.con.execute(sql, list(params))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]

    # sources
    def get(self, source_key: str) -> dict | None:
        rows = self._dicts("SELECT * FROM sources WHERE source_key = ?", [source_key])
        return rows[0] if rows else None

    def all(self) -> list[dict]:
        return self._dicts("SELECT * FROM sources ORDER BY source_key")

    def upsert(self, rec: dict) -> None:
        cols = ", ".join(SOURCE_COLUMNS)
        marks = ", ".join("?" * len(SOURCE_COLUMNS))
        self.con.execute(f"INSERT OR REPLACE INTO sources ({cols}) VALUES ({marks})",
                         [rec.get(c) for c in SOURCE_COLUMNS])

    def mark_missing(self, seen_keys: set[str]) -> int:
        gone = [r for r in self.all() if r["source_key"] not in seen_keys and r["status"] != "missing"]
        for rec in gone:
            rec.update(status="missing", status_reason="not found during discovery")
            self.upsert(rec)
        return len(gone)

    def assign_duplicate_groups(self) -> None:
        self.con.execute("""
            UPDATE sources SET duplicate_group = CASE WHEN sha256 IN (
                SELECT sha256 FROM sources WHERE sha256 IS NOT NULL GROUP BY sha256 HAVING count(*) > 1
            ) THEN left(sha256, 12) END
        """)

    def export_sources_csv(self, path: Path) -> None:
        self.con.execute(f"COPY (SELECT * FROM sources ORDER BY source_key) TO {sql_literal(str(path))} (HEADER)")

    # runs
    def start_run(self, command: str, args: dict) -> str:
        run_id = f"{datetime.now():%Y%m%dT%H%M%S}-{secrets.token_hex(3)}"
        self.con.execute("INSERT INTO runs VALUES (?, ?, ?, ?, NULL, 'running')",
                         [run_id, command, json.dumps(args, default=str), datetime.now()])
        return run_id

    def finish_run(self, run_id: str, status: str) -> None:
        self.con.execute("UPDATE runs SET finished_at = ?, status = ? WHERE run_id = ?",
                         [datetime.now(), status, run_id])

    # published-file ledger: the commit point for publishing
    def replace_published(self, source_key: str, run_id: str, entries: list[tuple[str, str, int]]) -> None:
        now = datetime.now()
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.execute("DELETE FROM published_files WHERE source_key = ?", [source_key])
            for dataset, path, rows in entries:
                self.con.execute("INSERT INTO published_files VALUES (?, ?, ?, ?, ?, ?)",
                                 [source_key, run_id, dataset, path, rows, now])
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise

    def unpublish(self, source_keys: list[str]) -> None:
        for key in source_keys:
            self.con.execute("DELETE FROM published_files WHERE source_key = ?", [key])

    def published_paths(self) -> set[str]:
        return {r[0] for r in self.con.execute("SELECT path FROM published_files").fetchall()}

    def published_for_sources(self, source_keys: list[str]) -> list[dict]:
        return self._dicts("SELECT * FROM published_files WHERE list_contains(?, source_key) ORDER BY path",
                           [list(source_keys)])

    def published_keys_for_releases(self, releases: list[str]) -> list[str]:
        rows = self.con.execute("""
            SELECT DISTINCT p.source_key FROM published_files p JOIN sources s USING (source_key)
            WHERE list_contains(?, s.nominal_release) ORDER BY 1
        """, [list(releases)]).fetchall()
        return [r[0] for r in rows]

    def record_source_run(self, run_id, rec, result, status, message, seconds) -> None:
        self.con.execute("INSERT INTO source_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            run_id, rec["source_key"], rec["nominal_release"], rec["rows"],
            result.staged_rows if result else None, result.quarantined_rows if result else None,
            status, message, seconds, datetime.now(),
        ])
```

- [ ] **Step 4: Write `src/citibike_pipeline/inventory.py`**

```python
"""`citibike inventory ...` subcommands."""

from __future__ import annotations

import json
from collections import Counter

from .discover import discover, open_candidate
from .manifest import Manifest
from .naming import parse_nominal
from .schemas import classify, fingerprint, parse_header


def run_discover(cfg, manifest: Manifest, run_id: str, out) -> dict:
    stats: Counter = Counter()
    seen: set[str] = set()
    for cand in discover(cfg):
        seen.add(cand.source_key)
        stats["candidates"] += 1
        prev = manifest.get(cand.source_key)
        if prev and prev["status"] != "missing" and \
                (prev["size"], prev["file_size"], prev["modified"]) == (cand.size, cand.file_size, cand.modified):
            stats["unchanged"] += 1
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
            run_id = manifest.start_run("inventory discover", {})
            stats = run_discover(cfg, manifest, run_id, out)
            manifest.finish_run(run_id, "ok")
            out(f"discover {run_id}: {stats}")
            return 0
    out(f"inventory {args.action}: not available yet")
    return 2
```

- [ ] **Step 5: Run the tests**

Run: `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/citibike_pipeline/manifest.py src/citibike_pipeline/inventory.py tests/test_manifest_discover.py
git commit -m "Add resumable DuckDB manifest and inventory discover"
```

### Task 6: Record streaming, timestamp parsing, and `inventory profile`

**Files:**
- Create: `src/citibike_pipeline/records.py`, `src/citibike_pipeline/profile.py`
- Modify: `src/citibike_pipeline/inventory.py` (replace `run_inventory`), `tests/conftest.py` (add `profiled_cfg`)
- Test: `tests/test_records.py`, `tests/test_profile.py`

**Interfaces:**
- Consumes: `Manifest`, `run_discover` (Task 5); `open_candidate`, `candidate_from_record` (Task 4); `START_COLUMN` (Task 3).
- Produces: `parse_local_ts`, `open_records`, `NULL_TOKENS`, `MASK`, `row_hash`, `profile_candidate`, `run_profile(cfg, manifest, run_id, releases: list[str] | None, force: bool, workers: int, out) -> dict[status, count]`. Profile fills `sha256`, `row_digest` (16 hex chars, order-independent sum of per-record BLAKE2b-64 mod 2^64), `rows`, `malformed_rows`, `invalid_start_rows`, `min_start_date`, `max_start_date`, `day_counts_json`.
- Row accounting rule shared with normalization: blank lines are not records; every other line after the header is one record and gets `source_row` 1, 2, 3...

- [ ] **Step 1: Write the failing tests**

`tests/test_records.py`:

```python
import io
from datetime import datetime

import pytest

from citibike_pipeline.records import open_records, parse_local_ts


@pytest.mark.parametrize("text, expected", [
    ("2018-04-01 03:07:39.0700", datetime(2018, 4, 1, 3, 7, 39, 70000)),
    ("2025-01-10 14:31:22.100", datetime(2025, 1, 10, 14, 31, 22, 100000)),
    ("2016-12-01 00:00:04", datetime(2016, 12, 1, 0, 0, 4)),
    ("9/1/2014 00:00:25", datetime(2014, 9, 1, 0, 0, 25)),
    ("10/12/2015 13:05:09", datetime(2015, 10, 12, 13, 5, 9)),
    ("1/1/2015 0:01", datetime(2015, 1, 1, 0, 1)),
    (" 2016-12-01 00:00:04 ", datetime(2016, 12, 1, 0, 0, 4)),
])
def test_parse_local_ts_formats(text, expected):
    assert parse_local_ts(text) == expected


@pytest.mark.parametrize("text", ["", "\\N", "NULL", "bad-date", "2025-01-10T14:31:22+00:00"])
def test_parse_local_ts_rejects(text):
    assert parse_local_ts(text) is None


def test_open_records_skips_blank_lines_and_numbers_records():
    header, records = open_records(io.BytesIO(b"a,b\n1,2\n\n3,4\n"))
    assert header == ("a", "b")
    assert list(records) == [(1, ["1", "2"]), (2, ["3", "4"])]


def test_open_records_strips_bom_and_unquotes():
    header, records = open_records(io.BytesIO(b'\xef\xbb\xbf"a","b"\n"1",""\n'))
    assert header == ("a", "b") and list(records) == [(1, ["1", ""])]
```

`tests/test_profile.py`:

```python
from archive_fixture import (
    EXPECTED_KEYS, KEY_2018_P1, KEY_2018_P2, KEY_2018_WHOLE, KEY_2023_NESTED, KEY_2025_CSV,
    KEY_2025_ZIP, KEY_UNKNOWN, QUIET,
)
from citibike_pipeline.inventory import run_discover
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.profile import MASK, run_profile


def _profile(cfg, **kw):
    with Manifest(cfg) as m:
        run_discover(cfg, m, "d", QUIET)
        counts = run_profile(cfg, m, "p", releases=None, force=False, workers=kw.get("workers", 1), out=QUIET)
        return counts, {r["source_key"]: r for r in m.all()}


def test_profile_counts(cfg):
    counts, recs = _profile(cfg)
    assert counts == {"profiled": len(EXPECTED_KEYS)}
    whole = recs[KEY_2018_WHOLE]
    assert (whole["rows"], whole["malformed_rows"], whole["invalid_start_rows"]) == (3, 0, 1)
    assert (whole["min_start_date"], whole["max_start_date"]) == ("2018-04-01", "2018-04-02")
    nested = recs[KEY_2023_NESTED]
    assert (nested["rows"], nested["malformed_rows"]) == (5, 1)


def test_whole_file_digest_equals_sum_of_parts(cfg):
    _, recs = _profile(cfg)
    parts = (int(recs[KEY_2018_P1]["row_digest"], 16) + int(recs[KEY_2018_P2]["row_digest"], 16)) & MASK
    assert int(recs[KEY_2018_WHOLE]["row_digest"], 16) == parts
    assert recs[KEY_2018_WHOLE]["sha256"] != recs[KEY_2018_P1]["sha256"]


def test_identical_copies_share_sha_and_duplicate_group(cfg):
    _, recs = _profile(cfg)
    assert recs[KEY_2025_CSV]["sha256"] == recs[KEY_2025_ZIP]["sha256"]
    assert recs[KEY_2025_CSV]["duplicate_group"] == recs[KEY_2025_CSV]["sha256"][:12]
    assert recs[KEY_2018_WHOLE]["duplicate_group"] is None


def test_unknown_family_is_counted_without_dates(cfg):
    _, recs = _profile(cfg)
    assert recs[KEY_UNKNOWN]["rows"] == 1 and recs[KEY_UNKNOWN]["min_start_date"] is None


def test_profile_resumes_and_force_reprofiles(cfg):
    with Manifest(cfg) as m:
        run_discover(cfg, m, "d", QUIET)
        run_profile(cfg, m, "p1", None, False, 1, QUIET)
        assert run_profile(cfg, m, "p2", None, False, 1, QUIET) == {}
        assert run_profile(cfg, m, "p3", ["2018-04"], True, 1, QUIET) == {"profiled": 3}


def test_parallel_profile_matches(cfg):
    counts, recs = _profile(cfg, workers=2)
    assert counts == {"profiled": len(EXPECTED_KEYS)}
    assert recs[KEY_2018_WHOLE]["rows"] == 3
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_records.py tests/test_profile.py -q`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write `src/citibike_pipeline/records.py`**

```python
"""Streaming CSV records with stable source_row numbers, and the one local-timestamp parser."""

from __future__ import annotations

import csv
import io
from datetime import datetime

NULL_TOKENS = frozenset({"", "\\N", "NULL"})
_FALLBACK_FORMATS = ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M")


def parse_local_ts(value: str) -> datetime | None:
    """Parses source wall-clock time. Offsets are rejected rather than silently converted."""
    s = value.strip()
    if s in NULL_TOKENS:
        return None
    ts = None
    try:
        ts = datetime.fromisoformat(s)
    except ValueError:
        for fmt in _FALLBACK_FORMATS:
            try:
                ts = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
    if ts is None or ts.tzinfo is not None:
        return None
    return ts


def open_records(binary):
    text = io.TextIOWrapper(binary, encoding="utf-8-sig", newline="")
    reader = csv.reader(text)
    header = tuple(h.strip() for h in next(reader, []))

    def records():
        n = 0
        for fields in reader:
            if not fields:
                continue
            n += 1
            yield n, fields

    return header, records()
```

- [ ] **Step 4: Write `src/citibike_pipeline/profile.py`**

```python
"""Full profiling pass: byte hash, record digest, row counts, and start-date coverage."""

from __future__ import annotations

import hashlib
import io
import json
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

from .discover import candidate_from_record, open_candidate
from .records import open_records, parse_local_ts
from .schemas import START_COLUMN

MASK = (1 << 64) - 1


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
        return "failed", f"{type(exc).__name__}: {exc}", time.monotonic() - started


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
            rec.update(status="failed", status_reason=payload)
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
```

- [ ] **Step 5: Replace `run_inventory` in `src/citibike_pipeline/inventory.py`** (add `from .profile import run_profile` to the imports)

```python
def run_inventory(cfg, args, out) -> int:
    with Manifest(cfg) as manifest:
        if args.action == "discover":
            run_id = manifest.start_run("inventory discover", {})
            stats = run_discover(cfg, manifest, run_id, out)
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
    out(f"inventory {args.action}: not available yet")
    return 2
```

- [ ] **Step 6: Add the `profiled_cfg` fixture to `tests/conftest.py`**

```python
@pytest.fixture
def profiled_cfg(cfg):
    from archive_fixture import QUIET
    from citibike_pipeline.inventory import run_discover
    from citibike_pipeline.manifest import Manifest
    from citibike_pipeline.profile import run_profile

    with Manifest(cfg) as m:
        run_discover(cfg, m, "t-discover", QUIET)
        run_profile(cfg, m, "t-profile", None, False, 1, QUIET)
    return cfg
```

- [ ] **Step 7: Run the tests**

Run: `uv run pytest -q`
Expected: all PASS. If `test_parse_local_ts_formats` fails on the 4-digit fraction, `datetime.fromisoformat` on this Python does not accept it: add `"%Y-%m-%d %H:%M:%S.%f"` as the first entry of `_FALLBACK_FORMATS` and rerun.

- [ ] **Step 8: Commit**

```bash
git add src/citibike_pipeline/records.py src/citibike_pipeline/profile.py src/citibike_pipeline/inventory.py tests/conftest.py tests/test_records.py tests/test_profile.py
git commit -m "Profile archive sources with hashes, record digests, and date coverage"
```

### Task 7: Equivalent copies, release decisions, and `inventory select`

**Files:**
- Create: `src/citibike_pipeline/sets.py`
- Modify: `src/citibike_pipeline/inventory.py` (replace `run_inventory`), `tests/conftest.py` (add `selected_cfg`)
- Test: `tests/test_sets.py`

**Interfaces:**
- Consumes: manifest records (Task 5 columns, Task 6 profile fields), `MASK` (Task 6).
- Produces: `SourceSet(set_id, release, layout, kind_rank, members, rows, digest, parts_gap)`, `ReleaseDecision(release, status, chosen, equivalent, reason)` with status in `selectable` / `conflict` / `unprofiled` / `unknown_schema`, `build_sets`, `decide_release`, `apply_selection(manifest, releases, overrides: dict[str, str], dry_run, out) -> int`.
- Set identity: `"<location>|<release>|<layout>"`, where location is the directory of the innermost file (prefixed by `container::` for ZIP members) and layout is `whole` (no part number) or `parts`.
- Choice rule among equivalent sets: no parts gap first, then plain CSV before ZIP member before nested member, then lowest `set_id`.

- [ ] **Step 1: Write the failing tests**

```python
import copy

from archive_fixture import (
    KEY_2018_P1, KEY_2018_P2, KEY_2018_WHOLE, KEY_2025_CSV, QUIET,
)
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.profile import run_profile
from citibike_pipeline.sets import apply_selection, build_sets, decide_release


def _records(cfg):
    with Manifest(cfg) as m:
        return m.all()


def _decide(records, release):
    families = {r["source_key"]: r["schema_family"] for r in records}
    return decide_release(release, build_sets(records)[release], families)


def test_2018_parts_set_is_chosen_over_whole_copy(profiled_cfg):
    d = _decide(_records(profiled_cfg), "2018-04")
    assert d.status == "selectable"
    assert d.chosen.members == (KEY_2018_P1, KEY_2018_P2)
    assert [s.members for s in d.equivalent] == [(KEY_2018_WHOLE,)]


def test_plain_csv_preferred_over_identical_zip_member(profiled_cfg):
    assert _decide(_records(profiled_cfg), "2025-01").chosen.members == (KEY_2025_CSV,)


def test_unknown_schema_is_not_selectable(profiled_cfg):
    assert _decide(_records(profiled_cfg), "2019-02").status == "unknown_schema"


def test_differing_content_is_a_conflict(profiled_cfg):
    records = copy.deepcopy(_records(profiled_cfg))
    next(r for r in records if r["source_key"] == KEY_2018_WHOLE)["row_digest"] = "0" * 16
    assert _decide(records, "2018-04").status == "conflict"


def test_unprofiled_release(cfg):
    from citibike_pipeline.inventory import run_discover

    with Manifest(cfg) as m:
        run_discover(cfg, m, "d", QUIET)
        assert _decide(m.all(), "2018-04").status == "unprofiled"


def test_parts_gap_detected():
    base = {"kind": "csv", "container": None, "member": None, "status": "profiled",
            "nominal_release": "2020-01", "rows": 1, "row_digest": "0" * 16}
    recs = [dict(base, source_key=f"d/202001-citibike-tripdata_{p}.csv", path=f"d/202001-citibike-tripdata_{p}.csv",
                 nominal_part=p) for p in (1, 3)]
    assert build_sets(recs)["2020-01"][0].parts_gap is True


def test_apply_selection_marks_statuses(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        assert apply_selection(m, ["2018-04"], {}, False, QUIET) == 0
        assert m.get(KEY_2018_P1)["status"] == "selected"
        assert m.get(KEY_2018_P2)["status"] == "selected"
        whole = m.get(KEY_2018_WHOLE)
        assert whole["status"] == "excluded" and whole["status_reason"].startswith("equivalent_to:")


def test_dry_run_changes_nothing(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        before = m.all()
        assert apply_selection(m, ["2018-04"], {}, True, QUIET) == 0
        assert m.all() == before


def test_unselectable_release_returns_1(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        assert apply_selection(m, ["2019-02", "2030-01"], {}, False, QUIET) == 1


def test_manual_override_on_conflict(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        whole = m.get(KEY_2018_WHOLE)
        whole["row_digest"] = "0" * 16
        m.upsert(whole)
        set_id = "2018-citibike-tripdata.zip::2018-citibike-tripdata|2018-04|whole"
        assert apply_selection(m, ["2018-04"], {"2018-04": set_id}, False, QUIET) == 0
        assert m.get(KEY_2018_WHOLE)["status"] == "selected"
        assert m.get(KEY_2018_P1)["status_reason"] == f"manual_override:{set_id}"


def test_selection_survives_reprofile(profiled_cfg):
    with Manifest(profiled_cfg) as m:
        apply_selection(m, ["2018-04"], {}, False, QUIET)
        run_profile(profiled_cfg, m, "again", ["2018-04"], True, 1, QUIET)
        assert m.get(KEY_2018_P1)["status"] == "selected"
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_sets.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'citibike_pipeline.sets'`.

- [ ] **Step 3: Write `src/citibike_pipeline/sets.py`**

```python
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
        if rec["nominal_release"] is None or rec["status"] == "missing":
            continue
        layout = "whole" if rec["nominal_part"] is None else "parts"
        groups[(rec["nominal_release"], _location(rec), layout)].append(rec)
    out: dict[str, list[SourceSet]] = defaultdict(list)
    for (release, location, layout), recs in sorted(groups.items()):
        recs.sort(key=lambda r: (r["nominal_part"] or 0, r["source_key"]))
        profiled = all(r["row_digest"] is not None for r in recs)
        parts = [r["nominal_part"] for r in recs if r["nominal_part"] is not None]
        out[release].append(SourceSet(
            set_id=f"{location}|{release}|{layout}",
            release=release,
            layout=layout,
            kind_rank=min(KIND_RANK[r["kind"]] for r in recs),
            members=tuple(r["source_key"] for r in recs),
            rows=sum(r["rows"] for r in recs) if profiled else None,
            digest=f"{sum(int(r['row_digest'], 16) for r in recs) & MASK:016x}" if profiled else None,
            parts_gap=bool(parts) and sorted(parts) != list(range(1, len(parts) + 1)),
        ))
    return dict(out)


def decide_release(release: str, sets: list[SourceSet], families: dict[str, str]) -> ReleaseDecision:
    if any(s.digest is None for s in sets):
        return ReleaseDecision(release, "unprofiled", None, (), "at least one candidate set is not profiled")
    if any(families[k] == "unknown" for s in sets for k in s.members):
        return ReleaseDecision(release, "unknown_schema", None, (), "a candidate has an unrecognized header")
    contents = {(s.rows, s.digest) for s in sets}
    if len(contents) > 1:
        return ReleaseDecision(release, "conflict", None, (),
                               f"{len(contents)} distinct contents across {len(sets)} sets; needs review")
    ordered = sorted(sets, key=lambda s: (s.parts_gap, s.kind_rank, s.set_id))
    return ReleaseDecision(release, "selectable", ordered[0], tuple(ordered[1:]),
                           f"{len(sets)} equivalent set(s)")


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
```

- [ ] **Step 4: Replace `run_inventory`** (add `from .sets import apply_selection` to the imports)

```python
def run_inventory(cfg, args, out) -> int:
    with Manifest(cfg) as manifest:
        if args.action == "discover":
            run_id = manifest.start_run("inventory discover", {})
            stats = run_discover(cfg, manifest, run_id, out)
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
    out(f"inventory {args.action}: not available yet")
    return 2
```

- [ ] **Step 5: Add `selected_cfg` to `tests/conftest.py`**

```python
@pytest.fixture
def selected_cfg(profiled_cfg):
    from archive_fixture import FIXTURE_RELEASES, QUIET
    from citibike_pipeline.manifest import Manifest
    from citibike_pipeline.sets import apply_selection

    with Manifest(profiled_cfg) as m:
        assert apply_selection(m, FIXTURE_RELEASES, {}, False, QUIET) == 0
    return profiled_cfg
```

- [ ] **Step 6: Run the tests**

Run: `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/citibike_pipeline/sets.py src/citibike_pipeline/inventory.py tests/conftest.py tests/test_sets.py
git commit -m "Detect equivalent source copies and select one set per release"
```

### Task 8: Inventory report, completeness policy, and `inventory report`

**Files:**
- Create: `src/citibike_pipeline/report.py`
- Modify: `src/citibike_pipeline/inventory.py` (replace `run_inventory`)
- Test: `tests/test_report.py`

**Interfaces:**
- Consumes: `build_sets`, `decide_release`, `SourceSet`, `ReleaseDecision` (Task 7); `Manifest.export_sources_csv` (Task 5).
- Produces: `parse_listing`, `completeness`, `build_report`, `write_report(cfg, manifest) -> Path` (writes `reports/inventory_report.md` and `reports/manifest_sources.csv`; reads `reports/tripdata_listing.xml` when present).
- Completeness policy (recorded with its basis, never assumed from a filename):
  - `incomplete`: fewer calendar days of the nominal month have trips than the month has, or the on-disk file size differs from the published listing.
  - `confirmed`: every day has trips and the on-disk file size matches the published listing.
  - `unverified`: every day has trips but there is no listing to compare, or no set is selectable.

- [ ] **Step 1: Write the failing tests**

```python
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
    chosen = SourceSet("x|2025-02|parts", "2025-02", "parts", 1, ("k",), 28, "0" * 16, False)
    return ReleaseDecision("2025-02", "selectable", chosen, (), ""), [rec]


def test_parse_listing():
    assert parse_listing(LISTING) == {"202502-citibike-tripdata.zip": 100, "2018-citibike-tripdata.zip": 5}


def test_completeness_states():
    listing = parse_listing(LISTING)
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
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_report.py -q`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write `src/citibike_pipeline/report.py`**

```python
"""Human-readable inventory report plus a CSV export of the manifest."""

from __future__ import annotations

import calendar
import json
import posixpath
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path

from .sets import ReleaseDecision, build_sets, decide_release

S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def parse_listing(xml_text: str) -> dict[str, int]:
    root = ET.fromstring(xml_text)
    return {c.findtext("s3:Key", namespaces=S3_NS): int(c.findtext("s3:Size", namespaces=S3_NS))
            for c in root.findall("s3:Contents", S3_NS)}


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
    lines += [f"| {k} | {v} |" for k, v in sorted(counter.items(), key=lambda kv: str(kv[0]))]
    return lines + [""]


def _months_between(first: str, last: str) -> list[str]:
    year, month = map(int, first.split("-"))
    out = []
    while f"{year:04d}-{month:02d}" <= last:
        out.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def build_report(cfg, records: list[dict], listing: dict[str, int] | None = None) -> str:
    lines = [
        "# Citi Bike archive inventory", "",
        f"- Source root: `{cfg.source_root}`",
        f"- Parser version: `{cfg.parser_version}`",
        f"- Candidates: {len(records)}",
        f"- Published listing compared: {'yes' if listing else 'no'}", "",
    ]
    lines += _table("Sources by status", Counter(r["status"] for r in records))
    lines += _table("Sources by kind", Counter(r["kind"] for r in records))
    lines += _table("Sources by schema family", Counter(r["schema_family"] for r in records))

    lines += ["## Header fingerprints", "", "| family | fingerprint | sources | header |", "|---|---|---|---|"]
    fingerprints: dict[tuple, list[dict]] = defaultdict(list)
    for rec in records:
        fingerprints[(rec["schema_family"], rec["schema_fingerprint"])].append(rec)
    for (family, fp), recs in sorted(fingerprints.items()):
        lines.append(f"| {family} | `{fp}` | {len(recs)} | `{', '.join(json.loads(recs[0]['header_json']))}` |")
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
        lines.append(f"| {release} | {len(sets[release])} | {decision.status}: {decision.reason} | "
                     f"{chosen} | {rows} | {state} | {basis} |")
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
    listing = parse_listing(listing_file.read_text()) if listing_file.exists() else None
    path = cfg.reports_dir / "inventory_report.md"
    path.write_text(build_report(cfg, manifest.all(), listing))
    manifest.export_sources_csv(cfg.reports_dir / "manifest_sources.csv")
    return path
```

- [ ] **Step 4: Replace `run_inventory`** (add `from .report import write_report` to the imports). Same as Task 7, with this branch added before the final `out(...)`:

```python
        if args.action == "report":
            out(f"wrote {write_report(cfg, manifest)}")
            return 0
```

The function now handles `discover`, `profile`, `select`, `report`, and the fallback line stays for safety.

- [ ] **Step 5: Run the tests**

Run: `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/citibike_pipeline/report.py src/citibike_pipeline/inventory.py tests/test_report.py
git commit -m "Add inventory report with completeness policy and listing check"
```

### Task 9: Inventory the real archive (read-only) and choose the pilots

**Files:**
- Create: `docs/archive_inventory.md` (copied from the generated report)
- Modify: `docs/implementation_status.md`

**Interfaces:**
- Consumes: the `citibike inventory` commands from Tasks 5 to 8.
- Produces: a profiled manifest on the RAID and the pilot selection for Stage 2.

This task runs against the real data. Nothing here writes inside the source root. Record wall-clock times as you go.

- [ ] **Step 1: Snapshot the source root so you can prove it was not modified**

```bash
P=/Volumes/BigRaid/RandyFiles/Cuny/622
find "$P/FinalProject" -type f -not -name .DS_Store -exec stat -f '%N %z %m' {} + | sort > /tmp/citibike_source_before.txt
wc -l /tmp/citibike_source_before.txt
```

- [ ] **Step 2: Discovery pass**

```bash
time uv run citibike inventory discover | tail -3
```

Expected: a `discover <run_id>: {...}` line with the `candidates` count (record it) and `missing: 0`. The first run extracts about 49 nested monthly ZIPs (roughly 22 GB) into `FinalProject_processed/staging/nested/`; reruns skip them.

- [ ] **Step 3: Full profiling pass (resumable; rerun the same command after any interruption)**

```bash
mkdir -p "$P/FinalProject_processed/reports"
time uv run citibike inventory profile --all --workers 4 > "$P/FinalProject_processed/reports/profile_all.log" 2>&1
tail -3 "$P/FinalProject_processed/reports/profile_all.log"
```

Expected: exit 0 and a final `profile <run_id>: {'profiled': N}` line. If any source is `failed`, read its `status_reason` in the report (Step 5), fix the cause in code with a test, bump `parser_version` in `config/local.yaml`, and rerun this step. Do not hand-edit the manifest.

- [ ] **Step 4: (Optional, recommended) Fetch the published bucket listing for the completeness check**

This is a read-only public GET of the bucket index.

```bash
curl -fsS https://s3.amazonaws.com/tripdata/ -o "$P/FinalProject_processed/reports/tripdata_listing.xml"
grep -c '<Key>' "$P/FinalProject_processed/reports/tripdata_listing.xml"
```

- [ ] **Step 5: Generate and read the report**

```bash
uv run citibike inventory report
cp "$P/FinalProject_processed/reports/inventory_report.md" docs/archive_inventory.md
```

Check, and record what you find in `docs/implementation_status.md`:
- Every candidate has a status, with no `discovered` left after `--all`.
- Header fingerprints: any `unknown` family is listed under "Sources needing attention". Do not add a family adapter without looking at the real header.
- Exact duplicates include `2013-citibike-tripdata/` vs the 2013 ZIP and `202608-citibike-tripdata/` vs its ZIP.
- 2018 releases show 2 or 3 equivalent sets. Any `conflict` is investigated before that release is used.
- Releases missing between first and last.
- Completeness for 2018-04 and every 2025 month.

- [ ] **Step 6: Select the pilot releases**

```bash
uv run citibike inventory select --release 2018-04 --release 2025-03 --dry-run
uv run citibike inventory select --release 2018-04 --release 2025-03
```

Expected: one `select ...` line per release and exit 0. If a pilot release is `conflict` or `unknown_schema`, choose a different pilot month from the report (a 2018 month with equivalent sets, and a fully covered 2025 month), update `pilot.releases` in `config/local.yaml`, and record the reason in `docs/implementation_status.md`.

- [ ] **Step 7: Prove the source root is unchanged**

```bash
find "$P/FinalProject" -type f -not -name .DS_Store -exec stat -f '%N %z %m' {} + | sort > /tmp/citibike_source_after.txt
diff /tmp/citibike_source_before.txt /tmp/citibike_source_after.txt && echo "source unchanged"
```

Expected: `source unchanged`.

- [ ] **Step 8: Update the status doc and commit**

Add to `docs/implementation_status.md`: discover and profile run IDs, wall-clock times, candidate and row totals, failures and fixes, duplicate findings, the pilot choices with their completeness state and basis, and the exact commands from Steps 2 to 6. Tick the Stage 1 box.

```bash
git add docs/archive_inventory.md docs/implementation_status.md config/local.yaml
git commit -m "Record archive inventory and pilot release selection"
```

## Stage 2: Normalize selected releases to Parquet

### Task 10: DST windows and the per-source normalize transform

**Files:**
- Create: `src/citibike_pipeline/dst.py`, `src/citibike_pipeline/normalize.py`, `docs/data_dictionary.md`
- Test: `tests/test_dst.py`, `tests/test_normalize.py`

**Interfaces:**
- Consumes: `connect`, `sql_literal` (Task 2); `open_candidate`, `candidate_from_record` (Task 4); `open_records`, `parse_local_ts` (Task 6); `RAW_NAMES`, `START_COLUMN`, `END_COLUMN` (Task 3); `selected_cfg` fixture (Task 7).
- Produces:
  - `dst_hours(years, tz_name) -> dict[(date, hour), "ambiguous" | "nonexistent"]`.
  - `SourceResult(source_key, slug, source_rows, staged_rows, quarantined_rows, staged_dir)`.
  - `normalize_source(cfg, con, rec, run_id, staging_root, dst) -> SourceResult`. It writes `staged_dir/trips/trip_year=YYYY/trip_month=MM/data_0.parquet` and, when any row is quarantined, `staged_dir/quarantine.parquet`. It raises `NormalizeError` on an unknown family, a changed header or row count, a failed readback, or a quarantine share above `cfg.max_quarantine_fraction`.
  - Canonical trip columns, in order: `started_at_local, ended_at_local, duration_seconds, source_duration_seconds, start_station_id, start_station_name, start_lat, start_lng, end_station_id, end_station_name, end_lat, end_lng, ride_id, rideable_type, membership_group, original_membership, source_key, source_row, schema_family, schema_version, run_id, quality_flags, eligible_departure, eligible_arrival` plus the partition keys `trip_year`, `trip_month` (zero-padded strings in paths).

- [ ] **Step 1: Write the failing tests**

`tests/test_dst.py`:

```python
from datetime import date

from citibike_pipeline.dst import dst_hours


def test_dst_hours_2025():
    assert dst_hours([2025], "America/New_York") == {
        (date(2025, 3, 9), 2): "nonexistent",
        (date(2025, 11, 2), 1): "ambiguous",
    }


def test_dst_hours_2023_fall_back():
    assert dst_hours([2023], "America/New_York")[(date(2023, 11, 5), 1)] == "ambiguous"
```

`tests/test_normalize.py`:

```python
import dataclasses
from datetime import datetime

import pyarrow.parquet as pq
import pytest

from archive_fixture import KEY_2018_P1
from citibike_pipeline.db import connect
from citibike_pipeline.dst import dst_hours
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.normalize import NormalizeError, normalize_source, slug_for, transform_sql


def _normalize(cfg, release, run_id="run-test"):
    with Manifest(cfg) as m:
        recs = [r for r in m.all() if r["nominal_release"] == release and r["status"] == "selected"]
    con = connect(cfg)
    dst = dst_hours(range(2013, 2028), cfg.timezone)
    results = [normalize_source(cfg, con, r, run_id, cfg.staging_dir / run_id, dst) for r in recs]
    files = [str(f) for res in results for f in (res.staged_dir / "trips").rglob("*.parquet")]
    rel = con.read_parquet(files, hive_partitioning=True).order("source_key, source_row")
    rows = [dict(zip(rel.columns, row)) for row in rel.fetchall()]
    return results, rows


def _quarantine(results):
    out = []
    for res in results:
        path = res.staged_dir / "quarantine.parquet"
        if path.exists():
            out += pq.read_table(path).to_pylist()
    return out


def test_legacy_parts_reconcile_and_quarantine(selected_cfg):
    results, rows = _normalize(selected_cfg, "2018-04")
    assert sum(r.source_rows for r in results) == 3
    assert sum(r.staged_rows for r in results) == 2 == len(rows)
    q = _quarantine(results)
    assert [(x["source_row"], x["reason"]) for x in q] == [(2, "start_ts_invalid")]
    assert q[0]["source_key"] == KEY_2018_P1


def test_legacy_field_mapping(selected_cfg):
    _, rows = _normalize(selected_cfg, "2018-04")
    first = next(r for r in rows if r["source_key"] == KEY_2018_P1)
    assert first["started_at_local"] == datetime(2018, 4, 1, 3, 7, 39, 70000)
    assert first["duration_seconds"] == pytest.approx(840.058)
    assert first["source_duration_seconds"] == 840
    assert (first["membership_group"], first["original_membership"]) == ("member", "Subscriber")
    assert first["start_station_id"] == "72" and first["quality_flags"] == []
    assert (first["trip_year"], first["trip_month"]) == (2018, 4)
    assert first["ride_id"] is None and first["schema_family"] == "legacy_lower"
    second = next(r for r in rows if r["source_key"] != KEY_2018_P1)
    assert second["start_station_id"] is None and second["start_station_name"] is None
    assert {"start_station_missing", "start_coords_invalid"} <= set(second["quality_flags"])
    assert second["membership_group"] == "casual" and second["eligible_departure"] is False


def test_modern_ids_strings_and_month_by_timestamp(selected_cfg):
    _, rows = _normalize(selected_cfg, "2025-01")
    by_id = {r["ride_id"]: r for r in rows}
    assert by_id["E4A8132DACF670C3"]["start_station_id"] == "6322.01"
    assert by_id["E4A8132DACF670C3"]["rideable_type"] == "electric_bike"
    assert (by_id["C3"]["trip_year"], by_id["C3"]["trip_month"]) == (2024, 12)
    assert {"end_station_missing", "end_coords_invalid"} <= set(by_id["B2"]["quality_flags"])
    assert by_id["B2"]["eligible_arrival"] is False and by_id["B2"]["eligible_departure"] is True


def test_dst_flags_and_field_count_quarantine(selected_cfg):
    results, rows = _normalize(selected_cfg, "2023-11")
    d4 = [r for r in rows if r["ride_id"] == "D4"]
    assert len(d4) == 2
    assert {"start_dst_ambiguous", "end_dst_ambiguous"} <= set(d4[0]["quality_flags"])
    assert [x["reason"] for x in _quarantine(results)] == ["field_count"]
    assert len(rows) == 4


def test_month_day_year_formats(selected_cfg):
    _, rows = _normalize(selected_cfg, "2015-01")
    assert rows[0]["started_at_local"] == datetime(2015, 1, 1, 0, 1)
    assert "duration_mismatch" in rows[0]["quality_flags"]
    assert rows[1]["membership_group"] == "casual" and "duration_mismatch" not in rows[1]["quality_flags"]


def test_title_case_family(selected_cfg):
    _, rows = _normalize(selected_cfg, "2016-12")
    assert rows[0]["started_at_local"] == datetime(2016, 12, 1, 0, 0, 4)
    assert rows[0]["schema_family"] == "legacy_title" and rows[0]["source_duration_seconds"] == 528


def test_unknown_family_has_no_adapter():
    with pytest.raises(NormalizeError):
        transform_sql("unknown")


def test_quarantine_guard_blocks_source(selected_cfg):
    strict = dataclasses.replace(selected_cfg, max_quarantine_fraction=0.1)
    with pytest.raises(NormalizeError, match="quarantine"):
        _normalize(strict, "2018-04")


def test_slug_is_short_and_unique():
    a = slug_for("a.zip::x/202501-citibike-tripdata_1.csv")
    b = slug_for("b.zip::x/202501-citibike-tripdata_1.csv")
    assert a.startswith("202501-citibike-tripdata_1-") and a != b
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_dst.py tests/test_normalize.py -q`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write `src/citibike_pipeline/dst.py`**

```python
"""Local wall-clock hours that are ambiguous (fall back) or nonexistent (spring forward)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def dst_hours(years, tz_name: str) -> dict[tuple[date, int], str]:
    tz = ZoneInfo(tz_name)
    out: dict[tuple[date, int], str] = {}
    for year in years:
        day = date(year, 1, 1)
        while day.year == year:
            for hour in range(24):
                wall = datetime(day.year, day.month, day.day, hour, 30)
                early, late = wall.replace(tzinfo=tz, fold=0), wall.replace(tzinfo=tz, fold=1)
                if early.utcoffset() != late.utcoffset():
                    round_trip = early.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None)
                    out[(day, hour)] = "ambiguous" if round_trip == wall else "nonexistent"
            day += timedelta(days=1)
    return out
```

- [ ] **Step 4: Write `src/citibike_pipeline/normalize.py`**

```python
"""Normalizes one selected source into staged, partitioned Parquet plus a quarantine file.

Python streams records (exact source_row, one timestamp parser); DuckDB types and flags them.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .db import sql_literal
from .discover import candidate_from_record, open_candidate
from .records import open_records, parse_local_ts
from .schemas import END_COLUMN, RAW_NAMES, START_COLUMN


class NormalizeError(RuntimeError):
    pass


TRIPS_DDL = """
CREATE OR REPLACE TABLE staged_trips (
  started_at_local TIMESTAMP, ended_at_local TIMESTAMP,
  duration_seconds DOUBLE, source_duration_seconds DOUBLE,
  start_station_id VARCHAR, start_station_name VARCHAR, start_lat DOUBLE, start_lng DOUBLE,
  end_station_id VARCHAR, end_station_name VARCHAR, end_lat DOUBLE, end_lng DOUBLE,
  ride_id VARCHAR, rideable_type VARCHAR, membership_group VARCHAR, original_membership VARCHAR,
  source_key VARCHAR, source_row BIGINT, schema_family VARCHAR, schema_version INTEGER, run_id VARCHAR,
  quality_flags VARCHAR[], eligible_departure BOOLEAN, eligible_arrival BOOLEAN,
  trip_year VARCHAR, trip_month VARCHAR
)
"""

QUARANTINE_SCHEMA = pa.schema([
    ("source_key", pa.string()), ("source_row", pa.int64()),
    ("reason", pa.string()), ("raw_record", pa.string()),
])


def slug_for(source_key: str) -> str:
    base = source_key.split("::")[-1].rsplit("/", 1)[-1].removesuffix(".csv")
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base)
    return f"{base}-{hashlib.sha1(source_key.encode()).hexdigest()[:8]}"


def _nz(col: str) -> str:
    return f"CASE WHEN trim({col}) IN ('', '\\N', 'NULL') THEN NULL ELSE trim({col}) END"


def _coords_ok(lat: str, lng: str) -> str:
    return f"coalesce({lat} BETWEEN 40.4 AND 41.1 AND {lng} BETWEEN -74.4 AND -73.6, false)"


def transform_sql(family: str) -> str:
    if family in ("legacy_lower", "legacy_title"):
        src_duration = f"TRY_CAST({_nz('src_duration')} AS DOUBLE)"
        ride_id, rideable = "NULL::VARCHAR", "NULL::VARCHAR"
        group = f"CASE {_nz('membership')} WHEN 'Subscriber' THEN 'member' WHEN 'Customer' THEN 'casual' END"
    elif family == "modern":
        src_duration = "NULL::DOUBLE"
        ride_id, rideable = _nz("ride_id"), _nz("rideable_type")
        group = f"CASE {_nz('membership')} WHEN 'member' THEN 'member' WHEN 'casual' THEN 'casual' END"
    else:
        raise NormalizeError(f"no adapter for schema family {family!r}")
    return f"""
INSERT INTO staged_trips
SELECT
  started_at_local, ended_at_local, duration_seconds, source_duration_seconds,
  start_station_id, start_station_name, start_lat, start_lng,
  end_station_id, end_station_name, end_lat, end_lng,
  ride_id, rideable_type, membership_group, original_membership,
  ?::VARCHAR, source_row, ?::VARCHAR, ?::INTEGER, ?::VARCHAR,
  list_filter([
    CASE WHEN start_station_id IS NULL THEN 'start_station_missing' END,
    CASE WHEN end_station_id IS NULL THEN 'end_station_missing' END,
    CASE WHEN ended_at_local IS NULL THEN 'end_ts_invalid' END,
    CASE WHEN duration_seconds <= 0 THEN 'duration_nonpositive' END,
    CASE WHEN abs(duration_seconds - source_duration_seconds) > 2 THEN 'duration_mismatch' END,
    CASE WHEN NOT {_coords_ok('start_lat', 'start_lng')} THEN 'start_coords_invalid' END,
    CASE WHEN NOT {_coords_ok('end_lat', 'end_lng')} THEN 'end_coords_invalid' END,
    CASE WHEN original_membership IS NULL THEN 'membership_missing'
         WHEN membership_group IS NULL THEN 'membership_unrecognized' END,
    CASE WHEN start_dst IS NOT NULL THEN 'start_dst_' || start_dst END,
    CASE WHEN end_dst IS NOT NULL THEN 'end_dst_' || end_dst END
  ], lambda f: f IS NOT NULL),
  start_station_id IS NOT NULL,
  ended_at_local IS NOT NULL AND end_station_id IS NOT NULL AND coalesce(duration_seconds >= 0, false),
  strftime(started_at_local, '%Y'),
  strftime(started_at_local, '%m')
FROM (
  SELECT
    start_local AS started_at_local,
    end_local AS ended_at_local,
    date_diff('microsecond', start_local, end_local) / 1e6 AS duration_seconds,
    {src_duration} AS source_duration_seconds,
    {_nz('start_station_id')} AS start_station_id,
    {_nz('start_station_name')} AS start_station_name,
    TRY_CAST({_nz('start_lat')} AS DOUBLE) AS start_lat,
    TRY_CAST({_nz('start_lng')} AS DOUBLE) AS start_lng,
    {_nz('end_station_id')} AS end_station_id,
    {_nz('end_station_name')} AS end_station_name,
    TRY_CAST({_nz('end_lat')} AS DOUBLE) AS end_lat,
    TRY_CAST({_nz('end_lng')} AS DOUBLE) AS end_lng,
    {ride_id} AS ride_id,
    {rideable} AS rideable_type,
    {group} AS membership_group,
    {_nz('membership')} AS original_membership,
    source_row, start_dst, end_dst
  FROM raw
) t
"""


@dataclass
class SourceResult:
    source_key: str
    slug: str
    source_rows: int
    staged_rows: int
    quarantined_rows: int
    staged_dir: Path


class _Batch:
    def __init__(self, names: tuple[str, ...]):
        self.names = names
        self.cols: list[list[str]] = [[] for _ in names]
        self.rows: list[int] = []
        self.starts, self.ends, self.start_dst, self.end_dst = [], [], [], []

    def add(self, source_row, fields, start, end, start_dst, end_dst):
        for i, value in enumerate(fields):
            self.cols[i].append(value)
        self.rows.append(source_row)
        self.starts.append(start)
        self.ends.append(end)
        self.start_dst.append(start_dst)
        self.end_dst.append(end_dst)

    def table(self) -> pa.Table:
        data = {name: pa.array(self.cols[i], pa.string()) for i, name in enumerate(self.names)}
        data["source_row"] = pa.array(self.rows, pa.int64())
        data["start_local"] = pa.array(self.starts, pa.timestamp("us"))
        data["end_local"] = pa.array(self.ends, pa.timestamp("us"))
        data["start_dst"] = pa.array(self.start_dst, pa.string())
        data["end_dst"] = pa.array(self.end_dst, pa.string())
        return pa.table(data)


def normalize_source(cfg, con, rec: dict, run_id: str, staging_root: Path, dst: dict) -> SourceResult:
    family = rec["schema_family"]
    sql = transform_sql(family)
    names = RAW_NAMES[family]
    width = len(names)
    start_idx, end_idx = START_COLUMN[family], END_COLUMN[family]
    slug = slug_for(rec["source_key"])
    out = staging_root / slug
    out.mkdir(parents=True, exist_ok=False)
    params = [rec["source_key"], family, cfg.schema_version, run_id]
    con.execute(TRIPS_DDL)

    seen = quarantined = 0
    q_buffer: list[dict] = []
    q_writer = None

    def flush(batch: _Batch):
        con.register("raw", batch.table())
        try:
            con.execute(sql, params)
        finally:
            con.unregister("raw")

    def flush_quarantine():
        nonlocal q_writer
        if not q_buffer:
            return
        if q_writer is None:
            q_writer = pq.ParquetWriter(out / "quarantine.parquet", QUARANTINE_SCHEMA)
        q_writer.write_table(pa.Table.from_pylist(q_buffer, QUARANTINE_SCHEMA))
        q_buffer.clear()

    def reject(source_row, reason, fields):
        nonlocal quarantined
        quarantined += 1
        q_buffer.append({"source_key": rec["source_key"], "source_row": source_row,
                         "reason": reason, "raw_record": json.dumps(fields)})

    try:
        with open_candidate(cfg, candidate_from_record(rec)) as fh:
            header, records = open_records(fh)
            if list(header) != json.loads(rec["header_json"]):
                raise NormalizeError("header differs from discovery; re-run inventory discover")
            batch = _Batch(names)
            for source_row, fields in records:
                seen += 1
                if len(fields) != width:
                    reject(source_row, "field_count", fields)
                else:
                    start = parse_local_ts(fields[start_idx])
                    if start is None:
                        reject(source_row, "start_ts_invalid", fields)
                    else:
                        end = parse_local_ts(fields[end_idx])
                        batch.add(source_row, fields, start, end, dst.get((start.date(), start.hour)),
                                  dst.get((end.date(), end.hour)) if end else None)
                if len(batch.rows) >= cfg.batch_rows:
                    flush(batch)
                    batch = _Batch(names)
                if len(q_buffer) >= cfg.batch_rows:
                    flush_quarantine()
            flush(batch)
            flush_quarantine()
    finally:
        if q_writer is not None:
            q_writer.close()

    if seen != rec["rows"]:
        raise NormalizeError(f"read {seen} rows but profiling saw {rec['rows']}; re-run inventory profile")
    if seen and quarantined / seen > cfg.max_quarantine_fraction:
        raise NormalizeError(f"quarantine share {quarantined}/{seen} exceeds {cfg.max_quarantine_fraction}")
    staged = con.execute("SELECT count(*) FROM staged_trips").fetchone()[0]
    if staged + quarantined != seen:
        raise NormalizeError(f"staged {staged} + quarantined {quarantined} != {seen} source rows")
    if staged:
        trips_dir = out / "trips"
        con.execute(f"COPY staged_trips TO {sql_literal(str(trips_dir))} "
                    "(FORMAT parquet, PARTITION_BY (trip_year, trip_month), COMPRESSION zstd)")
        files = [str(f) for f in sorted(trips_dir.rglob("*.parquet"))]
        written = con.execute("SELECT count(*) FROM read_parquet(?)", [files]).fetchone()[0]
        if written != staged:
            raise NormalizeError(f"readback found {written} rows, expected {staged}")
    return SourceResult(rec["source_key"], slug, seen, staged, quarantined, out)
```

- [ ] **Step 5: Run the tests**

Run: `uv run pytest -q`
Expected: all PASS. If DuckDB rejects `lambda f: f IS NOT NULL`, the installed DuckDB is older than 1.3: run `uv lock --upgrade-package duckdb && uv sync` rather than changing the syntax.

- [ ] **Step 6: Write `docs/data_dictionary.md`**

```markdown
# Data dictionary: normalized trips (schema_version 1)

Location: `FinalProject_processed/trips/trip_year=YYYY/trip_month=MM/*.parquet` (Hive partitions).
Partition month is the month of `started_at_local`, not the file's nominal release.

| column | type | meaning |
|---|---|---|
| started_at_local / ended_at_local | TIMESTAMP | Source wall-clock time, America/New_York, no UTC conversion. Legacy `starttime`/`stoptime` (also `Start Time`/`Stop Time`); modern `started_at`/`ended_at`. Formats accepted: ISO with or without fractions, `M/D/YYYY H:MM[:SS]`. |
| duration_seconds | DOUBLE | `ended_at_local - started_at_local`. Wall-clock based, so it is off by 3600 s across a DST change. |
| source_duration_seconds | DOUBLE | Legacy `tripduration`; null for modern sources. |
| start_station_id / end_station_id | VARCHAR | As in the source (`72`, `6322.01`). IDs are not equated across eras. |
| start_station_name / end_station_name | VARCHAR | Trimmed source value. |
| start_lat, start_lng, end_lat, end_lng | DOUBLE | `TRY_CAST`; unparseable values become null and are flagged. |
| ride_id, rideable_type | VARCHAR | Modern sources only; null for legacy. |
| membership_group | VARCHAR | `member` / `casual`. Legacy `Subscriber` -> member, `Customer` -> casual. Anything else is null and flagged. |
| original_membership | VARCHAR | The source label, unchanged. |
| source_key, source_row | VARCHAR, BIGINT | Provenance: manifest key and 1-based record number (blank lines are not records). |
| schema_family, schema_version, run_id | VARCHAR, INTEGER, VARCHAR | Adapter used, normalization version, producing run. |
| quality_flags | VARCHAR[] | See below. Flags never remove rows. |
| eligible_departure | BOOLEAN | Valid start time and start station present. |
| eligible_arrival | BOOLEAN | Valid end time, end station present, duration >= 0. |

Null tokens: empty string, `\N`, `NULL`.

## Quality flags

`start_station_missing`, `end_station_missing`, `end_ts_invalid`, `duration_nonpositive`,
`duration_mismatch` (legacy only: derived and source duration differ by more than 2 s; expected for
minute-resolution 2015 timestamps and DST changes), `start_coords_invalid` / `end_coords_invalid` (null or
outside lat 40.4 to 41.1, lng -74.4 to -73.6), `membership_missing`, `membership_unrecognized`,
`start_dst_ambiguous`, `start_dst_nonexistent`, `end_dst_ambiguous`, `end_dst_nonexistent`.

## Quarantine

`FinalProject_processed/quarantine/*.parquet`: `source_key`, `source_row`, `reason`, `raw_record`
(JSON array of the original fields). Reasons: `field_count` (wrong number of fields) and
`start_ts_invalid` (start time missing or unparseable, so no trip month can be assigned).
A source whose quarantine share exceeds `max_quarantine_fraction` is not published.

## Deliberately not carried

Legacy `bikeid`, `birth year`, `gender` (decision D6).

## Deduplication

Exact and re-quoted copies are removed at selection time (one source set per release). No record-level
removal happens in schema_version 1; `citibike validate` reports duplicate `ride_id`s as identical or
conflicting.
```

- [ ] **Step 7: Commit**

```bash
git add src/citibike_pipeline/dst.py src/citibike_pipeline/normalize.py tests/test_dst.py tests/test_normalize.py docs/data_dictionary.md
git commit -m "Normalize selected sources to staged Parquet with flags and quarantine"
```

### Task 11: Idempotent publishing and `citibike normalize`

**Files:**
- Create: `src/citibike_pipeline/publish.py`, `src/citibike_pipeline/pipeline.py`
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `normalize_source`, `SourceResult`, `NormalizeError` (Task 10); `dst_hours` (Task 10); `Manifest` ledger methods (Task 5); `connect` (Task 2).
- Produces: `retire_unlisted(cfg, manifest, run_id) -> list[Path]`, `publish_source(cfg, manifest, run_id, res) -> list[(dataset, rel_path, rows)]`, `run_normalize(cfg, manifest, releases, dry_run=False, out=print) -> int`.
- Publish protocol (ledger is the commit point):
  1. Move each staged file into place with `os.replace` (same RAID volume, atomic per file) under a name containing the run ID: `trips/<partition>/<slug>__<run_id>__<n>.parquet`, `quarantine/<slug>__<run_id>.parquet`.
  2. Replace that source's ledger rows in one transaction.
  3. Move every file under `trips/` and `quarantine/` that is not in the ledger to `retired/<run_id>/`.
  4. Each `normalize` run starts with step 3 and with unpublishing ledger sources of the requested releases that are no longer selected. An interrupted publish is therefore rolled back to the last committed ledger state on the next run.

- [ ] **Step 1: Write the failing tests**

```python
import duckdb

from archive_fixture import KEY_2025_CSV, KEY_2025_ZIP, QUIET
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.pipeline import run_normalize


def _trip_count(cfg):
    files = [str(p) for p in cfg.trips_dir.rglob("*.parquet")]
    if not files:
        return 0
    return duckdb.connect().execute("SELECT count(*) FROM read_parquet(?)", [files]).fetchone()[0]


def _normalize(cfg, releases, **kw):
    with Manifest(cfg) as m:
        return run_normalize(cfg, m, releases, out=QUIET, **kw)


def test_normalize_publishes_and_reconciles(selected_cfg):
    assert _normalize(selected_cfg, ["2018-04", "2025-01"]) == 0
    assert _trip_count(selected_cfg) == 5
    assert len(list(selected_cfg.quarantine_dir.glob("*.parquet"))) == 1
    with Manifest(selected_cfg) as m:
        runs = m.con.execute("SELECT status, count(*) FROM source_runs GROUP BY ALL").fetchall()
    assert runs == [("ok", 3)]


def test_rerun_is_idempotent(selected_cfg):
    _normalize(selected_cfg, ["2018-04", "2025-01"])
    first = sorted(p.name for p in selected_cfg.trips_dir.rglob("*.parquet"))
    assert _normalize(selected_cfg, ["2018-04", "2025-01"]) == 0
    assert _trip_count(selected_cfg) == 5
    second = sorted(p.name for p in selected_cfg.trips_dir.rglob("*.parquet"))
    assert len(second) == len(first) and set(second).isdisjoint(first)
    assert sorted(p.name for p in selected_cfg.retired_dir.rglob("*.parquet") if "trips" in p.parts) == first


def test_dry_run_writes_nothing(selected_cfg):
    assert _normalize(selected_cfg, ["2025-01"], dry_run=True) == 0
    assert not selected_cfg.trips_dir.exists()


def test_release_without_selection_fails(selected_cfg):
    assert _normalize(selected_cfg, ["2025-01", "2019-02"]) == 1
    assert not selected_cfg.trips_dir.exists()


def test_selection_change_retires_previous_source(selected_cfg):
    _normalize(selected_cfg, ["2025-01"])
    with Manifest(selected_cfg) as m:
        for key, status in ((KEY_2025_CSV, "excluded"), (KEY_2025_ZIP, "selected")):
            rec = m.get(key)
            rec["status"] = status
            m.upsert(rec)
    assert _normalize(selected_cfg, ["2025-01"]) == 0
    assert _trip_count(selected_cfg) == 3
    with Manifest(selected_cfg) as m:
        assert m.published_keys_for_releases(["2025-01"]) == [KEY_2025_ZIP]


def test_interrupted_publish_is_rolled_back(selected_cfg):
    stray = selected_cfg.trips_dir / "trip_year=2025" / "trip_month=01" / "stray__x__0.parquet"
    stray.parent.mkdir(parents=True)
    duckdb.connect().execute(f"COPY (SELECT 1 AS a) TO '{stray}' (FORMAT parquet)")
    assert _normalize(selected_cfg, ["2025-01"]) == 0
    assert not stray.exists() and _trip_count(selected_cfg) == 3
    assert any(p.name == stray.name for p in selected_cfg.retired_dir.rglob("*.parquet"))


def test_failed_source_is_not_published(selected_cfg):
    import dataclasses

    strict = dataclasses.replace(selected_cfg, max_quarantine_fraction=0.1)
    assert _normalize(strict, ["2018-04"]) == 1
    with Manifest(strict) as m:
        statuses = dict(m.con.execute("SELECT source_key, status FROM source_runs").fetchall())
    assert "failed" in statuses.values()
    trips = list(strict.trips_dir.rglob("*.parquet")) if strict.trips_dir.exists() else []
    assert all("201804-citibike-tripdata_1" not in p.name for p in trips)
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_pipeline.py -q`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write `src/citibike_pipeline/publish.py`**

```python
"""Moves staged files into the published layout. Superseded files are moved, never deleted."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow.parquet as pq


def retire_unlisted(cfg, manifest, run_id: str) -> list[Path]:
    listed = {cfg.processed_root / p for p in manifest.published_paths()}
    retired = []
    for root in (cfg.trips_dir, cfg.quarantine_dir):
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
    entries = []
    for dataset, src, dest in moves:
        rows = pq.ParquetFile(src).metadata.num_rows
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(src, dest)
        entries.append((dataset, dest.relative_to(cfg.processed_root).as_posix(), rows))
    manifest.replace_published(res.source_key, run_id, entries)
    retire_unlisted(cfg, manifest, run_id)
    return entries
```

- [ ] **Step 4: Write `src/citibike_pipeline/pipeline.py`**

```python
"""`citibike normalize`: stage, reconcile, and publish every selected source of the given releases."""

from __future__ import annotations

import csv
import time
import zipfile

import duckdb

from .db import connect
from .dst import dst_hours
from .normalize import NormalizeError, normalize_source
from .publish import publish_source, retire_unlisted

_SOURCE_ERRORS = (NormalizeError, OSError, UnicodeDecodeError, csv.Error, zipfile.BadZipFile, duckdb.Error)


def run_normalize(cfg, manifest, releases, dry_run=False, out=print) -> int:
    records = [r for r in manifest.all() if r["nominal_release"] in releases]
    selected = sorted((r for r in records if r["status"] == "selected"), key=lambda r: r["source_key"])
    missing = sorted(set(releases) - {r["nominal_release"] for r in selected})
    if missing:
        out(f"no selected sources for: {', '.join(missing)}; run `citibike inventory select` first")
        return 1
    if dry_run:
        for rec in selected:
            out(f"would normalize {rec['source_key']} ({rec['rows']} rows, {rec['schema_family']})")
        return 0

    run_id = manifest.start_run("normalize", {"releases": releases})
    selected_keys = {r["source_key"] for r in selected}
    manifest.unpublish([k for k in manifest.published_keys_for_releases(releases) if k not in selected_keys])
    retire_unlisted(cfg, manifest, run_id)

    con = connect(cfg)
    dst = dst_hours(range(2013, 2028), cfg.timezone)
    staging_root = cfg.staging_dir / run_id
    failures = 0
    try:
        for rec in selected:
            started = time.monotonic()
            try:
                res = normalize_source(cfg, con, rec, run_id, staging_root, dst)
                publish_source(cfg, manifest, run_id, res)
            except _SOURCE_ERRORS as exc:
                failures += 1
                seconds = time.monotonic() - started
                manifest.record_source_run(run_id, rec, None, "failed", f"{type(exc).__name__}: {exc}", seconds)
                out(f"FAILED {rec['source_key']}: {exc}")
                continue
            seconds = time.monotonic() - started
            manifest.record_source_run(run_id, rec, res, "ok", None, seconds)
            out(f"ok {seconds:7.1f}s {res.source_rows / max(seconds, 1e-9):10,.0f} rows/s "
                f"published={res.staged_rows} quarantined={res.quarantined_rows} {rec['source_key']}")
    finally:
        con.close()
    manifest.finish_run(run_id, "failed" if failures else "ok")
    out(f"normalize {run_id}: {len(selected) - failures} ok, {failures} failed")
    return 1 if failures else 0
```

- [ ] **Step 5: Run the tests**

Run: `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/citibike_pipeline/publish.py src/citibike_pipeline/pipeline.py tests/test_pipeline.py
git commit -m "Publish normalized sources through a ledger with rerun-safe retirement"
```

### Task 12: Reconciliation and quality report, `citibike validate`

**Files:**
- Create: `src/citibike_pipeline/validate.py`
- Test: `tests/test_validate.py`

**Interfaces:**
- Consumes: `Manifest.published_for_sources` (Task 5), `run_normalize` (Task 11), `connect` (Task 2).
- Produces: `validate_releases(cfg, manifest, releases) -> dict` with keys `releases, sources, source_rows, published_rows, quarantined_rows, reconciled, schema_consistent, months, flags, membership, rideable, duration, start_stations, ride_id_identical, ride_id_conflicting, date_range`; `write_validation_report(cfg, result) -> Path` (`reports/validate_<first>[_<last>].md`); `run_validate(cfg, releases, out) -> int` (validates each release, then the group when more than one is given; exit 1 if any unit fails reconciliation or schema consistency).

- [ ] **Step 1: Write the failing tests**

```python
from archive_fixture import QUIET
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.pipeline import run_normalize
from citibike_pipeline.validate import run_validate, validate_releases


def test_validate_reconciles_and_reports_ride_id_duplicates(selected_cfg):
    with Manifest(selected_cfg) as m:
        assert run_normalize(selected_cfg, m, ["2023-11", "2025-01"], out=QUIET) == 0
        res = validate_releases(selected_cfg, m, ["2023-11"])
    assert res["reconciled"] and res["schema_consistent"]
    assert (res["source_rows"], res["published_rows"], res["quarantined_rows"]) == (5, 4, 1)
    assert (res["ride_id_identical"], res["ride_id_conflicting"]) == (1, 1)
    assert res["months"] == [(2023, 11, 4)]
    assert dict(res["flags"])["start_dst_ambiguous"] == 2


def test_month_spillover_is_visible(selected_cfg):
    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2025-01"], out=QUIET)
        res = validate_releases(selected_cfg, m, ["2025-01"])
    assert res["months"] == [(2024, 12, 1), (2025, 1, 2)]


def test_unpublished_release_does_not_reconcile(selected_cfg):
    with Manifest(selected_cfg) as m:
        res = validate_releases(selected_cfg, m, ["2018-04"])
    assert res["reconciled"] is False and res["published_rows"] == 0


def test_run_validate_writes_reports(selected_cfg):
    with Manifest(selected_cfg) as m:
        run_normalize(selected_cfg, m, ["2018-04", "2025-01"], out=QUIET)
    assert run_validate(selected_cfg, ["2018-04", "2025-01"], QUIET) == 0
    names = sorted(p.name for p in selected_cfg.reports_dir.glob("validate_*.md"))
    assert names == ["validate_2018-04.md", "validate_2018-04_2025-01.md", "validate_2025-01.md"]
    assert "reconciled: True" in (selected_cfg.reports_dir / "validate_2018-04.md").read_text()


def test_run_validate_fails_when_unpublished(selected_cfg):
    assert run_validate(selected_cfg, ["2018-04"], QUIET) == 1
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_validate.py -q`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write `src/citibike_pipeline/validate.py`**

```python
"""Reconciles published rows to source rows and summarizes quality for one or more releases."""

from __future__ import annotations

from pathlib import Path

from .db import connect
from .manifest import Manifest


def validate_releases(cfg, manifest, releases: list[str]) -> dict:
    sources = [r for r in manifest.all() if r["nominal_release"] in releases and r["status"] == "selected"]
    ledger = manifest.published_for_sources([r["source_key"] for r in sources])
    trips = [cfg.processed_root / e["path"] for e in ledger if e["dataset"] == "trips"]
    res = {
        "releases": list(releases),
        "sources": len(sources),
        "source_rows": sum(r["rows"] or 0 for r in sources),
        "published_rows": sum(e["rows"] for e in ledger if e["dataset"] == "trips"),
        "quarantined_rows": sum(e["rows"] for e in ledger if e["dataset"] == "quarantine"),
        "months": [], "flags": [], "membership": [], "rideable": [], "duration": None,
        "start_stations": 0, "ride_id_identical": 0, "ride_id_conflicting": 0, "date_range": None,
    }
    res["reconciled"] = bool(sources) and res["published_rows"] > 0 and \
        res["source_rows"] == res["published_rows"] + res["quarantined_rows"]
    con = connect(cfg)
    try:
        layouts = set()
        for f in trips:
            rel = con.read_parquet(str(f))
            layouts.add(tuple(zip(rel.columns, map(str, rel.types))))
        res["schema_consistent"] = len(layouts) <= 1
        if not trips:
            return res
        con.sql("SELECT * FROM read_parquet(?, hive_partitioning = true, "
                "hive_types = {'trip_year': INTEGER, 'trip_month': INTEGER})",
                params=[[str(f) for f in trips]]).create_view("trips")
        q = lambda sql: con.execute(sql).fetchall()  # noqa: E731
        res["months"] = q("SELECT trip_year, trip_month, count(*) FROM trips GROUP BY ALL ORDER BY ALL")
        res["flags"] = q("SELECT f, count(*) FROM (SELECT unnest(quality_flags) AS f FROM trips) GROUP BY f ORDER BY f")
        res["membership"] = q("SELECT membership_group, original_membership, count(*) FROM trips GROUP BY ALL ORDER BY ALL")
        res["rideable"] = q("SELECT rideable_type, count(*) FROM trips GROUP BY ALL ORDER BY ALL")
        res["duration"] = q("SELECT quantile_cont(duration_seconds, 0.5), quantile_cont(duration_seconds, 0.99), "
                            "max(duration_seconds), count(*) FILTER (WHERE duration_seconds <= 0) FROM trips")[0]
        res["start_stations"] = q("SELECT count(DISTINCT start_station_id) FROM trips")[0][0]
        res["date_range"] = q("SELECT min(started_at_local), max(started_at_local) FROM trips")[0]
        res["ride_id_identical"], res["ride_id_conflicting"] = q("""
            WITH d AS (
              SELECT ride_id,
                     count(DISTINCT (started_at_local, ended_at_local, start_station_id, end_station_id)) AS variants
              FROM trips WHERE ride_id IS NOT NULL GROUP BY ride_id HAVING count(*) > 1
            )
            SELECT count(*) FILTER (WHERE variants = 1), count(*) FILTER (WHERE variants > 1) FROM d
        """)[0]
    finally:
        con.close()
    return res


def write_validation_report(cfg, res: dict) -> Path:
    cfg.reports_dir.mkdir(parents=True, exist_ok=True)
    rels = res["releases"]
    name = rels[0] if len(rels) == 1 else f"{rels[0]}_{rels[-1]}"
    path = cfg.reports_dir / f"validate_{name}.md"
    lines = [
        f"# Validation: {', '.join(rels)}", "",
        f"- reconciled: {res['reconciled']}",
        f"- schema_consistent: {res['schema_consistent']}",
        f"- selected sources: {res['sources']}",
        f"- source rows: {res['source_rows']}",
        f"- published rows: {res['published_rows']}",
        f"- quarantined rows: {res['quarantined_rows']}",
        f"- record-level removals: 0 (schema_version {cfg.schema_version} removes none)",
        f"- start date range: {res['date_range']}",
        f"- distinct start stations: {res['start_stations']}",
        f"- duration p50 / p99 / max / nonpositive: {res['duration']}",
        f"- duplicate ride_id: identical {res['ride_id_identical']}, conflicting {res['ride_id_conflicting']}",
        "", "## Rows by trip month", "", "| year | month | trips |", "|---|---|---|",
        *[f"| {y} | {m:02d} | {n} |" for y, m, n in res["months"]],
        "", "## Quality flags", "", "| flag | rows |", "|---|---|",
        *[f"| {f} | {n} |" for f, n in res["flags"]],
        "", "## Membership", "", "| group | original | rows |", "|---|---|---|",
        *[f"| {g} | {o} | {n} |" for g, o, n in res["membership"]],
        "", "## Rideable type", "", "| type | rows |", "|---|---|",
        *[f"| {t} | {n} |" for t, n in res["rideable"]],
    ]
    path.write_text("\n".join(lines) + "\n")
    return path


def run_validate(cfg, releases: list[str], out) -> int:
    units = [[r] for r in releases] + ([list(releases)] if len(releases) > 1 else [])
    rc = 0
    with Manifest(cfg) as manifest:
        for unit in units:
            res = validate_releases(cfg, manifest, unit)
            path = write_validation_report(cfg, res)
            out(f"{', '.join(unit)}: reconciled={res['reconciled']} schema_consistent={res['schema_consistent']} "
                f"published={res['published_rows']} quarantined={res['quarantined_rows']} -> {path}")
            if not (res["reconciled"] and res["schema_consistent"]):
                rc = 1
    return rc
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/citibike_pipeline/validate.py tests/test_validate.py
git commit -m "Add release validation with reconciliation and quality summaries"
```

### Task 13: Real pilot, rerun check, recent year 2025, and runbook

**Files:**
- Create: `docs/runbook.md`, `docs/validation/` (copies of the generated validation reports)
- Modify: `docs/implementation_status.md`

**Interfaces:**
- Consumes: everything above; the pilot selection from Task 9.
- Produces: published, reconciled Parquet for the pilots and for 2025 on the RAID, with measured throughput, memory, and disk footprint.

- [ ] **Step 1: Pilot dry run**

```bash
P=/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed
uv run citibike normalize --release 2018-04 --release 2025-03 --dry-run
```

Expected: one `would normalize ...` line per selected source; nothing written under `$P/trips`.

- [ ] **Step 2: Pilot run with time and peak memory**

```bash
/usr/bin/time -l uv run citibike normalize --release 2018-04 --release 2025-03 2>&1 | tee "$P/reports/normalize_pilot.log"
grep -E "maximum resident set size|real" "$P/reports/normalize_pilot.log"
du -sh "$P/trips" "$P/quarantine" "$P/tmp"
```

Expected: every source `ok`, final line `... N ok, 0 failed`, exit 0. If a source fails on `quarantine share`, read its quarantine file before changing anything:

```bash
uv run python -c "import duckdb,sys; print(duckdb.sql(\"SELECT reason, count(*), any_value(raw_record) FROM read_parquet('$P/staging/*/*/quarantine.parquet') GROUP BY 1\"))"
```

Fix the parser with a test (for example, a timestamp shape not in the fixture), bump `parser_version`, re-profile that release with `--force`, and rerun. Raise `max_quarantine_fraction` only with a written reason in `docs/implementation_status.md`.

- [ ] **Step 3: Validate the pilots**

```bash
uv run citibike validate --release 2018-04 --release 2025-03
```

Expected: `reconciled=True schema_consistent=True` for each release and for the group; exit 0. Read `validate_2018-04.md` and `validate_2025-03.md` and record: rows by trip month (any spillover into neighbouring months is reported, not assumed), DST flag counts, membership mapping, and duplicate ride IDs.

- [ ] **Step 4: Rerun check (idempotency on real data)**

```bash
cp "$P/reports/validate_2018-04_2025-03.md" /tmp/validate_pilot_run1.md
uv run citibike normalize --release 2018-04 --release 2025-03
uv run citibike validate --release 2018-04 --release 2025-03
diff <(grep -E "rows|reconciled|ride_id|\| 20" /tmp/validate_pilot_run1.md) \
     <(grep -E "rows|reconciled|ride_id|\| 20" "$P/reports/validate_2018-04_2025-03.md") && echo "rerun counts unchanged"
ls "$P/retired"
```

Expected: `rerun counts unchanged`; the first run's files are under `retired/<second_run_id>/`.

- [ ] **Step 5: Record the pilot gate in `docs/implementation_status.md`**

Write: run IDs; per-source rows/s from the logs; peak RSS; `du` sizes of trips, quarantine, and tmp; Parquet bytes per source row; projected disk for one year (from the measured ratio, not an assumed one); quarantine reasons and counts; flag counts; ride_id identical/conflicting counts; remaining uncertainties. Copy the reports:

```bash
mkdir -p docs/validation
cp "$P/reports/validate_2018-04.md" "$P/reports/validate_2025-03.md" "$P/reports/validate_2018-04_2025-03.md" docs/validation/
git add docs/implementation_status.md docs/validation
git commit -m "Record local pilot normalization and validation results"
```

Stop here and show the user the pilot numbers before Step 6. The workflow requires the pilot to reconcile before scaling.

- [ ] **Step 6: Process the recent full year 2025**

```bash
uv run citibike inventory select --release 2025 --dry-run
uv run citibike inventory select --release 2025
/usr/bin/time -l uv run citibike normalize --release 2025 2>&1 | tee "$P/reports/normalize_2025.log"
uv run citibike validate --release 2025
du -sh "$P/trips"
```

Expected: 12 `select` lines, all sources `ok`, every month and the 2025 group `reconciled=True`. If a month is `conflict` or `incomplete` in the inventory report, stop and record it rather than forcing a choice.

- [ ] **Step 7: Write `docs/runbook.md`**

```markdown
# Runbook: local Citi Bike pipeline

All commands run from `~/Sites/CUNY/citibike-data-pipeline/teams/team-6`. The RAID must be mounted.
Source: `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject` (read-only).
Outputs: `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed`.

## Setup

    uv sync
    uv run pytest -q
    uv run citibike doctor

## Inventory (resumable; rerun any step after an interruption)

    uv run citibike inventory discover
    uv run citibike inventory profile --all --workers 4
    curl -fsS https://s3.amazonaws.com/tripdata/ -o "$P/reports/tripdata_listing.xml"   # optional
    uv run citibike inventory report
    uv run citibike inventory select --release YYYY-MM [--release ...] [--dry-run]
    uv run citibike inventory select --release YYYY-MM --set "YYYY-MM=<set_id>"         # only for a reviewed conflict

## Normalize and validate

    uv run citibike normalize --release YYYY-MM|YYYY [--dry-run]
    uv run citibike validate --release YYYY-MM|YYYY

Reruns are safe: each run publishes new files, commits the ledger, and moves superseded files to
`retired/<run_id>/`. An interrupted run is rolled back to the last committed ledger at the start of
the next run.

## When something fails

- `failed` in profile: read `status_reason` in `reports/inventory_report.md`; fix with a test; bump
  `parser_version`; rerun profile.
- `conflict` in select: compare the sets' row counts in the report; record the decision in
  `docs/implementation_status.md`; then use `--set`.
- `quarantine share` in normalize: inspect `staging/<run_id>/*/quarantine.parquet`; fix the parser or
  record a reason before raising `max_quarantine_fraction`.
- `header differs` or `profiling saw`: the source changed since inventory; rerun discover and profile.

## Housekeeping (manual, user decision)

`retired/`, `staging/<run_id>/`, `staging/nested/` and `tmp/` are never deleted by the pipeline.
Check `du -sh` on each and remove old ones by hand when you are satisfied with a run.

## Querying

    uv run python -c "import duckdb; print(duckdb.sql(\"SELECT trip_year, trip_month, count(*) FROM read_parquet('$P/trips/*/*/*.parquet', hive_partitioning=true, hive_types={'trip_year': INTEGER, 'trip_month': INTEGER}) GROUP BY ALL ORDER BY ALL\"))"
```

(`$P` above means `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed`.)

- [ ] **Step 8: Final status and commit**

Update `docs/implementation_status.md` with the 2025 run (IDs, timings, peak RSS, footprint, per-month reconciliation, completeness per month from the inventory report) and tick the Stage 2 boxes. Copy the 2025 validation reports (twelve monthly files plus `validate_2025-01_2025-12.md`) into `docs/validation/`.

```bash
cp "$P"/reports/validate_2025-*.md docs/validation/
git add docs/runbook.md docs/implementation_status.md docs/validation
git commit -m "Normalize and validate 2025; add pipeline runbook"
```

---

## After this plan (separate plans, written once the evidence exists)

- **Stage 3 (aggregates and as-of features):** `station_hour_activity`, `station_month_activity`, `station_location_observations`, `historical_demand_profiles` with release-availability cutoffs and leakage tests, `station_capacity_utilization` only where effective-dated capacity exists. Its design depends on the pilot's station-ID and coverage findings, so it is planned after Task 13. Fills the `aggregate` subcommand.
- **Stage 4 (Databricks):** needs the class workspace URL, compute/runtime, granted catalog/schema/volume, and existing class tables. Separate `.venv-dbconnect` matched to the runtime's Python. Fills `publish-plan`. Includes the ACS transportation-access dataset and station-to-tract crosswalk.
- **Stage 5 (production MVP):** snapshot-based baseline, jobs via bundles, dashboard, monitoring. Snapshots already collected are in `FinalProject_snapshots/`.
- **Older years:** only after the pilot reconciles and the modeling question justifies them: `uv run citibike inventory select --release 2019` and so on, using the same commands.
