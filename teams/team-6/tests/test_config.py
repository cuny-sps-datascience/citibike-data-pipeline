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
