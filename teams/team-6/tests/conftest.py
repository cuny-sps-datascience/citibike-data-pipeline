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


@pytest.fixture
def selected_cfg(profiled_cfg):
    from archive_fixture import FIXTURE_RELEASES, QUIET
    from citibike_pipeline.manifest import Manifest
    from citibike_pipeline.sets import apply_selection

    with Manifest(profiled_cfg) as m:
        assert apply_selection(m, FIXTURE_RELEASES, {}, False, QUIET) == 0
    return profiled_cfg
