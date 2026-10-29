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


@pytest.mark.parametrize("text", ["paths: [unclosed\n", "- just\n- a list\n", "plain scalar\n", ""])
def test_malformed_or_non_mapping_config_returns_2(tmp_path, capsys, text):
    path = tmp_path / "local.yaml"
    path.write_text(text)
    assert main(["--config", str(path), "doctor"]) == 2
    assert "config error" in capsys.readouterr().err
