from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
IMPORT_NOTEBOOKS = (
    "00_workspace_pilot.py",
    "02_hourly_observations.py",
    "04_train_borough_benchmark.py",
    "07_public_station_audit.py",
    "08_import_weather_contribution.py",
    "11_import_borough_actuals.py",
    "12_import_station_subway_mapping.py",
    "14_release_aware_backtest.py",
    "16_refresh_released_outcomes.py",
    "20_station_pilot.py",
    "21_availability_train.py",
    "22_availability_dashboard.py",
)


def test_imports_overwrite_stable_tables_with_dataset_version():
    for name in IMPORT_NOTEBOOKS:
        source = (ROOT / "notebooks" / name).read_text()
        assert "dataset_version" in source, name
        assert re.search(r"mode\(\s*['\"]overwrite['\"]\s*\)", source), name
        assert "overwriteSchema" in source, name


def test_weather_deployment_uses_stable_table_name():
    source = (ROOT / "scripts" / "deploy_public_mvp.py").read_text()
    assert "'table':'external_weather_reference'" in source
    assert "external_weather_reference_" not in source


def test_migration_maps_unique_sources_to_unique_stable_tables():
    source = (ROOT / "notebooks" / "23_migrate_stable_catalog.py").read_text()
    assert "stable_table_count':len(results)" in source
    assert "assert all(not spark.catalog.tableExists" in source
    assert "immutable_volume_and_raid_copies_preserved':True" in source
