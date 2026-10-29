from datetime import date
from pathlib import Path

import duckdb
import pytest

from archive_fixture import FIXTURE_RELEASES, QUIET
from citibike_pipeline.bike_mix import build_halfhour_mix, day_trip_files
from citibike_pipeline.cli import main
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.pipeline import run_normalize


def test_halfhour_boundaries_mix_changes_and_zero_intervals():
    with duckdb.connect() as con:
        con.execute("""CREATE TABLE trips AS SELECT * FROM (VALUES
          ('A','Alpha','modern','classic_bike',TIMESTAMP '2025-04-15 08:00:00',true),
          ('A','Alpha','modern','electric_bike',TIMESTAMP '2025-04-15 08:29:59.999',true),
          ('A','Alpha','modern','electric_bike',TIMESTAMP '2025-04-15 08:30:00',true),
          ('A','Alpha','modern','electric_bike',TIMESTAMP '2025-04-15 08:59:59.999',true),
          ('A','Alpha','modern','electric_bike',TIMESTAMP '2025-04-15 23:59:59.999',true),
          ('A','Alpha','modern','classic_bike',TIMESTAMP '2025-04-16 00:00:00',true),
          ('A','Alpha','modern','classic_bike',TIMESTAMP '2025-04-15 08:00:00',false),
          ('B','Beta','modern',NULL,TIMESTAMP '2025-04-15 08:00:00',true),
          ('B','Beta','modern',' ',TIMESTAMP '2025-04-15 08:00:00',true),
          ('B','Beta','modern','cargo_bike',TIMESTAMP '2025-04-15 08:30:00',true),
          ('A','Legacy','legacy_lower',NULL,TIMESTAMP '2025-04-15 08:00:00',true)
        ) t(start_station_id,start_station_name,schema_family,rideable_type,started_at_local,eligible_departure)""")
        assert build_halfhour_mix(con, date(2025, 4, 15), min_rides=2) == 144
        rel = con.sql("SELECT * FROM halfhour_mix")
        rows = {(r['station_id'], r['id_scheme'], r['slot']): r
                for r in [dict(zip(rel.columns, row)) for row in rel.fetchall()]}
        a = rows['A', 'short_name', 16]
        b = rows['A', 'short_name', 17]
        assert (a['departures'], a['classic_pct'], a['electric_pct']) == (2, 50.0, 50.0)
        assert (b['departures'], b['electric_pct'], b['electric_change_from_previous_pp']) == (2, 100.0, 50.0)
        assert a['day_departures'] == 5 and a['day_electric_pct'] == 80.0
        assert a['electric_change_from_day_pp'] == -30.0
        assert not a['low_sample']
        assert rows['A', 'short_name', 47]['low_sample']
        zero = rows['A', 'short_name', 0]
        assert zero['departures'] == 0 and zero['electric_pct'] is None
        assert zero['electric_change_from_previous_pp'] is None
        assert rows['A', 'short_name', 47]['interval_end_local'].date() == date(2025, 4, 16)
        assert rows['B', 'short_name', 16]['unknown_departures'] == 2
        assert rows['B', 'short_name', 17]['other_departures'] == 1
        assert rows['B', 'short_name', 16]['electric_pct'] is None
        assert rows['A', 'legacy_id', 16]['unknown_departures'] == 1
        for row in rows.values():
            assert row['departures'] == sum(row[f'{b}_departures'] for b in ('classic', 'electric', 'unknown', 'other'))
        assert con.sql('SELECT sum(departures) FROM halfhour_mix').fetchone()[0] == 9
        assert build_halfhour_mix(con, date(2025, 4, 15), station_id='B') == 48
        with pytest.raises(ValueError, match='at least 1'):
            build_halfhour_mix(con, date(2025, 4, 15), min_rides=0)


def test_cli_exports_ledger_backed_day_report_and_ignores_stray_files(selected_cfg, tmp_path, capsys):
    import yaml

    with Manifest(selected_cfg) as manifest:
        assert run_normalize(selected_cfg, manifest, FIXTURE_RELEASES, out=QUIET) == 0
    files = day_trip_files(selected_cfg, date(2025, 1, 10))
    stray = Path(files[0]).parent / 'stray.parquet'
    stray.write_bytes(b'not parquet')
    assert str(stray) not in day_trip_files(selected_cfg, date(2025, 1, 10))
    config = tmp_path / 'mix.yaml'
    config.write_text(yaml.safe_dump({'schema_version': 1, 'parser_version': 'test', 'paths': {
        'source_root': str(selected_cfg.source_root), 'processed_root': str(selected_cfg.processed_root)}}))
    args = ['--config', str(config), 'bike-mix', '--date', '2025-01-10', '--station', '6322.01']
    assert main(args) == 0
    assert '1 stations, 48 half-hour rows, 1 departures' in capsys.readouterr().out
    reports = list((selected_cfg.reports_dir / 'bike_mix_halfhour').rglob('intervals.parquet'))
    assert len(reports) == 1
    with duckdb.connect() as con:
        assert con.execute('SELECT count(*), sum(departures) FROM read_parquet(?)', [str(reports[0])]).fetchone() == (48, 1)
    assert (reports[0].parent / 'intervals.csv').exists()
    assert (reports[0].parent / 'station_summary.csv').exists()
    assert main(args + ['--min-rides', '0']) == 2
    assert main(args[:-1] + ['missing-station']) == 2
    assert 'no eligible departures' in capsys.readouterr().err
    assert len(list((selected_cfg.reports_dir / 'bike_mix_halfhour').rglob('intervals.parquet'))) == 1
