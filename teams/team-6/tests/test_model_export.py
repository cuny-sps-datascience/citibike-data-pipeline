from datetime import date, datetime, timedelta, timezone
import calendar
import json

import duckdb
import pytest

from citibike_pipeline.model_export import (
    aggregate_departures, build_features, run_model_export, select_inputs, validate_package,
)


def test_duration_boundaries_missing_destination_and_dst():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE source_releases AS SELECT 's' AS source_key,'2025-03' AS nominal_release")
        con.execute("""CREATE TABLE trips AS SELECT 's' AS source_key,'modern' AS schema_family,
            'A' AS start_station_id,NULL::VARCHAR AS end_station_id,'member' AS membership_group,
            'electric_bike' AS rideable_type,true AS eligible_departure,
            NULL::DOUBLE AS source_duration_seconds,* FROM (VALUES
            (TIMESTAMP '2025-03-01 08:00:00',TIMESTAMP '2025-03-01 08:00:30',30.,[]::VARCHAR[]),
            (TIMESTAMP '2025-03-01 08:00:00',TIMESTAMP '2025-03-01 08:01:00',60.,[]::VARCHAR[]),
            (TIMESTAMP '2025-03-01 08:00:00',TIMESTAMP '2025-03-01 08:02:00',120.,[]::VARCHAR[]),
            (TIMESTAMP '2025-03-01 08:00:00',NULL,NULL,[]::VARCHAR[]),
            (TIMESTAMP '2025-03-01 08:00:00',TIMESTAMP '2025-03-01 08:00:00',0.,[]::VARCHAR[]),
            (TIMESTAMP '2025-03-09 01:59:30',TIMESTAMP '2025-03-09 03:00:30',3660.,['crosses_dst']),
            (TIMESTAMP '2025-03-01 01:00:00',TIMESTAMP '2025-03-01 01:02:00',120.,['start_dst_ambiguous'])
            ) v(started_at_local,ended_at_local,duration_seconds,quality_flags)""")
        aggregate_departures(con,date(2025,3,1),date(2025,4,1))
        assert con.execute('''SELECT sum(departures_published),sum(departures_ge30),sum(departures_ge60),
            sum(departures_ge120),sum(uncertain_duration_departures),sum(missing_duration_departures),
            sum(wall_nonpositive_departures) FROM departure_batch''').fetchone()==(7,4,3,1,3,1,1)


def prepare_export(cfg):
    cfg.manifest_db.parent.mkdir(parents=True,exist_ok=True)
    with duckdb.connect(str(cfg.manifest_db)) as ledger:
        ledger.execute('DROP TABLE IF EXISTS published_files')
        ledger.execute('DROP TABLE IF EXISTS sources')
        ledger.execute('''CREATE TABLE sources(source_key VARCHAR,status VARCHAR,nominal_release VARCHAR,
            schema_family VARCHAR,sha256 VARCHAR,row_digest VARCHAR)''')
        ledger.execute('''CREATE TABLE published_files(path VARCHAR,rows BIGINT,source_key VARCHAR,
            run_id VARCHAR,dataset VARCHAR)''')
        for year,month in [(2024,m) for m in range(8,13)]+[(2025,1)]:
            source=f'{year}-{month:02d}'
            relative=f'trips/trip_year={year}/trip_month={month:02d}/{source}.parquet'
            path=cfg.processed_root/relative
            path.parent.mkdir(parents=True,exist_ok=True)
            with duckdb.connect() as con:
                con.execute('''CREATE TABLE rows AS SELECT ?::VARCHAR AS source_key,'modern' AS schema_family,
                    'A' AS start_station_id,'Alpha' AS start_station_name,'B' AS end_station_id,
                    'member' AS membership_group,'electric_bike' AS rideable_type,true AS eligible_departure,
                    true AS eligible_arrival,NULL::DOUBLE AS source_duration_seconds,
                    ?::TIMESTAMP AS started_at_local,?::TIMESTAMP AS ended_at_local,
                    120.::DOUBLE AS duration_seconds,[]::VARCHAR[] AS quality_flags,
                    40.7 AS start_lat,-73.9 AS start_lng''',
                    [source,f'{source}-10 08:00:00',f'{source}-10 08:02:00'])
                con.execute('COPY rows TO ? (FORMAT PARQUET)',[str(path)])
            ledger.execute("INSERT INTO sources VALUES (?,'selected',?,'modern','original-sha','digest')",[source,source])
            ledger.execute("INSERT INTO published_files VALUES (?,1,?,'run','trips')",[relative,source])


def test_export_provenance_cutoffs_no_zeros_and_atomic_failure(selected_cfg, monkeypatch):
    import citibike_pipeline.model_export as module

    prepare_export(selected_cfg)
    start,end=date(2025,1,1),date(2025,2,1)
    stray=selected_cfg.processed_root/'trips/trip_year=2025/trip_month=01/stray.parquet'
    stray.write_bytes(b'not a parquet')
    records=select_inputs(selected_cfg,date(2024,8,1),end)
    assert len(records)==6 and all('stray' not in r['path'] for r in records)
    package=run_model_export(selected_cfg,start,end,out=lambda _:None)
    report=validate_package(package)
    assert all(report['checks'].values()) and report['label_rows']==1
    assert not report['main_evaluation_ready']
    with duckdb.connect() as con:
        labels=con.execute('SELECT departures_published,main_evaluation_eligible FROM read_parquet(?)',
                           [str(package/'labels/station_day_labels/2025-01.parquet')]).fetchone()
        assert labels==(1,False)
        features=con.execute('''SELECT latest_included_release,historical_departures,
            historical_weekday_departure_mean,census_available,release_availability_verified
            FROM read_parquet(?)''',[str(package/'features/forecast_features/2025-01.parquet')]).fetchone()
        assert features==('2024-10',2,None,False,False)
    pointer=selected_cfg.processed_root/'modeling_exports/CURRENT'
    repeated=run_model_export(selected_cfg,start,end,out=lambda _:None)
    first_parquet={a['path']:a['sha256'] for a in json.loads((package/'manifest.json').read_text())['artifacts']
                   if a['path'].endswith('.parquet')}
    repeated_parquet={a['path']:a['sha256'] for a in json.loads((repeated/'manifest.json').read_text())['artifacts']
                      if a['path'].endswith('.parquet')}
    assert first_parquet==repeated_parquet
    previous=pointer.read_text()
    def fail(*args,**kwargs):
        raise RuntimeError('simulated interrupted export')
    monkeypatch.setattr(module,'copy_table',fail)
    with pytest.raises(RuntimeError,match='interrupted'):
        run_model_export(selected_cfg,start,end,out=lambda _:None)
    assert pointer.read_text()==previous
    (package/'data_dictionary.md').write_text('tampered')
    with pytest.raises(ValueError,match='checksum'):
        validate_package(package)


def test_stale_and_missing_ledger_inputs_fail(selected_cfg):
    prepare_export(selected_cfg)
    with duckdb.connect(str(selected_cfg.manifest_db)) as ledger:
        ledger.execute("UPDATE sources SET status='retired' WHERE nominal_release='2025-01'")
    with pytest.raises(ValueError,match='Stale'):
        select_inputs(selected_cfg,date(2025,1,1),date(2025,2,1))
    with duckdb.connect(str(selected_cfg.manifest_db)) as ledger:
        ledger.execute("UPDATE sources SET status='selected'")
    file=selected_cfg.processed_root/'trips/trip_year=2025/trip_month=01/2025-01.parquet'
    file.unlink()
    with pytest.raises(FileNotFoundError):
        select_inputs(selected_cfg,date(2025,1,1),date(2025,2,1))


def test_monthly_profiles_equal_explicit_per_origin_release_filter():
    with duckdb.connect() as con:
        con.execute("SET TimeZone='UTC'")
        con.execute('CREATE TABLE labels(station_id VARCHAR,id_scheme VARCHAR,service_date DATE)')
        con.execute('''CREATE TABLE departure_daily(station_id VARCHAR,id_scheme VARCHAR,service_date DATE,
            nominal_release VARCHAR,departures_published BIGINT,member_departures BIGINT,
            casual_departures BIGINT,classic_departures BIGINT,electric_departures BIGINT)''')
        con.execute('''CREATE TABLE release_availability(nominal_release VARCHAR,release_end_date DATE,
            available_at_utc TIMESTAMPTZ)''')
        for year,month in [(2024,m) for m in range(9,13)]+[(2025,1),(2025,2)]:
            release=f'{year}-{month:02d}'
            last=date(year,month,calendar.monthrange(year,month)[1])
            available=datetime.combine(last+timedelta(days=45),datetime.min.time(),timezone.utc)
            con.execute('INSERT INTO release_availability VALUES (?,?,?)',[release,last,available])
            for day in (10,20):
                con.execute("INSERT INTO departure_daily VALUES ('A','short_name',?,?,4,3,1,1,3)",
                            [date(year,month,day),release])
        # An earlier trip supplied by a future release must remain unavailable.
        con.execute("INSERT INTO departure_daily VALUES ('A','short_name','2024-10-20','2025-02',99,99,0,0,99)")
        targets=[date(2025,month,day) for month in (1,2,3,4) for day in (1,10,15,28)]
        con.executemany("INSERT INTO labels VALUES ('A','short_name',?)",[(d,) for d in targets])
        build_features(con,date(2025,1,1),date(2025,5,1))
        for target,origin,asof,count,days,weekday_count,weekday_days,release in con.execute('''SELECT
            target_date,timezone('UTC',forecast_origin_utc),profile_as_of,historical_departures,
            historical_observed_departure_days,historical_weekday_departures,
            historical_weekday_observed_days,latest_included_release FROM features''').fetchall():
            reference=con.execute('''SELECT sum(departures_published),count(DISTINCT service_date),
                sum(departures_published) FILTER(WHERE isodow(service_date)=isodow(?::DATE)),
                count(DISTINCT service_date) FILTER(WHERE isodow(service_date)=isodow(?::DATE)),
                max(d.nominal_release) FROM departure_daily d JOIN release_availability r USING(nominal_release)
                WHERE r.available_at_utc < ? AND service_date <= ? AND service_date > ?::DATE-56
                AND service_date < ?::DATE-1''',[target,target,origin,asof,asof,target]).fetchone()
            assert (count,days,weekday_count,weekday_days,release)==reference
