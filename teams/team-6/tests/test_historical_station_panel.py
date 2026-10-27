from datetime import datetime, timedelta, timezone
import json

import duckdb
import pytest

from citibike_pipeline.historical_station_panel import (
    DATASETS, EVENT_VALUES, WEATHER_VALUES, assert_frames, checked_path, load_transfer, monthly_frames,
)
from citibike_pipeline.station_hour_integration import assert_weather_alignment, validate_station_hour_integration
from citibike_pipeline.forecast_sources import digest


UTC = timezone.utc


@pytest.fixture
def panel_sources():
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("""CREATE TABLE cohort(station_id VARCHAR,station_uuid VARCHAR,
        station_latitude DOUBLE,station_longitude DOUBLE,station_snapshot_cutoff_utc TIMESTAMPTZ,
        weather_mapping_distance_km DOUBLE,weather_request_class INTEGER,dataset_build_id VARCHAR);
        INSERT INTO cohort VALUES ('1234.50','uuid-a',40.75,-74.0,'2026-10-18',3.0,1,'build');
        CREATE TABLE splits(model_split VARCHAR,start_utc TIMESTAMPTZ,end_utc TIMESTAMPTZ);
        CREATE TABLE observations(station_id VARCHAR,id_scheme VARCHAR,hour_utc TIMESTAMPTZ,departures BIGINT);
        CREATE TABLE pilot_labels(station_id VARCHAR,origin_utc TIMESTAMPTZ,departures BIGINT,departure_operation_supported BOOLEAN);
        CREATE TABLE monthly_geography(station_id VARCHAR,id_scheme VARCHAR,month_local DATE,latitude DOUBLE,longitude DOUBLE);
        CREATE TABLE release_availability(month_local DATE,label_available_at_utc TIMESTAMPTZ)""")
    con.execute("""CREATE TABLE weather_classes(weather_request_class INTEGER,dataset_build_id VARCHAR,
        forecast_horizon INTEGER,forecast_origin_utc TIMESTAMP,target_hour_utc TIMESTAMP,
        selected_model_run_utc TIMESTAMP,selected_run_available_utc TIMESTAMP,
        is_fallback_weather_run BOOLEAN,weather_forecast_has_missing BOOLEAN,"""
        + ",".join(col+" DOUBLE" for col in WEATHER_VALUES) + ")")
    con.execute("""CREATE TABLE subway_alert_features(station_id VARCHAR,forecast_origin_utc TIMESTAMP,
        latest_publication_time_utc TIMESTAMP,latest_publication_age_minutes DOUBLE,
        archive_silence_last_3h BOOLEAN,archive_publications_last_3h BIGINT,
        has_recent_route_publication BOOLEAN,has_ambiguous_shuttle_link INTEGER,
        source_transfer_build_id VARCHAR,""" + ",".join(col+" BIGINT" for col in EVENT_VALUES) + ")")
    yield con
    con.close()


def populate(con, origin):
    con.execute("INSERT INTO splits VALUES ('train',?,?)", [origin,origin+timedelta(hours=3)])
    con.execute("INSERT INTO observations VALUES ('1234.50','short_name',?,7)", [origin])
    con.execute("INSERT INTO pilot_labels VALUES ('1234.50',?,0,true)", [origin+timedelta(hours=1)])
    con.execute("INSERT INTO release_availability VALUES (?,?)", [origin.date().replace(day=1),origin+timedelta(days=60)])
    con.execute("INSERT INTO monthly_geography VALUES ('1234.50','short_name',?,40.75,-74)", [origin.date().replace(day=1)])
    for source_offset, temperature in [(-1,42),(0,999)]:
        for horizon in [1,2,3]:
            src=origin+timedelta(hours=source_offset)
            values=[1,'build',horizon,src,src+timedelta(hours=horizon),origin-timedelta(hours=12),
                    origin-timedelta(hours=5),False,False,temperature,0,0,0,5]
            con.execute('INSERT INTO weather_classes VALUES ('+','.join('?' for _ in values)+')',values)
    for source_offset,count in [(-1,2),(0,99)]:
        src=origin+timedelta(hours=source_offset)
        values=['uuid-a',src,src-timedelta(minutes=10),10,False,6,True,0,'subway-build',count]+[0]*(len(EVENT_VALUES)-1)
        con.execute('INSERT INTO subway_alert_features VALUES ('+','.join('?' for _ in values)+')',values)


def test_exact_target_alignment_prior_snapshot_unknown_counts_and_supported_zero(panel_sources):
    con=panel_sources
    t=datetime(2025,5,1,12,tzinfo=UTC)
    populate(con,t)
    monthly_frames(con,t,t+timedelta(hours=1),5)
    assert con.execute("SELECT horizon,forecast_temperature_f,recent_event_count FROM panel_features ORDER BY horizon").fetchall()==[(1,42,2),(2,42,2),(3,42,2)]
    assert con.execute("SELECT departures,conditional_operation_supported FROM panel_labels ORDER BY horizon").fetchall()==[(7,False),(0,True),(None,False)]
    assert con.execute("SELECT bool_or(main_evaluation_eligible) FROM panel_labels").fetchone()==(False,)
    assert con.execute("SELECT count(*) FROM panel_features WHERE weather_eligible AND subway_eligible").fetchone()==(3,)
    assert assert_frames(con)['unsupported_zeros']==0
    # Predictor values and keys are invariant when target outcomes change.
    before=con.execute('SELECT * FROM panel_features ORDER BY horizon').fetchall()
    con.execute('UPDATE observations SET departures=100')
    monthly_frames(con,t,t+timedelta(hours=1),5)
    assert before==con.execute('SELECT * FROM panel_features ORDER BY horizon').fetchall()


def test_unmapped_and_silent_subway_never_become_zero_signals(panel_sources):
    con=panel_sources;t=datetime(2025,5,1,12,tzinfo=UTC);populate(con,t)
    con.execute('DELETE FROM subway_alert_features')
    monthly_frames(con,t,t+timedelta(hours=1),5)
    assert con.execute('SELECT bool_or(subway_eligible),count(recent_event_count) FROM panel_features').fetchone()==(False,0)


def test_purge_origins_crossing_split_end(panel_sources):
    con=panel_sources;t=datetime(2025,5,1,12,tzinfo=UTC);populate(con,t)
    monthly_frames(con,t,t+timedelta(hours=3),5)
    # Only the first origin fits all three complete target intervals.
    assert con.execute('SELECT count(*),count(DISTINCT origin_utc) FROM panel_features').fetchone()==(3,1)


def test_repeated_fall_hour_cannot_acquire_a_count(panel_sources):
    con=panel_sources;t=datetime(2025,11,2,5,tzinfo=UTC);populate(con,t)
    monthly_frames(con,t,t+timedelta(hours=1),5)
    assert con.execute('SELECT departures FROM panel_labels ORDER BY horizon').fetchall()==[(None,),(None,),(None,)]


def test_join_checks_reject_duplicate_keys_and_invented_zeros(panel_sources):
    con=panel_sources;t=datetime(2025,5,1,12,tzinfo=UTC);populate(con,t)
    monthly_frames(con,t,t+timedelta(hours=1),5)
    con.execute('UPDATE panel_labels SET departures=0 WHERE horizon=3')
    with pytest.raises(ValueError,match='unsupported-zero'):
        assert_frames(con)
    con.execute('UPDATE panel_labels SET departures=NULL WHERE horizon=3')
    con.execute('INSERT INTO panel_features SELECT * FROM panel_features LIMIT 1')
    with pytest.raises(ValueError,match='keys or row counts'):
        assert_frames(con)


def test_original_integration_rejects_weather_for_adjacent_label_hour():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE features(spatial_weather_covered BOOLEAN,target_hour_utc TIMESTAMP,origin_utc TIMESTAMP)")
        con.execute("INSERT INTO features VALUES (true,'2026-04-08 01:00','2026-04-08 00:00')")
        with pytest.raises(ValueError,match='target hour'):
            assert_weather_alignment(con)
        con.execute('UPDATE features SET target_hour_utc=origin_utc')
        assert_weather_alignment(con)


def test_manifest_paths_cannot_escape_package(tmp_path):
    with pytest.raises(ValueError,match='escapes'):
        checked_path(tmp_path,'../outside.parquet')


def test_transfer_checks_hash_before_attempting_parquet_read(tmp_path):
    p=tmp_path/'tampered.parquet';p.write_text('changed bytes')
    (tmp_path/'transfer_manifest.json').write_text(json.dumps({
        'datasets':{name:{} for name in DATASETS},
        'files':[{'path':p.name,'size_bytes':p.stat().st_size,'sha256':'0'*64}]}))
    with duckdb.connect() as con, pytest.raises(ValueError,match='checksum/size'):
        load_transfer(con,tmp_path)


def test_original_package_validator_checks_persisted_target_alignment(tmp_path):
    p=tmp_path/'features.parquet'
    with duckdb.connect() as con:
        con.execute("""COPY (SELECT true spatial_weather_covered,
            TIMESTAMP '2026-04-08 01:00' target_hour_utc,
            TIMESTAMP '2026-04-08 00:00' origin_utc) TO ? (FORMAT PARQUET)""",[str(p)])
    (tmp_path/'manifest.json').write_text(json.dumps({'status':'validated_with_coverage_gates',
        'artifacts':[{'path':p.name,'rows':1,'sha256':digest(p)}]}))
    with pytest.raises(ValueError,match='target hour'):
        validate_station_hour_integration(tmp_path)
