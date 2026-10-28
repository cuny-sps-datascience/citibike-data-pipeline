from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from citibike_pipeline.departure_preview import (
    active_days, assumed_label, publication_versions_asof, released_profile, weather_rows,
)

UTC=timezone.utc


def test_assumed_24h_zeros_need_complete_files_and_stay_inside_active_days():
    start,end=active_days([datetime(2026,6,2,16,tzinfo=UTC),datetime(2026,6,4,20,tzinfo=UTC)])
    quiet=datetime(2026,6,3,5,tzinfo=UTC)
    assert assumed_label(None,quiet,start,end,True)==(0,'assumed_24h_zero')
    assert assumed_label(None,quiet,start,end,False)==(None,'incomplete_trip_files')
    assert assumed_label(None,start-timedelta(hours=1),start,end,True)==(None,'outside_inferred_active_period')
    assert assumed_label(None,end,start,end,True)==(None,'outside_inferred_active_period')
    assert assumed_label(7,quiet,start,end,True)==(7,'observed_trips')


def test_repeated_fall_hour_is_not_an_assumed_zero():
    start=datetime(2025,11,1,tzinfo=UTC);end=start+timedelta(days=3)
    for hour in [5,6]:
        assert assumed_label(None,datetime(2025,11,2,hour,tzinfo=UTC),start,end,True)==(None,'ambiguous_trip_hour')


def test_profile_ignores_later_releases_in_counts_and_active_period_inference():
    cutoff=datetime(2026,6,15,13,55,tzinfo=UTC)
    target=datetime(2026,6,15,14,tzinfo=UTC)
    rows=[{'hour_utc':datetime(2026,4,6,14,tzinfo=UTC),'nominal_release':'2026-04','departures':8},
          {'hour_utc':datetime(2026,4,30,20,tzinfo=UTC),'nominal_release':'2026-04','departures':4}]
    profile=released_profile(rows,cutoff,target,{'2026-03','2026-04','2026-05'})
    future=rows+[{'hour_utc':datetime(2026,3,2,15,tzinfo=UTC),'nominal_release':'2026-05','departures':999},
                 {'hour_utc':datetime(2026,6,15,14,tzinfo=UTC),'nominal_release':'2026-06','departures':999}]
    assert released_profile(future,cutoff,target,{'2026-03','2026-04','2026-05'})==profile
    assert profile['profile_departures']==8 and profile['profile_hours']==4
    assert profile['released_profile_mean']==2
    assert profile['profile_latest_input_available_utc']<cutoff


def test_release_exactly_at_cutoff_is_not_used():
    cutoff=datetime(2026,6,14,tzinfo=UTC)
    r=released_profile([{'hour_utc':datetime(2026,4,6,14,tzinfo=UTC),'nominal_release':'2026-04','departures':8}],cutoff,cutoff,{'2026-04'})
    assert r['released_profile_mean'] is None


def test_precipitation_end_stamp_and_instant_start_stamp_cover_same_departure_hour():
    origin=datetime(2026,6,15,14,tzinfo=UTC);cutoff=origin-timedelta(minutes=5)
    with duckdb.connect() as con:
        con.execute("SET TimeZone='UTC'")
        con.execute('''CREATE TABLE weather_classes(weather_request_class INTEGER,dataset_build_id VARCHAR,
            forecast_origin_utc TIMESTAMPTZ,forecast_horizon INTEGER,valid_time_utc TIMESTAMPTZ,
            forecast_temperature_f DOUBLE,forecast_wind_speed_mph DOUBLE,forecast_precipitation_in DOUBLE,
            forecast_rain_in DOUBLE,forecast_snowfall_in DOUBLE,selected_model_run_utc TIMESTAMPTZ,
            selected_run_available_utc TIMESTAMPTZ)''')
        for h in [1,2,3,4]:
            con.execute('INSERT INTO weather_classes VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                [1,'build',origin-timedelta(hours=1),h,origin+timedelta(hours=h-1),60+h,5,h/10,h/10,0,
                 origin-timedelta(hours=12),origin-timedelta(hours=5)])
        result=weather_rows(con,1,'build',origin,cutoff)
        assert [r['forecast_precipitation_in'] for r in result]==[.2,.3,.4]
        assert [r['forecast_temperature_f'] for r in result]==[61,62,63]
        assert all(r['accumulation_valid_time_utc']==r['target_end_utc'] for r in result)
        assert all(r['instant_valid_time_utc']==r['target_start_utc'] for r in result)
        con.execute('UPDATE weather_classes SET selected_run_available_utc=?',[cutoff])
        with pytest.raises(ValueError,match='unavailable'):
            weather_rows(con,1,'build',origin,cutoff)


def test_0940_alert_is_eligible_for_1000_origin_without_0957_or_later_updates():
    def dt(hour,minute):return datetime(2026,6,15,hour,minute,tzinfo=UTC)
    rows=[{'event_id':'a','publication_utc':dt(9,40),'status':'delay'},
          {'event_id':'a','publication_utc':dt(10,5),'status':'cleared'},
          {'event_id':'b','publication_utc':dt(9,57),'status':'delay'},
          {'event_id':'c','publication_utc':dt(9,55),'status':'delay'}]
    assert publication_versions_asof(rows,dt(9,0))=={}
    selected=publication_versions_asof(rows,dt(9,55))
    assert list(selected)==['a'] and selected['a']['status']=='delay'
    assert publication_versions_asof(rows,dt(9,55),ingestion_delay_minutes=20)=={}
