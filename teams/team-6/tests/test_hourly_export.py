from datetime import date, datetime, timezone

import duckdb
import pytest

from citibike_pipeline.hourly_export import aggregate_hourly, eligible_hours


def test_hourly_counts_keep_published_departures_and_exclude_unassignable_dst():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE source_releases AS SELECT 'source' AS source_key,'2025-11' AS nominal_release")
        con.execute('''CREATE TABLE trips AS SELECT 'source' AS source_key,'modern' AS schema_family,
            '5997.10' AS start_station_id,* FROM (VALUES
            (TIMESTAMP '2025-11-01 08:00:00',true,[]::VARCHAR[]),
            (TIMESTAMP '2025-11-01 08:59:59',true,[]::VARCHAR[]),
            (TIMESTAMP '2025-11-01 09:00:00',true,[]::VARCHAR[]),
            (TIMESTAMP '2025-11-02 01:30:00',true,['start_dst_ambiguous']),
            (TIMESTAMP '2025-11-02 09:00:00',false,[]::VARCHAR[])
            ) v(started_at_local,eligible_departure,quality_flags)''')
        aggregate_hourly(con,date(2025,11,1),date(2025,12,1))
        result=con.execute('SELECT hour_utc,departures,main_evaluation_eligible FROM hourly_observed ORDER BY 1').fetchall()
        assert result==[(datetime(2025,11,1,12,tzinfo=timezone.utc),2,False),
                        (datetime(2025,11,1,13,tzinfo=timezone.utc),1,False)]
        assert con.execute('SELECT count(*) FROM hourly_observed WHERE departures=0').fetchone()[0]==0


def prepare_operation(con):
    con.execute('''CREATE TABLE operation_intervals(station_id VARCHAR,id_scheme VARCHAR,
        start_utc TIMESTAMPTZ,end_utc TIMESTAMPTZ,operation_evidence VARCHAR,
        source_coverage_complete BOOLEAN)''')
    con.execute('''CREATE TABLE hourly_observed(station_id VARCHAR,id_scheme VARCHAR,
        hour_utc TIMESTAMPTZ,departures BIGINT)''')


def test_zeros_require_full_operating_hour_and_source_coverage():
    with duckdb.connect() as con:
        prepare_operation(con)
        con.execute("""INSERT INTO operation_intervals VALUES
            ('A','short_name','2025-03-01 08:15:00+00','2025-03-01 10:30:00+00','class snapshots',true),
            ('B','short_name','2025-03-01 08:00:00+00','2025-03-01 11:00:00+00','class snapshots',false)""")
        eligible_hours(con)
        assert con.execute('SELECT station_id,hour_utc,departures,main_evaluation_eligible FROM supported_hourly').fetchall()==[
            ('A',datetime(2025,3,1,9,tzinfo=timezone.utc),0,True)]


def test_overlapping_operation_intervals_fail_instead_of_multiplying_counts():
    with duckdb.connect() as con:
        prepare_operation(con)
        con.execute("""INSERT INTO operation_intervals VALUES
            ('A','short_name','2025-03-01 08:00:00+00','2025-03-01 10:00:00+00','evidence',true),
            ('A','short_name','2025-03-01 09:00:00+00','2025-03-01 11:00:00+00','evidence',true)""")
        with pytest.raises(ValueError,match='overlapping'):
            eligible_hours(con)


def test_blank_operation_evidence_is_rejected():
    with duckdb.connect() as con:
        prepare_operation(con)
        con.execute("""INSERT INTO operation_intervals VALUES
            ('A','short_name','2025-03-01 08:00:00+00','2025-03-01 10:00:00+00','',true)""")
        with pytest.raises(ValueError,match='Invalid'):
            eligible_hours(con)
