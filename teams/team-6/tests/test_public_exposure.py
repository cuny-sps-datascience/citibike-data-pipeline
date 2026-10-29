from datetime import datetime, timezone

import duckdb

import json
import pyarrow as pa
import pyarrow.parquet as pq

from citibike_pipeline.public_exposure import bracket_hours, monthly_crosswalk, normalize_status


def test_short_active_brackets_support_whole_hours_but_not_gaps_or_closed_points():
    with duckdb.connect() as con:
        con.execute("SET TimeZone='UTC'")
        con.execute('''CREATE TABLE status AS
            SELECT 'A' AS station_id,t AS ts,true AS active,true AS fresh,true AS stable
            FROM generate_series(TIMESTAMPTZ '2025-03-01 08:00:00+00',
                TIMESTAMPTZ '2025-03-01 11:00:00+00',INTERVAL '5 minutes') v(t)''')
        con.execute("DELETE FROM status WHERE ts='2025-03-01 09:30:00+00'")
        con.execute("UPDATE status SET active=false WHERE ts='2025-03-01 10:30:00+00'")
        bracket_hours(con,600)
        assert con.execute('SELECT hour_utc FROM bracket_hours ORDER BY 1').fetchall()==[
            (datetime(2025,3,1,8,tzinfo=timezone.utc),),
            (datetime(2025,3,1,9,tzinfo=timezone.utc),)]
        bracket_hours(con,300)
        assert con.execute('SELECT count(*) FROM bracket_hours').fetchone()[0]==1


def test_large_irregular_interval_set_has_exact_grid_count():
    with duckdb.connect() as con:
        con.execute("SET TimeZone='UTC'")
        con.execute('''CREATE TABLE status AS SELECT station_id::VARCHAR AS station_id,
            TIMESTAMPTZ '2025-03-01 08:00:00+00' + station_id*INTERVAL '1 second'
                + t*INTERVAL '5 minutes' AS ts,true AS active,true AS fresh,true AS stable
            FROM range(1,301) s(station_id) CROSS JOIN range(49) v(t)''')
        bracket_hours(con)
        # Each station starts after 08:00 and ends after 12:00: 09,10,11 fully covered.
        assert con.execute('SELECT count(*) FROM bracket_hours').fetchone()[0]==900
        assert con.execute('SELECT count(*) FROM (SELECT station_id,hour_utc,count(*) AS n FROM bracket_hours GROUP BY ALL HAVING n>1)').fetchone()[0]==0


def test_stale_or_moved_points_break_exposure_and_empty_input_is_safe():
    with duckdb.connect() as con:
        con.execute('''CREATE TABLE status(station_id VARCHAR,ts TIMESTAMPTZ,
            active BOOLEAN,fresh BOOLEAN,stable BOOLEAN)''')
        bracket_hours(con)
        assert con.execute('SELECT count(*) FROM bracket_hours').fetchone()[0]==0
        con.execute('''INSERT INTO status SELECT 'A',t,true,true,true
            FROM generate_series(TIMESTAMPTZ '2025-03-01 08:00:00+00',
                TIMESTAMPTZ '2025-03-01 10:00:00+00',INTERVAL '5 minutes') v(t)''')
        con.execute("UPDATE status SET fresh=false WHERE ts='2025-03-01 08:30:00+00'")
        con.execute("UPDATE status SET stable=false WHERE ts='2025-03-01 09:30:00+00'")
        bracket_hours(con)
        assert con.execute('SELECT count(*) FROM bracket_hours').fetchone()[0]==0


def test_crosswalk_rejects_ambiguous_and_many_to_one_identity():
    with duckdb.connect() as con:
        con.execute('''CREATE TABLE raw_status AS SELECT * FROM (VALUES
            ('a','A',40.75,-73.98),('b','B',40.76,-73.98),('c','C',40.76,-73.98),
            ('d','D',40.77,-73.98)) v(nuid,name,latitude,longitude)''')
        con.execute('''CREATE TABLE geography AS SELECT *, 'short_name' AS id_scheme,'Manhattan' AS borough
            FROM (VALUES ('1',40.75,-73.98),('2',40.7501,-73.98),
                ('3',40.76,-73.98),('4',40.77,-73.98)) v(station_id,latitude,longitude)''')
        monthly_crosswalk(con)
        assert con.execute('SELECT nuid,station_id FROM crosswalk').fetchall()==[('d','4')]


def test_epoch_and_iso_station_report_times_have_same_age_and_invalid_stays_missing(tmp_path):
    timestamp=datetime(2026,2,1,12,0)
    reporting=datetime(2026,2,1,11,58,tzinfo=timezone.utc)
    rows=[{'nuid':str(i),'name':'A','latitude':40.75,'longitude':-73.98,'timestamp':timestamp,
           'extra':json.dumps({'renting':1,'last_updated':value})}
          for i,value in enumerate([int(reporting.timestamp()),'2026-02-01T11:58:00',
                                     '2026-02-01T11:58:00Z','invalid'])]
    path=tmp_path/'sample.parquet';pq.write_table(pa.Table.from_pylist(rows),path)
    with duckdb.connect() as con:
        con.execute("SET TimeZone='UTC'");normalize_status(con,path)
        assert con.execute('SELECT age_seconds FROM raw_status ORDER BY nuid').fetchall()==[(120.,),(120.,),(120.,),(None,)]
