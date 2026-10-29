import json
from datetime import datetime, timezone

import duckdb
import pytest

from citibike_pipeline.station_hour_integration import (
    create_flow_tables,
    dated_crosswalk,
    split_for,
)


PROTOCOL = {
    "train_target_local": ["2026-04-08", "2026-05-01"],
    "validation_target_local": ["2026-06-16", "2026-07-01"],
    "refit_target_local": ["2026-04-08", "2026-07-01"],
    "test_target_local": ["2026-09-01", "2026-10-01"],
}


def test_split_uses_new_york_dates_and_tracks_refit_separately():
    assert split_for(datetime(2026, 4, 8, 4, tzinfo=timezone.utc), PROTOCOL) == (
        "train",
        True,
    )
    assert split_for(datetime(2026, 6, 16, 4, tzinfo=timezone.utc), PROTOCOL) == (
        "validation",
        True,
    )
    assert split_for(datetime(2026, 9, 1, 4, tzinfo=timezone.utc), PROTOCOL) == (
        "test",
        False,
    )
    assert split_for(datetime(2026, 8, 1, 4, tzinfo=timezone.utc), PROTOCOL) == (
        "excluded",
        False,
    )
    with pytest.raises(ValueError, match="timezone"):
        split_for(datetime(2026, 4, 8), PROTOCOL)


def test_dated_crosswalk_preserves_uuid_short_name_and_availability(tmp_path):
    cohort = tmp_path / "cohort.json"
    cohort.write_text(json.dumps([{"uuid": "uuid-a", "station_id": "1234.56"}]))
    root = tmp_path / "station_information"
    root.mkdir()
    (root / "2026-04-07.json").write_text(
        json.dumps(
            {
                "last_updated": 1775520000,
                "data": {
                    "stations": [
                        {
                            "station_id": "uuid-a",
                            "short_name": "1234.56",
                            "name": "A",
                            "lat": 40.7,
                            "lon": -74.0,
                            "capacity": 20,
                        }
                    ]
                },
            }
        )
    )
    rows = dated_crosswalk(cohort, root)
    assert [(row["station_uuid"], row["station_id"]) for row in rows] == [
        ("uuid-a", "1234.56")
    ]
    assert rows[0]["available_at_utc"].isoformat() == "2026-04-08T01:00:00+00:00"


def test_hourly_flows_use_start_for_departures_end_for_returns_and_dst_flags():
    with duckdb.connect() as con:
        con.execute("SET TimeZone='UTC'")
        con.execute(
            """CREATE TABLE trips(
                schema_family VARCHAR,start_station_id VARCHAR,end_station_id VARCHAR,
                started_at_local TIMESTAMP,ended_at_local TIMESTAMP,
                eligible_departure BOOLEAN,eligible_arrival BOOLEAN,quality_flags VARCHAR[])"""
        )
        con.execute(
            """INSERT INTO trips VALUES
            ('modern','A','B','2026-04-08 00:10:00','2026-04-08 01:05:00',true,true,[]),
            ('modern','A','B','2026-04-08 00:20:00','2026-04-08 00:50:00',true,true,[]),
            ('modern','A','B','2026-04-08 00:30:00','2026-04-08 00:55:00',true,true,['start_dst_ambiguous']),
            ('modern','A','B','2026-04-08 00:40:00','2026-04-08 00:58:00',true,true,['end_dst_ambiguous']),
            ('legacy','A','B','2026-04-08 00:45:00','2026-04-08 00:59:00',true,true,[])"""
        )
        create_flow_tables(
            con,
            "trips",
            datetime(2026, 4, 8, 4, tzinfo=timezone.utc),
            datetime(2026, 4, 8, 5, tzinfo=timezone.utc),
        )
        assert con.execute(
            "SELECT station_id,origin_utc,departure_count FROM departures ORDER BY 2"
        ).fetchall() == [("A", datetime(2026, 4, 8, 4, tzinfo=timezone.utc), 3)]
        assert con.execute(
            "SELECT station_id,origin_utc,return_count FROM returns ORDER BY 2"
        ).fetchall() == [
            ("B", datetime(2026, 4, 8, 4, tzinfo=timezone.utc), 2),
            ("B", datetime(2026, 4, 8, 5, tzinfo=timezone.utc), 1),
        ]
