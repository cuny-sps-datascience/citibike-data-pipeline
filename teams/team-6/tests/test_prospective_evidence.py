from datetime import datetime,timedelta,timezone
import json
from google.transit import gtfs_realtime_pb2 as pb
import pytest
from citibike_pipeline.prospective_evidence import normalize_stations,parse_alerts,resolve_selector,station_alert_features,overlaps

NOW=datetime(2026,10,16,16,tzinfo=timezone.utc)
REF={'routes':{'A':{'route_type':'1'},'GS':{'route_type':'1'},'6X':{'route_type':'1'}},
     'route_parents':{'A':['A01'],'GS':['901'],'6X':['601']},
     'stop_parents':{'A01':'A01','A01N':'A01','A01S':'A01','B01':'B01','901':'901','601':'601'},
     'route_stops':{'A':['A01N','A01S'],'GS':['901'],'6X':['601']},
     'route_direction_stops':{'A|0':['A01N'],'A|1':['A01S']}}

def selector(**kwargs):return {'agency_id':'MTASBWY','route_id':None,'stop_id':None,'route_type':None,'direction_id':None,'trip_restricted':False}|kwargs

def test_subway_namespace_excludes_bus_route_collisions_and_conjoins_fields():
    assert resolve_selector(selector(route_id='A'),REF)==({'A01'},'resolved')
    assert not resolve_selector(selector(agency_id='MTA NYCT',route_id='A'),REF)[0]
    assert not resolve_selector(selector(route_id='A',stop_id='B01'),REF)[0]
    assert resolve_selector(selector(route_id='A',stop_id='A01N'),REF)[0]=={'A01'}
    assert resolve_selector(selector(route_id='A',direction_id=0),REF)[0]=={'A01'}
    assert not resolve_selector(selector(route_id='A',stop_id='A01S',direction_id=0),REF)[0]
    assert not resolve_selector(selector(route_id='A',trip_restricted=True),REF)[0]

def test_shuttle_and_express_keep_separate_gtfs_ids():
    assert resolve_selector(selector(route_id='GS'),REF)[0]=={'901'}
    assert resolve_selector(selector(route_id='6X'),REF)[0]=={'601'}
    assert resolve_selector(selector(route_id='S'),REF)[1]=='unknown_route_id'

def feed():
    f=pb.FeedMessage();f.header.gtfs_realtime_version='2.0';f.header.timestamp=int(NOW.timestamp())
    e=f.entity.add();e.id='lmm:alert:1';a=e.alert
    for route,stop in [('A',None),(None,'A01N')]:
        s=a.informed_entity.add();s.agency_id='MTASBWY'
        if route:s.route_id=route
        if stop:s.stop_id=stop
    return f

def information(lon=-74):
    raw=json.dumps({'last_updated':int(NOW.timestamp()),'data':{'stations':[
        dict(station_id='uuid',short_name='0123.00',name='Station',lat=40,lon=lon,capacity=20)]}}).encode()
    return normalize_stations(raw,'snapshot',NOW)[0]

def features(f,info=None):
    alerts,selectors,meta=parse_alerts(f.SerializeToString(),'snapshot',NOW,REF)
    cov=[dict(bike_station_id='uuid',bike_short_name='0123.00',bike_latitude=40,bike_longitude=-74,
              radius_m=r,mapping_version='v1') for r in (250,500)]
    pts=[dict(bike_station_id='uuid',radius_m=r,gtfs_stop_id='A01') for r in (250,500)]
    return station_alert_features(alerts,meta,information() if info is None else info,pts,cov,REF,NOW)

def test_counts_deduplicate_across_stop_and_route_selectors():
    rows=features(feed())
    assert all(r['matched_active_alert_count']==1 for r in rows)
    assert all(r['matched_known_display_overlap_next_3h']==0 for r in rows)
    assert all(r['matched_alerts_without_explicit_period']==1 for r in rows)
    assert not rows[0]['normal_service_verified']

def test_old_feeds_or_relocated_bikes_yield_missing_counts():
    f=feed();f.header.timestamp=int((NOW-timedelta(hours=2)).timestamp())
    assert features(f)[0]['matched_active_alert_count'] is None
    assert features(feed(),information(-73))[0]['matched_active_alert_count'] is None
    assert features(feed(),[])[0]['matched_active_alert_count'] is None

def test_future_display_period_is_not_currently_active():
    f=feed();p=f.entity[0].alert.active_period.add()
    p.start=int((NOW+timedelta(hours=1)).timestamp());p.end=int((NOW+timedelta(hours=2)).timestamp())
    row=features(f)[0]
    assert row['matched_active_alert_count']==0 and row['matched_known_display_overlap_next_3h']==1

def test_open_intervals_and_exclusive_end():
    assert overlaps([dict(start=None,end=None)],NOW,NOW+timedelta(hours=1))
    assert not overlaps([dict(start=None,end=int(NOW.timestamp()))],NOW,NOW+timedelta(hours=1))

def test_station_state_hash_preserves_ids_and_tracks_name_or_location():
    row=information()[0]
    assert row['short_name']=='0123.00' and not row['historical_operation_verified']
    assert row['configuration_sha256']!=information(-73)[0]['configuration_sha256']

def test_differential_feeds_rejected():
    f=feed();f.header.incrementality=pb.FeedHeader.DIFFERENTIAL
    with pytest.raises(ValueError,match='Differential'):parse_alerts(f.SerializeToString(),'run',NOW,REF)
