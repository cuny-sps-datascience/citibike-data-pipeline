from math import nan
import pytest
from citibike_pipeline.station_subway import distance_m, proximity_tables

def station(identifier, lon, routes='A C', complex_id='001'):
    return dict(station_id=identifier, complex_id=complex_id, gtfs_stop_id='00'+identifier,
                gtfs_latitude='40', gtfs_longitude=str(lon), daytime_routes=routes)

def mapping(subway=None, bikes=None):
    return proximity_tables(bikes or [dict(station_id='uuid-01',short_name='0123.00',lat=40,lon=-74)],
        subway or [station('01',-74),station('02',-74.001),station('03',-73.996,'Z')],
        'snapshot', '2026-10-16T14:00:00+00:00')

def test_distances():
    assert distance_m(40,-74,40,-74)==0
    assert distance_m(0,0,0,1)==pytest.approx(111195.08,abs=.01)

def test_radius_deduplication_and_distant_complex_routes():
    result=mapping()
    small=[r for r in result['route_proximity'] if r['radius_m']==250]
    large=[r for r in result['route_proximity'] if r['radius_m']==500]
    assert {r['route_id'] for r in small}=={'A','C'}
    assert {r['route_id'] for r in large}=={'A','C','Z'}
    assert all(r['nearby_complexes']==1 for r in small)
    assert len(result['complex_proximity'])==2
    assert all(r['distance_m']<=r['radius_m'] for r in result['station_proximity'])
    assert small[0]['bike_short_name']=='0123.00'
    assert not small[0]['historical_validity_verified']

def test_unmatched_and_routeless_stations_retained():
    result=mapping([station('01',-74,'')], [dict(station_id='NJ',lat=40,lon=-75)])
    assert len(result['coverage'])==2
    assert all(r['nearby_station_points']==0 for r in result['coverage'])
    result=mapping([station('01',-74,'')])
    assert result['station_proximity'] and not result['route_proximity']

def test_multiple_lines_share_mta_station_id_without_losing_routes():
    one=station('01',-74,'A');two=station('01',-74,'B')
    two['gtfs_stop_id']='D13'
    result=mapping([one,two])
    assert {r['route_id'] for r in result['route_proximity']}=={'A','B'}
    assert len(result['station_proximity'])==4

def test_bad_reference_fails_instead_of_silently_dropping():
    with pytest.raises(ValueError,match='Duplicate'):
        mapping([station('01',-74),station('01',-74)])
    with pytest.raises(ValueError,match='coordinates'):
        mapping(bikes=[dict(station_id='bad',lat=nan,lon=-74)])
