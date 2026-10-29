"""Prospective, versioned straight-line Citi Bike / subway proximity references."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
from math import asin, cos, isfinite, radians, sin, sqrt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .forecast_sources import digest, fetch, socrata_url, stamp

RADII = (250, 500)


def distance_m(lat1, lon1, lat2, lon2):
    a, b = radians(lat1), radians(lat2)
    h = sin((b-a)/2)**2 + cos(a)*cos(b)*sin(radians(lon2-lon1)/2)**2
    return 2*6371008.8*asin(sqrt(min(1, max(0, h))))


def coordinates(row, latitude, longitude):
    lat, lon = float(row[latitude]), float(row[longitude])
    if not (isfinite(lat) and isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 180):
        raise ValueError('Invalid reference coordinates')
    return lat, lon


def proximity_tables(bikes, subway, version, available_from):
    """Retain unmatched bikes; deduplicate complexes/routes within each radius.

    Routes come only from reference station points inside the radius, never
    from a distant member of the same complex. IDs are opaque strings.
    """
    when = datetime.fromisoformat(available_from)
    if when.tzinfo is None:
        raise ValueError('Snapshot availability must have a timezone')
    if not bikes or not subway:
        raise ValueError('Both reference sources must be nonempty')
    if len({str(b['station_id']) for b in bikes}) != len(bikes):
        raise ValueError('Duplicate GBFS station ID')
    if len({str(s['gtfs_stop_id']) for s in subway}) != len(subway):
        raise ValueError('Duplicate MTA GTFS stop ID')
    points = []
    for station in subway:
        lat, lon = coordinates(station, 'gtfs_latitude', 'gtfs_longitude')
        if not all(station.get(k) for k in ('station_id', 'complex_id', 'gtfs_stop_id')):
            raise ValueError('Missing MTA identifier')
        points.append((station, lat, lon))
    output = {k: [] for k in ('bike_stations', 'station_proximity', 'complex_proximity',
                              'route_proximity', 'coverage')}
    for bike in sorted(bikes, key=lambda b: str(b['station_id'])):
        lat, lon = coordinates(bike, 'lat', 'lon')
        if not bike['station_id']:
            raise ValueError('Missing GBFS identifier')
        identity = {'bike_station_id': str(bike['station_id']),
                    'bike_station_id_scheme': 'citibike_gbfs_station_id',
                    'bike_short_name': str(bike['short_name']) if bike.get('short_name') else None,
                    'bike_short_name_scheme': 'citibike_gbfs_short_name',
                    'bike_name': bike.get('name'), 'bike_latitude': lat, 'bike_longitude': lon,
                    'mapping_version': version, 'available_from_utc': available_from,
                    'historical_validity_verified': False,
                    'station_operation_verified': False,
                    'distance_method': 'haversine_sphere_6371008.8m',
                    'route_scope': 'MTA reference daytime_routes; not verified all-hours service'}
        output['bike_stations'].append(identity)
        candidates = sorted([(distance_m(lat, lon, slat, slon), str(s['station_id']), s)
                             for s, slat, slon in points], key=lambda r: (r[0], r[1], r[2]['gtfs_stop_id']))
        for radius in RADII:
            common = identity | {'radius_m': radius}
            nearby = [r for r in candidates if r[0] <= radius]
            complexes, routes = defaultdict(list), defaultdict(list)
            for distance, station_id, station in nearby:
                row = common | {'mta_station_id': station_id,
                    'mta_station_id_scheme': 'mta_station_id',
                    'mta_complex_id': str(station['complex_id']),
                    'gtfs_stop_id': str(station['gtfs_stop_id']),
                    'subway_name': station.get('stop_name'), 'distance_m': distance,
                    'daytime_routes': station.get('daytime_routes', '')}
                output['station_proximity'].append(row)
                complexes[row['mta_complex_id']].append(row)
                for route in sorted(set(row['daytime_routes'].split())):
                    routes[route].append(row)
            for complex_id, members in sorted(complexes.items()):
                nearest = members[0]
                output['complex_proximity'].append(common | {'mta_complex_id': complex_id,
                    'nearest_mta_station_id': nearest['mta_station_id'],
                    'nearest_gtfs_stop_id': nearest['gtfs_stop_id'],
                    'distance_m': nearest['distance_m'], 'nearby_station_points': len(members)})
            for route, members in sorted(routes.items()):
                nearest = members[0]
                output['route_proximity'].append(common | {'route_id': route,
                    'route_id_scheme': 'mta_subway_daytime_route',
                    'distance_m': nearest['distance_m'],
                    'nearest_mta_station_id': nearest['mta_station_id'],
                    'nearest_gtfs_stop_id': nearest['gtfs_stop_id'],
                    'nearest_mta_complex_id': nearest['mta_complex_id'],
                    'nearby_complexes': len({r['mta_complex_id'] for r in members})})
            output['coverage'].append(common | {'nearby_station_points': len(nearby),
                'nearby_complexes': len(complexes), 'nearby_routes': len(routes),
                'nearest_subway_distance_m': candidates[0][0],
                'nearest_mta_station_id': candidates[0][1]})
    return output


def build_mapping(output_root: Path):
    version = stamp()
    root = output_root/version
    root.mkdir(parents=True, exist_ok=False)
    artifacts = []

    def get(name, url):
        metadata = fetch(url, root/'raw'/(name+'.json'))
        metadata['path'] = str(Path(metadata['path']).relative_to(root))
        artifacts.append(metadata | {'name': name})
        return json.loads((root/metadata['path']).read_text())

    discovery = get('gbfs_discovery', 'https://gbfs.citibikenyc.com/gbfs/gbfs.json')
    url = next(f['url'] for f in discovery['data']['en']['feeds'] if f['name']=='station_information')
    feed = get('gbfs_station_information', url)
    metadata = get('mta_metadata', 'https://data.ny.gov/api/views/39hk-dx4f.json')
    count = int(get('mta_expected_count', socrata_url('39hk-dx4f', **{'$select':'count(*) as rows'}))[0]['rows'])
    subway = []
    while len(subway) < count:
        page = get('mta_stations_'+str(len(subway)), socrata_url('39hk-dx4f', **{
            '$limit': 1000, '$offset': len(subway), '$order': 'gtfs_stop_id'}))
        if not page:
            raise ValueError('MTA pagination ended before expected count')
        subway.extend(page)
    after = int(get('mta_final_count', socrata_url('39hk-dx4f', **{'$select':'count(*) as rows'}))[0]['rows'])
    final_metadata = get('mta_metadata_after', 'https://data.ny.gov/api/views/39hk-dx4f.json')
    if len(subway) != count or after != count or metadata.get('rowsUpdatedAt') != final_metadata.get('rowsUpdatedAt'):
        raise ValueError('MTA source changed during retrieval')
    available = max(a['retrieved_at_utc'] for a in artifacts)
    tables = proximity_tables(feed['data']['stations'], subway, version, available)
    for name, rows in tables.items():
        if not rows:
            raise ValueError('Unexpected empty mapping table: '+name)
        path = root/(name+'.parquet')
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, path, compression='zstd')
        # UTF-8 strings, booleans and full precision numeric distances survive export.
        with (root/(name+'.csv')).open('w') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=table.column_names)
            writer.writeheader();writer.writerows(rows)
        for file in (path, root/(name+'.csv')):
            artifacts.append({'name':name, 'path':file.name, 'rows':len(rows),
                'bytes':file.stat().st_size, 'sha256':digest(file)})
    summary = {str(r): {'bike_stations':len(tables['bike_stations']),
        'with_subway_station':sum(x['nearby_station_points']>0 for x in tables['coverage'] if x['radius_m']==r),
        'with_daytime_route':sum(x['nearby_routes']>0 for x in tables['coverage'] if x['radius_m']==r)} for r in RADII}
    manifest = {'schema_version':'station-subway-proximity-v1', 'status':'built',
        'mapping_version':version, 'built_at_utc':datetime.now(timezone.utc).isoformat(),
        'available_from_utc':available, 'gbfs_feed_last_updated':feed['last_updated'],
        'mta_rows_updated_at':metadata.get('rowsUpdatedAt'), 'mta_station_points':len(subway),
        'radii_m':list(RADII), 'coverage':summary, 'artifacts':artifacts,
        'historical_validity_verified':False, 'station_operation_verified':False,
        'limitations':['Prospective reference; do not backdate to historical training origins.',
            'Straight-line distance to station reference points, not entrance/walking distance.',
            'Daytime reference routes do not prove all-hours or disruption-specific service.',
            'No departure labels, operation exposure, alert effects or model retraining.']}
    (root/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    (output_root/'CURRENT_MAPPING').write_text(str(root)+'\n')
    return root
