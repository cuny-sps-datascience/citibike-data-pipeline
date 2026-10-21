"""Timestamped GBFS geography and conservative GTFS alert-display exposure.

Raw retrieval, observed feed times, reference availability and derived publication
are distinct clocks. Counts describe matched advisories, never normal service,
actual disruptions, continuous operation or departure labels.
"""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime,timedelta,timezone
import csv,hashlib,io,json
from math import asin,cos,isfinite,radians,sin,sqrt
from pathlib import Path
import zipfile


def utc(value):
    dt=value if isinstance(value,datetime) else datetime.fromisoformat(str(value).replace('Z','+00:00'))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def epoch(value):
    if value is None:return None
    try:return datetime.fromtimestamp(float(value),timezone.utc)
    except (TypeError,ValueError):return utc(value)


def build_gtfs_reference(path: Path):
    """Use actual scheduled route/stop associations to retain shuttles/express IDs."""
    with zipfile.ZipFile(path) as archive:
        def records(name):
            return csv.DictReader(io.TextIOWrapper(archive.open(name),encoding='utf-8-sig'))
        routes={r['route_id']:{k:r[k] for k in ('agency_id','route_short_name','route_type')} for r in records('routes.txt')}
        stops={r['stop_id']:r['parent_station'] or r['stop_id'] for r in records('stops.txt')}
        if not routes or not stops:raise ValueError('Empty GTFS reference')
        trips={r['trip_id']:(r['route_id'],r.get('direction_id','')) for r in records('trips.txt')}
        parents=defaultdict(set);route_stops=defaultdict(set);direction_stops=defaultdict(set)
        for row in records('stop_times.txt'):
            if row['trip_id'] not in trips or row['stop_id'] not in stops:
                raise ValueError('GTFS stop_times foreign key mismatch')
            route,direction=trips[row['trip_id']]
            parents[route].add(stops[row['stop_id']])
            route_stops[route].add(row['stop_id'])
            if direction:direction_stops[route+'|'+direction].add(row['stop_id'])
    return {'routes':routes,'stop_parents':stops,'route_parents':{k:sorted(v) for k,v in parents.items()},
        'route_stops':{k:sorted(v) for k,v in route_stops.items()},
        'route_direction_stops':{k:sorted(v) for k,v in direction_stops.items()},
        'agency_crosswalk':{'MTASBWY':'MTA NYCT'},
        'agency_crosswalk_source':'https://www.mta.info/document/90881',
        'scope':'Regular GTFS scheduled service at any time, not actual service at a target hour'}


def resolve_selector(selector, reference):
    """Fields within a selector are ANDed; unsupported restrictions stay unresolved."""
    if selector.get('agency_id')!='MTASBWY':return set(),'non_subway_or_missing_agency'
    if selector.get('trip_restricted'):
        return set(),'unsupported_trip_restriction'
    route=selector.get('route_id');stop=selector.get('stop_id');kind=selector.get('route_type')
    direction=selector.get('direction_id')
    routes=reference['routes'];parents=reference['stop_parents']
    if route and route not in routes:return set(),'unknown_route_id'
    if stop and stop not in parents:return set(),'unknown_stop_id'
    if direction is not None and not route:return set(),'direction_without_route'
    selected=[route] if route else list(routes)
    if kind is not None:selected=[r for r in selected if int(routes[r]['route_type'])==kind]
    if direction is not None:
        key=route+'|'+str(direction)
        if key not in reference['route_direction_stops']:return set(),'unverified_route_direction'
        raw_stops=set(reference['route_direction_stops'][key]) if route in selected else set()
    else:
        raw_stops=set().union(*(set(reference['route_stops'].get(r,[])) for r in selected))
    if stop:
        raw_stops={s for s in raw_stops if parents[s]==stop} if parents[stop]==stop else raw_stops&{stop}
    matches={parents[s] for s in raw_stops}
    return matches,'resolved' if matches else 'resolved_no_scheduled_match'


def parse_alerts(raw,run_id,observed,reference):
    from google.transit import gtfs_realtime_pb2 as pb
    feed=pb.FeedMessage();feed.ParseFromString(raw)
    if not feed.IsInitialized():raise ValueError('Invalid GTFS alert message')
    if feed.header.incrementality!=pb.FeedHeader.FULL_DATASET:raise ValueError('Differential alert feeds require state reconstruction')
    observed=utc(observed);timestamp=epoch(feed.header.timestamp) if feed.header.HasField('timestamp') else None
    lag=(observed-timestamp).total_seconds() if timestamp else None
    fresh=lag is not None and -300<=lag<=900
    common={'snapshot_run_id':run_id,'observed_at_utc':observed,'feed_timestamp_utc':timestamp,
        'feed_age_seconds':lag,'feed_fresh':fresh}
    alerts=[];selectors=[];seen=set()
    for entity in feed.entity:
        if entity.is_deleted or not entity.HasField('alert'):continue
        if entity.id in seen:raise ValueError('Duplicate entity ID in alert snapshot')
        seen.add(entity.id);alert=entity.alert
        revision=hashlib.sha256(alert.SerializeToString(deterministic=True)).hexdigest()
        periods=[{'start':int(p.start) if p.HasField('start') else None,
                  'end':int(p.end) if p.HasField('end') else None} for p in alert.active_period]
        if any(p['start'] is not None and p['end'] is not None and p['start']>=p['end'] for p in periods):
            raise ValueError('Invalid alert display interval')
        text=next((t.text for t in alert.header_text.translation if t.language=='en'),None)
        item=common|{'entity_id':entity.id,'revision_sha256':revision,'periods_json':json.dumps(periods),
            'header_text':text,'planned_work':entity.id.startswith('lmm:planned_work'),
            'cause':pb.Alert.Cause.Name(alert.cause) if alert.HasField('cause') else None,
            'effect':pb.Alert.Effect.Name(alert.effect) if alert.HasField('effect') else None,
            'provider_publication_time_verified':False}
        resolved=set();subway=False;unresolved=0
        for index,s in enumerate(alert.informed_entity):
            row={'agency_id':s.agency_id or None,'route_id':s.route_id or None,'stop_id':s.stop_id or None,
                'route_type':int(s.route_type) if s.HasField('route_type') else None,
                'direction_id':int(s.direction_id) if s.HasField('direction_id') else None,
                'trip_restricted':s.HasField('trip')}
            parents,status=resolve_selector(row,reference)
            if row['agency_id']=='MTASBWY':
                subway=True
                if not status.startswith('resolved'):unresolved+=1
            selectors.append(common|row|{'entity_id':entity.id,'revision_sha256':revision,'selector_index':index,
                'resolution':status,'resolved_parent_count':len(parents)})
            resolved.update(parents)
        alerts.append(item|{'is_subway_alert':subway,'unresolved_subway_selectors':unresolved,
            '_resolved_parents':resolved,'_periods':periods})
    return alerts,selectors,common|{'entity_count':len(alerts),'feed_incrementality':'FULL_DATASET'}


def normalize_stations(raw,run_id,observed):
    payload=json.loads(raw);observed=utc(observed);feed_at=epoch(payload.get('last_updated'))
    rows=[];seen=set()
    for s in payload['data']['stations']:
        if not s.get('station_id'):raise ValueError('Missing GBFS station ID')
        identifier=str(s['station_id'])
        if not identifier or identifier in seen:raise ValueError('Missing or duplicate GBFS station ID')
        seen.add(identifier);lat,lon=float(s['lat']),float(s['lon'])
        if not (isfinite(lat) and isfinite(lon) and -90<=lat<=90 and -180<=lon<=180):
            raise ValueError('Invalid GBFS station geography')
        state={'short_name':str(s['short_name']) if s.get('short_name') else None,
            'name':s.get('name'),'latitude':lat,'longitude':lon,
            'capacity':int(s['capacity']) if s.get('capacity') is not None else None}
        rows.append({'snapshot_run_id':run_id,'station_id':identifier,'observed_at_utc':observed,
            'feed_timestamp_utc':feed_at,**state,
            'configuration_sha256':hashlib.sha256(json.dumps(state,sort_keys=True).encode()).hexdigest(),
            'station_id_scheme':'citibike_gbfs_station_id','short_name_scheme':'citibike_gbfs_short_name',
            'historical_operation_verified':False,'effective_change_time_verified':False})
    return rows,feed_at


def distance_m(a,b,c,d):
    x,y=radians(a),radians(c)
    h=sin((y-x)/2)**2+cos(x)*cos(y)*sin(radians(d-b)/2)**2
    return 12742017.6*asin(sqrt(min(1,max(0,h))))


def overlaps(periods,start,end):
    """Union of display intervals; missing bounds are open, ending is exclusive."""
    return any((p['start'] is None or p['start']<end.timestamp()) and
        (p['end'] is None or p['end']>start.timestamp()) for p in periods)


def station_alert_features(alerts,snapshot,information,points,coverage,reference,reference_available):
    """Deduplicate the alert across route/stop selectors and nearby stop points."""
    observed=snapshot['observed_at_utc'];current={s['station_id']:s for s in information}
    index=defaultdict(set);groups={}
    for row in coverage:
        key=(row['bike_station_id'],row['radius_m']);groups[key]=row
    for row in points:
        parent=reference['stop_parents'].get(row['gtfs_stop_id'])
        if parent:index[parent].add((row['bike_station_id'],row['radius_m']))
    # A new bike station must stay visible even before it has an approved mapping.
    for identifier in current:
        for radius in (250,500):groups.setdefault((identifier,radius),None)
    active=defaultdict(set);upcoming=defaultdict(set);unbounded=defaultdict(set)
    for alert in alerts:
        keys=set().union(*(index.get(p,set()) for p in alert['_resolved_parents']))
        periods=alert['_periods']
        now_active=not periods or overlaps(periods,observed,observed+timedelta(microseconds=1))
        known_future=bool(periods) and overlaps(periods,observed,observed+timedelta(hours=3))
        for key in keys:
            if now_active:active[key].add(alert['entity_id'])
            if known_future:upcoming[key].add(alert['entity_id'])
            if not periods:unbounded[key].add(alert['entity_id'])
    unresolved=sum(a['unresolved_subway_selectors'] for a in alerts)
    result=[]
    for (identifier,radius),mapping in sorted(groups.items()):
        info=current.get(identifier);compatible=False
        if info and mapping:
            compatible=(info['short_name']==mapping['bike_short_name'] and distance_m(
                info['latitude'],info['longitude'],mapping['bike_latitude'],mapping['bike_longitude'])<=25)
        info_age=(observed-info['feed_timestamp_utc']).total_seconds() if info and info['feed_timestamp_utc'] else None
        info_fresh=info_age is not None and -300<=info_age<=5400
        valid=compatible and snapshot['feed_fresh'] and info_fresh
        key=identifier,radius
        result.append({'snapshot_run_id':snapshot['snapshot_run_id'],'bike_station_id':identifier,'radius_m':radius,
            'observed_at_utc':observed,'mapping_version':mapping['mapping_version'] if mapping else None,
            'reference_available_at_utc':utc(reference_available),'feed_fresh':snapshot['feed_fresh'],
            'mapping_compatible':compatible,'gbfs_info_fresh':info_fresh,
            'matched_active_alert_count':len(active[key]) if valid else None,
            'matched_known_display_overlap_next_3h':len(upcoming[key]) if valid else None,
            'matched_alerts_without_explicit_period':len(unbounded[key]) if valid else None,
            'unresolved_subway_selectors':unresolved,
            'normal_service_verified':False,'historical_operation_verified':False,
            'interpretation':'Resolved advisory proximity at observed snapshot; not verified service impact or departure demand'})
    return result
