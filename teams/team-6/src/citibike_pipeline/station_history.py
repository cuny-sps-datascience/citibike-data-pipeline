"""Conservative historical station evidence and dated GTFS advisory matching.

These helpers create a separate, assumption-qualified pilot. They never promote
the canonical observed station export or interpret empty inventory as zero trips.
"""
from __future__ import annotations
from collections import Counter, defaultdict
import csv, io, json, zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from .prospective_evidence import distance_m, resolve_selector

UTC=timezone.utc
HOUR=timedelta(hours=1)


def information(path: Path, object_day: date):
    data=json.loads(path.read_text())
    timestamp=datetime.fromtimestamp(int(data['last_updated']),UTC)
    available=max(timestamp,datetime.combine(object_day+timedelta(days=1),datetime.min.time(),UTC)+HOUR)
    stations=data['data']['stations']
    uuids=Counter(str(s.get('station_id') or '') for s in stations)
    shorts=Counter(str(s.get('short_name') or '') for s in stations)
    result={}
    for s in stations:
        uuid=str(s.get('station_id') or '');short=str(s.get('short_name') or '')
        if not uuid or not short or uuids[uuid]!=1 or shorts[short]!=1:continue
        lat=float(s['lat']);lon=float(s['lon'])
        if not -90<=lat<=90 or not -180<=lon<=180:continue
        result[uuid]={'uuid':uuid,'station_id':short,'latitude':lat,'longitude':lon,
            'capacity':int(s.get('capacity',0)), 'name':s.get('name'),
            'provider_at_utc':timestamp,'assumed_available_at_utc':available,
            'object_date':object_day.isoformat()}
    return result


def stable(a,b,max_distance=25):
    return bool(a and b and a['uuid']==b['uuid'] and a['station_id']==b['station_id']
        and distance_m(a['latitude'],a['longitude'],b['latitude'],b['longitude'])<=max_distance)


def snapshot_good(row):
    """No inventory-activity requirement: empty/quiet renting stations are retained."""
    return (row['is_installed']==1 and row['is_renting']==1
        and -300<=row['polled_at']-row['ts']<=120
        and -300<=row['polled_at']-row['last_reported']<=600)


def supported_hours(rows, max_gap_seconds=120):
    """Full hours inside consecutive fresh, renting poll chains; no extrapolation."""
    ordered=sorted(rows,key=lambda r:r['polled_at'])
    seen=set();last=None;start=None;intervals=[]
    for row in ordered:
        key=row['polled_at']
        if key in seen:raise ValueError('Duplicate station/poll timestamp')
        seen.add(key)
        good=snapshot_good(row)
        joined=last is not None and good and snapshot_good(last) and 0<key-last['polled_at']<=max_gap_seconds
        if not joined:
            if start is not None and last is not None:intervals.append((start,last['polled_at']))
            start=key if good else None
        last=row
    if start is not None and last is not None:intervals.append((start,last['polled_at']))
    hours=set()
    for first,last in intervals:
        hour=((first+3599)//3600)*3600
        while hour+3600<=last:
            hours.add(datetime.fromtimestamp(hour,UTC));hour+=3600
    return hours


def label_available(hour,cohorts):
    from .borough_mvp import release_at
    from zoneinfo import ZoneInfo
    return max([release_at(hour.astimezone(ZoneInfo('America/New_York')).strftime('%Y-%m'))]
        +[release_at(c) for c in cohorts])


class DatedGTFS:
    """Parse once per archive; route-stop relationships filtered by service date.

    Matching identifies scheduled reference associations on a date, not actual
    service or a train operating at the target hour. Overnight trip timing is not
    used to infer normal service.
    """
    def __init__(self,path: Path):
        self.path=path
        version=path.stem.removeprefix('mta_gtfs_')
        self.available=datetime.strptime(version,'%Y%m%d%H%M').replace(tzinfo=UTC)
        with zipfile.ZipFile(path) as z:
            if z.testzip():raise ValueError('Invalid GTFS ZIP')
            def read(name):
                return list(csv.DictReader(io.TextIOWrapper(z.open(name),encoding='utf-8-sig')))
            self.routes={r['route_id']:{k:r[k] for k in ('agency_id','route_short_name','route_type')} for r in read('routes.txt')}
            rawstops=read('stops.txt')
            self.parents={r['stop_id']:r.get('parent_station') or r['stop_id'] for r in rawstops}
            self.points={r['stop_id']:(float(r['stop_lat']),float(r['stop_lon'])) for r in rawstops if self.parents[r['stop_id']]==r['stop_id']}
            self.calendar=read('calendar.txt')
            self.exceptions=read('calendar_dates.txt') if 'calendar_dates.txt' in z.namelist() else []
            trips={r['trip_id']:(r['service_id'],r['route_id'],r.get('direction_id','')) for r in read('trips.txt')}
            self.associations=set()
            for r in read('stop_times.txt'):
                service,route,direction=trips[r['trip_id']]
                if r['stop_id'] not in self.parents:raise ValueError('Unresolved GTFS stop')
                self.associations.add((service,route,direction,r['stop_id']))
        self.known_directions={r+'|'+d for _,r,d,_ in self.associations if d}
        self._cache={}

    def reference(self,day: date):
        if day in self._cache:return self._cache[day]
        d=day.strftime('%Y%m%d');weekday=['monday','tuesday','wednesday','thursday','friday','saturday','sunday'][day.weekday()]
        services={r['service_id'] for r in self.calendar if r['start_date']<=d<=r['end_date'] and r[weekday]=='1'}
        for r in self.exceptions:
            if r['date']==d:
                if r['exception_type']=='1':services.add(r['service_id'])
                elif r['exception_type']=='2':services.discard(r['service_id'])
        routes=defaultdict(set);directions={key:set() for key in self.known_directions}
        for service,route,direction,stop in self.associations:
            if service in services:
                routes[route].add(stop)
                if direction:directions[route+'|'+direction].add(stop)
        result={'routes':self.routes,'stop_parents':self.parents,
            'route_stops':routes,'route_direction_stops':directions}
        self._cache[day]=result
        return result


TRIP_FIELDS=('trip_id','trip_route_id','trip_direction_id','trip_start_time','trip_start_date',
    'trip_schedule_relationship','trip_modified_trip_modifications_id','trip_modified_trip_affected_trip_id',
    'trip_modified_trip_start_date','trip_modified_trip_start_time')


def period_overlap(periods,start,end):
    """Half-open intervals; unknown/empty periods are counted separately."""
    if not periods:return None
    for p in periods:
        first=p.get('start');last=p.get('end')
        if first is not None and last is not None and int(first)>=int(last):
            raise ValueError('Invalid advisory interval')
    return any((p.get('start') is None or int(p['start'])<end.timestamp()) and
        (p.get('end') is None or int(p['end'])>start.timestamp()) for p in periods)


def advisory_snapshot(rows,cutoff,reference,nearby,origin):
    """One full flattened snapshot. Reject stale/differential/unsupported evidence.

    Return station counts, plus an audit. Affected entities are deduplicated over
    selectors and platforms. Empty trip strings still denote a restriction.
    """
    if not rows:return None,{'reason':'missing_snapshot'}
    snapshots={(r['source_file'],r['fetch_timestamp'],r['feed_timestamp'],r.get('incrementality')) for r in rows}
    if len(snapshots)!=1:raise ValueError('Mixed alert snapshots')
    _,observed,provider,incrementality=next(iter(snapshots))
    age=(observed-datetime.fromtimestamp(provider,UTC)).total_seconds() if provider else None
    if observed>=cutoff:raise ValueError('Future advisory observation')
    if (cutoff-observed).total_seconds()>120 or age is None or not -300<=age<=900:
        return None,{'reason':'stale_snapshot','feed_age_seconds':age}
    if incrementality not in (None,0) or any(r.get('is_deleted') for r in rows):
        return None,{'reason':'differential_or_deleted'}
    entities={};unresolved=Counter()
    for r in rows:
        selector={k:r.get(k) for k in ('agency_id','route_id','route_type','stop_id','direction_id')}
        selector['trip_restricted']=any(r.get(k) is not None for k in TRIP_FIELDS)
        stops,resolution=resolve_selector(selector,reference)
        if selector['agency_id']=='MTASBWY' and not resolution.startswith('resolved'):
            unresolved[resolution]+=1
        if selector['agency_id']!='MTASBWY':continue
        periods=json.loads(r['active_periods_json']) if r.get('active_periods_json') else []
        key=r['entity_id'];entry=entities.setdefault(key,{'stops':set(),'periods':periods})
        if entry['periods']!=periods:raise ValueError('Conflicting entity active periods')
        entry['stops'].update(stops)
    # Unsupported restrictions can affect an unknown station; don't encode zero.
    if unresolved:return None,{'reason':'unresolved_selectors','unresolved':dict(unresolved)}
    result={}
    for station,stops in nearby.items():
        counts=[0,0,0]
        for e in entities.values():
            if not (stops&e['stops']):continue
            current=period_overlap(e['periods'],origin,origin+timedelta(seconds=1))
            future=period_overlap(e['periods'],origin,origin+3*HOUR)
            if current is None:counts[2]+=1
            else:counts[0]+=int(current);counts[1]+=int(future)
        result[station]={'advisory_active':counts[0],'advisory_next3h':counts[1],
            'advisory_unspecified':counts[2], 'advisory_observed_at_utc':observed,
            'advisory_feed_age_seconds':age}
    return result,{'reason':'covered','entities':len(entities),'feed_age_seconds':age}
