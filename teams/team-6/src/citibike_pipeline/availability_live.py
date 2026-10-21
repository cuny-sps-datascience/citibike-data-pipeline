"""Strict live GBFS adapter for the independent availability dashboard."""
from __future__ import annotations
from collections import Counter
from datetime import datetime,timedelta,timezone
from math import asin,cos,radians,sin,sqrt
from zoneinfo import ZoneInfo
import hashlib,json
from urllib.request import Request,urlopen
import numpy as np

UTC=timezone.utc;ZONE=ZoneInfo('America/New_York')


def epoch(value):
    if isinstance(value,(int,float)):return datetime.fromtimestamp(value,UTC)
    return datetime.fromisoformat(str(value).replace('Z','+00:00')).astimezone(UTC)


def distance(a,b):
    lat1,lon1,lat2,lon2=map(radians,(a['latitude'],a['longitude'],float(b['lat']),float(b['lon'])))
    return 2*6371008.8*asin(min(1,sqrt(sin((lat2-lat1)/2)**2+cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2)))


def collect(root):
    evidence=[]
    def get(name,url):
        with urlopen(Request(url,headers={'User-Agent':'CUNY-DATA622-availability-pilot/0.1'}),timeout=30) as response:
            raw=response.read()
        at=datetime.now(UTC);(root/(name+'.json')).write_bytes(raw)
        evidence.append({'name':name,'url':url,'observed_at_utc':at.isoformat(),'sha256':hashlib.sha256(raw).hexdigest()})
        return json.loads(raw),at
    root.mkdir(parents=True,exist_ok=False)
    discovery,_=get('discovery','https://gbfs.citibikenyc.com/gbfs/gbfs.json')
    data=discovery['data'];feeds=data['feeds'] if 'feeds' in data else data.get('en',next(iter(data.values())))['feeds']
    urls={r['name']:r['url'] for r in feeds}
    info,info_at=get('information',urls['station_information'])
    status,status_at=get('status',urls['station_status'])
    manifest={'artifacts':evidence,'collected_at_utc':datetime.now(UTC).isoformat()}
    (root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return info,status,info_at,status_at


def inputs(cohort,info,status,info_at,status_at,issue,feature_names):
    if not info_at<issue or not status_at<issue:raise ValueError('Live observation must precede issue')
    information=info['data']['stations'];statuses=status['data']['stations']
    ic=Counter(str(r.get('station_id','')) for r in information)
    sc=Counter(str(r.get('station_id','')) for r in statuses)
    shorts=Counter(str(r.get('short_name','')) for r in information)
    im={str(r['station_id']):r for r in information};sm={str(r['station_id']):r for r in statuses}
    provider=epoch(status['last_updated']);info_provider=epoch(info['last_updated'])
    rows=[];states=[]
    for index,station in enumerate(cohort):
        uuid=station['uuid'];m=im.get(uuid);s=sm.get(uuid);reason='ready'
        if not m or ic[uuid]!=1 or shorts[str(m.get('short_name',''))]!=1:reason='missing_or_ambiguous_information'
        elif str(m.get('short_name',''))!=station['station_id'] or distance(station,m)>25:reason='station_identity_or_location_changed'
        elif not s or sc[uuid]!=1:reason='missing_or_ambiguous_status'
        elif not (s.get('is_installed')==1 and s.get('is_renting')==1 and s.get('is_returning')==1):reason='station_not_accepting_both_rentals_and_returns'
        elif not (-300<=(status_at-provider).total_seconds()<=120
            and 0<(issue-status_at).total_seconds()<=120
            and -300<=(status_at-epoch(s.get('last_reported',0))).total_seconds()<=600
            and -300<=(info_at-info_provider).total_seconds()<=5400):reason='stale_source_or_station'
        values={}
        if reason=='ready':
            values={'current_bikes':s.get('num_bikes_available',s.get('num_vehicles_available')),
                'current_docks':s.get('num_docks_available'),'current_ebikes':s.get('num_ebikes_available'),
                'disabled_bikes':s.get('num_bikes_disabled',s.get('num_vehicles_disabled')),
                'disabled_docks':s.get('num_docks_disabled'),'capacity':m.get('capacity')}
            if any(v is None or not isinstance(v,(int,float)) or not np.isfinite(v) or v<0 for v in values.values()) or values['capacity']<=0:
                reason='missing_or_invalid_inventory_fields'
        states.append({'station_id':station['station_id'],'station_name':m.get('name',station['name']) if m else station['name'],
            'status':reason,'source_observed_at_utc':status_at,'provider_at_utc':provider,
            'current_bikes':values.get('current_bikes') if reason=='ready' else None,
            'current_docks':values.get('current_docks') if reason=='ready' else None})
        if reason!='ready':continue
        t=issue.astimezone(ZONE)
        for h in (15,30,60,180):
            row={'station_id':station['station_id'],'station_name':m.get('name',station['name']),
                'station_index':index,'origin_utc':issue,'horizon_minutes':h,
                'target_utc':issue+timedelta(minutes=h),'input_observed_at_utc':status_at,
                'information_available_at_utc':info_at,'local_hour':t.hour,'dow':t.weekday(),
                'hour_sin':np.sin(2*np.pi*t.hour/24),'hour_cos':np.cos(2*np.pi*t.hour/24),**values}
            rows.append(row)
    X=np.array([[r[k] for k in feature_names] for r in rows],dtype=float)
    if not rows:X=np.empty((0,len(feature_names)))
    return rows,X,states
