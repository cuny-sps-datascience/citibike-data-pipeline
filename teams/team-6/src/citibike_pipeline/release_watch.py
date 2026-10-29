"""Public object observations and a strict gate for prepared geographic outcomes."""
from __future__ import annotations
from datetime import datetime,timedelta,timezone
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET
from urllib.parse import urlencode,quote
from urllib.request import Request,urlopen


def parse_listing(raw):
    tree=ET.fromstring(raw);ns={'s':'http://s3.amazonaws.com/doc/2006-03-01/'}
    rows=[]
    for node in tree.findall('s:Contents',ns):
        key=node.findtext('s:Key',namespaces=ns)
        match=re.fullmatch(r'(JC-)?(20\d{2})(0[1-9]|1[0-2])-citibike-tripdata(?:\.csv)?\.zip',key)
        if not match:continue
        row={'object_key':key,'family':'jersey_city' if match[1] else 'nyc',
            'nominal_release':match[2]+'-'+match[3],
            'etag':node.findtext('s:ETag',namespaces=ns).strip('"'),
            'bytes':int(node.findtext('s:Size',namespaces=ns)),
            'provider_last_modified_utc':node.findtext('s:LastModified',namespaces=ns),
            'url':'https://s3.amazonaws.com/tripdata/'+quote(key,safe='')}
        row['object_version']=hashlib.sha256(json.dumps(row,sort_keys=True).encode()).hexdigest()
        rows.append(row)
    truncated=tree.findtext('s:IsTruncated',namespaces=ns)=='true'
    token=tree.findtext('s:NextContinuationToken',namespaces=ns)
    if truncated and not token:raise ValueError('Truncated listing has no continuation token')
    return rows,token if truncated else None


def fetch_listing(root: Path):
    """Fetch all pages or fail; never publish a partial candidate listing."""
    rows=[];token=None;tokens=set();artifacts=[]
    for index in range(100):
        params={'list-type':'2','max-keys':1000}
        if token:params['continuation-token']=token
        url='https://s3.amazonaws.com/tripdata/?'+urlencode(params)
        with urlopen(Request(url,headers={'User-Agent':'CUNY-DATA622-research/0.1'}),timeout=30) as response:
            raw=response.read()
        path=root/f'listing_{index:03d}.xml';path.write_bytes(raw)
        artifacts.append({'url':url,'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)})
        page,token=parse_listing(raw);rows.extend(page)
        if not token:break
        if token in tokens:raise ValueError('Listing continuation cycle')
        tokens.add(token)
    else:raise ValueError('Listing exceeded bounded pagination')
    if not rows or len({r['object_key'] for r in rows})!=len(rows):
        raise ValueError('Empty or duplicate monthly object listing')
    return rows,artifacts


def validate_outcomes(root: Path):
    """Only complete, checksummed, uniquely keyed, geographic label builds qualify."""
    import pyarrow.parquet as pq
    manifest=json.loads((root/'manifest.json').read_text())
    if manifest.get('status')!='built' or manifest.get('schema_version')!='manhattan-aggregate-labels-v2':
        raise ValueError('Require a built version-2 geographic outcome export')
    artifact=next(a for a in manifest['artifacts'] if a['path']=='borough_hourly.parquet')
    path=(root/artifact['path']).resolve()
    if not path.is_relative_to(root.resolve()):raise ValueError('Artifact path escapes package')
    if hashlib.sha256(path.read_bytes()).hexdigest()!=artifact['sha256']:raise ValueError('Outcome checksum mismatch')
    rows=pq.read_table(path).to_pylist()
    if len(rows)!=artifact['rows'] or len(rows)!=manifest['label_hours']:raise ValueError('Outcome count mismatch')
    if not rows:raise ValueError('Empty outcome export')
    start=datetime.fromisoformat(manifest['start']);end=datetime.fromisoformat(manifest['end_exclusive'])
    expected=int((end-start).total_seconds()/3600)
    if len(rows)!=expected or len({r['hour_utc'] for r in rows})!=expected:raise ValueError('Outcome hours incomplete/duplicated')
    expected_hours={start+timedelta(hours=i) for i in range(expected)}
    if {r['hour_utc'] for r in rows}!=expected_hours:raise ValueError('Outcome calendar gap')
    for row in rows:
        if row['borough']!='Manhattan':raise ValueError('Wrong outcome scope')
        value=row['departures']
        if row['label_assignable']:
            if value is None or value<0 or int(value)!=value:raise ValueError('Invalid departure count')
        elif value is not None:raise ValueError('Unassignable hour must have null label')
        if row.get('station_operation_required') is not False:raise ValueError('Require geographic, not station exposure labels')
    if sum(r['assigned_departures'] for r in rows)!=manifest['departures']:raise ValueError('Assigned counts do not reconcile')
    # The build must retain selected input evidence; an arbitrary zero-filled file is insufficient.
    if not manifest.get('input_files') or not manifest.get('observed_manifest_sha256'):
        raise ValueError('Missing selected-source provenance')
    return {'manifest':manifest,'rows':len(rows),'latest_hour_utc':max(r['hour_utc'] for r in rows).isoformat(),
        'manifest_sha256':hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest(),
        'artifact':artifact}
