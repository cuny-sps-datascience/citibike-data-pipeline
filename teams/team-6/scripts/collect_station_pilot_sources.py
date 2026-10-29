"""Acquire pinned public station/alert objects; never touch canonical trip manifests.

Single writer, streamed atomic downloads, SHA-256 receipts and bounded retries.
Restart with the same output directory; previously receipted objects are verified.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib, json, shutil, time
from pathlib import Path
from urllib.request import Request, urlopen
from citibike_pipeline.forecast_sources import digest


def collect(research: Path, root: Path):
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / 'manifest.json'
    heads = json.loads((research / 'daily_archive_heads.json').read_text())
    research_hashes={r['path']:r['sha256'] for r in json.loads((research/'manifest.json').read_text())['artifacts']}
    if any(r['status'] != 200 or not r.get('bytes') or not r.get('etag') for r in heads):
        raise ValueError('Every selected object needs a successful pinned HEAD')
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {
        'schema_version': 'station-pilot-public-inputs-v1',
        'created_at_utc': datetime.now(timezone.utc).isoformat(),
        'research_root': str(research), 'head_sha256': digest(research/'daily_archive_heads.json'),
        'objects': [], 'status': 'acquiring',
        'eligibility_promoted': False, 'canonical_manifest_changed': False}
    if manifest['head_sha256'] != digest(research/'daily_archive_heads.json'):
        raise ValueError('Pinned source inventory changed')
    def save():
        temporary = root / 'manifest.json.partial'
        temporary.write_text(json.dumps(manifest, indent=2)+'\n')
        temporary.replace(manifest_path)
    known = {r['url']: r for r in manifest['objects']}
    # Include previous-day metadata and dated schedules without modifying research.
    supporting = [p for p in research.iterdir() if p.name.startswith('mta_gtfs_') and p.suffix=='.zip']
    supporting += [research/'ctbk_info_2026-04-06.json']
    manifest.setdefault('references', [])
    for source in supporting:
        if not source.exists():
            if source.name=='ctbk_info_2026-04-06.json':
                source = next(p for p in research.iterdir() if '2026-04-06' in p.name and p.suffix=='.json')
            else: raise FileNotFoundError(source)
        target = root/'references'/source.name
        target.parent.mkdir(exist_ok=True)
        if not target.exists(): shutil.copyfile(source,target)
        if digest(target)!=digest(source): raise ValueError('Reference mismatch')
        item={'path':str(target.relative_to(root)), 'sha256':digest(target),'bytes':target.stat().st_size}
        if item not in manifest['references']: manifest['references'].append(item)
    save()
    # Date ordering permits processing as acquisition progresses.
    for head in sorted(heads, key=lambda r:(r['date'], r['source'])):
        extension='.json' if head['source']=='station_information' else '.parquet'
        target=root/head['source']/(head['date']+extension)
        if head['url'] in known:
            prior=known[head['url']]
            if target.stat().st_size!=prior['bytes'] or digest(target)!=prior['sha256']:
                raise ValueError('Previously receipted source changed: '+str(target))
            continue
        target.parent.mkdir(exist_ok=True)
        sample_prefix={'station_status':'ctbk_status_', 'station_information':'ctbk_info_',
                       'subway_alerts':'gtfsrt_subway_alerts_'}[head['source']]
        sample=research/(sample_prefix+head['date']+extension)
        metadata={}
        if sample.exists() and sample.stat().st_size==head['bytes']:
            if digest(sample)!=research_hashes.get(sample.name):raise ValueError('Research sample checksum mismatch')
            shutil.copyfile(sample,target)
            metadata={'acquisition':'verified research copy', 'research_path':str(sample)}
        else:
            for attempt in range(4):
                temporary=target.with_suffix(target.suffix+'.partial')
                try:
                    request=Request(head['url'],headers={'If-Match':head['etag'],
                        'User-Agent':'CUNY-DATA622-academic-station-pilot/0.1'})
                    h=hashlib.sha256(); size=0
                    with urlopen(request,timeout=90) as response, temporary.open('wb') as stream:
                        if response.status!=200 or response.headers.get('ETag')!=head['etag']:
                            raise ValueError('Object version differs from pinned inventory')
                        for block in iter(lambda:response.read(2**20), b''):
                            stream.write(block);h.update(block);size+=len(block)
                        metadata={'response_etag':response.headers.get('ETag'),
                            'response_last_modified':response.headers.get('Last-Modified')}
                    if size!=head['bytes']: raise ValueError('Incomplete object')
                    temporary.replace(target);break
                except Exception:
                    if attempt==3: raise
                    time.sleep(2**attempt)
        if target.stat().st_size!=head['bytes']: raise ValueError('Size mismatch')
        item=head|metadata|{'path':str(target.relative_to(root)), 'sha256':digest(target),
            'retrieved_at_utc':datetime.now(timezone.utc).isoformat()}
        manifest['objects'].append(item);known[item['url']]=item;save()
        if len(known)%6==0:
            print(json.dumps({'completed':len(known),'expected':len(heads), 'last_date':head['date'],
                'bytes':sum(r['bytes'] for r in manifest['objects'])}),flush=True)
    manifest['status']='downloaded';manifest['completed_at_utc']=datetime.now(timezone.utc).isoformat();save()
    print('Downloaded',len(known),'pinned public objects to',root,flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--research',type=Path,required=True)
    parser.add_argument('--root',type=Path,required=True)
    args=parser.parse_args();collect(args.research,args.root)
