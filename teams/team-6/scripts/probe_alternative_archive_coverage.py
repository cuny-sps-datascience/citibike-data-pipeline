"""Check daily public archive object availability, without downloading bulk history.

HEAD presence establishes accessible objects, not complete station/feed coverage.
The results are research evidence and never change model eligibility or pointers.
"""
from __future__ import annotations
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import date,datetime,timedelta,timezone
import json,threading,time
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from citibike_pipeline.forecast_sources import digest


def probe(root, first, last, reuse_existing=False):
    if first>=last:raise ValueError('Increasing date boundaries required')
    inventory=json.loads((root/'gtfsrt_inventory.json').read_text())
    feed=next(r for r in inventory if r.get('system_id')=='subway-alerts')
    items=[];day=first
    while day<last:
        value=day.isoformat()
        for source,url in [('station_status','https://data.ctbk.dev/gbfs/status/'+value+'.parquet'),
                ('station_information','https://data.ctbk.dev/gbfs/info/'+value+'.json'),
                ('subway_alerts','https://storage.googleapis.com/parquet.gtfsrt.io/service_alerts/date='+value+'/base64url='+feed['base64url']+'/data.parquet')]:
            items.append((source,value,url))
        day+=timedelta(days=1)
    lock=threading.Lock();next_start=[0.0]
    def get(item):
        source,day,url=item
        with lock:
            pause=max(0,next_start[0]-time.monotonic())
            if pause:time.sleep(pause)
            next_start[0]=time.monotonic()+1/3
        result={'source':source,'date':day,'url':url,'checked_at_utc':datetime.now(timezone.utc).isoformat()}
        try:
            with urlopen(Request(url,method='HEAD',headers={'User-Agent':'CUNY-DATA622-source-research/0.1'}),timeout=25) as response:
                size=response.headers.get('Content-Length')
                result|={'status':response.status,'bytes':int(size) if size else None,
                    'etag':response.headers.get('ETag'),'last_modified':response.headers.get('Last-Modified')}
        except HTTPError as exc:result|={'status':exc.code,'error':str(exc)}
        except Exception as exc:result|={'status':None,'error':str(exc)}
        return result
    evidence=root/'daily_archive_heads.json'
    wanted={url for _,_,url in items}
    results=[r for r in json.loads(evidence.read_text()) if r['url'] in wanted] if reuse_existing and evidence.exists() else []
    known={r['url'] for r in results}
    pending=[item for item in items if item[2] not in known]
    with ThreadPoolExecutor(max_workers=4) as pool:
        for future in as_completed([pool.submit(get,item) for item in pending]):
            results.append(future.result())
            if len(results)%60==0:print('Archive HEAD checks',len(results),'/',len(items),flush=True)
    results.sort(key=lambda r:(r['source'],r['date']))
    # Preserve prior metadata observations before refreshing their current files.
    manifest_path=root/'manifest.json'
    manifest=json.loads(manifest_path.read_text()) if manifest_path.exists() else None
    if manifest:
        history=root/'metadata_history';history.mkdir(exist_ok=True)
        for name in ('daily_archive_heads.json','daily_archive_coverage_summary.json'):
            old=root/name
            if not old.exists():continue
            saved=history/(digest(old)+'.json')
            if not saved.exists():saved.write_bytes(old.read_bytes())
            for item in manifest['artifacts']:
                if item['path']==name:
                    item['path']=str(saved.relative_to(root))
                    item['previous_name']=name
    path=root/'daily_archive_heads.json';path.write_text(json.dumps(results,indent=2)+'\n')
    summary={}
    for source in sorted({r['source'] for r in results}):
        subset=[r for r in results if r['source']==source]
        summary[source]={'expected_dates':len(subset),'accessible_dates':sum(r['status']==200 for r in subset),
            'status_counts':dict(Counter(str(r['status']) for r in subset)),
            'unavailable_dates':[{'date':r['date'],'status':r['status'],'error':r.get('error')} for r in subset if r['status']!=200],
            'total_advertised_bytes':sum(r.get('bytes') or 0 for r in subset if r['status']==200)}
    report={'checked_at_utc':datetime.now(timezone.utc).isoformat(),'start_inclusive':str(first),
        'end_exclusive':str(last),'object_presence_only':True,'sources':summary,
        'evidence_path':str(path),'evidence_sha256':digest(path)}
    (root/'daily_archive_coverage_summary.json').write_text(json.dumps(report,indent=2)+'\n')
    if manifest:
        for name in ('daily_archive_heads.json','daily_archive_coverage_summary.json'):
            current=root/name
            manifest['artifacts'].append({'path':name,'sha256':digest(current),
                'bytes':current.stat().st_size,'kind':'derived research metadata'})
        manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps(report,indent=2));return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--start',type=date.fromisoformat,default=date(2026,4,7))
    parser.add_argument('--end-exclusive',type=date.fromisoformat,default=date(2026,10,2))
    parser.add_argument('--reuse-existing',action='store_true',help='reuse recorded HEAD observations with their original check times')
    args=parser.parse_args();probe(args.root,args.start,args.end_exclusive,args.reuse_existing)
