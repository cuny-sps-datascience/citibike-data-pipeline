"""Read-only full-inventory integrity/schema audit; never rewrites source receipts."""
from __future__ import annotations
import argparse,json
from collections import Counter
from datetime import datetime,timezone
from pathlib import Path
import pyarrow.parquet as pq
from citibike_pipeline.forecast_sources import digest
from citibike_pipeline.station_history import TRIP_FIELDS


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',required=True,type=Path)
    p.add_argument('--output',required=True,type=Path);args=p.parse_args()
    root=args.sources;m=json.loads((root/'manifest.json').read_text())
    if m['status']!='downloaded' or len(m['objects'])!=534:raise ValueError('Incomplete source package')
    required={'active_periods_json','direction_id','incrementality','is_deleted',*TRIP_FIELDS}
    schemas=[];sizes=Counter();counts=Counter();rows=Counter()
    for i,item in enumerate(m['objects']):
        path=root/item['path']
        if path.stat().st_size!=item['bytes'] or digest(path)!=item['sha256']:raise ValueError('Source integrity failed: '+str(path))
        sizes[item['source']]+=item['bytes'];counts[item['source']]+=1
        if path.suffix=='.parquet':rows[item['source']]+=pq.read_metadata(path).num_rows
        if item['source']=='subway_alerts':
            missing=sorted(required-set(pq.read_schema(path).names))
            schemas.append({'date':item['date'],'missing_columns':missing,'complete_selector_schema':not missing,
                'sha256':item['sha256'],'rows':pq.read_metadata(path).num_rows})
        if (i+1)%60==0:print('Source integrity audit',i+1,'/',len(m['objects']),flush=True)
    for item in m['references']:
        path=root/item['path']
        if path.stat().st_size!=item['bytes'] or digest(path)!=item['sha256']:raise ValueError('Reference integrity failed')
    complete=sorted(r['date'] for r in schemas if r['complete_selector_schema'])
    incomplete=sorted(r['date'] for r in schemas if not r['complete_selector_schema'])
    report={'checked_at_utc':datetime.now(timezone.utc).isoformat(),'source_root':str(root),
        'manifest_sha256':digest(root/'manifest.json'),'objects':len(m['objects']),
        'counts_by_source':dict(counts),'bytes_by_source':dict(sizes),'rows_by_source':dict(rows),
        'references':m['references'],'all_sha256_checks_passed':True,
        'complete_alert_schema_dates':len(complete),'incomplete_alert_schema_dates':len(incomplete),
        'first_complete_alert_schema_date':min(complete) if complete else None,
        'last_incomplete_alert_schema_date':max(incomplete) if incomplete else None,
        'alert_daily_schemas':sorted(schemas,key=lambda r:r['date']),
        'publisher_schema_issue':'https://github.com/JarvusInnovations/gtfs-realtime-archiver/issues/91',
        'original_2025_scope_supported':False,'canonical_station_eligibility_promoted':False}
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k not in ('alert_daily_schemas','references')},indent=2))
