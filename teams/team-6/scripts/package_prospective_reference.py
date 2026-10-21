"""Prepare regular GTFS IDs/served stops and retain dated source provenance."""
from pathlib import Path
import json
import shutil
from urllib.error import HTTPError
from citibike_pipeline.forecast_sources import digest,fetch,stamp
from citibike_pipeline.prospective_evidence import build_gtfs_reference

if __name__=='__main__':
    base=Path('/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour')
    root=base/'prospective_reference'/stamp();root.mkdir(parents=True)
    source=fetch('https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip',root/'gtfs_subway.zip')
    reference=build_gtfs_reference(root/'gtfs_subway.zip')
    reference['available_from_utc']=source['retrieved_at_utc']
    reference['reference_version']=root.name
    (root/'gtfs_reference.json').write_text(json.dumps(reference,sort_keys=True,indent=2)+'\n')
    try:
        dictionary=fetch('https://www.mta.info/document/90881',root/'mta_alert_dictionary.pdf')
    except HTTPError as exc:
        cache=Path(__file__).resolve().parents[1]/'.local/execution/mta_alert_dictionary.pdf'
        if exc.code!=403 or not cache.exists():raise
        shutil.copyfile(cache,root/'mta_alert_dictionary.pdf')
        dictionary={'url':'https://www.mta.info/document/90881','cache_path':str(cache),
            'retrieved_at_utc':None,'sha256':digest(root/'mta_alert_dictionary.pdf'),
            'note':'Previously downloaded official dictionary; current HTTP fetch returned 403. Rechecked text using web reader.'}
    manifest={'schema_version':'prospective-gtfs-reference-v1','status':'built','source':source,'dictionary':dictionary,
        'reference_version':root.name,'artifacts':[]}
    for path in (root/'gtfs_reference.json',root/'gtfs_subway.zip',root/'mta_alert_dictionary.pdf'):
        manifest['artifacts'].append({'path':path.name,'sha256':digest(path),'bytes':path.stat().st_size})
    (root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(root)
