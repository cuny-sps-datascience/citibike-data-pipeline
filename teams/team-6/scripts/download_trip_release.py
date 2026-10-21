"""Stream a version-pinned NYC monthly ZIP into a separate raw download archive."""
import argparse
from datetime import datetime,timezone
import hashlib,json,os,time
from pathlib import Path
from urllib.request import Request,urlopen
import zipfile
from citibike_pipeline.forecast_sources import digest,stamp
from citibike_pipeline.release_watch import fetch_listing

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--month',required=True)
    parser.add_argument('--root',type=Path,default=Path('/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_downloads'))
    args=parser.parse_args();root=args.root
    listing=root/'observations'/stamp();listing.mkdir(parents=True)
    objects,artifacts=fetch_listing(listing)
    selected=[r for r in objects if r['family']=='nyc' and r['nominal_release']==args.month]
    if len(selected)!=1:raise ValueError('Require exactly one NYC whole-month ZIP')
    item=selected[0];target=root/'objects'/args.month/item['object_version']/Path(item['object_key']).name
    target.parent.mkdir(parents=True,exist_ok=True);metadata=target.with_suffix('.zip.json')
    if target.exists() and metadata.exists():
        previous=json.loads(metadata.read_text())
        if target.stat().st_size==item['bytes'] and digest(target)==previous['sha256']:
            print(json.dumps(previous,indent=2));raise SystemExit(0)
        raise ValueError('Published local ZIP failed its checksum; preserve and investigate')
    partial=target.with_suffix('.zip.partial')
    size=partial.stat().st_size if partial.exists() else 0
    headers={'User-Agent':'CUNY-DATA622-research/0.1','If-Match':'"'+item['etag']+'"'}
    if size:headers['Range']=f'bytes={size}-'
    with urlopen(Request(item['url'],headers=headers),timeout=60) as response:
        if size and response.status!=206:size=0
        if size and not response.headers.get('Content-Range','').startswith(f'bytes {size}-'):
            raise ValueError('Resume Content-Range mismatch')
        etag=response.headers.get('ETag','').strip('"')
        if etag!=item['etag']:raise ValueError('Download object version changed')
        began=time.monotonic();next_progress=size+64*2**20
        with partial.open('ab' if size else 'wb') as output:
            while block:=response.read(2**20):
                output.write(block);size+=len(block)
                if size>=next_progress:
                    print(f'Downloaded {size:,}/{item["bytes"]:,} bytes',flush=True)
                    next_progress=size+64*2**20
    if size!=item['bytes']:raise ValueError('Incomplete ZIP; rerun to resume')
    sha=digest(partial)
    with zipfile.ZipFile(partial) as archive:
        members=[{'member':i.filename,'bytes':i.file_size,'crc32':f'{i.CRC:08x}'} for i in archive.infolist()
            if i.filename.lower().endswith('.csv') and '__MACOSX' not in i.filename]
        if not members:raise ValueError('ZIP has no CSV members')
    os.replace(partial,target)
    report=item|{'path':str(target),'sha256':sha,'retrieved_at_utc':datetime.now(timezone.utc).isoformat(),
        'listing_artifacts':artifacts,'csv_members':members,'bytes_verified':True,
        'crc_policy':'Verified while profiling every selected CSV member to EOF',
        'original_source_archive_modified':False,'seconds':round(time.monotonic()-began,2)}
    metadata.write_text(json.dumps(report,indent=2)+'\n')
    (listing/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
