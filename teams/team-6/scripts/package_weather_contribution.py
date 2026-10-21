"""Package a portable, documented external dataset for later class-catalog import."""
import argparse
import json
from pathlib import Path
import shutil
import zipfile

from citibike_pipeline.forecast_sources import digest


def package(source: Path, destination: Path):
    manifest=json.loads((source/'manifest.json').read_text())
    data=next(a for a in manifest['artifacts'] if a['path']=='weather_runs.parquet')
    if digest(source/data['path'])!=data['sha256']:raise ValueError('Weather checksum mismatch')
    root=destination/('weather_reference_'+source.name);root.mkdir(parents=True,exist_ok=False)
    shutil.copyfile(source/data['path'],root/data['path'])
    project=Path(__file__).resolve().parents[1]
    shutil.copyfile(project/'notebooks/08_import_weather_contribution.py',root/'import_weather.py')
    shutil.copyfile(project/'docs/weather_reference_datasheet.md',root/'DATASHEET.md')
    report={'schema_version':'weather-reference-handoff-v1','data':data,
        'feature_package_manifest_sha256':digest(source/'manifest.json'),
        'raw_weather_manifest_sha256':manifest['weather_manifest_sha256'],
        'source':'https://single-runs-api.open-meteo.com/v1/forecast',
        'attribution':'Weather data by Open-Meteo.com; ECMWF IFS; CC BY 4.0',
        'class_catalog_publication_completed':False,'files':[]}
    for filename in ('weather_runs.parquet','import_weather.py','DATASHEET.md'):
        path=root/filename;report['files'].append({'path':filename,'sha256':digest(path),'bytes':path.stat().st_size})
    (root/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
    archive=root.with_suffix('.zip')
    with zipfile.ZipFile(archive,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for path in sorted(root.iterdir()):z.write(path,path.name)
    print(json.dumps({'root':str(root),'archive':str(archive),'sha256':digest(archive),'bytes':archive.stat().st_size,
        'rows':data['rows']},indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--package',required=True,type=Path)
    p.add_argument('--destination',required=True,type=Path);args=p.parse_args();package(args.package,args.destination)
