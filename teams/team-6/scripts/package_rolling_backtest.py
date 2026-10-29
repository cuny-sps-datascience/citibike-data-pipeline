"""Package existing compact features and source-cohort label timing; no trip rescan."""
import json
from pathlib import Path
import shutil
from citibike_pipeline.forecast_sources import stamp,digest

if __name__=='__main__':
    project=Path(__file__).resolve().parents[1]
    base=Path('/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour')
    features=Path((base/'borough_features/CURRENT_BOROUGH_FEATURES').read_text().strip())
    labels=Path((base/'borough_labels/CURRENT_BOROUGH_LABELS').read_text().strip())
    root=base/'rolling_backtests'/stamp();root.mkdir(parents=True)
    artifacts=[]
    for source,names in ((features,['features.parquet']),(labels,['borough_hourly_release.parquet'])):
        manifest=json.loads((source/'manifest.json').read_text())
        for name in names:
            expected=next(a['sha256'] for a in manifest['artifacts'] if a['path']==name)
            assert digest(source/name)==expected
            shutil.copyfile(source/name,root/name)
            artifacts.append({'path':name,'sha256':expected,'bytes':(root/name).stat().st_size})
    for name in ('model_runtime','rolling_backtest'):
        path=root/(name+'.py');shutil.copyfile(project/'src/citibike_pipeline'/(name+'.py'),path)
        artifacts.append({'path':path.name,'sha256':digest(path),'bytes':path.stat().st_size})
    manifest={'schema_version':'rolling-backtest-package-v1','status':'built',
        'feature_manifest_sha256':digest(features/'manifest.json'),'label_manifest_sha256':digest(labels/'manifest.json'),
        'months':['2026-06','2026-07','2026-08'],'artifacts':artifacts}
    (root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(root)
