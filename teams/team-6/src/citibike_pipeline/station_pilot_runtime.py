"""Standalone Free Edition station pilot training; no production activation."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib, json, warnings
from zoneinfo import ZoneInfo
import joblib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.compose import ColumnTransformer
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_poisson_deviance
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ZONE=ZoneInfo('America/New_York');UTC=timezone.utc
CATEGORIES=['station_hour','dow_hour','horizon']
WEATHER=['temperature','temperature_squared','precipitation','wind','wet']
ADVISORY=['advisory_active','advisory_next3h','advisory_unspecified']
ALL_FEATURES=CATEGORIES+WEATHER+ADVISORY
FAMILIES=('calendar','weather','advisory')


def metrics(y,prediction):
    prediction=np.maximum(np.asarray(prediction,dtype=float),1e-9);y=np.asarray(y,dtype=float)
    if not len(y) or not np.isfinite(prediction).all():raise ValueError('Empty/nonfinite evaluation')
    return {'rows':len(y),'mae':float(mean_absolute_error(y,prediction)),
        'rmse':float(np.sqrt(mean_squared_error(y,prediction))),
        'poisson_deviance':float(mean_poisson_deviance(y,prediction)),
        'bias':float(np.mean(prediction-y)),'actual_mean':float(np.mean(y)),
        'zero_labels':int(np.sum(y==0))}


def local(value):return datetime.fromisoformat(value).replace(tzinfo=ZONE).astimezone(UTC)


def in_partition(row,bounds):
    first,last=map(local,bounds)
    return first<=row['origin_utc'] and row['origin_utc']+timedelta(hours=3)<=last


def partition(rows,protocol):
    validation_fit=datetime.fromisoformat(protocol['validation_fit_origin_local']).astimezone(UTC)-timedelta(minutes=5)
    test_fit=datetime.fromisoformat(protocol['test_fit_origin_local']).astimezone(UTC)-timedelta(minutes=5)
    seen=set()
    for r in rows:
        key=(r['station_id'],r['origin_utc'],r['horizon'])
        if key in seen:raise ValueError('Duplicate station/origin/horizon key')
        seen.add(key)
        cutoff=r['issue_cutoff_utc']
        if cutoff!=r['origin_utc']-timedelta(minutes=5):raise ValueError('Wrong origin cutoff')
        covered=r.get('advisory_covered',True)
        fields=['weather_assumed_available_at_utc','information_assumed_available_at_utc']
        if covered:fields+=['gtfs_archive_available_at_utc','advisory_observed_at_utc']
        for f in fields:
            if not r[f]<cutoff:raise ValueError('Future predictor availability: '+f)
        if covered and not 0<=(cutoff-r['advisory_observed_at_utc']).total_seconds()<=120:
            raise ValueError('Stale archived advisory observation')
        if covered and not -300<=r['advisory_feed_age_seconds']<=900:raise ValueError('Stale advisory provider')
        if r['target_start_utc']!=r['origin_utc']+timedelta(hours=r['horizon']):raise ValueError('Target/horizon mismatch')
        if r['target_end_utc']!=r['target_start_utc']+timedelta(hours=1):raise ValueError('Target interval mismatch')
        if r['departures']<0:raise ValueError('Negative departure count')
    result={
        'train':[r for r in rows if in_partition(r,protocol['train_target_local']) and r['label_available_at_utc']<validation_fit],
        'validation':[r for r in rows if in_partition(r,protocol['validation_target_local']) and r['label_available_at_utc']<test_fit],
        'refit':[r for r in rows if in_partition(r,protocol['refit_target_local']) and r['label_available_at_utc']<test_fit],
        'test':[r for r in rows if in_partition(r,protocol['test_target_local'])]}
    if any(not v for v in result.values()):raise ValueError('Empty release-eligible partition')
    return result,validation_fit,test_fit


def matrix(rows):
    # Object arrays keep string station/hour keys and numeric columns together;
    # ColumnTransformer casts only the selected numeric subset.
    return np.array([[r[k] for k in ALL_FEATURES] for r in rows],dtype=object)


def fit(X,y,family,alpha):
    numeric=[] if family=='calendar' else list(range(3,3+len(WEATHER)))
    if family=='advisory':numeric+=list(range(3+len(WEATHER),len(ALL_FEATURES)))
    transforms=[('categories',OneHotEncoder(handle_unknown='ignore'),[0,1,2])]
    if numeric:transforms.append(('numeric',StandardScaler(),numeric))
    model=Pipeline([('features',ColumnTransformer(transforms)),
        ('poisson',PoissonRegressor(alpha=alpha,max_iter=500,tol=1e-6))])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always',ConvergenceWarning);model.fit(X,y)
    return model,{'converged':not any(issubclass(w.category,ConvergenceWarning) for w in caught),
        'iterations':int(model[-1].n_iter_)}


def run(package: Path,output: Path):
    manifest=json.loads((package/'manifest.json').read_text())
    if manifest['schema_version']!='station-pilot-package-v1' or manifest['status']!='built':raise ValueError('Wrong package')
    for item in manifest['artifacts']:
        if hashlib.sha256((package/item['path']).read_bytes()).hexdigest()!=item['sha256']:raise ValueError('Package checksum mismatch')
    protocol=json.loads((package/'protocol.json').read_text())
    rows=pq.read_table(package/'features.parquet').to_pylist()
    parts,validation_fit,test_fit=partition(rows,protocol)
    rich_parts={s:[r for r in part if r.get('advisory_covered',False)] for s,part in parts.items()}
    ablation_ready=all(rich_parts.values())
    if ablation_ready:parts=rich_parts
    families=FAMILIES if ablation_ready else FAMILIES[:2]
    output.mkdir(parents=True,exist_ok=False)
    arrays={s:matrix(r) for s,r in parts.items()}
    outcomes={s:np.array([r['departures'] for r in part],dtype=float) for s,part in parts.items()}
    validation={};convergence={};models={};selected={}
    for family in families:
        for alpha in protocol['alphas']:
            name=family+'_'+str(alpha)
            model,quality=fit(arrays['train'],outcomes['train'],family,alpha)
            models[name]=model;convergence[name]=quality
            validation[name]=metrics(outcomes['validation'],model.predict(arrays['validation']))
            print('Station validation',name,validation[name]['mae'],quality,flush=True)
        eligible=[n for n in models if n.startswith(family+'_') and convergence[n]['converged']]
        if not eligible:raise ValueError('No converged validation candidate in '+family)
        selected[family]=min(eligible,key=lambda n:(validation[n]['mae'],n))
    selection={'selected_by_family':selected,'validation':validation,'convergence':convergence,
        'validation_fit_cutoff_utc':str(validation_fit),'test_fit_cutoff_utc':str(test_fit),
        'frozen_at_utc':datetime.now(UTC).isoformat(),'protocol_sha256':manifest['protocol_sha256']}
    # Persist candidate choices BEFORE fitting refits or computing any test metric.
    (output/'selection.json').write_text(json.dumps(selection,indent=2)+'\n')
    refits={};refit_convergence={}
    for family,name in selected.items():
        refits[family],refit_convergence[family]=fit(arrays['refit'],outcomes['refit'],family,float(name.split('_')[1]))
        if not refit_convergence[family]['converged']:raise ValueError('Refit did not converge: '+family)
    refit_stations={r['station_id'] for r in parts['refit']}
    report={**selection,'schema_version':'station-pilot-result-v1', 'package_root':str(package),
        'output_root':str(output),'cohort_stations':manifest['cohort_stations'],
        'split_rows':{s:len(v) for s,v in parts.items()},
        'split_stations':{s:len({r['station_id'] for r in v}) for s,v in parts.items()},
        'test_rows_with_station_absent_from_refit':sum(r['station_id'] not in refit_stations for r in parts['test']),
        'max_label_available_at_utc':{s:str(max(r['label_available_at_utc'] for r in parts[s])) for s in ('train','validation','refit')},
        'refit_convergence':refit_convergence,'test':{},'test_by_horizon':{},'daily_paired_comparison':[],
        'alert_ablation_ready':ablation_ready,
        'alert_ablation_gate':None if ablation_ready else 'Semantically complete, release-eligible alert examples are missing in one or more partitions',
        'rich_schema_rows_by_split':{s:len(v) for s,v in rich_parts.items()},
        'active_model_changed':False,'prospective_forecast':False,'limitations':protocol['limitations']}
    predictions=[];values={};test=parts['test']
    for family,model in refits.items():
        prediction=model.predict(arrays['test']);values[family]=prediction
        report['test'][family]=metrics(outcomes['test'],prediction)
        report['test_by_horizon'][family]={}
        for h in range(3):
            mask=np.array([r['horizon']==h for r in test])
            report['test_by_horizon'][family][str(h)]=metrics(outcomes['test'][mask],prediction[mask])
        for row,p in zip(test,prediction):
            predictions.append({k:row[k] for k in ('station_id','origin_utc','target_start_utc',
                'target_end_utc','horizon','departures')}|{'model':family,'prediction':float(p),
                'fit_cutoff_utc':test_fit,'advisory_active':row['advisory_active']})
    for day in sorted({r['origin_utc'].astimezone(ZONE).date() for r in test}):
        mask=np.array([r['origin_utc'].astimezone(ZONE).date()==day for r in test])
        daily={family:float(np.mean(np.abs(values[family][mask]-outcomes['test'][mask]))) for family in families}
        report['daily_paired_comparison'].append({'day':str(day),'rows':int(mask.sum()),**daily,
            **({'advisory_minus_weather_mae':daily['advisory']-daily['weather']} if ablation_ready else {})})
    report['advisory_minus_weather_mae']=(report['test']['advisory']['mae']-report['test']['weather']['mae']) if ablation_ready else None
    report['advisory_active_nonzero_test_rows']=sum((r.get('advisory_active') or 0)>0 for r in test)
    report['test_unique_origins']=len({r['origin_utc'] for r in test})
    report['test_unique_station_hours']=len({(r['station_id'],r['target_start_utc']) for r in test})
    pq.write_table(pa.Table.from_pylist(predictions),output/'predictions.parquet',compression='zstd')
    joblib.dump({'models':refits,'features':ALL_FEATURES,'protocol':protocol,
        'scope':'32-station conditional historical pilot; not live production'},output/'models.joblib')
    (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'split_rows':report['split_rows'],'test':report['test'],
        'advisory_minus_weather_mae':report['advisory_minus_weather_mae']},indent=2),flush=True)
    return report
