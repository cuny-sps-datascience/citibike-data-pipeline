"""Portable numerical transform, bounded training and forecasting runtime.

Only standard scientific Python is needed in Databricks. No RAID or class paths.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import warnings

import joblib
import numpy as np
import pyarrow.parquet as pq
from sklearn.compose import ColumnTransformer
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import mean_absolute_error, mean_poisson_deviance, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


FEATURES = ['log_profile','log_pooled','hour_sin','hour_cos','dow','month_sin','month_cos',
            'horizon','temperature','temperature_squared','precipitation','wind','wet']


def weather_inputs(hourly, target):
    """Instant temperature/wind at start; precipitation accumulated through end."""
    instant=hourly.get(target,{})
    accumulated=hourly.get(target+timedelta(hours=1),{})
    values=[instant.get('temperature_2m'),accumulated.get('precipitation'),instant.get('wind_speed_10m')]
    valid=all(v is not None and np.isfinite(v) for v in values)
    return values,valid


def select_profile(profiles, cutoff, local_target):
    available=[p for p in profiles if p['available_at_utc']<cutoff]
    if not available:raise ValueError('No profile released before cutoff')
    release=max(p['nominal_release'] for p in available)
    available=[p for p in available if p['nominal_release']==release]
    selected=[p for p in available if (p['dow'],p['hour'])==(local_target.weekday(),local_target.hour)]
    if len(selected)!=1:raise ValueError('Missing or duplicate profile key')
    hour=[p for p in available if p['hour']==local_target.hour]
    denominator=sum(p['covered_hours'] for p in hour)
    if denominator<=0:raise ValueError('No covered profile hours')
    return selected[0],sum(p['departures'] for p in hour)/denominator


def numerical_features(profile, pooled, target: datetime, horizon, temperature, precipitation, wind):
    return [np.log1p(profile),np.log1p(pooled),np.sin(2*np.pi*target.hour/24),
            np.cos(2*np.pi*target.hour/24),target.weekday(),
            np.sin(2*np.pi*target.month/12),np.cos(2*np.pi*target.month/12),horizon,
            temperature,temperature**2,precipitation,wind,float(precipitation>0)]


def metrics(y, prediction):
    prediction=np.maximum(np.asarray(prediction,dtype=float),1e-9)
    if not np.isfinite(prediction).all():
        raise ValueError('Nonfinite predictions')
    return {'rows':len(y),'mae':float(mean_absolute_error(y,prediction)),
            'rmse':float(np.sqrt(mean_squared_error(y,prediction))),
            'poisson_deviance':float(mean_poisson_deviance(y,prediction)),
            'mean_bias':float(np.mean(prediction-y)),'actual_mean':float(np.mean(y)),
            'zero_labels':int(np.sum(y==0))}


def predict(bundle, rows, model_name=None):
    """Rows contain named features plus the two arithmetic baseline values."""
    name=model_name or bundle['selected_model']
    if name in ('profile','pooled'):
        return np.asarray([r[name+'_baseline'] for r in rows],dtype=float)
    values=np.array([[r[f] for f in FEATURES] for r in rows],dtype=float)
    return np.maximum(bundle['models'][name].predict(values),1e-9)


def fit_candidate(rows, name):
    """Fit one fixed-design candidate; preprocessing sees only these rows."""
    kind, alpha = name.split('_')
    numeric=[i for i in range(8 if kind=='calendar' else len(FEATURES)) if i not in (4,7)]
    preprocessing=ColumnTransformer([
        ('numeric',StandardScaler(),numeric),
        ('categories',OneHotEncoder(handle_unknown='ignore'),[4,7])],remainder='drop')
    model=Pipeline([('features',preprocessing),
        ('poisson',PoissonRegressor(alpha=float(alpha.replace('p','.')),max_iter=300,tol=1e-6))])
    X=np.array([[r[f] for f in FEATURES] for r in rows],dtype=float)
    y=np.array([r['departures'] for r in rows],dtype=float)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always',ConvergenceWarning)
        model.fit(X,y)
    return model, {'iterations':int(model[-1].n_iter_),
        'converged':not any(issubclass(w.category,ConvergenceWarning) for w in caught)}


def train(package_root: Path, model_root: Path, out=print):
    """Choose on validation MAE; touch test only after the winner is frozen.

    All comparisons use identical weather-covered keys. Calendar model is also
    retained as fallback for missing prospective weather.
    """
    manifest=json.loads((package_root/'manifest.json').read_text())
    rows=pq.read_table(package_root/'features.parquet').to_pylist()
    covered=[r for r in rows if r['weather_covered']]
    parts={s:[r for r in covered if r['split']==s] for s in ('train','validation','test')}
    if any(not part for part in parts.values()):
        raise ValueError('Every chronological split needs covered examples')
    for row in rows:
        if not (row['profile_available_at_utc']<row['issue_cutoff_utc']<row['origin_utc']):
            raise ValueError('Profile availability leakage')
        if row['weather_assumed_available_at_utc']>=row['issue_cutoff_utc']:
            raise ValueError('Weather initialization availability leakage')
    X=np.array([[r[f] for f in FEATURES] for r in parts['train']],dtype=float)
    y=np.array([r['departures'] for r in parts['train']],dtype=float)
    models={};convergence={}
    for kind in ('calendar','weather'):
        numeric=[i for i in range(8 if kind=='calendar' else len(FEATURES)) if i not in (4,7)]
        for alpha in (.1,1.0):
            name=kind+'_'+str(alpha).replace('.','p')
            preprocessing=ColumnTransformer([
                ('numeric',StandardScaler(),numeric),
                ('categories',OneHotEncoder(handle_unknown='ignore'),[4,7])],remainder='drop')
            model=Pipeline([('features',preprocessing),
                ('poisson',PoissonRegressor(alpha=alpha,max_iter=300,tol=1e-6))])
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always',ConvergenceWarning)
                model.fit(X,y)
            convergence[name]={'iterations':int(model[-1].n_iter_),
                               'converged':not any(issubclass(w.category,ConvergenceWarning) for w in caught)}
            models[name]=model;out('Fitted '+name,flush=True)
    bundle={'models':models,'selected_model':'profile','fallback_model':'profile',
            'features':FEATURES,'scope':'Manhattan total departures; not station forecasts'}
    validation={}
    for name in ['profile','pooled',*models]:
        validation[name]=metrics(np.array([r['departures'] for r in parts['validation']]),
                                 predict(bundle,parts['validation'],name))
    eligible=['profile','pooled']+[n for n in models if convergence[n]['converged']]
    bundle['selected_model']=min(eligible,key=lambda n:validation[n]['mae'])
    fallback=[n for n in eligible if n in ('profile','pooled') or n.startswith('calendar_')]
    bundle['fallback_model']=min(fallback,key=lambda n:validation[n]['mae'])
    # Freeze selection before evaluating test.
    model_root.mkdir(parents=True,exist_ok=False)
    (model_root/'selection.json').write_text(json.dumps({'selected_model':bundle['selected_model'],
        'fallback_model':bundle['fallback_model'],'validation':validation},indent=2)+'\n')
    report={'scope':bundle['scope'],'study':manifest['study'],'selected_model':bundle['selected_model'],
        'fallback_model':bundle['fallback_model'],'validation':validation,'convergence':convergence,
        'feature_names':FEATURES,'split_rows':{s:len(p) for s,p in parts.items()},
        'all_split_rows':{s:sum(r['split']==s for r in rows) for s in parts},
        'weather_delay_hours':manifest['weather_delay_hours'],
        'release_rule':manifest['release_rule'],'test':{},'test_by_horizon':{},'test_by_weather':{}}
    for name in ['profile','pooled',*models]:
        report['test'][name]=metrics(np.array([r['departures'] for r in parts['test']]),predict(bundle,parts['test'],name))
    for horizon in (1,2,3):
        subset=[r for r in parts['test'] if r['horizon']==horizon]
        report['test_by_horizon'][str(horizon)]=metrics(np.array([r['departures'] for r in subset]),predict(bundle,subset))
    for condition in ('wet','dry'):
        subset=[r for r in parts['test'] if bool(r['wet'])==(condition=='wet')]
        if subset:report['test_by_weather'][condition]=metrics(np.array([r['departures'] for r in subset]),predict(bundle,subset))
    joblib.dump(bundle,model_root/'model.joblib')
    restored=joblib.load(model_root/'model.joblib')
    report['reload_max_absolute_error']=float(np.max(np.abs(predict(bundle,parts['test'][:100])-predict(restored,parts['test'][:100]))))
    predictions=[]
    forecast=predict(bundle,parts['test'])
    for row,value in zip(parts['test'],forecast):
        predictions.append({k:row[k] for k in ('origin_utc','target_start_utc','horizon','departures','profile_baseline','pooled_baseline')}|
                           {'prediction':float(value),'model':bundle['selected_model']})
    import pyarrow as pa
    pq.write_table(pa.Table.from_pylist(predictions),model_root/'test_predictions.parquet',compression='zstd')
    report['limitations']=['Borough benchmark is a spatial scope reduction from the station proposal',
        'Weather is a common Manhattan reference point and early archive includes hindcasts',
        'Source release dates and seven-hour weather delay are assumptions',
        'No supported historical alert features; no station-operation interpolation used in this model',
        'Retrospective model selection, not a historical deployment simulation']
    (model_root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report
