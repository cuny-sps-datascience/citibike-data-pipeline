"""Portable monthly backtests with release-gated fitting, selection and refitting."""
from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

NY=ZoneInfo('America/New_York')
CANDIDATES=('profile','pooled','calendar_0p1','calendar_1p0','weather_0p1','weather_1p0')


def release_at(month):
    year,number=map(int,month.split('-'))
    return datetime.combine(date(year,number,calendar.monthrange(year,number)[1])+timedelta(days=45),
        datetime.min.time(),timezone.utc)


def month_start(month):
    return datetime.fromisoformat(month+'-01').replace(tzinfo=NY).astimezone(timezone.utc)


def next_month(month):
    year,number=map(int,month.split('-'))
    return f'{year+(number==12):04d}-{number%12+1:02d}'


def label_availability(rows, cohorts):
    dependencies=defaultdict(set)
    for row in cohorts:
        dependencies[row['hour_utc']].add(row['nominal_release'])
    result=[]
    for row in rows:
        target=row['target_start_utc']
        own=target.astimezone(NY).strftime('%Y-%m')
        months=dependencies[target]|{own}  # zero labels still need their full target-month release
        result.append(row|{'label_available_at_utc':max(release_at(m) for m in months),
            'label_source_months':','.join(sorted(months))})
    return result


def plan_fold(rows, month):
    start=month_start(month);end=month_start(next_month(month));cutoff=start-timedelta(minutes=5)
    released=[r for r in rows if r['label_available_at_utc']<cutoff and r['target_end_utc']<=cutoff]
    months=sorted({r['target_start_utc'].astimezone(NY).strftime('%Y-%m') for r in rows})
    validation=None
    for candidate in reversed([m for m in months if m<month]):
        first,last=month_start(candidate),month_start(next_month(candidate))
        all_labels=[r for r in rows if first<=r['target_start_utc']<last]
        # The entire reference month's labels must be mature, including spillover dependencies.
        if all_labels and max(r['label_available_at_utc'] for r in all_labels)<cutoff:
            validation=candidate;break
    if validation is None:raise ValueError('No completely released validation month')
    vs,ve=month_start(validation),month_start(next_month(validation))
    train=[r for r in released if r['weather_covered'] and r['target_end_utc']<=vs]
    valid=[r for r in released if r['weather_covered'] and vs<=r['origin_utc'] and r['origin_utc']+timedelta(hours=3)<=ve]
    refit=[r for r in released if r['weather_covered']]
    test=[r for r in rows if r['weather_covered'] and start<=r['origin_utc'] and r['origin_utc']+timedelta(hours=3)<=end]
    if any(not p for p in (train,valid,refit,test)):raise ValueError('Empty rolling partition')
    if max(r['target_end_utc'] for r in train)>min(r['origin_utc'] for r in valid):
        raise ValueError('Training/validation target overlap')
    return {'month':month,'fit_cutoff_utc':cutoff,'validation_month':validation,
            'train':train,'validation':valid,'refit':refit,'test':test}


def run(rows, cohorts, output_root: Path, runtime, months=('2026-06','2026-07','2026-08')):
    rows=label_availability(rows,cohorts)
    for row in rows:
        if not row['profile_available_at_utc']<row['issue_cutoff_utc']<row['origin_utc']:
            raise ValueError('Input profile cutoff violation')
        if not row['weather_assumed_available_at_utc']<row['issue_cutoff_utc']:
            raise ValueError('Weather cutoff violation')
    output_root.mkdir(parents=True,exist_ok=False)
    folds=[];predictions=[]
    for month in months:
        fold=plan_fold(rows,month);models={};convergence={}
        for name in CANDIDATES[2:]:
            models[name],convergence[name]=runtime.fit_candidate(fold['train'],name)
        bundle={'models':models,'selected_model':'profile'}
        y=np.array([r['departures'] for r in fold['validation']])
        eligible=list(CANDIDATES[:2])+[n for n in models if convergence[n]['converged']]
        validation={n:runtime.metrics(y,runtime.predict(bundle,fold['validation'],n)) for n in eligible}
        winner=min(eligible,key=lambda n:(validation[n]['mae'],CANDIDATES.index(n)))
        fallback=min([n for n in eligible if n in CANDIDATES[:2] or n.startswith('calendar_')],
            key=lambda n:(validation[n]['mae'],CANDIDATES.index(n)))
        # Persist selection before any fold test metric is computed.
        selection={'month':month,'fit_cutoff_utc':fold['fit_cutoff_utc'].isoformat(),
            'validation_month':fold['validation_month'],'selected_model':winner,'fallback_model':fallback,
            'validation':validation,'convergence':convergence}
        (output_root/(month+'_selection.json')).write_text(json.dumps(selection,indent=2)+'\n')
        refit_convergence={}
        for name in dict.fromkeys((winner,fallback)):
            if name not in CANDIDATES[:2]:
                models[name],refit_convergence[name]=runtime.fit_candidate(fold['refit'],name)
                if not refit_convergence[name]['converged']:
                    raise ValueError('Selected refit did not converge; no post-test substitution')
        bundle['selected_model']=winner
        y=np.array([r['departures'] for r in fold['test']]);comparison={}
        for name in dict.fromkeys(('profile','pooled',fallback,winner)):
            values=runtime.predict(bundle,fold['test'],name)
            comparison[name]=runtime.metrics(y,values)
            for row,value in zip(fold['test'],values):
                predictions.append({k:row[k] for k in ('origin_utc','target_start_utc','target_end_utc','horizon','departures')}|
                    {'fold_month':month,'model':name,'prediction':float(value),'is_selected':name==winner,
                     'fit_cutoff_utc':fold['fit_cutoff_utc'],'validation_month':fold['validation_month']})
        record=selection|{'refit_convergence':refit_convergence,
            'partition_rows':{n:len(fold[n]) for n in ('train','validation','refit','test')},
            'max_label_available_at_utc':{n:max(r['label_available_at_utc'] for r in fold[n]).isoformat()
                                        for n in ('train','validation','refit')},
            'latest_refit_target_end_utc':max(r['target_end_utc'] for r in fold['refit']).isoformat(),
            'test':comparison}
        folds.append(record);print(month,winner,comparison[winner]['mae'],flush=True)
    pq.write_table(pa.Table.from_pylist(predictions),output_root/'predictions.parquet',compression='zstd')
    selected=[r for r in predictions if r['is_selected']]
    baseline=[r for r in predictions if r['model']=='profile']
    report={'schema_version':'release-aware-rolling-v1','months':list(months),'folds':folds,
        'selected_pooled_metrics':runtime.metrics(np.array([r['departures'] for r in selected]),[r['prediction'] for r in selected]),
        'profile_pooled_metrics':runtime.metrics(np.array([r['departures'] for r in baseline]),[r['prediction'] for r in baseline]),
        'design':{'candidates':list(CANDIDATES),'validation':'latest completely matured target month; earlier training targets only',
            'refit':'selected candidate and fallback on all matured weather-covered rows before fold cutoff',
            'label_rule':'max(target-month release, all supplying source-cohort releases), strict before fit cutoff',
            'release_assumption':'source month end +45 days at 00:00 UTC; unverified',
            'cutoff':'five minutes before first monthly origin in America/New_York',
            'test_boundary':'all three-target origins wholly inside fold month; original split boundary exclusions retained'},
        'limitations':['New retrospective analysis on previously inspected months, not a fresh untouched test set.',
            'Temporal gates use assumed publication delays and archived weather includes early hindcasts.',
            'Overlapping horizons are not independent observations; station and alert models remain excluded.'],
        'active_model_changed':False}
    (output_root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report
