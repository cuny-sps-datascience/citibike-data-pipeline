"""Portable inventory forecasts, separately calibrated risks and count intervals."""
from __future__ import annotations
from datetime import datetime,timedelta,timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import hashlib,json,math
import joblib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.ensemble import HistGradientBoostingRegressor,HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import mean_absolute_error,mean_squared_error,brier_score_loss,average_precision_score
from threadpoolctl import threadpool_limits

FEATURES=['station_index','horizon_minutes','current_bikes','current_docks','current_ebikes',
    'disabled_bikes','disabled_docks','capacity','local_hour','dow','hour_sin','hour_cos']
TARGETS=('bikes','docks');ZONE=ZoneInfo('America/New_York');UTC=timezone.utc


def matrix(table):return np.column_stack([table[k].to_numpy(zero_copy_only=False) for k in FEATURES]).astype(float)


def utc_boundary(value):return datetime.fromisoformat(value).replace(tzinfo=ZONE).astimezone(UTC)


def partition(features,labels,protocol):
    if not np.array_equal(features['example_id'].to_numpy(),labels['example_id'].to_numpy()):raise ValueError('Label/predictor key mismatch')
    origins=features['origin_utc'].to_pylist();observed=labels['label_observed_at_utc'].to_pylist()
    for origin,input_at,info_at,target,label_at,h in zip(origins,features['input_observed_at_utc'].to_pylist(),
        features['information_available_at_utc'].to_pylist(),labels['target_utc'].to_pylist(),observed,
        features['horizon_minutes'].to_pylist()):
        if not input_at<origin or (origin-input_at).total_seconds()>120:raise ValueError('Future/stale input')
        if not info_at<origin:raise ValueError('Future information')
        if target!=origin+timedelta(minutes=h) or not 0<=(label_at-target).total_seconds()<=120:
            raise ValueError('Invalid future endpoint label')
    # Require complete maximum-horizon window inside each temporal partition.
    masks={}
    for name in ('train','validation','refit','calibration','test'):
        first,last=map(utc_boundary,protocol[name+'_local'])
        masks[name]=np.array([first<=o and o+timedelta(minutes=182)<last and label_at<last
            for o,label_at in zip(origins,observed)])
        if not masks[name].any():raise ValueError('Empty '+name+' partition')
    return masks


def finite_quantile(scores,coverage=.9):
    scores=np.asarray(scores,dtype=float)
    if not len(scores) or not np.isfinite(scores).all():raise ValueError('Invalid calibration residuals')
    return float(np.sort(scores)[min(math.ceil((len(scores)+1)*coverage),len(scores))-1])


def logit(p):
    p=np.clip(p,1e-6,1-1e-6);return np.log(p/(1-p)).reshape(-1,1)


def probability(model,X):
    if isinstance(model,float):return np.full(len(X),model)
    return model.predict_proba(X)[:,1]


def calibrate(raw,y):
    if len(np.unique(y))<2:return float((sum(y)+1)/(len(y)+2))
    return LogisticRegression(C=1.0,max_iter=200).fit(logit(raw),y)


def calibrated(calibrator,p):
    if isinstance(calibrator,float):return np.full(len(p),calibrator)
    return calibrator.predict_proba(logit(p))[:,1]


def count_prediction(bundle,target,X):
    model=bundle['count_models'][target]
    current=X[:,FEATURES.index('current_'+target)]
    return current.copy() if model is None else np.maximum(model.predict(X),0)


def forecast(bundle,X):
    result={};horizons=X[:,FEATURES.index('horizon_minutes')].astype(int)
    for target in TARGETS:
        point=count_prediction(bundle,target,X);raw=probability(bundle['risk_models'][target],X)
        p=np.empty(len(X));q=np.empty(len(X))
        for h in np.unique(horizons):
            mask=horizons==h;key=target+'_'+str(h)
            p[mask]=calibrated(bundle['calibrators'][key],raw[mask]);q[mask]=bundle['interval_radius'][key]
        result[target]={'prediction':point,'lower':np.maximum(0,point-q),'upper':point+q,'p_zero':p}
    return result


def count_metrics(y,p):return {'mae':float(mean_absolute_error(y,p)),'rmse':float(np.sqrt(mean_squared_error(y,p)))}


def run(package: Path,output: Path):
    manifest=json.loads((package/'manifest.json').read_text());protocol=json.loads((package/'protocol.json').read_text())
    if manifest['schema_version']!='availability-package-v1' or manifest['status']!='built':raise ValueError('Wrong package')
    for item in manifest['artifacts']:
        if hashlib.sha256((package/item['path']).read_bytes()).hexdigest()!=item['sha256']:raise ValueError('Package checksum mismatch')
    f=pq.read_table(package/'features.parquet');l=pq.read_table(package/'labels.parquet')
    masks=partition(f,l,protocol);X=matrix(f);Y={t:l[t].to_numpy().astype(float) for t in TARGETS}
    output.mkdir(parents=True,exist_ok=False);selection={};validation={};models={}
    with threadpool_limits(limits=4):
        for target in TARGETS:
            validation[target]={};current=X[:,FEATURES.index('current_'+target)]
            val=masks['validation'];train=masks['train']
            validation[target]['persistence']=count_metrics(Y[target][val],current[val]);candidates={}
            for leaves in (15,31):
                name='boosting_'+str(leaves)
                model=HistGradientBoostingRegressor(max_iter=protocol['boosting_iterations'],
                    max_leaf_nodes=leaves,min_samples_leaf=80,l2_regularization=1.0,
                    early_stopping=False,categorical_features=[0],random_state=622)
                model.fit(X[train],Y[target][train]);candidates[name]=model
                validation[target][name]=count_metrics(Y[target][val],np.maximum(model.predict(X[val]),0))
                print('Availability validation',target,name,validation[target][name],flush=True)
            selection[target]=min(validation[target],key=lambda n:(validation[target][n]['mae'],n!='persistence',n))
        selection_record={'selected_counts':selection,'validation':validation,
            'frozen_at_utc':datetime.now(UTC).isoformat(),'test_metrics_viewed':False}
        (output/'selection.json').write_text(json.dumps(selection_record,indent=2)+'\n')
        bundle={'count_models':{},'risk_models':{},'calibrators':{},'interval_radius':{},
            'features':FEATURES,'cohort':json.loads((package/'cohort.json').read_text()),'protocol':protocol,
            'selection':selection,'scope':'conditional station inventory and point-in-time zero-count risk'}
        refit=masks['refit'];cal=masks['calibration'];horizon=X[:,FEATURES.index('horizon_minutes')].astype(int)
        calibration={};prevalence={}
        for target in TARGETS:
            winner=selection[target]
            if winner=='persistence':bundle['count_models'][target]=None
            else:
                model=HistGradientBoostingRegressor(max_iter=protocol['boosting_iterations'],
                    max_leaf_nodes=int(winner.split('_')[1]),min_samples_leaf=80,
                    l2_regularization=1.0,early_stopping=False,categorical_features=[0],random_state=622)
                model.fit(X[refit],Y[target][refit]);bundle['count_models'][target]=model
            yzero=(Y[target]==0).astype(int)
            if len(np.unique(yzero[refit]))<2:risk=float((yzero[refit].sum()+1)/(refit.sum()+2))
            else:
                risk=HistGradientBoostingClassifier(max_iter=protocol['boosting_iterations'],
                    max_leaf_nodes=15,min_samples_leaf=80,l2_regularization=1.0,
                    early_stopping=False,categorical_features=[0],random_state=622)
                risk.fit(X[refit],yzero[refit])
            bundle['risk_models'][target]=risk
            point=count_prediction(bundle,target,X[cal]);raw=probability(risk,X[cal])
            for h in protocol['horizons_minutes']:
                hm=horizon[cal]==h;key=target+'_'+str(h)
                if not hm.any():raise ValueError('Missing calibration horizon')
                residual=np.abs(Y[target][cal][hm]-point[hm]);q=finite_quantile(residual)
                bundle['interval_radius'][key]=q
                bundle['calibrators'][key]=calibrate(raw[hm],yzero[cal][hm])
                calibration[key]={'rows':int(hm.sum()),'zero_events':int(yzero[cal][hm].sum()),'interval_radius':q}
                hmfit=refit&(horizon==h);prevalence[key]=float((yzero[hmfit].sum()+1)/(hmfit.sum()+2))
        # Freeze all fitted/calibrated components before September metrics.
        joblib.dump(bundle,output/'models.joblib')
        (output/'calibration.json').write_text(json.dumps(calibration,indent=2)+'\n')
        test=masks['test'];pred=forecast(bundle,X[test]);report={'schema_version':'availability-result-v1',
            **selection_record,'calibration':calibration,'split_rows':{k:int(v.sum()) for k,v in masks.items()},
            'feature_names':FEATURES,'test_by_horizon':{},'risk_reliability':{},'nominal_interval_coverage':.9,
            'cohort_stations':32,'test_stations':len(set(np.array(f['station_id'].to_pylist())[test])),
            'scope':bundle['scope'],'active_departure_model_changed':False,'limitations':protocol['limitations']}
        for h in protocol['horizons_minutes']:
            hm=horizon[test]==h;record={'rows':int(hm.sum())}
            for target in TARGETS:
                y=Y[target][test][hm];p=pred[target];zero=(y==0).astype(int)
                baseline=X[test][hm,FEATURES.index('current_'+target)];prob=p['p_zero'][hm]
                low=p['lower'][hm];high=p['upper'][hm]
                record[target]={'model':count_metrics(y,p['prediction'][hm]),'persistence':count_metrics(y,baseline),
                    'interval_coverage':float(np.mean((low<=y)&(y<=high))),
                    'mean_interval_width':float(np.mean(high-low)), 'zero_events':int(zero.sum()),
                    'brier':float(brier_score_loss(zero,prob)),
                    'persistence_brier':float(brier_score_loss(zero,(baseline==0).astype(float))),
                    'prevalence_brier':float(brier_score_loss(zero,np.full(len(zero),prevalence[target+'_'+str(h)]))),
                    'average_precision':float(average_precision_score(zero,prob)) if zero.any() else None}
                bins=[]
                for i in range(10):
                    b=(prob>=i/10)&(prob<(i+1)/10 if i<9 else prob<=1)
                    if b.any():bins.append({'lower':i/10,'rows':int(b.sum()),'mean_probability':float(prob[b].mean()),'observed_rate':float(zero[b].mean())})
                report['risk_reliability'][target+'_'+str(h)]=bins
            report['test_by_horizon'][str(h)]=record
        indices=np.flatnonzero(test);selected_features=f.take(pa.array(indices));selected_labels=l.take(pa.array(indices))
        prediction_table=pa.table({k:selected_features[k] for k in ('example_id','station_id','origin_utc','horizon_minutes','current_bikes','current_docks')})
        for k in ('target_utc','label_observed_at_utc','bikes','docks'):prediction_table=prediction_table.append_column(k,selected_labels[k])
        for target in TARGETS:
            for name,values in pred[target].items():prediction_table=prediction_table.append_column(target+'_'+name,pa.array(values))
        pq.write_table(prediction_table,output/'predictions.parquet',compression='zstd')
        report['test_forecast_rows']=len(indices);report['package_root']=str(package);report['output_root']=str(output)
        # Round-trip against persisted predictions, independently of object identity.
        reloaded=joblib.load(output/'models.joblib');sample=min(100,len(indices))
        check=forecast(reloaded,X[test][:sample]);report['save_reload_checked_rows']=sample
        report['save_reload_max_difference']=max(float(np.max(np.abs(check[t][k]-pred[t][k][:sample]))) for t in TARGETS for k in pred[t])
        if report['save_reload_max_difference']!=0:raise ValueError('Save/reload parity failed')
        (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'split_rows':report['split_rows'],'selected_counts':selection,
        'test_by_horizon':report['test_by_horizon']},indent=2),flush=True);return report
