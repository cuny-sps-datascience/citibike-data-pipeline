from datetime import datetime,timedelta,timezone
import json
import numpy as np
import pyarrow as pa
import pytest
from citibike_pipeline import availability_runtime as runtime
from citibike_pipeline import availability_live as live

UTC=timezone.utc


def test_finite_sample_interval_rank_and_nonfinite_guard():
    assert runtime.finite_quantile(np.arange(10),.9)==9
    assert runtime.finite_quantile([1,2,3],.5)==2
    with pytest.raises(ValueError):runtime.finite_quantile([])
    with pytest.raises(ValueError):runtime.finite_quantile([float('nan')])


def test_calibration_handles_rare_or_absent_events():
    calibrator=runtime.calibrate(np.array([.1,.2,.3]),np.array([0,0,0]))
    assert isinstance(calibrator,float) and 0<calibrator<1
    assert np.allclose(runtime.calibrated(calibrator,np.array([0,1])),[calibrator,calibrator])
    sigmoid=runtime.calibrate(np.array([.1,.2,.8,.9]),np.array([0,0,1,1]))
    p=runtime.calibrated(sigmoid,np.array([.1,.9]));assert p[0]<p[1]


def tables():
    rows=[];labels=[]
    for i,month in enumerate((4,8,8,9)):
        day=(8,1,15,1)[i];origin=datetime(2026,month,day,4,tzinfo=UTC)
        rows.append({'example_id':i,'origin_utc':origin,'input_observed_at_utc':origin-timedelta(seconds=30),
            'information_available_at_utc':origin-timedelta(days=1),'horizon_minutes':15})
        labels.append({'example_id':i,'target_utc':origin+timedelta(minutes=15),
            'label_observed_at_utc':origin+timedelta(minutes=15,seconds=30)})
    return pa.Table.from_pylist(rows),pa.Table.from_pylist(labels)


def test_partition_strict_source_cutoff_label_window_and_boundary_purge():
    from pathlib import Path
    protocol=json.loads(Path('docs/availability_protocol.json').read_text())
    f,l=tables();parts=runtime.partition(f,l,protocol)
    assert [parts[k].sum() for k in ('train','validation','refit','calibration','test')]==[1,1,2,1,1]
    bad=f.to_pylist();bad[0]['input_observed_at_utc']=bad[0]['origin_utc']
    with pytest.raises(ValueError,match='Future/stale'):runtime.partition(pa.Table.from_pylist(bad),l,protocol)
    bad=l.to_pylist();bad[0]['label_observed_at_utc']+=timedelta(minutes=3)
    with pytest.raises(ValueError,match='endpoint'):runtime.partition(f,pa.Table.from_pylist(bad),protocol)
    bad=l.to_pylist();bad[0]['example_id']=99
    with pytest.raises(ValueError,match='key mismatch'):runtime.partition(f,pa.Table.from_pylist(bad),protocol)
    # Maximum horizon purge applies even to the short-horizon row.
    bad=f.to_pylist();origin=datetime(2026,8,1,2,tzinfo=UTC)
    extra=bad[0]|{'example_id':4,'origin_utc':origin,'input_observed_at_utc':origin-timedelta(seconds=30)}
    ll=l.to_pylist()+[{'example_id':4,'target_utc':origin+timedelta(minutes=15),'label_observed_at_utc':origin+timedelta(minutes=16)}]
    parts=runtime.partition(pa.Table.from_pylist(bad+[extra]),pa.Table.from_pylist(ll),protocol)
    assert not parts['train'][-1]


def live_fixture():
    issue=datetime(2026,10,16,21,tzinfo=UTC);observed=issue-timedelta(seconds=1)
    station={'uuid':'u','station_id':'0001.01','latitude':40.7,'longitude':-74.,'name':'A'}
    info={'last_updated':int(observed.timestamp())-20,'data':{'stations':[{'station_id':'u',
        'short_name':'0001.01','lat':40.7,'lon':-74.,'capacity':20,'name':'A'}]}}
    status={'last_updated':int(observed.timestamp())-20,'data':{'stations':[{'station_id':'u',
        'last_reported':int(observed.timestamp())-30,'num_bikes_available':0,'num_docks_available':20,
        'num_ebikes_available':0,'num_bikes_disabled':0,'num_docks_disabled':0,
        'is_installed':1,'is_renting':1,'is_returning':1}]}}
    return station,info,status,observed,issue


def test_empty_open_station_is_valid_but_closed_stale_missing_are_unknown():
    c,i,s,at,issue=live_fixture()
    rows,X,states=live.inputs([c],i,s,at,at,issue,runtime.FEATURES)
    assert len(rows)==4 and X.shape==(4,len(runtime.FEATURES))
    assert states[0]['current_bikes']==0 and states[0]['status']=='ready'
    assert rows[0]['target_utc']==issue+timedelta(minutes=15)
    for kind in ('closed','stale','missing','moved','negative','duplicate'):
        c,i,s,at,issue=live_fixture()
        if kind=='closed':s['data']['stations'][0]['is_returning']=0
        if kind=='stale':s['last_updated']-=1000
        if kind=='missing':s['data']['stations'][0].pop('num_docks_available')
        if kind=='moved':i['data']['stations'][0]['lon']=-73.9
        if kind=='negative':s['data']['stations'][0]['num_bikes_available']=-1
        if kind=='duplicate':s['data']['stations']*=2
        rows,X,states=live.inputs([c],i,s,at,at,issue,runtime.FEATURES)
        assert not rows and states[0]['status']!='ready' and states[0]['current_bikes'] is None


def test_forecast_interval_nonnegative_and_risk_separate_from_count_point():
    c,i,s,at,issue=live_fixture();rows,X,_=live.inputs([c],i,s,at,at,issue,runtime.FEATURES)
    bundle={'count_models':{'bikes':None,'docks':None},'risk_models':{'bikes':.2,'docks':.3},
        'calibrators':{t+'_'+str(h):.25 for t in runtime.TARGETS for h in (15,30,60,180)},
        'interval_radius':{t+'_'+str(h):3. for t in runtime.TARGETS for h in (15,30,60,180)}}
    result=runtime.forecast(bundle,X)
    assert np.all(result['bikes']['prediction']==0)
    assert np.all(result['bikes']['lower']==0) and np.all(result['bikes']['upper']==3)
    assert np.all(result['bikes']['p_zero']==.25)
    assert np.all(result['docks']['upper']==23) # uncertainty is not silently capacity-clipped


def test_ensemble_count_routing_selects_models_by_horizon():
    from citibike_pipeline import availability_ensemble_runtime as ensemble
    class Constant:
        def __init__(self,value):self.value=value
        def predict(self,X):return np.full(len(X),self.value)
    X=np.zeros((4,len(ensemble.FEATURES)))
    X[:,ensemble.FEATURES.index('horizon_minutes')]=[15,30,60,180]
    X[:,ensemble.FEATURES.index('current_bikes')]=[1,2,3,4]
    bundle={'count_routing':{'bikes':{'15':'persistence','30':'forest_depth_10','60':'boosting_15','180':'persistence'}},
        'count_models':{'bikes':{'forest_depth_10':Constant(7),'boosting_15':Constant(8)}}}
    assert ensemble.routed_count_prediction(bundle,'bikes',X).tolist()==[1,7,8,4]


def test_ensemble_candidate_grid_and_tie_break_are_frozen():
    from pathlib import Path
    from citibike_pipeline import availability_ensemble_runtime as ensemble
    protocol=json.loads(Path('docs/availability_ensemble_protocol.json').read_text())
    assert [r['id'] for r in ensemble.candidate_specs(protocol)]==[
        'boosting_15','boosting_31','forest_depth_10','forest_depth_16']
    assert min(('forest_depth_16','boosting_31','persistence','boosting_15'),
        key=lambda name:ensemble.selection_key(name,1.0))=='persistence'
