from datetime import date, datetime, timedelta, timezone
import csv, io, json, zipfile
import pytest
from citibike_pipeline.station_history import (supported_hours, snapshot_good, information,
    stable, DatedGTFS, label_available, advisory_snapshot, period_overlap)
from citibike_pipeline.station_pilot_runtime import partition, in_partition

UTC=timezone.utc
T=datetime(2026,4,8,tzinfo=UTC)


def poll(seconds,**changes):
    epoch=int(T.timestamp())+seconds
    return {'polled_at':epoch,'ts':epoch-20,'last_reported':epoch-30,
        'is_installed':1,'is_renting':1,'num_bikes_available':0,**changes}


def test_quiet_empty_stations_have_exposure_without_inventory_changes():
    assert snapshot_good(poll(0))
    assert supported_hours([poll(t) for t in range(0,3601,60)])=={T}


@pytest.mark.parametrize('bad', ['gap','renting','installed','provider','station'])
def test_interior_failure_never_creates_full_zero_hour(bad):
    rows=[poll(t) for t in range(0,3601,60)]
    if bad=='gap':rows=[r for i,r in enumerate(rows) if i not in (29,30)]
    if bad=='renting':rows[30]['is_renting']=0
    if bad=='installed':rows[30]['is_installed']=0
    if bad=='provider':rows[30]['ts']-=1000
    if bad=='station':rows[30]['last_reported']-=1000
    assert supported_hours(rows)==set()


def test_no_extrapolation_before_first_or_after_last_observation():
    assert supported_hours([poll(t) for t in range(1,3601,60)])==set()
    assert supported_hours([poll(t) for t in range(0,3600,60)])==set()
    with pytest.raises(ValueError,match='Duplicate'):supported_hours([poll(0),poll(0)])


def test_information_is_delayed_and_ambiguous_ids_are_removed(tmp_path):
    s={'station_id':'u','short_name':'004.01','lat':40.7,'lon':-74,'capacity':20}
    p=tmp_path/'i.json';p.write_text(json.dumps({'last_updated':int(T.timestamp())-1,'data':{'stations':[s]}}))
    row=information(p,date(2026,4,7))['u']
    assert row['station_id']=='004.01'
    assert row['assumed_available_at_utc']==T+timedelta(hours=1)
    assert stable(row,row)
    assert not stable(row,row|{'longitude':-73.9})
    p.write_text(json.dumps({'last_updated':int(T.timestamp())-1,'data':{'stations':[s,s|{'station_id':'v'}]}}))
    assert information(p,date(2026,4,7))=={}


def reference():
    return {'routes':{'A':{'route_type':'1'}},'stop_parents':{'P':'P','PN':'P','Q':'Q','QN':'Q'},
        'route_stops':{'A':{'PN','QN'}},'route_direction_stops':{'A|0':{'PN','QN'}}}


def alert(**changes):
    observed=datetime(2026,9,1,3,54,tzinfo=UTC)
    return {'source_file':'s','fetch_timestamp':observed,'feed_timestamp':int(observed.timestamp())-60,
        'incrementality':0,'is_deleted':False,'entity_id':'e','agency_id':'MTASBWY','route_id':'A',
        'stop_id':'PN','direction_id':None,'active_periods_json':json.dumps([
            {'start':int(observed.timestamp())-3600,'end':int(observed.timestamp())-1800},
            {'start':int(observed.timestamp())+360,'end':None}]),**changes}


def test_all_display_periods_and_selector_entity_deduplication():
    origin=datetime(2026,9,1,4,tzinfo=UTC);cutoff=origin-timedelta(minutes=5)
    r=alert();result,audit=advisory_snapshot([r,r|{'stop_id':'P'}],cutoff,reference(),{'bike':{'P'}},origin)
    assert audit['reason']=='covered'
    assert result['bike']['advisory_active']==1
    assert result['bike']['advisory_next3h']==1
    assert result['bike']['advisory_unspecified']==0


def test_restrictions_are_not_broadened_and_missing_is_not_zero():
    origin=datetime(2026,9,1,4,tzinfo=UTC);cutoff=origin-timedelta(minutes=5)
    for changes in ({'trip_id':''},{'route_id':'unknown'},{'feed_timestamp':1},{'incrementality':1}):
        values,audit=advisory_snapshot([alert(**changes)],cutoff,reference(),{'bike':{'P'}},origin)
        assert values is None
    with pytest.raises(ValueError,match='Future'):
        advisory_snapshot([alert(fetch_timestamp=origin)],cutoff,reference(),{'bike':{'P'}},origin)


def test_selector_fields_are_and_and_entities_are_or():
    origin=datetime(2026,9,1,4,tzinfo=UTC);cutoff=origin-timedelta(minutes=5)
    result,_=advisory_snapshot([alert(stop_id='QN')],cutoff,reference(),{'bike':{'P'}},origin)
    assert result['bike']['advisory_active']==0
    result,_=advisory_snapshot([alert(stop_id='PN'),alert(stop_id='QN')],cutoff,reference(),{'bike':{'P','Q'}},origin)
    assert result['bike']['advisory_active']==1


def test_period_end_is_exclusive_and_no_period_is_unknown():
    t=datetime(2026,9,1,tzinfo=UTC)
    assert period_overlap([],t,t+timedelta(hours=1)) is None
    assert not period_overlap([{'end':int(t.timestamp())}],t,t+timedelta(hours=1))
    with pytest.raises(ValueError):period_overlap([{'start':2,'end':1}],t,t+timedelta(hours=1))


def test_zero_label_requires_own_month_release_and_spillover_delays():
    hour=datetime(2026,6,30,22,tzinfo=UTC)
    assert label_available(hour,[])==datetime(2026,8,14,tzinfo=UTC)
    assert label_available(hour,['2026-07'])==datetime(2026,9,14,tzinfo=UTC)


def write_gtfs(path):
    tables={
        'routes.txt':[['route_id','agency_id','route_short_name','route_type'],['A','MTA NYCT','A','1']],
        'stops.txt':[['stop_id','parent_station','stop_lat','stop_lon'],['P','','40.7','-74'],['PN','P','40.7','-74']],
        'trips.txt':[['trip_id','service_id','route_id','direction_id'],['t','weekday','A','0']],
        'stop_times.txt':[['trip_id','stop_id'],['t','PN']],
        'calendar.txt':[['service_id','start_date','end_date','monday','tuesday','wednesday','thursday','friday','saturday','sunday'],
            ['weekday','20260101','20261231','1','1','1','1','1','0','0']],
        'calendar_dates.txt':[['service_id','date','exception_type'],['weekday','20260408','2'],['weekday','20260411','1']]}
    with zipfile.ZipFile(path,'w') as z:
        for name,rows in tables.items():
            s=io.StringIO();csv.writer(s).writerows(rows);z.writestr(name,s.getvalue())


def test_historical_gtfs_calendar_and_exceptions_are_applied(tmp_path):
    p=tmp_path/'mta_gtfs_202602270137.zip';write_gtfs(p);gtfs=DatedGTFS(p)
    assert gtfs.available==datetime(2026,2,27,1,37,tzinfo=UTC)
    assert gtfs.reference(date(2026,4,7))['route_stops']['A']=={'PN'}
    assert not gtfs.reference(date(2026,4,8))['route_stops']
    assert gtfs.reference(date(2026,4,11))['route_stops']['A']=={'PN'}


def example(origin,available):
    cutoff=origin-timedelta(minutes=5)
    return {'station_id':'s','origin_utc':origin,'issue_cutoff_utc':cutoff,'horizon':0,
        'target_start_utc':origin,'target_end_utc':origin+timedelta(hours=1),'departures':0,
        'label_available_at_utc':available,'weather_assumed_available_at_utc':cutoff-timedelta(hours=1),
        'information_assumed_available_at_utc':cutoff-timedelta(days=1),
        'gtfs_archive_available_at_utc':cutoff-timedelta(days=2),
        'advisory_observed_at_utc':cutoff-timedelta(seconds=30),'advisory_feed_age_seconds':60}


def test_partitions_enforce_label_maturity_and_predictor_availability():
    p={'validation_fit_origin_local':'2026-06-16T00:00:00-04:00','test_fit_origin_local':'2026-09-01T00:00:00-04:00',
       'train_target_local':['2026-04-08','2026-05-01'],'validation_target_local':['2026-06-16','2026-07-01'],
       'refit_target_local':['2026-04-08','2026-07-01'],'test_target_local':['2026-09-01','2026-10-01']}
    train=example(datetime(2026,4,8,4,tzinfo=UTC),datetime(2026,6,14,tzinfo=UTC))
    val=example(datetime(2026,6,16,4,tzinfo=UTC),datetime(2026,8,14,tzinfo=UTC))
    test=example(datetime(2026,9,1,4,tzinfo=UTC),datetime(2026,11,14,tzinfo=UTC))
    late=val|{'origin_utc':val['origin_utc']+timedelta(hours=1),'issue_cutoff_utc':val['issue_cutoff_utc']+timedelta(hours=1),
        'target_start_utc':val['target_start_utc']+timedelta(hours=1),'target_end_utc':val['target_end_utc']+timedelta(hours=1),
        'advisory_observed_at_utc':val['advisory_observed_at_utc']+timedelta(hours=1),
        'label_available_at_utc':datetime(2026,9,14,tzinfo=UTC)}
    parts,_,_=partition([train,val,late,test],p)
    assert parts['validation']==[val]
    assert late not in parts['refit']
    assert len(parts['test'])==1
    with pytest.raises(ValueError,match='Future predictor'):
        partition([train,val,test|{'weather_assumed_available_at_utc':test['issue_cutoff_utc']}],p)
    with pytest.raises(ValueError,match='Duplicate'):partition([train,train,val,test],p)


def test_three_target_partition_boundary_purge():
    assert not in_partition({'origin_utc':datetime(2026,7,1,2,tzinfo=UTC)},['2026-06-16','2026-07-01'])
    assert in_partition({'origin_utc':datetime(2026,7,1,1,tzinfo=UTC)},['2026-06-16','2026-07-01'])


def test_ensemble_tree_features_are_explicit_and_candidate_grid_is_bounded():
    from pathlib import Path
    from citibike_pipeline import station_ensemble_runtime as ensemble
    protocol=json.loads(Path('docs/station_ensemble_protocol.json').read_text())
    origin=datetime(2026,9,1,4,tzinfo=UTC)
    row=example(origin,datetime(2026,11,14,tzinfo=UTC))|{
        'station_hour':'s|1','dow_hour':'1|1','temperature':10.,'temperature_squared':100.,
        'precipitation':0.,'wind':2.,'wet':0}
    row['target_start_utc']=origin+timedelta(hours=1);row['target_end_utc']=origin+timedelta(hours=2);row['horizon']=1
    tree=ensemble.values([row],'random_forest','weather')
    local=row['target_start_utc'].astimezone(ensemble.ZONE)
    assert tree[0,:4].tolist()==['s',local.hour,local.weekday(),1]
    specs=ensemble.candidate_specs(protocol)
    assert len(specs)==10
    assert sum(r['family']=='random_forest' for r in specs)==4
    assert len({r['id'] for r in specs})==10


def test_ensemble_tie_breaks_prefer_declared_family_and_smaller_forest():
    from citibike_pipeline.station_ensemble_runtime import selection_key
    specs=[
        {'id':'f16','family':'random_forest','max_depth':16},
        {'id':'f10','family':'random_forest','max_depth':10},
        {'id':'h','family':'histogram_boosting'},
        {'id':'p','family':'poisson','alpha':1.0},
    ]
    assert min(specs,key=lambda r:selection_key(r,1.0))['id']=='p'
    assert min(specs[:2],key=lambda r:selection_key(r,1.0))['id']=='f10'


@pytest.mark.parametrize('complete_alerts',[False,True])
def test_portable_training_preserves_missing_alert_gate_and_common_keys(tmp_path,complete_alerts):
    import hashlib
    import pyarrow as pa
    import pyarrow.parquet as pq
    from citibike_pipeline.station_pilot_runtime import run
    protocol={'validation_fit_origin_local':'2026-06-16T00:00:00-04:00',
        'test_fit_origin_local':'2026-09-01T00:00:00-04:00',
        'train_target_local':['2026-04-08','2026-05-01'],
        'validation_target_local':['2026-06-16','2026-07-01'],
        'refit_target_local':['2026-04-08','2026-07-01'],
        'test_target_local':['2026-09-01','2026-10-01'],'alphas':[0.1,1.0],'limitations':['synthetic fixture']}
    rows=[]
    for month,day,release in [(4,8,datetime(2026,6,14,tzinfo=UTC)),
                              (6,16,datetime(2026,8,14,tzinfo=UTC)),
                              (9,1,datetime(2026,11,14,tzinfo=UTC))]:
        for h in range(6):
            for station in ('a','b'):
                origin=datetime(2026,month,day,4+h,tzinfo=UTC)
                r=example(origin,release)
                r.update(station_id=station,departures=(h+int(station=='b'))%3,
                    station_hour=station+'|'+str(h),dow_hour='0|'+str(h),
                    temperature=10+h,temperature_squared=(10+h)**2,precipitation=0.0,wind=2.0,wet=0,
                    advisory_covered=complete_alerts,advisory_active=1 if complete_alerts else None,
                    advisory_next3h=1 if complete_alerts else None,advisory_unspecified=0 if complete_alerts else None)
                rows.append(r)
    rows=[r|{'horizon':h,'target_start_utc':r['origin_utc']+timedelta(hours=h),
        'target_end_utc':r['origin_utc']+timedelta(hours=h+1)} for r in rows for h in range(3)]
    package=tmp_path/'input';package.mkdir();output=tmp_path/'result'
    pq.write_table(pa.Table.from_pylist(rows),package/'features.parquet')
    (package/'protocol.json').write_text(json.dumps(protocol))
    manifest={'schema_version':'station-pilot-package-v1','status':'built','protocol_sha256':'fixture',
        'cohort_stations':2,'artifacts':[{'path':name,'sha256':hashlib.sha256((package/name).read_bytes()).hexdigest()}
        for name in ('features.parquet','protocol.json')]}
    (package/'manifest.json').write_text(json.dumps(manifest))
    report=run(package,output)
    assert report['alert_ablation_ready']==complete_alerts
    assert set(report['test'])==({'calendar','weather','advisory'} if complete_alerts else {'calendar','weather'})
    assert {v['rows'] for v in report['test'].values()}=={36}
    assert (output/'selection.json').stat().st_mtime_ns<=(output/'report.json').stat().st_mtime_ns
    assert (output/'models.joblib').is_file()
