"""Collect measured cloud, temporal, live-publication and local test evidence."""
from __future__ import annotations
from datetime import datetime,timezone
from pathlib import Path
from collections import Counter
import json,math,re
import pyarrow.parquet as pq
from citibike_pipeline.forecast_sources import digest


if __name__=='__main__':
    def read(path):return json.loads(Path(path).read_text())
    train=read('.local/execution/availability_result.json')
    dashboard=read('.local/execution/availability_dashboard_result.json')
    package=Path(train['local_package']);m=read(package/'manifest.json')
    input_audit=read('docs/validation/availability_package_2026-10-16.json')
    result_audit=read('docs/validation/availability_results_2026-10-16.json')
    assert input_audit['passed'] and result_audit['passed']
    assert train['status']['result_state']==dashboard['status']['result_state']=='SUCCESS'
    report=train['report'];live=dashboard['report']
    assert report['input_manifest_sha256']==digest(package/'manifest.json')
    assert report['active_departure_pointer_sha256']==live['active_departure_pointer_sha256']=='61e1eaa0f924dfcbf9e8440b9e477477629a18dbc34af4f0f03c344c8c1bff08'
    predictions=read(Path(dashboard['result_root'])/'forecast_rows.json')
    states=pq.read_table(Path(dashboard['result_root'])/'station_states.parquet').to_pylist()
    ready={s['station_id'] for s in states if s['status']=='ready'}
    issue=datetime.fromisoformat(live['issue_time_utc']);published=datetime.fromisoformat(live['published_at_utc'])
    keys=set();issues=[];horizons=Counter()
    for r in predictions:
        origin=datetime.fromisoformat(r['origin_utc']);target=datetime.fromisoformat(r['target_utc'])
        observed=datetime.fromisoformat(r['input_observed_at_utc'])
        key=(r['station_id'],r['horizon_minutes'])
        if key in keys:issues.append('duplicate live key')
        keys.add(key);horizons[r['station_id']]+=1
        if r['station_id'] not in ready:issues.append('forecast from unknown station')
        if not observed<origin==issue<target or not published<target:issues.append('late or future live input/publication')
        if abs((target-origin).total_seconds()-r['horizon_minutes']*60)>1e-6:issues.append('live horizon mismatch')
        for target_name in ('bikes','docks'):
            if not 0<=r[target_name+'_p_zero']<=1:issues.append('invalid probability')
            if not 0<=r[target_name+'_lower']<=r[target_name+'_prediction']<=r[target_name+'_upper']:issues.append('invalid count interval')
            if not all(math.isfinite(r[target_name+'_'+k]) for k in ('prediction','lower','upper','p_zero')):issues.append('nonfinite live forecast')
    assert not issues and set(horizons)==ready and set(horizons.values())=={4}
    tests=Path('.local/execution/availability_tests.log').read_text();match=re.search(r'(\d+) passed in ([\d.]+)s',tests)
    assert match and int(match[1])>=261
    proposal=Path('docs/citibike_proposal_4_pages_for_revisions.docx')
    assert digest(proposal)=='fde305547639fb55442a84d0bb5f737ea5b4a4da7d24cc4988593a1231c2c90d'
    receipt={'recorded_at_utc':datetime.now(timezone.utc).isoformat(),'scope':'conditional station inventory and point-in-time empty/no-dock risk',
        'package_root':str(package),'package_manifest_sha256':digest(package/'manifest.json'),
        'source_root':m['sources_root'],'example_rows':m['rows'],'cohort_stations':32,
        'test_stations':report['test_stations'],'split_rows':report['split_rows'],
        'selected_counts':report['selected_counts'],'test_by_horizon':report['test_by_horizon'],
        'training_run_id':train['run_id'],'training_task_run_id':train['task_run_id'],'training_status':train['status'],
        'model_root':train['cloud_output_root'],'local_result_root':train['result_root'],
        'model_artifacts':train['artifacts'],'mlflow_run_id':report['mlflow_run_id'],
        'predictions_table':report['predictions_table'],'dashboard_notebook_url':dashboard['notebook_url'],
        'dashboard_run_url':dashboard['run_url'],'dashboard_run_id':dashboard['run_id'],
        'dashboard_status':dashboard['status'],'live_report':live,'live_checks_passed':True,
        'live_forecast_rows':len(predictions),'live_unknown_stations':[s for s in states if s['status']!='ready'],
        'save_reload_checked_rows':report['save_reload_checked_rows'],'save_reload_max_difference':report['save_reload_max_difference'],
        'package_checks':input_audit['checks'],'result_checks':result_audit['checks'],
        'independent_metric_recomputation_passed':result_audit['passed'],
        'local_tests_passed':int(match[1]),'local_tests_seconds':float(match[2]),
        'active_departure_pointer_sha256':live['active_departure_pointer_sha256'],
        'active_departure_model_changed':False,'canonical_eligibility_promoted':False,'new_schedule_enabled':False,
        'original_proposal_sha256':digest(proposal),'limitations':report['limitations']}
    visual=Path('.local/execution/availability_visual_review.json')
    if visual.exists():receipt['visual_review']=read(visual)
    path=Path('docs/validation/availability_mvp_2026-10-16.json');path.write_text(json.dumps(receipt,default=str,indent=2)+'\n')
    print(json.dumps({'receipt':str(path),'example_rows':m['rows'],'live_rows':len(predictions),
        'tests_passed':int(match[1]),'dashboard_run_url':dashboard['run_url']},indent=2))
