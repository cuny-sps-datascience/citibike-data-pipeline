"""Wait asynchronously from the CLI, then preserve availability cloud evidence."""
from __future__ import annotations
import argparse,hashlib,json,os,time
from pathlib import Path
from databricks.sdk import WorkspaceClient


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['train','dashboard']);args=p.parse_args()
    project=Path(__file__).resolve().parents[1];os.environ['PATH']=str(project/'.local/bin')+os.pathsep+os.environ['PATH']
    state=json.loads((project/('.local/execution/databricks_availability_'+args.action+'.json')).read_text())
    w=WorkspaceClient(profile='DATA622');last=None
    for _ in range(180):
        run=w.jobs.get_run(state['run_id']).as_dict();status=run.get('state',{})
        if status!=last:print(json.dumps(status),flush=True);last=status
        if status.get('life_cycle_state') in ('TERMINATED','SKIPPED','INTERNAL_ERROR'):break
        time.sleep(10)
    else:raise TimeoutError('Availability job has not finished in 30 minutes')
    task=run['tasks'][0];output=w.jobs.get_run_output(task['run_id']).as_dict()
    if status.get('result_state')!='SUCCESS':raise RuntimeError(json.dumps({'state':status,'output':output}))
    report=json.loads(output['notebook_output']['result'])
    cloud_root=report['output_root'] if args.action=='train' else report['snapshot_root']
    destination=Path('/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour')/('availability_results' if args.action=='train' else 'availability_live')/Path(cloud_root).name
    destination.mkdir(parents=True,exist_ok=True);artifacts=[]
    names=('report.json','selection.json','calibration.json','models.joblib','predictions.parquet') if args.action=='train' else ('report.json','station_states.parquet','forecast_rows.json')
    for name in names:
        data=w.files.download(cloud_root+'/'+name).contents.read();path=destination/name
        if path.exists() and path.read_bytes()!=data:raise ValueError('Existing result differs')
        path.write_bytes(data);artifacts.append({'path':name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()})
    active='/Volumes/workspace/citibike_3h/models/borough_active.json'
    pointer=hashlib.sha256(w.files.download(active).contents.read()).hexdigest()
    if pointer!=report['active_departure_pointer_sha256']:raise ValueError('Departure model pointer changed')
    receipt=state|{'report':report,'result_root':str(destination),'cloud_output_root':cloud_root,
        'status':status,'task_run_id':task['run_id'],'artifacts':artifacts,'active_departure_pointer_sha256':pointer,
        'run_url':run.get('run_page_url',state['run_url'])}
    name='availability_result' if args.action=='train' else 'availability_dashboard_result'
    (project/('.local/execution/'+name+'.json')).write_text(json.dumps(receipt,indent=2)+'\n')
    (project/('.local/execution/'+name+'_root.txt')).write_text(str(destination)+'\n')
    print(json.dumps({'run_id':state['run_id'],'status':status,'result_root':str(destination),
        'report':report if args.action=='dashboard' else {k:report[k] for k in ('split_rows','selected_counts','test_by_horizon')}},indent=2),flush=True)
