"""Collect a completed private cloud pilot and preserve its immutable results."""
from __future__ import annotations
import hashlib,json,os,time
from pathlib import Path
from databricks.sdk import WorkspaceClient


if __name__=='__main__':
    project=Path(__file__).resolve().parents[1]
    os.environ['PATH']=str(project/'.local/bin')+os.pathsep+os.environ['PATH']
    state=json.loads((project/'.local/execution/databricks_station_pilot.json').read_text())
    before=json.loads((project/'.local/execution/station_pilot_active_before.json').read_text())
    w=WorkspaceClient(profile='DATA622');last=None
    for _ in range(180):
        run=w.jobs.get_run(state['run_id']).as_dict();status=run.get('state',{})
        if status!=last:print(json.dumps(status),flush=True);last=status
        if status.get('life_cycle_state') in ('TERMINATED','SKIPPED','INTERNAL_ERROR'):break
        time.sleep(10)
    else:raise TimeoutError('Station pilot did not finish within 30 minutes')
    task=run['tasks'][0];output=w.jobs.get_run_output(task['run_id']).as_dict()
    if status.get('result_state')!='SUCCESS':raise RuntimeError(json.dumps({'state':status,'output':output}))
    report=json.loads(output['notebook_output']['result'])
    destination=Path('/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/station_pilot_results')/Path(report['output_root']).name
    destination.mkdir(parents=True,exist_ok=True);files=[]
    for name in ('report.json','selection.json','models.joblib','predictions.parquet'):
        data=w.files.download(report['output_root']+'/'+name).contents.read();path=destination/name
        if path.exists() and path.read_bytes()!=data:raise ValueError('Existing result differs')
        path.write_bytes(data);files.append({'path':name,'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)})
    active=hashlib.sha256(w.files.download(before['path']).contents.read()).hexdigest()
    if active!=before['sha256'] or active!=report['active_pointer_sha256']:raise ValueError('Aggregate model pointer changed')
    receipt=state|{'result_root':str(destination),'cloud_output_root':report['output_root'],
        'status':status,'task_run_id':task['run_id'],'artifacts':files,
        'active_pointer_sha256':active,'active_model_changed':False,'report':report}
    (project/'.local/execution/station_pilot_result.json').write_text(json.dumps(receipt,indent=2)+'\n')
    (project/'.local/execution/station_pilot_result_root.txt').write_text(str(destination)+'\n')
    print(json.dumps({'run_id':state['run_id'],'status':status,'result_root':str(destination),
        'split_rows':report['split_rows'],'test':report['test'],'alert_ablation_ready':report['alert_ablation_ready'],
        'active_model_changed':False},indent=2),flush=True)
