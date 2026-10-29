"""Submit separate availability training/dashboard; existing schedules stay intact."""
from __future__ import annotations
import argparse,json,os
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs,workspace,compute
from citibike_pipeline.forecast_sources import digest
from deploy_three_hour_pilot import upload_immutable


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['train','dashboard'])
    parser.add_argument('--package',required=True,type=Path)
    parser.add_argument('--station',default='All',help='initial dashboard selection; All or a frozen cohort short-name')
    args=parser.parse_args()
    project=Path(__file__).resolve().parents[1];os.environ['PATH']=str(project/'.local/bin')+os.pathsep+os.environ['PATH']
    w=WorkspaceClient(profile='DATA622');package=args.package
    manifest=json.loads((package/'manifest.json').read_text())
    if manifest['schema_version']!='availability-package-v1' or manifest['status']!='built':raise ValueError('Incomplete package')
    destination='/Volumes/workspace/citibike_3h/inputs/availability_pilots/'+package.name
    for item in manifest['artifacts']:
        path=package/item['path']
        if digest(path)!=item['sha256']:raise ValueError('Checksum mismatch')
        upload_immutable(w,destination+'/'+item['path'],path.read_bytes())
    upload_immutable(w,destination+'/manifest.json',(package/'manifest.json').read_bytes())
    live=project/'src/citibike_pipeline/availability_live.py'
    upload_immutable(w,destination+'/availability_live.py',live.read_bytes())
    filename='21_availability_train' if args.action=='train' else '22_availability_dashboard'
    folder='/Users/'+w.current_user.me().user_name+'/DATA622_ThreeHour';notebook=folder+'/'+filename
    content=(project/'notebooks'/(filename+'.py')).read_text()
    parameters={'package_root':destination}
    if args.action=='dashboard':
        state=json.loads((project/'.local/execution/availability_result.json').read_text())
        model_hash=next(r['sha256'] for r in state['artifacts'] if r['path']=='models.joblib')
        parameters.update({'model_root':state['cloud_output_root'],'live_helper_sha256':digest(live),
            'model_sha256':model_hash,'station':args.station})
        # Embed valid defaults so Run all refreshes only the dashboard without
        # requiring users to reconstruct a previous job's parameters.
        for key,value in parameters.items():content=content.replace("dbutils.widgets.text('"+key+"','')", "dbutils.widgets.text('"+key+"',"+repr(value)+")")
    w.workspace.upload(notebook,content.encode(),format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,overwrite=True)
    response=w.jobs.submit(run_name='DATA622 availability '+args.action,
        environments=[jobs.JobEnvironment(environment_key='availability',spec=compute.Environment(
            environment_version='5',dependencies=['scikit-learn==1.7.2']))],
        tasks=[jobs.SubmitTask(task_key='availability_'+args.action,notebook_task=jobs.NotebookTask(
            notebook,base_parameters=parameters),environment_key='availability',timeout_seconds=1800)],timeout_seconds=1800)
    status=w.workspace.get_status(notebook)
    state={'host':w.config.host,'run_id':response.run_id,'notebook':notebook,
        'notebook_url':w.config.host+'/editor/notebooks/'+str(status.object_id),
        'package_root':destination,'local_package':str(package),'manifest_sha256':digest(package/'manifest.json'),
        'live_helper_sha256':digest(live),'notebook_sha256':__import__('hashlib').sha256(content.encode()).hexdigest(),
        'parameters':parameters,'action':args.action,'scheduled':False,'active_departure_model_changed':False,
        'run_url':w.config.host+'/jobs/runs/'+str(response.run_id)}
    (project/('.local/execution/databricks_availability_'+args.action+'.json')).write_text(json.dumps(state,indent=2)+'\n')
    print(json.dumps(state,indent=2))
