"""Submit one independent historical station pilot to Databricks Free Edition."""
from __future__ import annotations
import argparse,json,os
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs,workspace,compute
from citibike_pipeline.forecast_sources import digest
from deploy_three_hour_pilot import upload_immutable


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--package',required=True,type=Path)
    args=parser.parse_args();project=Path(__file__).resolve().parents[1]
    os.environ['PATH']=str(project/'.local/bin')+os.pathsep+os.environ['PATH']
    w=WorkspaceClient(profile='DATA622')
    root=args.package;manifest=json.loads((root/'manifest.json').read_text())
    if manifest['schema_version']!='station-pilot-package-v1' or manifest['status']!='built':raise ValueError('Incomplete station package')
    destination='/Volumes/workspace/citibike_3h/inputs/station_pilots/'+root.name
    for item in manifest['artifacts']:
        path=root/item['path']
        if digest(path)!=item['sha256']:raise ValueError('Package checksum mismatch')
        upload_immutable(w,destination+'/'+item['path'],path.read_bytes())
    upload_immutable(w,destination+'/manifest.json',(root/'manifest.json').read_bytes())
    folder='/Users/'+w.current_user.me().user_name+'/DATA622_ThreeHour'
    notebook=folder+'/20_station_pilot'
    w.workspace.upload(notebook,(project/'notebooks/20_station_pilot.py').read_bytes(),
        format=workspace.ImportFormat.SOURCE,language=workspace.Language.PYTHON,overwrite=True)
    response=w.jobs.submit(run_name='DATA622 independent historical station pilot',
        environments=[jobs.JobEnvironment(environment_key='station',spec=compute.Environment(
            environment_version='5',dependencies=['scikit-learn==1.7.2']))],
        tasks=[jobs.SubmitTask(task_key='station_pilot',notebook_task=jobs.NotebookTask(notebook,
            base_parameters={'package_root':destination}),environment_key='station',timeout_seconds=1800)],
        timeout_seconds=1800)
    state={'host':w.config.host,'run_id':response.run_id,'notebook':notebook,
        'package_root':destination,'local_package':str(root),'manifest_sha256':digest(root/'manifest.json'),
        'notebook_sha256':digest(project/'notebooks/20_station_pilot.py'),
        'active_model_changed':False,'scheduled':False,
        'run_url':w.config.host+'/jobs/runs/'+str(response.run_id)}
    (project/'.local/execution/databricks_station_pilot.json').write_text(json.dumps(state,indent=2)+'\n')
    print(json.dumps(state,indent=2))
