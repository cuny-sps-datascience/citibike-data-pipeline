"""Submit or inspect the one-time stable-table Unity Catalog migration."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute,jobs,workspace


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['deploy','submit','wait'])
    parser.add_argument('--profile',default='DATA622')
    args=parser.parse_args()
    project=Path(__file__).resolve().parents[1]
    os.environ['PATH']=str(project/'.local/bin')+os.pathsep+os.environ['PATH']
    state_path=project/'.local/execution/stable_catalog_migration.json'
    client=WorkspaceClient(profile=args.profile)
    if args.action=='deploy':
        folder='/Users/'+client.current_user.me().user_name+'/DATA622_ThreeHour'
        names=('00_workspace_pilot','02_hourly_observations','04_train_borough_benchmark',
            '07_public_station_audit','08_import_weather_contribution','11_import_borough_actuals',
            '12_import_station_subway_mapping','14_release_aware_backtest',
            '16_refresh_released_outcomes','19_verify_september_refresh','20_station_pilot',
            '21_availability_train','22_availability_dashboard','23_migrate_stable_catalog')
        deployed=[]
        for name in names:
            source=project/'notebooks'/(name+'.py')
            target=folder+'/'+name
            client.workspace.upload(target,source.read_bytes(),format=workspace.ImportFormat.SOURCE,
                language=workspace.Language.PYTHON,overwrite=True)
            deployed.append({'notebook':target,'sha256':hashlib.sha256(source.read_bytes()).hexdigest()})
        print(json.dumps({'deployed':deployed},indent=2));return
    if args.action=='submit':
        source=project/'notebooks/23_migrate_stable_catalog.py'
        notebook='/Users/'+client.current_user.me().user_name+'/DATA622_ThreeHour/23_migrate_stable_catalog'
        client.workspace.upload(notebook,source.read_bytes(),format=workspace.ImportFormat.SOURCE,
            language=workspace.Language.PYTHON,overwrite=True)
        response=client.jobs.submit(run_name='DATA622 stable catalog migration',
            environments=[jobs.JobEnvironment(environment_key='migration',spec=compute.Environment(environment_version='5'))],
            tasks=[jobs.SubmitTask(task_key='migrate',notebook_task=jobs.NotebookTask(notebook),
                environment_key='migration',timeout_seconds=3600)],timeout_seconds=3600)
        state={'run_id':response.run_id,'notebook':notebook,
            'notebook_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
            'submitted_at_utc':__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()}
        state_path.parent.mkdir(parents=True,exist_ok=True);state_path.write_text(json.dumps(state,indent=2)+'\n')
        print(json.dumps(state,indent=2));return
    state=json.loads(state_path.read_text());last=None
    for _ in range(360):
        run=client.jobs.get_run(state['run_id']);status=run.state.as_dict()
        if status!=last:print(json.dumps(status),flush=True);last=status
        if run.state.life_cycle_state in (jobs.RunLifeCycleState.TERMINATED,jobs.RunLifeCycleState.SKIPPED,
                jobs.RunLifeCycleState.INTERNAL_ERROR):break
        time.sleep(10)
    else:raise TimeoutError('Migration did not finish within one hour')
    output=client.jobs.get_run_output(run.tasks[0].run_id)
    if run.state.result_state!=jobs.RunResultState.SUCCESS:
        raise RuntimeError(json.dumps({'state':status,'error':output.error,'trace':output.error_trace}))
    report=json.loads(output.notebook_output.result)
    state.update({'status':status,'task_run_id':run.tasks[0].run_id,'report':report})
    state_path.write_text(json.dumps(state,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
