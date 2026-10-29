"""Reproducible hourly import, live collection deployment, status, and collector pause."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace

from citibike_pipeline.hourly_export import validate_hourly_package
from deploy_three_hour_pilot import upload_immutable


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['status','deploy-hourly','deploy-collector','pause-collector'])
    p.add_argument('--profile',default='DATA622')
    p.add_argument('--package',type=Path)
    p.add_argument('--schedule',action='store_true',help='enable hourly collection at minute 55 UTC after a successful test')
    args=p.parse_args()
    root=Path(__file__).resolve().parents[1]
    os.environ['PATH']=str(root/'.local/bin')+os.pathsep+os.environ['PATH']
    w=WorkspaceClient(profile=args.profile)
    state=root/'.local/execution';state.mkdir(parents=True,exist_ok=True)
    if args.action=='status':
        for name in ('pilot','hourly','collector'):
            path=state/f'databricks_{name}.json'
            if not path.exists():continue
            info=json.loads(path.read_text());job=w.jobs.get(info['job_id']);run=w.jobs.get_run(info['run_id'])
            print(json.dumps({'component':name,'job_url':info['job_url'],
                'run_state':run.state.as_dict() if run.state else None,
                'schedule':job.settings.schedule.as_dict() if job.settings.schedule else None}))
        return
    state_path=state/('databricks_hourly.json' if args.action=='deploy-hourly' else 'databricks_collector.json')
    previous=json.loads(state_path.read_text()) if state_path.exists() else None
    if args.action=='pause-collector':
        if not previous:raise ValueError('No collector deployment state found')
        current=w.jobs.get(previous['job_id']).settings.schedule
        if current:
            current.pause_status=jobs.PauseStatus.PAUSED
            w.jobs.update(job_id=previous['job_id'],new_settings=jobs.JobSettings(schedule=current))
        print('Collector schedule paused; existing observations are preserved.')
        return
    folder='/Users/'+w.current_user.me().user_name+'/DATA622_ThreeHour'
    if args.action=='deploy-hourly':
        if not args.package:p.error('--package is required for deploy-hourly')
        manifest=validate_hourly_package(args.package)
        destination='/Volumes/workspace/citibike_3h/inputs/hourly/'+args.package.name
        for artifact in manifest['artifacts']:
            upload_immutable(w,destination+'/'+artifact['path'],(args.package/artifact['path']).read_bytes())
        upload_immutable(w,destination+'/manifest.json',(args.package/'manifest.json').read_bytes())
        filename='02_hourly_observations';name='DATA622 Hourly Observation Import'
        parameters={'export_root':destination};timeout=900
    else:
        filename='03_collect_live_sources';name='DATA622 Prospective Source Collection'
        parameters={};timeout=300
    notebook=folder+'/'+filename
    if previous and previous['notebook']!=notebook:
        raise ValueError('Deployment state refers to a different workspace user')
    w.workspace.mkdirs(folder)
    w.workspace.upload(notebook,(root/'notebooks'/(filename+'.py')).read_bytes(),
        format=workspace.ImportFormat.SOURCE,language=workspace.Language.PYTHON,overwrite=True)
    task=jobs.Task(task_key='hourly_import' if args.action=='deploy-hourly' else 'collect_sources',
        notebook_task=jobs.NotebookTask(notebook,base_parameters=parameters),max_retries=0,timeout_seconds=timeout)
    if previous:
        job_id=previous['job_id']
        w.jobs.update(job_id=job_id,new_settings=jobs.JobSettings(tasks=[task],max_concurrent_runs=1,timeout_seconds=timeout))
    else:
        job_id=w.jobs.create(name=name,tasks=[task],max_concurrent_runs=1,timeout_seconds=timeout).job_id
    if args.schedule:
        if args.action!='deploy-collector':p.error('--schedule applies only to deploy-collector')
        if not previous or w.jobs.get_run(previous['run_id']).state.result_state!=jobs.RunResultState.SUCCESS:
            raise ValueError('Complete and verify a manual collector run before enabling the schedule')
        w.jobs.update(job_id=job_id,new_settings=jobs.JobSettings(schedule=jobs.CronSchedule(
            quartz_cron_expression='0 55 * * * ?',timezone_id='UTC',pause_status=jobs.PauseStatus.UNPAUSED)))
    run=w.jobs.run_now(job_id=job_id)
    report={'job_id':job_id,'run_id':run.run_id,'notebook':notebook,'job_url':w.config.host+'/jobs/'+str(job_id)}
    if args.action=='deploy-hourly':report['source_root']=destination
    state_path.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))


if __name__=='__main__':main()
