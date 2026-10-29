"""Deploy/run the public-only benchmark in the existing authenticated Free workspace."""
from __future__ import annotations

import argparse
import io
import json
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs, workspace

from citibike_pipeline.forecast_sources import digest
from deploy_three_hour_pilot import upload_immutable


ENV_KEY='public_mvp_v5'


def environments():
    return [jobs.JobEnvironment(environment_key=ENV_KEY,spec=compute.Environment(
        environment_version='5',dependencies=['scikit-learn==1.7.2']))]


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['train','status','activate','dashboard','audit','verify','sensitivity','actuals','weather','mapping','health','rolling'])
    p.add_argument('--package',type=Path)
    p.add_argument('--schedule',action='store_true',help='Enable daily health only after its successful manual run')
    p.add_argument('--profile',default='DATA622')
    args=p.parse_args()
    if args.schedule and args.action!='health':p.error('--schedule applies only to health')
    root=Path(__file__).resolve().parents[1]
    os.environ['PATH']=str(root/'.local/bin')+os.pathsep+os.environ['PATH']
    w=WorkspaceClient(profile=args.profile)
    state=root/'.local/execution/databricks_borough.json'
    previous=json.loads(state.read_text()) if state.exists() else None
    folder='/Users/'+w.current_user.me().user_name+'/DATA622_ThreeHour'
    if args.action=='health':
        health_state=state.parent/'databricks_health.json'
        old=json.loads(health_state.read_text()) if health_state.exists() else None
        if args.schedule:
            if not old or w.jobs.get_run(old['run_id']).state.result_state!=jobs.RunResultState.SUCCESS:
                raise ValueError('Daily health must pass its manual cloud run before scheduling')
            schedule=jobs.CronSchedule(quartz_cron_expression='0 15 13 * * ?',timezone_id='UTC',
                pause_status=jobs.PauseStatus.UNPAUSED)
            w.jobs.update(job_id=old['job_id'],new_settings=jobs.JobSettings(schedule=schedule))
            old['schedule']=schedule.as_dict()
            health_state.write_text(json.dumps(old,indent=2)+'\n')
            print(json.dumps(old,indent=2));return
        helper=root/'src/citibike_pipeline/daily_health.py'
        checksum=digest(helper)
        helper_path='/Volumes/workspace/citibike_3h/inputs/health_helpers/'+checksum+'/daily_health.py'
        upload_immutable(w,helper_path,helper.read_bytes())
        notebook=folder+'/13_daily_health'
        w.workspace.upload(notebook,(root/'notebooks/13_daily_health.py').read_bytes(),
            format=workspace.ImportFormat.SOURCE,language=workspace.Language.PYTHON,overwrite=True)
        task=jobs.Task(task_key='daily_health',notebook_task=jobs.NotebookTask(notebook,
            base_parameters={'helper_path':helper_path,'helper_sha256':checksum,
                'monitoring_start_utc':'2026-10-16T02:00:00+00:00'}),
            timeout_seconds=600,max_retries=0,environment_key=ENV_KEY)
        if old:
            job_id=old['job_id'];w.jobs.update(job_id=job_id,new_settings=jobs.JobSettings(
                tasks=[task],timeout_seconds=600,environments=environments()))
        else:
            job_id=w.jobs.create(name='DATA622 Daily Forecast Health',tasks=[task],
                max_concurrent_runs=1,timeout_seconds=600,environments=environments(),
                description='Daily output coverage, timing, fallback and HTTP retrieval freshness; reports in borough_daily_health.').job_id
        run=w.jobs.run_now(job_id=job_id)
        report={'job_id':job_id,'run_id':run.run_id,'notebook':notebook,
            'job_url':w.config.host+'/jobs/'+str(job_id),'helper_sha256':checksum}
        if old:report['previous_run_ids']=old.get('previous_run_ids',[])+[old['run_id']]
        health_state.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2));return
    if args.action=='status':
        if not previous:raise ValueError('No borough deployment state')
        run=w.jobs.get_run(previous['run_id'])
        print(json.dumps({'state':run.state.as_dict(),'run_id':run.run_id,
            'tasks':[{ 'key':t.task_key,'run_id':t.run_id,'state':t.state.as_dict()} for t in (run.tasks or [])]},indent=2))
        for task in run.tasks or []:
            if task.state.life_cycle_state==jobs.RunLifeCycleState.TERMINATED:
                output=w.jobs.get_run_output(task.run_id)
                if output.error:print(output.error)
                if output.notebook_output and output.notebook_output.result:
                    report=json.loads(output.notebook_output.result)
                    (state.parent/'borough_training_report.json').write_text(json.dumps(report,indent=2)+'\n')
                    print(json.dumps(report,indent=2))
        return
    if args.action=='train':
        if not args.package:p.error('train requires --package')
        manifest=json.loads((args.package/'manifest.json').read_text())
        if manifest['status']!='built':raise ValueError('Incomplete feature package')
        destination='/Volumes/workspace/citibike_3h/inputs/borough/'+args.package.name
        for artifact in manifest['artifacts']:
            path=args.package/artifact['path']
            if digest(path)!=artifact['sha256']:raise ValueError('Package checksum mismatch')
            upload_immutable(w,destination+'/'+artifact['path'],path.read_bytes())
        upload_immutable(w,destination+'/manifest.json',(args.package/'manifest.json').read_bytes())
        upload_immutable(w,destination+'/model_runtime.py',(root/'src/citibike_pipeline/model_runtime.py').read_bytes())
        filename='04_train_borough_benchmark';parameters={'package_root':destination};timeout=1200
        notebook=folder+'/'+filename
        w.workspace.upload(notebook,(root/'notebooks'/(filename+'.py')).read_bytes(),
            format=workspace.ImportFormat.SOURCE,language=workspace.Language.PYTHON,overwrite=True)
        task=jobs.Task(task_key='train_borough',notebook_task=jobs.NotebookTask(notebook,base_parameters=parameters),
            max_retries=0,timeout_seconds=timeout,environment_key=ENV_KEY)
        if previous:
            job_id=previous['job_id'];w.jobs.update(job_id=job_id,new_settings=jobs.JobSettings(tasks=[task],timeout_seconds=timeout,environments=environments()))
        else:
            job_id=w.jobs.create(name='DATA622 Public Manhattan Forecast Benchmark',tasks=[task],
                max_concurrent_runs=1,timeout_seconds=timeout,environments=environments()).job_id
        run=w.jobs.run_now(job_id=job_id)
        report={'job_id':job_id,'run_id':run.run_id,'notebook':notebook,'package_root':destination,
            'job_url':w.config.host+'/jobs/'+str(job_id)}
        if previous:report['previous_run_ids']=previous.get('previous_run_ids',[])+[previous['run_id']]
        state.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2));return
    if args.action=='activate':
        if not previous or w.jobs.get_run(previous['run_id']).state.result_state!=jobs.RunResultState.SUCCESS:
            raise ValueError('Verify a successful benchmark training run first')
        candidate=json.loads(w.files.download('/Volumes/workspace/citibike_3h/models/borough_candidate.json').contents.read())
        if candidate['package_root']!=previous['package_root'] or candidate['reload_max_absolute_error']>1e-9:
            raise ValueError('Candidate version/reload gate failed')
        # Pointer is project-owned mutable deployment metadata; models stay immutable.
        w.files.upload('/Volumes/workspace/citibike_3h/models/borough_active.json',
            io.BytesIO(json.dumps(candidate,indent=2).encode()),overwrite=True)
        collector_state=json.loads((state.parent/'databricks_collector.json').read_text())
        job=w.jobs.get(collector_state['job_id'])
        for filename in ('03_collect_live_sources','05_score_borough','06_borough_dashboard'):
            w.workspace.upload(folder+'/'+filename,(root/'notebooks'/(filename+'.py')).read_bytes(),
                format=workspace.ImportFormat.SOURCE,language=workspace.Language.PYTHON,overwrite=True)
        collect=jobs.Task(task_key='collect_sources',notebook_task=jobs.NotebookTask(folder+'/03_collect_live_sources'),
            timeout_seconds=300,max_retries=0,environment_key=ENV_KEY)
        score=jobs.Task(task_key='score_borough',depends_on=[jobs.TaskDependency('collect_sources')],
            run_if=jobs.RunIf.ALL_DONE,timeout_seconds=300,max_retries=0,environment_key=ENV_KEY,
            notebook_task=jobs.NotebookTask(folder+'/05_score_borough',base_parameters={
                'collection_run_id':'{{tasks.collect_sources.values.collection_run_id}}',
                'trigger_ms':'{{job.trigger.time.timestamp_ms}}'}))
        w.jobs.update(job_id=job.job_id,new_settings=jobs.JobSettings(tasks=[collect,score],
            max_concurrent_runs=1,timeout_seconds=600,environments=environments()))
        run=w.jobs.run_now(job_id=job.job_id)
        collector_state['run_id']=run.run_id
        (state.parent/'databricks_collector.json').write_text(json.dumps(collector_state,indent=2)+'\n')
        report={'model_version':candidate['model_version'],'job_id':job.job_id,'run_id':run.run_id,
            'schedule':job.settings.schedule.as_dict() if job.settings.schedule else None}
        (state.parent/'borough_activation.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report,indent=2));return
    filename={'dashboard':'06_borough_dashboard','audit':'07_public_station_audit','verify':'09_verify_public_mvp',
        'sensitivity':'10_weather_delay_sensitivity','actuals':'11_import_borough_actuals',
        'weather':'08_import_weather_contribution','mapping':'12_import_station_subway_mapping',
        'rolling':'14_release_aware_backtest'}[args.action]
    parameters={}
    if args.action=='verify':
        retry_path=state.parent/'borough_retry_test.json'
        if retry_path.exists():
            retry=json.loads(retry_path.read_text())
            parameters={'expected_first_origin':retry['origin_utc'],
                        'expected_first_score_run_id':retry['expected_first_score_run_id']}
    if args.action=='audit':
        if not args.package:p.error('audit requires --package')
        manifest=json.loads((args.package/'manifest.json').read_text())
        if manifest['status']!='conditional_exposure_built':raise ValueError('Incomplete station audit')
        destination='/Volumes/workspace/citibike_3h/inputs/public_station_audit/'+args.package.name
        for item in manifest['artifacts']:
            path=args.package/item['path']
            if digest(path)!=item['sha256']:raise ValueError('Checksum mismatch')
            upload_immutable(w,destination+'/'+item['path'],path.read_bytes())
        upload_immutable(w,destination+'/manifest.json',(args.package/'manifest.json').read_bytes())
        parameters={'package_root':destination}
    if args.action in ('actuals','weather','mapping','rolling'):
        if not args.package:p.error(args.action+' requires --package')
        manifest=json.loads((args.package/'manifest.json').read_text())
        destination='/Volumes/workspace/citibike_3h/inputs/'+args.action+'/'+args.package.name
        if args.action=='mapping' and (manifest['status']!='built' or manifest['schema_version']!='station-subway-proximity-v1'):
            raise ValueError('Incomplete station/subway mapping')
        artifacts=manifest['files'] if args.action=='weather' else manifest['artifacts']
        for item in artifacts:
            path=args.package/item['path']
            if digest(path)!=item['sha256']:raise ValueError('Checksum mismatch')
            upload_immutable(w,destination+'/'+item['path'],path.read_bytes())
        upload_immutable(w,destination+'/manifest.json',(args.package/'manifest.json').read_bytes())
        parameters={'package_root':destination} if args.action in ('actuals','mapping','rolling') else {
            'handoff_root':destination,'catalog':'workspace','schema':'citibike_3h',
            'table':'external_weather_reference'}
    w.workspace.upload(folder+'/'+filename,(root/'notebooks'/(filename+'.py')).read_bytes(),
        format=workspace.ImportFormat.SOURCE,language=workspace.Language.PYTHON,overwrite=True)
    response=w.jobs.submit(run_name='DATA622 '+filename,environments=environments(),tasks=[jobs.SubmitTask(task_key='review',
        notebook_task=jobs.NotebookTask(folder+'/'+filename,base_parameters=parameters),timeout_seconds=600,environment_key=ENV_KEY)])
    report={'run_id':response.run_id,'notebook':folder+'/'+filename}
    (state.parent/('borough_'+args.action+'.json')).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
