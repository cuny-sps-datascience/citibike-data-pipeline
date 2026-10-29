"""Stage validated outcomes and deploy the serial daily public-evidence pipeline."""
import argparse
from datetime import datetime,timezone
import io,json,os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs,workspace,compute
from citibike_pipeline.forecast_sources import digest
from citibike_pipeline.release_watch import validate_outcomes
from deploy_three_hour_pilot import upload_immutable


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['stage-outcomes','deploy','schedule','status'])
    parser.add_argument('--package',type=Path)
    parser.add_argument('--include-prospective',action='store_true')
    args=parser.parse_args();project=Path(__file__).resolve().parents[1]
    os.environ['PATH']=str(project/'.local/bin')+os.pathsep+os.environ['PATH']
    w=WorkspaceClient(profile='DATA622')
    state_path=project/'.local/execution/databricks_evidence.json'
    old=json.loads(state_path.read_text()) if state_path.exists() else None
    if args.action=='deploy' and old and 'prospective_evidence' in old.get('tasks',[]) and not args.include_prospective:
        parser.error('Existing job includes prospective evidence; preserve it with --include-prospective and its --package')
    if args.action=='stage-outcomes':
        if not args.package:parser.error('stage-outcomes requires --package')
        result=validate_outcomes(args.package)
        destination='/Volumes/workspace/citibike_3h/inputs/approved_outcomes/'+args.package.name
        # Upload just the outcome artifact and the provenance manifest; ready marker is last.
        for name in (result['artifact']['path'],'manifest.json'):
            upload_immutable(w,destination+'/'+name,(args.package/name).read_bytes())
        receipt={'manifest_sha256':result['manifest_sha256'],'approved_at_utc':datetime.now(timezone.utc).isoformat(),
            'validation':'complete geographic calendar, keys, counts, DST nulls, selected input evidence and SHA-256',
            'source_package':str(args.package)}
        try:w.files.download(destination+'/ready.json')
        except Exception as exc:
            from databricks.sdk.errors import NotFound
            if not isinstance(exc,NotFound):raise
            w.files.upload(destination+'/ready.json',io.BytesIO(json.dumps(receipt,indent=2).encode()),overwrite=False)
        print(json.dumps({'destination':destination,'rows':result['rows'],'latest_hour_utc':result['latest_hour_utc']},indent=2));return
    if args.action=='status':
        if not old:raise ValueError('No evidence deployment')
        run=w.jobs.get_run(old['run_id']);print(json.dumps(run.state.as_dict()))
        for task in run.tasks or []:
            print(task.task_key,task.state.as_dict())
            if task.state.life_cycle_state in (jobs.RunLifeCycleState.TERMINATED,jobs.RunLifeCycleState.INTERNAL_ERROR):
                output=w.jobs.get_run_output(task.run_id)
                if output.error:print(output.error,output.error_trace or '')
                if output.notebook_output and output.notebook_output.result:
                    result=json.loads(output.notebook_output.result)
                    (state_path.parent/('evidence_'+task.task_key+'_result.json')).write_text(json.dumps(result,indent=2)+'\n')
                    print(json.dumps(result,indent=2)[:7000])
        return
    if args.action=='schedule':
        if not old or w.jobs.get_run(old['run_id']).state.result_state!=jobs.RunResultState.SUCCESS:
            raise ValueError('Successful manual evidence run required')
        schedule=jobs.CronSchedule(quartz_cron_expression='0 0 13 * * ?',timezone_id='UTC',pause_status=jobs.PauseStatus.UNPAUSED)
        w.jobs.update(job_id=old['job_id'],new_settings=jobs.JobSettings(schedule=schedule))
        old['schedule']=schedule.as_dict();state_path.write_text(json.dumps(old,indent=2)+'\n');print(json.dumps(old,indent=2));return
    folder='/Users/'+w.current_user.me().user_name+'/DATA622_ThreeHour'
    helper=project/'src/citibike_pipeline/release_watch.py';checksum=digest(helper)
    helper_path='/Volumes/workspace/citibike_3h/inputs/evidence_helpers/'+checksum+'/release_watch.py'
    upload_immutable(w,helper_path,helper.read_bytes())
    parameters={'helper_path':helper_path,'helper_sha256':checksum}
    tasks=[]
    for key,filename,depends in [('watch_releases','15_watch_trip_releases',None),
            ('evaluate_outcomes','16_refresh_released_outcomes','watch_releases')]:
        notebook=folder+'/'+filename
        w.workspace.upload(notebook,(project/'notebooks'/(filename+'.py')).read_bytes(),
            format=workspace.ImportFormat.SOURCE,language=workspace.Language.PYTHON,overwrite=True)
        tasks.append(jobs.Task(task_key=key,notebook_task=jobs.NotebookTask(notebook,base_parameters=parameters),
            depends_on=[jobs.TaskDependency(depends)] if depends else None,run_if=jobs.RunIf.ALL_DONE if depends else None,
            max_retries=0,timeout_seconds=600,environment_key='evidence_v5'))
    if args.include_prospective:
        if not old or ('prospective_evidence' not in old.get('tasks',[]) and
                w.jobs.get_run(old['run_id']).state.result_state!=jobs.RunResultState.SUCCESS):
            raise ValueError('Complete release/evaluation deployment first')
        if not args.package:parser.error('--include-prospective requires --package with the GTFS reference')
        manifest=json.loads((args.package/'manifest.json').read_text())
        if manifest.get('schema_version')!='prospective-gtfs-reference-v1' or manifest.get('status')!='built':
            raise ValueError('Require a completed GTFS reference package')
        destination='/Volumes/workspace/citibike_3h/inputs/prospective_reference/'+args.package.name
        for item in manifest['artifacts']:
            path=args.package/item['path']
            if digest(path)!=item['sha256']:raise ValueError('Reference checksum mismatch')
            upload_immutable(w,destination+'/'+item['path'],path.read_bytes())
        upload_immutable(w,destination+'/manifest.json',(args.package/'manifest.json').read_bytes())
        helper=project/'src/citibike_pipeline/prospective_evidence.py';checksum=digest(helper)
        helper_path='/Volumes/workspace/citibike_3h/inputs/evidence_helpers/'+checksum+'/prospective_evidence.py'
        upload_immutable(w,helper_path,helper.read_bytes())
        filename='17_build_prospective_evidence';notebook=folder+'/'+filename
        w.workspace.upload(notebook,(project/'notebooks'/(filename+'.py')).read_bytes(),
            format=workspace.ImportFormat.SOURCE,language=workspace.Language.PYTHON,overwrite=True)
        tasks.append(jobs.Task(task_key='prospective_evidence',depends_on=[jobs.TaskDependency('evaluate_outcomes')],
            run_if=jobs.RunIf.ALL_DONE,notebook_task=jobs.NotebookTask(notebook,base_parameters={
                'helper_path':helper_path,'helper_sha256':checksum,'reference_root':destination}),
            max_retries=0,timeout_seconds=900,environment_key='evidence_v5'))
    env=[jobs.JobEnvironment(environment_key='evidence_v5',spec=compute.Environment(environment_version='5',
        dependencies=['gtfs-realtime-bindings==3.0.0']))]
    if old:
        job_id=old['job_id'];w.jobs.update(job_id=job_id,new_settings=jobs.JobSettings(tasks=tasks,environments=env,timeout_seconds=1800))
    else:job_id=w.jobs.create(name='DATA622 Daily Public Evidence',tasks=tasks,environments=env,
        max_concurrent_runs=1,timeout_seconds=1800).job_id
    run=w.jobs.run_now(job_id=job_id)
    result={'job_id':job_id,'run_id':run.run_id,'job_url':w.config.host+'/jobs/'+str(job_id),
        'tasks':[t.task_key for t in tasks]}
    schedule=w.jobs.get(job_id).settings.schedule
    if schedule:result['schedule']=schedule.as_dict()
    if old:result['previous_run_ids']=old.get('previous_run_ids',[])+[old['run_id']]
    state_path.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))


if __name__=='__main__':main()
