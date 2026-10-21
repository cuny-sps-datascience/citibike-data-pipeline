"""Record successful cloud runs, schedules, replay and temporal checks for review."""
from datetime import datetime,timezone
import hashlib,json,os
from pathlib import Path
from zoneinfo import ZoneInfo
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

if __name__=='__main__':
    root=Path(__file__).resolve().parents[1];state_dir=root/'.local/execution'
    os.environ['PATH']=str(root/'.local/bin')+os.pathsep+os.environ['PATH']
    w=WorkspaceClient(profile='DATA622')
    def read_run(run_id):
        run=w.jobs.get_run(run_id)
        if run.state.result_state!=jobs.RunResultState.SUCCESS:
            raise ValueError('Run not successful: '+str(run_id)+' '+str(run.state))
        outputs={};attempts=[]
        for task in sorted(run.tasks or [],key=lambda t:(t.attempt_number or 0,t.end_time or 0)):
            output=w.jobs.get_run_output(task.run_id)
            attempts.append({'task_key':task.task_key,'run_id':task.run_id,'state':task.state.as_dict(),
                'attempt_number':task.attempt_number,'execution_duration_ms':task.execution_duration,
                'error':output.error})
            if output.notebook_output and output.notebook_output.result:
                result=output.notebook_output.result
                try:result=json.loads(result)
                except ValueError:pass
                outputs[task.task_key]=result
        return {'run_id':run_id,'state':run.state.as_dict(),'attempts':attempts,'outputs':outputs}
    states={key:json.loads((state_dir/(filename+'.json')).read_text()) for key,filename in (
        ('rolling','borough_rolling'),('evidence','databricks_evidence'),
        ('verification','evidence_verification'),('dashboard','borough_dashboard'))}
    runs={key:read_run(state['run_id']) for key,state in states.items()}
    initial=None
    for run_id in states['evidence'].get('previous_run_ids',[]):
        previous=read_run(run_id)
        output=previous['outputs'].get('prospective_evidence',{})
        if output.get('processed_inputs',0)>0:
            initial=previous;break
    if not initial:raise ValueError('Missing initial nonempty evidence run')
    replay=runs['verification']['outputs']['replay']
    verification=runs['verification']['outputs']['verify']
    assert replay['processed_inputs']==0 and all(n==0 for n in replay['batch_rows'].values())
    assert replay['total_rows']==initial['outputs']['prospective_evidence']['total_rows']
    assert all(n==0 for n in verification['checks'].values())
    assert verification['active_pointer_unchanged'] and verification['rolling_release_gates_passed']
    assert runs['dashboard']['outputs']['review']=='Manhattan forecast dashboard queries succeeded'
    schedules={}
    for name,job_id,cron in (('hourly_forecast',1072725421467387,'0 55 * * * ?'),
            ('daily_health',899230762229374,'0 15 13 * * ?'),
            ('daily_evidence',states['evidence']['job_id'],'0 0 13 * * ?')):
        job=w.jobs.get(job_id);schedule=job.settings.schedule
        assert schedule.pause_status==jobs.PauseStatus.UNPAUSED and schedule.timezone_id=='UTC'
        assert schedule.quartz_cron_expression==cron and job.settings.max_concurrent_runs==1
        schedules[name]={'job_id':job_id,'schedule':schedule.as_dict(),'max_concurrent_runs':job.settings.max_concurrent_runs,
            'tasks':[t.task_key for t in job.settings.tasks]}
    assert set(schedules['daily_evidence']['tasks'])=={'watch_releases','evaluate_outcomes','prospective_evidence'}
    evidence_settings=w.jobs.get(states['evidence']['job_id']).settings
    dependencies={t.task_key:[d.task_key for d in (t.depends_on or [])] for t in evidence_settings.tasks}
    assert dependencies['evaluate_outcomes']==['watch_releases']
    assert dependencies['prospective_evidence']==['evaluate_outcomes']
    active=w.files.download('/Volumes/workspace/citibike_3h/models/borough_active.json').contents.read()
    assert hashlib.sha256(active).hexdigest()==runs['rolling']['outputs']['review']['active_pointer_sha256']
    now=datetime.now(timezone.utc)
    code_files=['src/citibike_pipeline/model_runtime.py','src/citibike_pipeline/rolling_backtest.py',
        'src/citibike_pipeline/release_watch.py','src/citibike_pipeline/prospective_evidence.py',
        'scripts/deploy_evidence_pipeline.py',*[str(p.relative_to(root)) for p in sorted((root/'notebooks').glob('1[4-8]_*.py'))]]
    report={'recorded_at_utc':now.isoformat(),'recorded_at_new_york':now.astimezone(ZoneInfo('America/New_York')).isoformat(),
        'runs':runs,'initial_full_evidence_run':initial,'schedules':schedules,
        'station_configuration_audit':json.loads((state_dir/'evidence_station_configuration_audit.json').read_text()),
        'code_sha256':{name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in code_files},
        'local_tests':json.loads((state_dir/'evidence_local_tests.json').read_text()),
        'scope':'Release-aware retrospective aggregate tests and prospective advisory/geography observations; no historical station-operation or causal-alert claim.',
        'active_model_unchanged':True,'replay_added_rows':0,'live_accuracy_available':False,
        'view_refresh_fix':'Stable view definitions are retained during routine daily appends; the outcome pointer changes only for a different prepared package.'}
    output=root/'docs/validation/release_aware_evidence_2026-10-16.json'
    output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'output':str(output),'cloud_runs':{k:v['state']['result_state'] for k,v in runs.items()},
        'checks':verification['checks'],'replay_added_rows':0,'schedules':schedules},indent=2))
