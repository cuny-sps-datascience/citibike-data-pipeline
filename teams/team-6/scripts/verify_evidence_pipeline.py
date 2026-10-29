"""Run an incremental replay followed by independent cloud evidence checks."""
import json,os
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs,workspace

if __name__=='__main__':
    root=Path(__file__).resolve().parents[1]
    os.environ['PATH']=str(root/'.local/bin')+os.pathsep+os.environ['PATH']
    w=WorkspaceClient(profile='DATA622')
    state=json.loads((root/'.local/execution/databricks_evidence.json').read_text())
    job=w.jobs.get(state['job_id'])
    task=next(t for t in job.settings.tasks if t.task_key=='prospective_evidence')
    rolling=json.loads((root/'.local/execution/borough_rolling_result.json').read_text())
    folder=task.notebook_task.notebook_path.rsplit('/',1)[0]
    filename='18_verify_evidence';notebook=folder+'/'+filename
    w.workspace.upload(notebook,(root/'notebooks'/(filename+'.py')).read_bytes(),
        format=workspace.ImportFormat.SOURCE,language=workspace.Language.PYTHON,overwrite=True)
    run=w.jobs.submit(run_name='DATA622 Evidence Replay and Verification',environments=job.settings.environments,
        tasks=[jobs.SubmitTask(task_key='replay',notebook_task=task.notebook_task,environment_key=task.environment_key,
            timeout_seconds=900),jobs.SubmitTask(task_key='verify',depends_on=[jobs.TaskDependency('replay')],
            notebook_task=jobs.NotebookTask(notebook,base_parameters={
                'expected_active_pointer_sha256':rolling['active_pointer_sha256']}),
            environment_key=task.environment_key,timeout_seconds=600)])
    result={'run_id':run.run_id,'notebook':notebook,'expected_active_pointer_sha256':rolling['active_pointer_sha256']}
    (root/'.local/execution/evidence_verification.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
