"""Deploy the bounded capability pilot to an explicitly authenticated Free Edition workspace."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import jobs, workspace, catalog


def upload_immutable(w, destination: str, content: bytes):
    """A resumed deployment reuses identical data and refuses a different overwrite."""
    import io
    try:
        previous = w.files.download(destination).contents.read()
    except NotFound:
        w.files.upload(destination, io.BytesIO(content), overwrite=False)
    else:
        if hashlib.sha256(previous).digest() != hashlib.sha256(content).digest():
            raise ValueError('Existing uploaded content differs: ' + destination)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-package', type=Path, required=True)
    parser.add_argument('--profile', default='DATA622')
    parser.add_argument('--catalog', default='workspace')
    parser.add_argument('--schema', default='citibike_3h')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    state_path = root/'.local/execution/databricks_pilot.json'
    state_path.parent.mkdir(parents=True,exist_ok=True)
    os.environ['PATH'] = str(root/'.local/bin') + os.pathsep + os.environ['PATH']
    w = WorkspaceClient(profile=args.profile)
    qualified = args.catalog + '.' + args.schema
    try:
        w.schemas.get(qualified)
    except NotFound:
        w.schemas.create(args.schema, args.catalog, comment='DATA 622 three-hour Citi Bike project; Randy Howk operates this workspace.')
    for name in ('inputs', 'models', 'exports'):
        try:
            w.volumes.read(qualified + '.' + name)
        except NotFound:
            w.volumes.create(args.catalog, args.schema, name, catalog.VolumeType.MANAGED)
    manifest = json.loads((args.source_package/'manifest.json').read_text())
    if manifest['status'] != 'collected_with_documented_coverage_gaps':
        raise ValueError('Incomplete source package')
    destination = f'/Volumes/{args.catalog}/{args.schema}/inputs/sources/{args.source_package.name}'
    for artifact in manifest['artifacts']:
        if not artifact['path'].startswith('tables/'):
            continue
        path = (args.source_package/artifact['path']).resolve()
        if not path.is_relative_to(args.source_package.resolve()):
            raise ValueError('Source artifact escapes package')
        if hashlib.sha256(path.read_bytes()).hexdigest() != artifact['sha256']:
            raise ValueError('Source artifact checksum mismatch')
        upload_immutable(w,destination+'/'+artifact['path'],path.read_bytes())
    upload_immutable(w,destination+'/manifest.json',(args.source_package/'manifest.json').read_bytes())
    user = w.current_user.me().user_name
    folder = '/Users/' + user + '/DATA622_ThreeHour'
    w.workspace.mkdirs(folder)
    notebook = folder + '/00_workspace_pilot'
    w.workspace.upload(notebook, (root/'notebooks/00_workspace_pilot.py').read_bytes(),
        format=workspace.ImportFormat.SOURCE, language=workspace.Language.PYTHON, overwrite=True)
    if state_path.exists():
        previous=json.loads(state_path.read_text())
        if previous['host']!=w.config.host or previous['source_root']!=destination or previous['schema']!=qualified:
            raise ValueError('Existing deployment state belongs to a different package or destination')
        job_id=previous['job_id']
    else:
        result = w.jobs.create(name='DATA622 Three Hour Capability Pilot',
            description='Bounded source import and runtime check. Synthetic test model is not a production forecast.',
            max_concurrent_runs=1, timeout_seconds=900,
            tasks=[jobs.Task(task_key='capability', notebook_task=jobs.NotebookTask(notebook,
                base_parameters={'catalog':args.catalog,'schema':args.schema,'source_root':destination}),
                max_retries=0, timeout_seconds=900)])
        job_id=result.job_id
    run = w.jobs.run_now(job_id=job_id)
    report = {'host': w.config.host, 'job_id': job_id, 'run_id': run.run_id,
        'notebook': notebook, 'source_root': destination, 'schema': qualified,
        'job_url': w.config.host + '/jobs/' + str(job_id),
        'status': 'submitted; inspect run before enabling a schedule'}
    state_path.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
