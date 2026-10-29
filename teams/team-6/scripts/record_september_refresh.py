"""Record local integrity, cloud verification and presentation checks for the refresh."""
from __future__ import annotations
from datetime import datetime,timezone
import json,os,re,zipfile
from pathlib import Path
import pyarrow.parquet as pq
import pymupdf
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from citibike_pipeline.config import load_config
from citibike_pipeline.forecast_sources import digest
from citibike_pipeline.hourly_export import validate_hourly_package
from citibike_pipeline.release_watch import validate_outcomes


def main():
    root=Path(__file__).resolve().parents[1];state=root/'.local/execution'
    os.environ['PATH']=str(root/'.local/bin')+os.pathsep+os.environ['PATH']
    w=WorkspaceClient(profile='DATA622');cfg=load_config()
    base=cfg.processed_root/'modeling_exports/three_hour'
    runs={}
    for name,filename in [('evidence','databricks_evidence'),('weather','borough_weather'),
            ('health','databricks_health'),('dashboard','borough_dashboard'),('verification','september_verification')]:
        deployment=json.loads((state/(filename+'.json')).read_text())
        run=w.jobs.get_run(deployment['run_id'])
        assert run.state.result_state==jobs.RunResultState.SUCCESS,(name,run.state.as_dict())
        outputs={};tasks=[]
        for task in run.tasks or []:
            assert task.state.result_state==jobs.RunResultState.SUCCESS
            output=w.jobs.get_run_output(task.run_id)
            assert not output.error,(name,output.error)
            value=output.notebook_output.result if output.notebook_output else None
            try:value=json.loads(value) if value else None
            except json.JSONDecodeError:pass
            outputs[task.task_key]=value
            tasks.append({'key':task.task_key,'run_id':task.run_id,'state':task.state.as_dict()})
        runs[name]={'run_id':run.run_id,'state':run.state.as_dict(),'tasks':tasks,'outputs':outputs}
    verification=runs['verification']['outputs']
    assert all(n==0 for n in verification['evidence_checks']['checks'].values())
    assert verification['refresh_checks']['active_pointer_unchanged']
    assert verification['refresh_checks']['weather']['rows']==110928
    active=digest_bytes(w.files.download('/Volumes/workspace/citibike_3h/models/borough_active.json').contents.read())
    before=json.loads((state/'september_cloud_before.json').read_text())
    assert active==before['active_pointer_sha256']
    schedules={}
    for name,item in before['jobs'].items():
        schedule=w.jobs.get(item['job_id']).settings.schedule.as_dict()
        assert schedule==item['schedule'],(name,schedule)
        schedules[name]=schedule
    observed=base/'builds'/(base/'CURRENT_OBSERVED').read_text().strip()
    hourly=validate_hourly_package(observed)
    labels=Path((base/'borough_labels/CURRENT_BOROUGH_LABELS').read_text().strip())
    label_validation=validate_outcomes(labels)
    previous=json.loads((state/'september_previous_pointers.json').read_text())
    assert (base/'borough_features/CURRENT_BOROUGH_FEATURES').read_text().strip()==previous['borough_features/CURRENT_BOROUGH_FEATURES']
    weather=base/'weather_reference_builds/20261016T173331Z-84df9caa'
    weather_manifest=json.loads((weather/'manifest.json').read_text())
    for item in weather_manifest['artifacts']:
        assert digest(weather/item['path'])==item['sha256']
        assert pq.read_metadata(weather/item['path']).num_rows==item['rows']
    handoff=base/'handoffs/weather_reference_20261016T173331Z-84df9caa'
    handoff_manifest=json.loads((handoff/'manifest.json').read_text())
    for item in handoff_manifest['files']:assert digest(handoff/item['path'])==item['sha256']
    with zipfile.ZipFile(handoff.with_suffix('.zip')) as archive:assert archive.testzip() is None
    presentation=root/'docs/presentations'
    with zipfile.ZipFile(presentation/'data622_mvp.pptx') as archive:
        assert len([n for n in archive.namelist() if re.fullmatch(r'ppt/slides/slide\d+\.xml',n)])==3
        assert len([n for n in archive.namelist() if re.fullmatch(r'ppt/notesSlides/notesSlide\d+\.xml',n)])==3
    with pymupdf.open(presentation/'data622_mvp.pdf') as doc:assert len(doc)==3
    with pymupdf.open(presentation/'data622_whitepaper_draft.pdf') as doc:
        paper_pages=len(doc);assert 4<=paper_pages<=5
        assert all(page.get_text().strip() for page in doc)
    paper_words=len((root/'docs/technical_whitepaper_draft.md').read_text().split())
    assert 1800<=paper_words<=2300,paper_words
    documents=['README.md','docs/september_refresh.md','docs/technical_whitepaper_draft.md',
        'docs/midterm_demo_script.md','docs/borough_departure_datasheet.md',
        'docs/prospective_station_alert_datasheet.md','docs/weather_reference_datasheet.md',
        'docs/public_mvp_demo.md','docs/three_hour_execution_status.md','docs/runbook.md']
    destination=root/'docs/validation/september_refresh_2026-10-16.json'
    checked_links=0
    for name in documents:
        path=root/name
        for target in re.findall(r'\[[^\]]+\]\(([^)]+)\)',path.read_text()):
            if re.match(r'[a-zA-Z]+:',target) or target.startswith('#'):continue
            linked=(path.parent/target.split('#')[0]).resolve()
            assert linked.exists() or linked==destination,('broken link',name,target)
            checked_links+=1
    assert digest(root/'docs/citibike_proposal_4_pages_for_revisions.docx')=='fde305547639fb55442a84d0bb5f737ea5b4a4da7d24cc4988593a1231c2c90d'
    artifacts=documents+['docs/presentations/'+n for n in ['data622_mvp.pptx','data622_mvp.pdf',
        'slide_1.png','slide_2.png','slide_3.png','build_manifest.json','data622_whitepaper_draft.pdf']]
    report={'recorded_at_utc':datetime.now(timezone.utc).isoformat(),'runs':runs,
        'health':{'report':runs['health']['outputs']['daily_health']},'schedules':schedules,
        'active_pointer_sha256':active,'active_model_and_feature_pointer_unchanged':True,
        'observed_root':str(observed),'observed_input_files':len(hourly['input_files']),
        'outcome_validation':label_validation,
        'label_revision':json.loads((state/'september_label_revision.json').read_text()),
        'weather_manifest':weather_manifest,'weather_handoff_sha256':digest(handoff.with_suffix('.zip')),
        'local_tests':json.loads((state/'september_local_tests.json').read_text()),
        'presentation_checks':{'slides':3,'notes':3,'pdf_pages':3,'all_previews_visually_reviewed':True,
            'whitepaper_pages':paper_pages,'whitepaper_words':paper_words,'repository_links_checked':checked_links,
            'spoken_rehearsal_completed':False},
        'artifacts':[{'path':name,'sha256':digest(root/name),'bytes':(root/name).stat().st_size} for name in artifacts]}
    destination.write_text(json.dumps(report,default=str,indent=2)+'\n')
    print(json.dumps({'receipt':str(destination),'run_ids':{k:v['run_id'] for k,v in runs.items()},
        'outcome_hours':label_validation['rows'],'weather_rows':weather_manifest['rows'],
        'paper_words':paper_words,'links_checked':checked_links},indent=2))


def digest_bytes(value):
    import hashlib
    return hashlib.sha256(value).hexdigest()


if __name__=='__main__':main()
