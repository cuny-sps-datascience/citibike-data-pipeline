"""Read-only audit of scheduled scoring, including actual attempt publication time."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo


def attempt_timeliness(score):
    """A retry's original publication time cannot establish attempt timeliness."""
    value=score.get('attempt_publication_completed_at_utc')
    if not value:return None
    return datetime.fromisoformat(value)<datetime.fromisoformat(score['origin_utc'])


def missing_slots(starts, now, grace_minutes=10):
    """Check :55 UTC slots from first observed scored run; allow startup grace."""
    if not starts:return []
    normalized={dt.astimezone(timezone.utc).replace(minute=55,second=0,microsecond=0) for dt in starts}
    slot=min(normalized);missing=[]
    while slot+timedelta(minutes=grace_minutes)<=now:
        if slot not in normalized:missing.append(slot.isoformat())
        slot+=timedelta(hours=1)
    return missing


def main():
    from databricks.sdk import WorkspaceClient
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile',default='DATA622')
    parser.add_argument('--hours',type=int,default=24)
    parser.add_argument('--job-id',type=int,default=1072725421467387)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.hours<=0:parser.error('--hours must be positive')
    root=Path(__file__).resolve().parents[1]
    os.environ['PATH']=str(root/'.local/bin')+os.pathsep+os.environ['PATH']
    w=WorkspaceClient(profile=args.profile)
    now=datetime.now(timezone.utc);first=now-timedelta(hours=args.hours)
    job=w.jobs.get(args.job_id);records=[]
    for run in w.jobs.list_runs(job_id=args.job_id,expand_tasks=True,
            start_time_from=int(first.timestamp()*1000)):
        if not run.trigger or run.trigger.value!='PERIODIC':continue
        tasks={t.task_key:t for t in run.tasks or []}
        if 'score_borough' not in tasks:continue  # Earlier collection-only deployment.
        started=datetime.fromtimestamp(run.start_time/1000,timezone.utc)
        record={'run_id':run.run_id,'start_utc':started.isoformat(),
            'state':run.state.as_dict() if run.state else None,'tasks':{},'sources':[]}
        for name,task in tasks.items():
            item={'run_id':task.run_id,'state':task.state.as_dict() if task.state else None}
            if task.state and task.state.life_cycle_state.value in ('TERMINATED','INTERNAL_ERROR'):
                output=w.jobs.get_run_output(task.run_id)
                if output.error:item['error']=output.error[:1000]
                if output.notebook_output and output.notebook_output.result:
                    item['output']=json.loads(output.notebook_output.result)
            record['tasks'][name]=item
        score=record['tasks']['score_borough'].get('output',{})
        # The job's nominal origin comes from its trigger. A late task start
        # can fall in the following clock hour and must not shift its slot.
        record['scheduled_slot_utc']=(datetime.fromisoformat(score['origin_utc'])-
            timedelta(minutes=5)).isoformat() if score.get('origin_utc') else None
        record['attempt_published_before_origin']=attempt_timeliness(score)
        if score.get('attempt_publication_completed_at_utc'):
            published=datetime.fromisoformat(score['attempt_publication_completed_at_utc'])
            origin=datetime.fromisoformat(score['origin_utc'])
            record['publication_lead_seconds']=(origin-published).total_seconds()
        record['attempt_fallback_reasons']=score.get('attempted_fallback_reasons',[])
        record['prediction_rows_returned']=len(score.get('predictions',[]))
        record['sources']=record['tasks'].get('collect_sources',{}).get('output',{}).get('sources',[])
        records.append(record)
    records.sort(key=lambda r:r['start_utc'])
    gaps=missing_slots([datetime.fromisoformat(r['scheduled_slot_utc']) for r in records
        if r['scheduled_slot_utc']],now)
    report={'audited_at_utc':now.isoformat(),
        'audited_at_new_york':now.astimezone(ZoneInfo('America/New_York')).isoformat(),
        'requested_lookback_hours':args.hours,
        'audit_scope':'periodic runs with scoring; missing slots checked from first verified nominal origin, using trigger-derived slots',
        'job_url':w.config.host+'/jobs/'+str(args.job_id),
        'schedule':job.settings.schedule.as_dict() if job.settings.schedule else None,
        'scheduled_scoring_runs':len(records),
        'successful_runs':sum(r['state'].get('result_state')=='SUCCESS' for r in records),
        'on_time_attempts':sum(r['attempt_published_before_origin'] is True for r in records),
        'late_attempts':sum(r['attempt_published_before_origin'] is False for r in records),
        'unverified_attempt_times':sum(r['attempt_published_before_origin'] is None for r in records),
        'runs_with_fallback':sum(any(r['attempt_fallback_reasons']) for r in records),
        'source_failures':sum(not source['success'] for r in records for source in r['sources']),
        'source_attempts':sum(len(r['sources']) for r in records),
        'unverified_scheduled_slots':sum(r['scheduled_slot_utc'] is None for r in records),
        'missing_elapsed_slots_utc':gaps,'runs':records,
        'prospective_accuracy':'Unavailable until matching trip outcomes are published and imported'}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k!='runs'},indent=2))


if __name__=='__main__':main()
