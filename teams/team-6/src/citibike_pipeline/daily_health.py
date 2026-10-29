"""Small, dependency-free checks of forecast availability and public input logs."""
from datetime import datetime, timedelta, timezone
from math import isfinite

SOURCES = ('gbfs_discovery', 'gbfs_station_information', 'gbfs_station_status', 'mta_alerts', 'weather')


def utc(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00'))
    # Spark TIMESTAMP rows are UTC-naive because the notebook sets its session zone.
    return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)


def evaluate_health(forecasts, collections, now, monitoring_start, model_version,
                    matured_forecasts=0, grace_minutes=10, stale_minutes=90):
    now, start = utc(now), max(utc(now)-timedelta(hours=24), utc(monitoring_start))
    first = start.replace(minute=0, second=0, microsecond=0)
    if first < start:
        first += timedelta(hours=1)
    origins = []
    current = first
    while current+timedelta(minutes=grace_minutes) <= now:
        origins.append(current)
        current += timedelta(hours=1)
    forecasts = [r for r in forecasts if r.get('scope')=='Manhattan' and r.get('model_version')==model_version]
    details = []
    for origin in origins:
        rows = [r for r in forecasts if utc(r['origin_utc'])==origin]
        complete = len(rows)==3 and {r['horizon'] for r in rows}=={1,2,3}
        on_time = complete and all(r.get('publication_completed_at_utc') is not None and
            utc(r['publication_completed_at_utc'])<origin for r in rows)
        valid = complete and all(r.get('prediction') is not None and
            isfinite(float(r['prediction'])) and float(r['prediction'])>=0 for r in rows)
        details.append({'origin_utc':origin.isoformat(),'rows':len(rows),
            'complete_three_horizons':complete,'published_before_origin':on_time,
            'valid_predictions':valid,'fallback_rows':sum(bool(r.get('fallback_reason')) for r in rows)})
    recent = [r for r in collections if now-timedelta(hours=24) <= utc(r['observed_at_utc']) <= now]
    source_health = []
    for source in SOURCES:
        rows = [r for r in recent if r['source']==source]
        success = [utc(r['observed_at_utc']) for r in rows if r['success']]
        latest = max(success) if success else None
        age = (now-latest).total_seconds()/60 if latest else None
        source_health.append({'source':source,'attempts':len(rows),
            'failed_attempts':sum(not r['success'] for r in rows),
            'last_success_utc':latest.isoformat() if latest else None,
            'minutes_since_success':round(age,2) if age is not None else None,
            'stale':age is None or age>stale_minutes})
    incomplete = sum(not r['complete_three_horizons'] for r in details)
    late = sum(r['complete_three_horizons'] and not r['published_before_origin'] for r in details)
    invalid = sum(r['complete_three_horizons'] and not r['valid_predictions'] for r in details)
    fallback = sum(r['fallback_rows'] for r in details)
    failures = sum(r['failed_attempts'] for r in source_health)
    stale = sum(r['stale'] for r in source_health)
    status = 'ERROR' if incomplete or late or invalid else 'WARN' if fallback or failures or stale or not origins else 'OK'
    return {'schema_version':'borough-daily-health-v1','checked_at_utc':now.isoformat(),
        'window_start_utc':start.isoformat(),'monitoring_start_utc':utc(monitoring_start).isoformat(),
        'model_version':model_version,'status':status,'expected_origins':len(origins),
        'complete_origins':sum(r['complete_three_horizons'] for r in details),
        'incomplete_origins':incomplete,'late_origins':late,'invalid_prediction_origins':invalid,
        'fallback_rows':fallback,'source_failures':failures,'stale_sources':stale,
        'grace_minutes':grace_minutes,'source_stale_after_minutes':stale_minutes,
        'matured_forecasts':matured_forecasts,'prospective_accuracy_available':matured_forecasts>0,
        'origins':details,'sources':source_health,
        'measurement':'Canonical persisted forecasts and collection logs; scheduler attempt/job-state audit is separate.',
        'freshness_definition':'Successful HTTP retrieval time; does not establish provider payload age or verified forecast availability.',
        'accuracy_note':'Operational availability is not prediction accuracy. Accuracy waits for matching published trip outcomes.'}
