from datetime import datetime, timedelta, timezone
from citibike_pipeline.daily_health import SOURCES, evaluate_health

ORIGIN=datetime(2026,10,16,12,tzinfo=timezone.utc)

def inputs():
    forecasts=[dict(scope='Manhattan', model_version='v1', origin_utc=ORIGIN,
        horizon=h, prediction=100., publication_completed_at_utc=ORIGIN-timedelta(minutes=1),
        fallback_reason=None) for h in (1,2,3)]
    logs=[dict(source=s, success=True, observed_at_utc=ORIGIN-timedelta(minutes=3)) for s in SOURCES]
    return forecasts,logs

def run(forecasts, logs, now=None):
    return evaluate_health(forecasts,logs,now or ORIGIN+timedelta(minutes=15),ORIGIN,'v1')

def test_healthy_and_no_prospective_accuracy():
    report=run(*inputs())
    assert report['status']=='OK' and report['expected_origins']==1
    assert not report['prospective_accuracy_available']

def test_all_three_horizons_required_duplicates_fail():
    forecasts,logs=inputs()
    assert run(forecasts[:2],logs)['status']=='ERROR'
    assert run(forecasts+[forecasts[0]],logs)['incomplete_origins']==1

def test_publication_must_precede_origin_even_for_later_horizons():
    forecasts,logs=inputs()
    forecasts[2]['publication_completed_at_utc']=ORIGIN+timedelta(seconds=1)
    assert run(forecasts,logs)['late_origins']==1

def test_grace_does_not_redefine_on_time():
    forecasts,logs=inputs()
    assert run([],logs,ORIGIN+timedelta(minutes=5))['expected_origins']==0
    assert run([],logs)['incomplete_origins']==1

def test_source_failure_staleness_and_fallback_are_warnings():
    forecasts,logs=inputs()
    forecasts[0]['fallback_reason']='weather unavailable'
    assert run(forecasts,logs)['status']=='WARN'
    logs[0]['success']=False
    report=run(forecasts,logs)
    assert report['source_failures']==1 and report['stale_sources']==1

def test_wrong_version_and_nonfinite_predictions():
    forecasts,logs=inputs()
    forecasts[0]['prediction']=float('nan')
    assert run(forecasts,logs)['invalid_prediction_origins']==1
    forecasts[0]['model_version']='old'
    assert run(forecasts,logs)['incomplete_origins']==1
