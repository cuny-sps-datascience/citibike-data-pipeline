from datetime import date,datetime,timezone
from urllib.error import HTTPError

import pytest

from citibike_pipeline.forecast_sources import alert_coverage,fetch,normalize_run


def test_quiet_archive_dates_remain_unknown_instead_of_becoming_normal_service():
    result=alert_coverage([{'date':'2026-07-30T23:55:00.000'}],date(2026,7,30),date(2026,8,2))
    assert result['dates_without_records']==['2026-07-31','2026-08-01']
    assert not result['continuous_feed_coverage_verified']
    assert not result['timestamp_timezone_verified']


def test_model_initializations_are_validated_after_utc_conversion():
    assert normalize_run('2025-03-01T01:00+01:00')==datetime(2025,3,1,tzinfo=timezone.utc)
    assert normalize_run('2025-03-01T06:00')==datetime(2025,3,1,6,tzinfo=timezone.utc)
    with pytest.raises(ValueError,match='UTC'):
        normalize_run('2025-03-01T05:00')


def test_nonretryable_http_error_does_not_publish_a_response(tmp_path,monkeypatch):
    import citibike_pipeline.forecast_sources as module
    calls=[]
    def denied(*args,**kwargs):
        calls.append(args)
        raise HTTPError('https://example.test',403,'Forbidden',{},None)
    monkeypatch.setattr(module,'urlopen',denied)
    with pytest.raises(HTTPError):
        fetch('https://example.test',tmp_path/'source.json')
    assert len(calls)==1
    assert not (tmp_path/'source.json').exists()
