import importlib.util
import json
from pathlib import Path
import pytest
from citibike_pipeline.forecast_sources import digest


def collector():
    path=Path(__file__).resolve().parents[1]/'scripts/collect_public_history.py'
    spec=importlib.util.spec_from_file_location('weather_history_collector',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_extension_preserves_previous_manifest_and_publishes_full_requested_window(tmp_path,monkeypatch):
    module=collector();target=tmp_path/'weather';target.mkdir()
    old=target/'manifest.json';old.write_text(json.dumps({'expected_runs':1,'version':'original'}))
    previous=old.read_bytes();previous_sha=digest(old)
    def cached(url,path):
        path.write_text(json.dumps({'hourly':{'time':list(range(48))}}))
        return {'path':str(path),'sha256':digest(path),'url':url}
    monkeypatch.setattr(module,'cached',cached)
    monkeypatch.setattr(module.time,'sleep',lambda delay:None)
    report=module.weather(tmp_path,'2026-09-01','2026-09-02')
    assert report['expected_runs']==4 and len(report['artifacts'])==4 and not report['failures']
    assert (target/'manifests'/(previous_sha+'.json')).read_bytes()==previous
    assert (target/'manifests'/(digest(old)+'.json')).read_bytes()==old.read_bytes()
    assert json.loads(old.read_text())==report
    assert not (target/'manifest.pending.json').exists()


def test_invalid_weather_window_cannot_replace_existing_evidence(tmp_path):
    module=collector();target=tmp_path/'weather';target.mkdir()
    old=target/'manifest.json';old.write_text('original')
    for first,last in [('2026-09-02','2026-09-01'),('2026-09-01T06:00','2026-09-02')]:
        with pytest.raises(ValueError):module.weather(tmp_path,first,last)
    assert old.read_text()=='original'
