from dataclasses import replace
from pathlib import Path
import json
import pytest
import yaml
from archive_fixture import csv_bytes,zip_bytes,QUIET
from citibike_pipeline.config import load_config,ConfigError
from citibike_pipeline.discover import discover,open_candidate,source_path
from citibike_pipeline.inventory import run_discover
from citibike_pipeline.manifest import Manifest

def test_external_archive_is_readable_without_changing_original_keys(cfg,tmp_path):
    from citibike_pipeline.schemas import MODERN
    original_keys={c.source_key for c in discover(cfg)}
    extra=tmp_path/'new_raw';extra.mkdir()
    path=extra/'202609-citibike-tripdata.zip'
    path.write_bytes(zip_bytes({'202609-citibike-tripdata_1.csv':csv_bytes(MODERN,[])}))
    expanded=replace(cfg,additional_source_roots=(extra,))
    rows=list(discover(expanded))
    assert original_keys < {c.source_key for c in rows}
    candidate=next(c for c in rows if c.source_key not in original_keys)
    assert candidate.source_key.startswith('external/') and Path(candidate.path).is_absolute()
    before=path.read_bytes()
    with open_candidate(expanded,candidate) as f:assert f.readline().startswith(b'ride_id')
    assert path.read_bytes()==before
    with pytest.raises(ValueError,match='escapes'):source_path(cfg,str(path))

def test_missing_extra_root_does_not_mark_any_sources_missing(cfg,tmp_path):
    with Manifest(cfg) as manifest:
        run_discover(cfg,manifest,'initial',QUIET)
        expanded=replace(cfg,additional_source_roots=(tmp_path/'unmounted',))
        with pytest.raises(FileNotFoundError):run_discover(expanded,manifest,'missing',QUIET)
        assert all(r['status']!='missing' for r in manifest.all())

def test_additional_roots_cannot_overlap_processed_or_code(tmp_path):
    for extra in (tmp_path/'processed'/'raw',tmp_path/'code'/'raw',tmp_path/'source'/'raw'):
        path=tmp_path/'config.yaml'
        path.write_text(yaml.safe_dump({'schema_version':2,'parser_version':'test','paths':{
            'source_root':str(tmp_path/'source'),'processed_root':str(tmp_path/'processed'),
            'additional_source_roots':[str(extra)]}}))
        with pytest.raises(ConfigError):load_config(path,project_root=tmp_path/'code')

def test_scoped_discovery_preserves_primary_inventory_and_detects_missing_download(cfg,tmp_path):
    from citibike_pipeline.schemas import MODERN
    extra=tmp_path/'new_raw';extra.mkdir()
    path=extra/'202609-citibike-tripdata.zip'
    path.write_bytes(zip_bytes({'202609-citibike-tripdata_1.csv':csv_bytes(MODERN,[])}))
    with Manifest(cfg) as manifest:
        run_discover(cfg,manifest,'original',QUIET)
        original={r['source_key']:r for r in manifest.all()}
        expanded=replace(cfg,additional_source_roots=(extra,))
        result=run_discover(expanded,manifest,'external',QUIET,additional_only=True)
        assert result['new']==1 and result['missing']==0
        assert {k:manifest.get(k) for k in original}==original
        path.unlink()
        result=run_discover(expanded,manifest,'external_missing',QUIET,additional_only=True)
        assert result['missing']==1
        assert {k:manifest.get(k) for k in original}==original
