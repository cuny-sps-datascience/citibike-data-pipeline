from datetime import datetime,timedelta,timezone
from pathlib import Path
import hashlib,json
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from citibike_pipeline.release_watch import parse_listing,validate_outcomes

def listing(contents,truncated=False,token=''):
    return ('<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        +f'<IsTruncated>{str(truncated).lower()}</IsTruncated><NextContinuationToken>{token}</NextContinuationToken>'
        +''.join(f'<Contents><Key>{name}</Key><ETag>"hash"</ETag><Size>123</Size>'
                '<LastModified>2026-10-16T00:00:00Z</LastModified></Contents>' for name in contents)
        +'</ListBucketResult>').encode()

def test_listing_families_versions_and_unsupported_objects():
    rows,token=parse_listing(listing(['202609-citibike-tripdata.zip','JC-202609-citibike-tripdata.csv.zip','2026.zip','index.html']))
    assert len(rows)==2 and token is None
    assert rows[0]['family']=='nyc' and rows[1]['family']=='jersey_city'
    assert rows[0]['object_version']!=rows[1]['object_version']
    assert rows[0]['nominal_release']=='2026-09'

def test_truncation_without_token_fails():
    with pytest.raises(ValueError,match='continuation'):
        parse_listing(listing([],True))

def package(root, rows=None):
    start=datetime(2026,1,1,tzinfo=timezone.utc)
    rows=rows if rows is not None else [dict(hour_utc=start+timedelta(hours=i),borough='Manhattan',departures=1,
        assigned_departures=1,label_assignable=True,station_operation_required=False) for i in range(3)]
    file=root/'borough_hourly.parquet';pq.write_table(pa.Table.from_pylist(rows),file)
    manifest={'schema_version':'manhattan-aggregate-labels-v2','status':'built','start':start.isoformat(),
        'end_exclusive':(start+timedelta(hours=3)).isoformat(),'label_hours':3,'departures':3,
        'input_files':[{'path':'selected.parquet'}],'observed_manifest_sha256':'ledger-export-hash',
        'artifacts':[{'path':file.name,'rows':len(rows),'sha256':hashlib.sha256(file.read_bytes()).hexdigest()}]}
    (root/'manifest.json').write_text(json.dumps(manifest));return rows

def test_prepared_outcome_checksums_calendar_and_scope(tmp_path):
    rows=package(tmp_path)
    assert validate_outcomes(tmp_path)['rows']==3
    rows[2]['hour_utc']=rows[1]['hour_utc'];package(tmp_path,rows)
    with pytest.raises(ValueError,match='incomplete/duplicated'):validate_outcomes(tmp_path)

def test_unassignable_hours_cannot_be_zero_labels(tmp_path):
    rows=package(tmp_path);rows[0]['label_assignable']=False;rows[0]['departures']=0
    package(tmp_path,rows)
    with pytest.raises(ValueError,match='null label'):validate_outcomes(tmp_path)

def test_arbitrary_labels_without_source_evidence_fail(tmp_path):
    package(tmp_path);file=tmp_path/'manifest.json';manifest=json.loads(file.read_text())
    manifest['input_files']=[];file.write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match='provenance'):validate_outcomes(tmp_path)
