# Databricks notebook source
# /// script
# dependencies = ["gtfs-realtime-bindings==3.0.0"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Observed station history and subway advisory exposure
# MAGIC Processes timestamped raw collections incrementally. MTASBWY identifies subway selectors; MTA NYCT identifies buses in the alert feed. Selector fields are conjunctive. Regular GTFS stop associations retain separate shuttle/express route IDs. Advisory periods describe display, not verified service impact. No departure labels or continuous station operation are inferred. Derived availability is the completed publication time, including for backfilled snapshots.

# COMMAND ----------
from datetime import datetime,timezone
import gzip,hashlib,importlib.util,json
from pathlib import Path
from uuid import uuid4
import pyarrow as pa
import pyarrow.parquet as pq

dbutils.widgets.text('helper_path','');dbutils.widgets.text('helper_sha256','')
dbutils.widgets.text('reference_root','')
path=Path(dbutils.widgets.get('helper_path'));checksum=dbutils.widgets.get('helper_sha256')
assert hashlib.sha256(path.read_bytes()).hexdigest()==checksum
spec=importlib.util.spec_from_file_location('prospective_evidence',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
ref_root=Path(dbutils.widgets.get('reference_root'));manifest=json.loads((ref_root/'manifest.json').read_text())
for item in manifest['artifacts']:
    assert hashlib.sha256((ref_root/item['path']).read_bytes()).hexdigest()==item['sha256']
reference=json.loads((ref_root/'gtfs_reference.json').read_text())
mapping=json.loads(Path('/Volumes/workspace/citibike_3h/exports/station_subway_mapping_latest.json').read_text())
map_root=Path(mapping['package_root']);map_manifest=json.loads((map_root/'manifest.json').read_text())
for name in ('coverage','station_proximity'):
    artifact=next(a for a in map_manifest['artifacts'] if a['path']==name+'.parquet')
    assert hashlib.sha256((map_root/artifact['path']).read_bytes()).hexdigest()==artifact['sha256']
coverage=pq.read_table(map_root/'coverage.parquet').to_pylist()
points=pq.read_table(map_root/'station_proximity.parquet').to_pylist()
reference_available=max(module.utc(reference['available_from_utc']),module.utc(map_manifest['built_at_utc']))
processing_version=hashlib.sha256((checksum+reference['reference_version']+mapping['mapping_version']).encode()).hexdigest()
spark.conf.set('spark.sql.session.timeZone','UTC')
qualified='workspace.citibike_3h.'
spark.sql('''CREATE TABLE IF NOT EXISTS workspace.citibike_3h.prospective_publications
    (batch_id STRING,publication_completed_at_utc TIMESTAMP,processing_version STRING) USING DELTA''')
completed=set()
if spark.catalog.tableExists(qualified+'prospective_source_coverage'):
    completed={(r['snapshot_run_id'],r['source']) for r in spark.table(qualified+'prospective_source_coverage').where(
        "_processing_version='"+processing_version+"'").select('snapshot_run_id','source').collect()}
logs=[r.asDict() for r in spark.table(qualified+'collection_log').where(
    "source IN ('gbfs_station_information','mta_alerts')").orderBy('observed_at_utc','run_id','source').collect()]
pending=[r for r in logs if (r['run_id'],r['source']) not in completed][:72]
batch_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8]
root=Path('/Volumes/workspace/citibike_3h/exports/prospective_evidence')/batch_id;root.mkdir(parents=True,exist_ok=False)
S=pa.string();D=pa.float64();L=pa.int64();B=pa.bool_();T=pa.timestamp('us',tz='UTC')
common={'_batch_id':S,'_processing_version':S,'_raw_sha256':S}
snapshot={'snapshot_run_id':S,'observed_at_utc':T,'feed_timestamp_utc':T}
schemas={
 'station_information_observations':snapshot|{'station_id':S,'short_name':S,'name':S,'latitude':D,'longitude':D,
    'capacity':L,'configuration_sha256':S,'station_id_scheme':S,'short_name_scheme':S,
    'historical_operation_verified':B,'effective_change_time_verified':B},
 'mta_alert_observations':snapshot|{'feed_age_seconds':D,'feed_fresh':B,'entity_id':S,'revision_sha256':S,
    'periods_json':S,'header_text':S,'planned_work':B,'cause':S,'effect':S,
    'provider_publication_time_verified':B,'is_subway_alert':B,'unresolved_subway_selectors':L},
 'mta_alert_selectors':snapshot|{'feed_age_seconds':D,'feed_fresh':B,'entity_id':S,'revision_sha256':S,
    'selector_index':L,'agency_id':S,'route_id':S,'stop_id':S,'route_type':L,'direction_id':L,
    'trip_restricted':B,'resolution':S,'resolved_parent_count':L},
 'station_subway_alert_features':{'snapshot_run_id':S,'bike_station_id':S,'radius_m':L,'observed_at_utc':T,
    'mapping_version':S,'reference_available_at_utc':T,'feed_fresh':B,'mapping_compatible':B,'gbfs_info_fresh':B,
    'matched_active_alert_count':L,'matched_known_display_overlap_next_3h':L,'matched_alerts_without_explicit_period':L,
    'unresolved_subway_selectors':L,'normal_service_verified':B,'historical_operation_verified':B,'interpretation':S},
 'prospective_source_coverage':{'snapshot_run_id':S,'source':S,'observed_at_utc':T,'feed_timestamp_utc':T,
    'success':B,'rows':L,'raw_path':S,'error':S}}
buffers={name:[] for name in schemas};info_cache={}
source_lookup={(r['run_id'],r['source']):r for r in logs}
def raw_input(record):
    raw=gzip.decompress(Path(record['path']).read_bytes())
    assert hashlib.sha256(raw).hexdigest()==record['sha256']
    return raw
def information(run_id):
    if run_id not in info_cache:
        record=source_lookup.get((run_id,'gbfs_station_information'))
        info_cache[run_id]=module.normalize_stations(raw_input(record),run_id,record['observed_at_utc'])[0] if record and record['success'] else []
    return info_cache[run_id]
for record in pending:
    run_id=record['run_id'];source=record['source'];observed=module.utc(record['observed_at_utc'])
    additions={};feed_at=None;count=0
    if record['success']:
        if source=='gbfs_station_information':
            rows=information(run_id);feed_at=rows[0]['feed_timestamp_utc'] if rows else None
            additions={'station_information_observations':rows};count=len(rows)
        else:
            alerts,selectors,meta=module.parse_alerts(raw_input(record),run_id,observed,reference)
            features=module.station_alert_features(alerts,meta,information(run_id),points,coverage,reference,reference_available)
            additions={'mta_alert_observations':alerts,'mta_alert_selectors':selectors,'station_subway_alert_features':features}
            feed_at=meta['feed_timestamp_utc'];count=len(alerts)
    additions['prospective_source_coverage']=[{'snapshot_run_id':run_id,'source':source,'observed_at_utc':observed,
        'feed_timestamp_utc':feed_at,'success':record['success'],'rows':count,'raw_path':record['path'],'error':record['error']}]
    for name,rows in additions.items():
        for row in rows:
            buffers[name].append(row|{'_batch_id':batch_id,'_processing_version':processing_version,'_raw_sha256':record['sha256']})

# COMMAND ----------
keys={'station_information_observations':['snapshot_run_id','station_id'],
    'mta_alert_observations':['snapshot_run_id','entity_id'],
    'mta_alert_selectors':['snapshot_run_id','entity_id','selector_index'],
    'station_subway_alert_features':['snapshot_run_id','bike_station_id','radius_m'],
    'prospective_source_coverage':['snapshot_run_id','source']}
artifacts=[]
for name,fields in schemas.items():
    rows=buffers[name];unique={tuple(r[k] for k in keys[name]) for r in rows}
    assert len(unique)==len(rows),name+' duplicate keys in batch'
    artifact=root/(name+'.parquet')
    pq.write_table(pa.Table.from_pylist(rows,schema=pa.schema(fields|common)),artifact,compression='zstd')
    frame=spark.read.parquet(str(artifact));assert frame.count()==len(rows)
    frame.write.format('delta').mode('append').saveAsTable(qualified+name+'_raw')
    artifacts.append({'path':str(artifact),'rows':len(rows),'sha256':hashlib.sha256(artifact.read_bytes()).hexdigest()})
    partition=','.join('r.'+k for k in keys[name])+',r._processing_version'
    spark.sql(f'''CREATE VIEW IF NOT EXISTS {qualified}{name} AS
        SELECT * EXCEPT(_rank) FROM (
            SELECT r.*,p.publication_completed_at_utc,
            row_number() OVER(PARTITION BY {partition} ORDER BY p.publication_completed_at_utc,r._batch_id) AS _rank
            FROM {qualified}{name}_raw r JOIN {qualified}prospective_publications p ON r._batch_id=p.batch_id
        ) WHERE _rank=1''')
completed_at=datetime.now(timezone.utc)
spark.createDataFrame([(batch_id,completed_at,processing_version)],
    'batch_id STRING,publication_completed_at_utc TIMESTAMP,processing_version STRING').write.format('delta').mode('append').saveAsTable(
        qualified+'prospective_publications')

# COMMAND ----------
spark.sql('''CREATE VIEW IF NOT EXISTS workspace.citibike_3h.station_geography_changes AS
    WITH observations AS (
      SELECT *,lag(configuration_sha256) OVER w AS previous_configuration_sha256,
      lag(latitude) OVER w AS previous_latitude,lag(longitude) OVER w AS previous_longitude,
      lag(short_name) OVER w AS previous_short_name,lag(name) OVER w AS previous_name,
      lag(observed_at_utc) OVER w AS previous_observed_at_utc
      FROM workspace.citibike_3h.station_information_observations
      WINDOW w AS (PARTITION BY station_id,_processing_version ORDER BY observed_at_utc,snapshot_run_id)
    ) SELECT * FROM observations WHERE previous_configuration_sha256 IS NOT NULL
      AND configuration_sha256<>previous_configuration_sha256''')
spark.sql('''CREATE VIEW IF NOT EXISTS workspace.citibike_3h.station_geography_observed_intervals AS
    WITH observations AS (
      SELECT *,lag(configuration_sha256) OVER w AS previous_hash,lag(observed_at_utc) OVER w AS previous_seen
      FROM workspace.citibike_3h.station_information_observations
      WINDOW w AS (PARTITION BY station_id,_processing_version ORDER BY observed_at_utc,snapshot_run_id)
    ), groups AS (
      SELECT *,sum(CASE WHEN previous_hash IS NULL OR previous_hash<>configuration_sha256 THEN 1 ELSE 0 END)
      OVER(PARTITION BY station_id,_processing_version ORDER BY observed_at_utc,snapshot_run_id ROWS UNBOUNDED PRECEDING) AS observed_version
      FROM observations
    ) SELECT station_id,_processing_version,observed_version,short_name,name,latitude,longitude,capacity,configuration_sha256,
      min(observed_at_utc) AS first_observed_utc,max(observed_at_utc) AS last_observed_utc,count(*) AS observations,
      max(timestampdiff(SECOND,previous_seen,observed_at_utc)) AS maximum_previous_observation_gap_seconds,
      false AS effective_change_time_verified,false AS historical_operation_verified
    FROM groups GROUP BY ALL''')
spark.sql('''CREATE VIEW IF NOT EXISTS workspace.citibike_3h.station_observation_gaps AS
    WITH observations AS (
      SELECT station_id,_processing_version,observed_at_utc,lag(observed_at_utc) OVER(
      PARTITION BY station_id,_processing_version ORDER BY observed_at_utc,snapshot_run_id) AS previous_seen
      FROM workspace.citibike_3h.station_information_observations
    ) SELECT *,timestampdiff(SECOND,previous_seen,observed_at_utc) AS gap_seconds
    FROM observations WHERE timestampdiff(SECOND,previous_seen,observed_at_utc)>5400''')
spark.sql('''CREATE VIEW IF NOT EXISTS workspace.citibike_3h.prospective_source_gaps AS
    WITH observations AS (
      SELECT source,_processing_version,observed_at_utc,lag(observed_at_utc) OVER(
      PARTITION BY source,_processing_version ORDER BY observed_at_utc,snapshot_run_id) AS previous_success
      FROM workspace.citibike_3h.prospective_source_coverage WHERE success
    ) SELECT *,timestampdiff(SECOND,previous_success,observed_at_utc) AS gap_seconds
    FROM observations WHERE timestampdiff(SECOND,previous_success,observed_at_utc)>5400''')
spark.sql('''CREATE VIEW IF NOT EXISTS workspace.citibike_3h.mta_alert_revision_history AS
    SELECT entity_id,revision_sha256,min(observed_at_utc) AS first_seen_utc,max(observed_at_utc) AS last_seen_utc,
      count(*) AS snapshots,is_subway_alert,planned_work,header_text,periods_json,
      false AS provider_publication_time_verified
    FROM workspace.citibike_3h.mta_alert_observations GROUP BY ALL''')
spark.sql('''CREATE VIEW IF NOT EXISTS workspace.citibike_3h.station_subway_alert_features_available AS
    SELECT *,greatest(observed_at_utc,reference_available_at_utc,publication_completed_at_utc) AS available_from_utc
    FROM workspace.citibike_3h.station_subway_alert_features''')
report={'batch_id':batch_id,'processing_version':processing_version,'publication_completed_at_utc':completed_at.isoformat(),
    'processed_inputs':len(pending),'remaining_inputs':sum((r['run_id'],r['source']) not in completed for r in logs)-len(pending),
    'reference_version':reference['reference_version'],'mapping_version':mapping['mapping_version'],
    'artifacts':artifacts,'batch_rows':{k:len(v) for k,v in buffers.items()},
    'total_rows':{k:spark.table(qualified+k).count() for k in schemas},
    'geography_changes':spark.table(qualified+'station_geography_changes').count(),
    'station_observation_gaps':spark.table(qualified+'station_observation_gaps').count(),
    'source_gaps':spark.table(qualified+'prospective_source_gaps').count(),
    'unresolved_subway_selectors':spark.table(qualified+'mta_alert_selectors').where(
        "agency_id='MTASBWY' AND resolution NOT LIKE 'resolved%'").count(),
    'station_operation_verified':False,'historical_alert_effect_estimated':False,'active_forecast_model_changed':False}
(root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
Path('/Volumes/workspace/citibike_3h/exports/prospective_evidence_latest.json').write_text(json.dumps(report,indent=2)+'\n')
display(spark.table(qualified+'station_geography_observed_intervals').limit(20))
display(spark.table(qualified+'mta_alert_revision_history').where('is_subway_alert').limit(20))
dbutils.notebook.exit(json.dumps(report))
