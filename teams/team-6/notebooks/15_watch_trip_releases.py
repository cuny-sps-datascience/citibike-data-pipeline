# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Observe public trip releases
# MAGIC First-seen times are our retrieval evidence, not original publication dates. Provider LastModified can describe revisions. Candidate listing does not approve an archive or assert complete labels. No bulk ZIPs are downloaded and no canonical archive is modified.

# COMMAND ----------
from datetime import datetime,timezone
import hashlib,importlib.util,json
from pathlib import Path
from uuid import uuid4

dbutils.widgets.text('helper_path','');dbutils.widgets.text('helper_sha256','')
path=Path(dbutils.widgets.get('helper_path'))
assert hashlib.sha256(path.read_bytes()).hexdigest()==dbutils.widgets.get('helper_sha256')
spec=importlib.util.spec_from_file_location('release_watch',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
spark.conf.set('spark.sql.session.timeZone','UTC')
poll_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8]
root=Path('/Volumes/workspace/citibike_3h/exports/release_watch')/poll_id;root.mkdir(parents=True,exist_ok=False)
rows,artifacts=module.fetch_listing(root)
observed=datetime.now(timezone.utc)
for row in rows:row.update({'poll_id':poll_id,'observed_at_utc':observed})
frame=spark.createDataFrame(rows,'''object_key STRING,family STRING,nominal_release STRING,etag STRING,bytes LONG,
    provider_last_modified_utc STRING,url STRING,object_version STRING,poll_id STRING,observed_at_utc TIMESTAMP''')
frame.write.format('delta').mode('append').saveAsTable('workspace.citibike_3h.trip_release_observations')
spark.sql('''CREATE VIEW IF NOT EXISTS workspace.citibike_3h.trip_release_object_versions AS
    SELECT object_key,object_version,family,nominal_release,etag,bytes,provider_last_modified_utc,url,
    min(observed_at_utc) AS first_seen_utc,max(observed_at_utc) AS last_seen_utc,count(*) AS sightings,
    false AS original_publication_time_verified,false AS archive_complete_verified
    FROM workspace.citibike_3h.trip_release_observations GROUP BY ALL''')
# Only objects present in the latest complete successful listing are current candidates.
spark.sql('''CREATE VIEW IF NOT EXISTS workspace.citibike_3h.trip_release_candidates AS
    SELECT v.*, CASE WHEN v.nominal_release > date_format(from_utc_timestamp(a.latest_actual_hour,'America/New_York'),'yyyy-MM')
    THEN 'awaiting_local_ingestion_and_label_validation' ELSE 'within_imported_outcome_period' END AS readiness,
    false AS auto_ingestion_approved
    FROM workspace.citibike_3h.trip_release_object_versions v
    JOIN (SELECT object_version FROM workspace.citibike_3h.trip_release_observations
          WHERE observed_at_utc=(SELECT max(observed_at_utc) FROM workspace.citibike_3h.trip_release_observations)) c
    ON v.object_version=c.object_version
    CROSS JOIN (SELECT max(hour_utc) AS latest_actual_hour FROM workspace.citibike_3h.borough_actual_hourly) a
    WHERE v.family='nyc' ''')
pending=[r.asDict() for r in spark.table('workspace.citibike_3h.trip_release_candidates').where(
    "readiness='awaiting_local_ingestion_and_label_validation'").collect()]
report={'poll_id':poll_id,'observed_at_utc':observed.isoformat(),'monthly_objects':len(rows),
    'latest_nyc_release':max(r['nominal_release'] for r in rows if r['family']=='nyc'),
    'pending_candidates':pending,'raw_listing_artifacts':artifacts,
    'publication_time_verified':False,'bulk_downloaded':False}
(root/'report.json').write_text(json.dumps(report,default=str,indent=2)+'\n')
Path('/Volumes/workspace/citibike_3h/exports/trip_release_watch_latest.json').write_text(json.dumps(report,default=str,indent=2)+'\n')
display(spark.table('workspace.citibike_3h.trip_release_candidates').orderBy('nominal_release',ascending=False))
dbutils.notebook.exit(json.dumps(report,default=str))
