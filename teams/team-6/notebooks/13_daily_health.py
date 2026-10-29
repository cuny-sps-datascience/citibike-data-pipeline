# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Daily Manhattan forecast health
# MAGIC Rolling 24-hour checks of persisted forecasts and public-source retrieval logs. Three valid, on-time horizons are required per due origin. A ten-minute reporting grace allows pending writes but never changes the strict publication-before-origin rule. This monitors delivered output; scheduler attempt timing is audited separately. ERROR fails the job after preserving evidence. WARN covers fallbacks, failed retrievals or stale sources. No email or third-party messages are sent.

# COMMAND ----------
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
from uuid import uuid4

dbutils.widgets.text('helper_path','')
dbutils.widgets.text('helper_sha256','')
dbutils.widgets.text('monitoring_start_utc','2026-10-16T02:00:00+00:00')
helper=Path(dbutils.widgets.get('helper_path'))
assert hashlib.sha256(helper.read_bytes()).hexdigest()==dbutils.widgets.get('helper_sha256')
spec=importlib.util.spec_from_file_location('daily_health',helper)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
spark.conf.set('spark.sql.session.timeZone','UTC')
now=datetime.now(timezone.utc)
start=(now-timedelta(hours=24)).isoformat()
metadata=json.loads(Path('/Volumes/workspace/citibike_3h/models/borough_active.json').read_text())
forecasts=[r.asDict() for r in spark.table('workspace.citibike_3h.borough_forecasts').where(
    f"origin_utc >= TIMESTAMP '{start}'").collect()]
collections=[r.asDict() for r in spark.table('workspace.citibike_3h.collection_log').where(
    f"observed_at_utc >= TIMESTAMP '{start}'").collect()]
matured=spark.table('workspace.citibike_3h.borough_prospective_evaluation').count() if spark.catalog.tableExists(
    'workspace.citibike_3h.borough_prospective_evaluation') else 0
report=module.evaluate_health(forecasts,collections,now,dbutils.widgets.get('monitoring_start_utc'),
    metadata['model_version'],matured_forecasts=matured)
monitor_id=now.strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8]
root=Path('/Volumes/workspace/citibike_3h/exports/health')/monitor_id
root.mkdir(parents=True,exist_ok=False)
report['monitor_run_id']=monitor_id
report['report_path']=str(root/'report.json')
(root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
summary={k:report[k] for k in ('monitor_run_id','status','model_version','expected_origins','complete_origins',
    'incomplete_origins','late_origins','invalid_prediction_origins','fallback_rows','source_failures',
    'stale_sources','matured_forecasts','prospective_accuracy_available','report_path')}
summary['checked_at_utc']=now
frame=spark.createDataFrame([summary],'''monitor_run_id STRING,status STRING,model_version STRING,
    expected_origins LONG,complete_origins LONG,incomplete_origins LONG,late_origins LONG,
    invalid_prediction_origins LONG,fallback_rows LONG,source_failures LONG,stale_sources LONG,
    matured_forecasts LONG,prospective_accuracy_available BOOLEAN,report_path STRING,checked_at_utc TIMESTAMP''')
frame.write.format('delta').mode('append').saveAsTable('workspace.citibike_3h.borough_daily_health')
spark.sql('''CREATE OR REPLACE VIEW workspace.citibike_3h.borough_daily_health_latest AS
    SELECT * FROM workspace.citibike_3h.borough_daily_health ORDER BY checked_at_utc DESC LIMIT 1''')
Path('/Volumes/workspace/citibike_3h/exports/borough_health_latest.json').write_text(json.dumps(report,indent=2)+'\n')
display(frame)
if report['status']=='ERROR':
    raise RuntimeError('Forecast health ERROR: inspect '+report['report_path'])
dbutils.notebook.exit(json.dumps(report))
