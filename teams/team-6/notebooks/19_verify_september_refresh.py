# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Verify the September data refresh
# MAGIC Read-only checks of imported outcomes and the separate extended weather contribution. Original features and the active model are preserved. September outcomes cannot evaluate October forecasts.

# COMMAND ----------
from datetime import datetime,timezone
import hashlib,json
from pathlib import Path

dbutils.widgets.text('weather_table','')
dbutils.widgets.text('expected_active_pointer_sha256','')
table=dbutils.widgets.get('weather_table') or 'workspace.citibike_3h.external_weather_reference'
assert table=='workspace.citibike_3h.external_weather_reference'
active=Path('/Volumes/workspace/citibike_3h/models/borough_active.json')
assert hashlib.sha256(active.read_bytes()).hexdigest()==dbutils.widgets.get('expected_active_pointer_sha256')
spark.conf.set('spark.sql.session.timeZone','UTC')
outcomes=spark.sql('''SELECT count(*) AS hours,count(DISTINCT hour_utc) AS unique_hours,
    min(hour_utc) AS first_hour,max(hour_utc) AS last_hour,
    sum(CASE WHEN NOT label_assignable THEN 1 ELSE 0 END) AS unassignable,
    sum(assigned_departures) AS assigned_departures
    FROM workspace.citibike_3h.borough_actual_hourly''').first().asDict()
assert outcomes['hours']==outcomes['unique_hours']==18984
assert outcomes['unassignable']==4 and outcomes['assigned_departures']==63393151
assert outcomes['last_hour'].isoformat()=='2026-10-01T03:00:00'
september=spark.sql('''SELECT count(*) AS hours,sum(departures) AS departures
    FROM workspace.citibike_3h.borough_actual_hourly
    WHERE hour_utc>=TIMESTAMP '2026-09-01 04:00:00' AND hour_utc<TIMESTAMP '2026-10-01 04:00:00' ''').first().asDict()
assert september=={'hours':720,'departures':3049503}
weather=spark.sql(f'''SELECT count(*) AS rows,count(DISTINCT initialization_utc) AS runs,
    count(DISTINCT CASE WHEN initialization_utc>=TIMESTAMP '2026-09-01' AND
        initialization_utc<TIMESTAMP '2026-10-01' THEN initialization_utc END) AS september_runs,
    sum(CASE WHEN availability_verified THEN 1 ELSE 0 END) AS incorrectly_verified
    FROM {table}''').first().asDict()
assert weather=={'rows':110928,'runs':2311,'september_runs':120,'incorrectly_verified':0}
weather_versions=[r[0] for r in spark.table(table).select('dataset_version').distinct().collect()]
assert len(weather_versions)==1 and weather_versions[0]
assert spark.sql(f'''SELECT count(*) FROM (SELECT initialization_utc,valid_time_utc,count(*) AS n
    FROM {table} GROUP BY ALL HAVING n>1)''').first()[0]==0
candidate=spark.sql('''SELECT readiness FROM workspace.citibike_3h.trip_release_candidates
    WHERE family='nyc' AND nominal_release='2026-09' ''').collect()
assert len(candidate)==1 and candidate[0]['readiness']=='within_imported_outcome_period'
evaluated=spark.table('workspace.citibike_3h.borough_prospective_evaluation').count()
assert evaluated==0
report={'checked_at_utc':datetime.now(timezone.utc).isoformat(),'outcomes':outcomes,
    'september':september,'weather':weather,'weather_table':table,
    'weather_dataset_version':weather_versions[0],
    'september_candidate_readiness':candidate[0]['readiness'],'evaluated_forecasts':evaluated,
    'active_pointer_unchanged':True}
Path('/Volumes/workspace/citibike_3h/exports/september_refresh_verification.json').write_text(json.dumps(report,default=str,indent=2)+'\n')
dbutils.notebook.exit(json.dumps(report,default=str))
