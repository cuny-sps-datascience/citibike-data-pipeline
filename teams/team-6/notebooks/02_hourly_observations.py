# Databricks notebook source
# MAGIC %md
# MAGIC # Citi Bike hourly observations
# MAGIC This package preserves recorded departures and supplying releases. Unsupported zero hours are excluded.
# MAGIC Borough assignments use retrospective monthly trip coordinates; historical operation and geography remain unverified.

# COMMAND ----------
from pathlib import Path
import hashlib
import json
import re
from datetime import datetime, timezone
from pyspark.sql import functions as F

dbutils.widgets.text('export_root', '')
export_root = Path(dbutils.widgets.get('export_root'))
manifest = json.loads((export_root/'manifest.json').read_text())
assert manifest['status']=='validated_observations_operation_gate_pending'
spark.conf.set('spark.sql.session.timeZone','UTC')
version = re.sub('[^a-zA-Z0-9_]','_',export_root.name).lower()
dataset_version=export_root.name
qualified = 'workspace.citibike_3h'
imports=[]
for name in ('hourly_observed','hourly_release','station_geography_observed'):
    artifacts=[a for a in manifest['artifacts'] if a['table']==name]
    for a in artifacts:
        assert hashlib.sha256((export_root/a['path']).read_bytes()).hexdigest()==a['sha256']
    expected=sum(a['rows'] for a in artifacts)
    table=f'{qualified}.{name}'
    frame=spark.read.parquet(*[str(export_root/a['path']) for a in artifacts])
    assert 'dataset_version' not in frame.columns and frame.count()==expected
    frame.withColumn('dataset_version',F.lit(dataset_version)).write.format('delta').mode(
        'overwrite').option('overwriteSchema','true').saveAsTable(table)
    saved=spark.table(table);actual=saved.count()
    assert actual==expected and saved.filter(F.col('dataset_version')!=dataset_version).count()==0
    imports.append({'table':table,'dataset_version':dataset_version,'rows':actual})

# COMMAND ----------
spark.sql(f'''CREATE OR REPLACE VIEW {qualified}.manhattan_hourly_observed AS
    SELECT h.*,g.borough,g.geography_verified,g.evidence AS geography_evidence
    FROM {qualified}.hourly_observed h
    JOIN {qualified}.station_geography_observed g
        ON h.station_id=g.station_id AND h.id_scheme=g.id_scheme
        AND CAST(date_trunc('month',from_utc_timestamp(h.hour_utc,'America/New_York')) AS DATE)=g.month_local
    WHERE g.borough='Manhattan' ''')
display(spark.sql(f'''SELECT date_trunc('month',from_utc_timestamp(hour_utc,'America/New_York')) AS month,
    count(*) AS observed_station_hours,sum(departures) AS recorded_departures,
    count_if(main_evaluation_eligible) AS main_evaluation_eligible_hours
    FROM {qualified}.manhattan_hourly_observed GROUP BY 1 ORDER BY 1'''))
report={'completed_at_utc':datetime.now(timezone.utc).isoformat(), 'imports':imports,
    'main_evaluation_ready':False,'reason':'historical operating exposure and geography remain unverified'}
Path(f'/Volumes/workspace/citibike_3h/exports/hourly_import_{version}.json').write_text(json.dumps(report,indent=2)+'\n')
dbutils.notebook.exit(json.dumps(report))
