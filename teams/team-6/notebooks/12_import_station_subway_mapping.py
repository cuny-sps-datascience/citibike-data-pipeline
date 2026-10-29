# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Prospective Citi Bike to subway proximity
# MAGIC Fresh, dated station references. Distances are straight-line to MTA reference points (250m / 500m), not walking distances. Daytime routes are reference service only. No historical mapping, station operation or disruption effect is established. Stable current tables record `dataset_version`; immutable packages remain in the volume and on the RAID.

# COMMAND ----------
import hashlib
import json
from pathlib import Path
import re
from pyspark.sql import functions as F

dbutils.widgets.text('package_root','')
root=Path(dbutils.widgets.get('package_root'))
manifest=json.loads((root/'manifest.json').read_text())
assert manifest['status']=='built' and manifest['schema_version']=='station-subway-proximity-v1'
assert manifest['historical_validity_verified'] is False
dataset_version=manifest['mapping_version']
tables={}
for artifact in manifest['artifacts']:
    assert hashlib.sha256((root/artifact['path']).read_bytes()).hexdigest()==artifact['sha256']
    if not artifact['path'].endswith('.parquet'):
        continue
    name=artifact['name'];table='workspace.citibike_3h.station_subway_'+name
    frame=spark.read.parquet(str(root/artifact['path']))
    assert 'dataset_version' not in frame.columns and frame.count()==artifact['rows']
    frame.withColumn('dataset_version',F.lit(dataset_version)).write.format('delta').mode(
        'overwrite').option('overwriteSchema','true').saveAsTable(table)
    saved=spark.table(table)
    assert saved.count()==artifact['rows'] and saved.filter(F.col('dataset_version')!=dataset_version).count()==0
    spark.sql(f"COMMENT ON TABLE {table} IS 'Prospective reference only. Haversine distance to station point; daytime routes; historical validity and operation unverified.'")
    tables[name]={'table':table,'dataset_version':dataset_version,'rows':artifact['rows']}
coverage=spark.sql('''SELECT radius_m,count(*) AS bike_stations,
    sum(CASE WHEN nearby_station_points>0 THEN 1 ELSE 0 END) AS with_subway_station,
    sum(CASE WHEN nearby_routes>0 THEN 1 ELSE 0 END) AS with_daytime_route
    FROM workspace.citibike_3h.station_subway_coverage GROUP BY radius_m ORDER BY radius_m''')
report={'mapping_version':manifest['mapping_version'],'available_from_utc':manifest['available_from_utc'],
    'tables':tables,'coverage':[r.asDict() for r in coverage.collect()],
    'historical_validity_verified':False,'package_root':str(root)}
Path('/Volumes/workspace/citibike_3h/exports/station_subway_mapping_latest.json').write_text(json.dumps(report,indent=2)+'\n')
display(coverage)
dbutils.notebook.exit(json.dumps(report))
