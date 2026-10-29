# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Import documented external weather contribution
# MAGIC Upload the portable handoff to an authorized class volume and replace the stable current table. Every row records the immutable volume package in `dataset_version`; the source handoff remains on the RAID. This notebook uses no class Citi Bike data. Credit Open-Meteo and ECMWF, CC BY 4.0. Initialization-plus-seven-hours is assumed availability; early archive includes hindcasts. The grid is a single Manhattan reference point, not a station weather map.

# COMMAND ----------
from pathlib import Path
import hashlib
import json
import re
from pyspark.sql import functions as F

for key in ('handoff_root','catalog','schema','table'):dbutils.widgets.text(key,'')
values={key:dbutils.widgets.get(key) for key in ('handoff_root','catalog','schema','table')}
for key in ('catalog','schema','table'):
    if not re.fullmatch('[a-zA-Z_][a-zA-Z0-9_]*',values[key]):raise ValueError('Set an authorized '+key)
root=Path(values['handoff_root']);manifest=json.loads((root/'manifest.json').read_text())
artifact=manifest['data'];path=root/artifact['path']
assert path.resolve().is_relative_to(root.resolve())
assert hashlib.sha256(path.read_bytes()).hexdigest()==artifact['sha256']
qualified='.'.join(values[k] for k in ('catalog','schema','table'))
dataset_version=root.name
spark.conf.set('spark.sql.session.timeZone','UTC')
frame=spark.read.parquet(str(path));assert frame.count()==artifact['rows']
frame.createOrReplaceTempView('external_weather_to_validate')
assert spark.sql('''SELECT count(*) AS n FROM (
    SELECT initialization_utc,valid_time_utc,count(*) AS n FROM external_weather_to_validate GROUP BY ALL HAVING n<>1)''').first()['n']==0
assert spark.sql('''SELECT count(*) AS n FROM external_weather_to_validate
    WHERE assumed_available_at_utc<>initialization_utc+INTERVAL 7 HOURS
        OR availability_verified OR precipitation_interval_end_utc<>valid_time_utc
        OR precipitation_interval_start_utc<>valid_time_utc-INTERVAL 1 HOUR''').first()['n']==0
assert 'dataset_version' not in frame.columns
frame.withColumn('dataset_version',F.lit(dataset_version)).write.format('delta').mode(
    'overwrite').option('overwriteSchema','true').saveAsTable(qualified)
saved=spark.table(qualified)
assert saved.count()==artifact['rows'] and saved.filter(F.col('dataset_version')!=dataset_version).count()==0
spark.sql(f'''COMMENT ON TABLE {qualified} IS
    'ECMWF IFS individual forecast runs, Open-Meteo.com / ECMWF, CC BY 4.0.
    Requested Manhattan reference point 40.75,-73.98; returned cell coordinates retained.
    Temperature C, wind m/s, precipitation mm over preceding hour. Seven-hour availability
    is assumed, not verified. Early archived runs include hindcasts. Missing values are not zero.
    See supplied DATASHEET.md for coverage, fields, limitations and source provenance.' ''')
display(frame.limit(20))
dbutils.notebook.exit(json.dumps({'table':qualified,'dataset_version':dataset_version,
    'rows':artifact['rows'],'source_sha256':artifact['sha256']}))
