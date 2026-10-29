# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Public station archive audit
# MAGIC CityBikes, https://data.citybik.es/, ODbL-1.0. The archive records inventory changes. Active brackets interpolate only short gaps, with fresh reports and stable geography. Eligibility therefore depends on inventory activity. These are conditional audit hours; **operation_verified and main_evaluation_eligible stay false**. They are not used to train the Manhattan aggregate benchmark.

# COMMAND ----------
from pathlib import Path
import hashlib
import json
import re
from pyspark.sql import functions as F

dbutils.widgets.text('package_root','')
root=Path(dbutils.widgets.get('package_root'));manifest=json.loads((root/'manifest.json').read_text())
assert manifest['status']=='conditional_exposure_built'
spark.conf.set('spark.sql.session.timeZone','UTC')
dataset_version=root.name
for kind,name in [('crosswalk','public_station_crosswalk'),('public_hours','public_station_hours')]:
    items=[a for a in manifest['artifacts'] if a['table']==kind]
    for item in items:assert hashlib.sha256((root/item['path']).read_bytes()).hexdigest()==item['sha256']
    table='workspace.citibike_3h.'+name
    expected=sum(a['rows'] for a in items)
    frame=spark.read.parquet(*[str(root/a['path']) for a in items])
    assert 'dataset_version' not in frame.columns and frame.count()==expected
    frame.withColumn('dataset_version',F.lit(dataset_version)).write.format('delta').mode(
        'overwrite').option('overwriteSchema','true').saveAsTable(table)
    saved=spark.table(table)
    assert saved.count()==expected and saved.filter(F.col('dataset_version')!=dataset_version).count()==0
assert spark.sql('SELECT count(*) AS n FROM workspace.citibike_3h.public_station_hours WHERE main_evaluation_eligible OR operation_verified').first()['n']==0
display(spark.sql('''SELECT date_trunc('month',target_local) AS month,count(*) AS conditional_hours,
    count(DISTINCT station_id) AS stations,sum(CASE WHEN departures=0 THEN 1 ELSE 0 END) AS zero_hours
    FROM workspace.citibike_3h.public_station_hours GROUP BY 1 ORDER BY 1'''))
dbutils.notebook.exit(json.dumps({'status':'conditional station audit imported','policy':manifest['policy'],
    'main_evaluation_eligible':False,'source':'CityBikes / data.citybik.es; ODbL-1.0'}))
