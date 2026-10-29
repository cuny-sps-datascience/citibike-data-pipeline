# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # One-time Unity Catalog stable-table migration
# MAGIC Replace pass-through view/version-table pairs with one managed current table.
# MAGIC Each table receives a non-null `dataset_version`. Immutable source and result
# MAGIC packages remain in project volumes and on the RAID.

# COMMAND ----------
from datetime import datetime,timezone
from pathlib import Path
from uuid import uuid4
import json

schema='workspace.citibike_3h'
migrations=[
    ('subway_stations_20261015t234334z_d29974e2','subway_stations','20261015T234334Z-d29974e2'),
    ('gbfs_station_information_20261015t234334z_d29974e2','gbfs_station_information','20261015T234334Z-d29974e2'),
    ('gbfs_station_status_20261015t234334z_d29974e2','gbfs_station_status','20261015T234334Z-d29974e2'),
    ('subway_alert_archive_20261015t234334z_d29974e2','subway_alert_archive','20261015T234334Z-d29974e2'),
    ('subway_alert_live_snapshot_20261015t234334z_d29974e2','subway_alert_live_snapshot','20261015T234334Z-d29974e2'),
    ('nyc_weather_forecast_run_20261015t234334z_d29974e2','nyc_weather_forecast_run','20261015T234334Z-d29974e2'),
    ('hourly_observed_20261015t234524z_030bb826','hourly_observed','20261015T234524Z-030bb826'),
    ('hourly_release_20261015t234524z_030bb826','hourly_release','20261015T234524Z-030bb826'),
    ('station_geography_observed_20261015t234524z_030bb826','station_geography_observed','20261015T234524Z-030bb826'),
    ('public_station_crosswalk_20261016t004849z_def3dd32','public_station_crosswalk','20261016T004849Z-def3dd32'),
    ('public_station_hours_20261016t004849z_def3dd32','public_station_hours','20261016T004849Z-def3dd32'),
    ('borough_features_20261016t005315z_35753b1b','borough_features','20261016T005315Z-35753b1b'),
    ('borough_profiles_20261016t005315z_35753b1b','borough_profiles','20261016T005315Z-35753b1b'),
    ('borough_weather_runs_20261016t005315z_35753b1b','borough_weather_runs','20261016T005315Z-35753b1b'),
    ('station_subway_bike_stations_20261016t135632z_c198ce44','station_subway_bike_stations','20261016T135632Z-c198ce44'),
    ('station_subway_station_proximity_20261016t135632z_c198ce44','station_subway_station_proximity','20261016T135632Z-c198ce44'),
    ('station_subway_complex_proximity_20261016t135632z_c198ce44','station_subway_complex_proximity','20261016T135632Z-c198ce44'),
    ('station_subway_route_proximity_20261016t135632z_c198ce44','station_subway_route_proximity','20261016T135632Z-c198ce44'),
    ('station_subway_coverage_20261016t135632z_c198ce44','station_subway_coverage','20261016T135632Z-c198ce44'),
    ('borough_rolling_predictions_20261016t151919z_aaf4a8e8','borough_rolling_predictions','20261016T151919Z-aaf4a8e8'),
    ('borough_actual_hourly_approved_20261016t172924z_8e688cba','borough_actual_hourly','20261016T172924Z-8e688cba'),
]
standalone=[
    ('external_weather_reference_20261016t173331z_84df9caa','external_weather_reference','20261016T173331Z-84df9caa'),
    ('availability_live_predictions_20261016t205658z_53093e33','availability_live_predictions','legacy-live-history-through-20261016T234523Z-6ce3e4c1'),
]

def q(value):return "'"+value.replace("'","''")+"'"

def add_version(table,version):
    full=schema+'.'+table
    if 'dataset_version' not in spark.table(full).columns:
        spark.sql(f'ALTER TABLE {full} ADD COLUMNS (dataset_version STRING)')
    spark.sql(f'UPDATE {full} SET dataset_version={q(version)} WHERE dataset_version IS NULL')
    versions=[r[0] for r in spark.table(full).select('dataset_version').distinct().collect()]
    assert versions==[version],(full,versions)
    return spark.table(full).count()

results=[]
for source,stable,version in migrations:
    source_full=schema+'.'+source;stable_full=schema+'.'+stable
    if spark.catalog.tableExists(source_full):
        assert spark.catalog.tableExists(stable_full)
        definition=spark.sql('SHOW CREATE TABLE '+stable_full).first()[0]
        assert 'CREATE VIEW' in definition.upper() and source_full.lower() in definition.lower()
        before=spark.table(source_full).count()
        spark.sql('DROP VIEW '+stable_full)
        spark.sql(f'ALTER TABLE {source_full} RENAME TO {stable_full}')
    else:
        assert spark.catalog.tableExists(stable_full)
        definition=spark.sql('SHOW CREATE TABLE '+stable_full).first()[0]
        assert 'CREATE VIEW' not in definition.upper()
        before=spark.table(stable_full).count()
    after=add_version(stable,version);assert after==before
    results.append({'source':source_full,'stable_table':stable_full,'dataset_version':version,'rows':after})

for source,stable,version in standalone:
    source_full=schema+'.'+source;stable_full=schema+'.'+stable
    if spark.catalog.tableExists(source_full):
        assert not spark.catalog.tableExists(stable_full)
        before=spark.table(source_full).count()
        spark.sql(f'ALTER TABLE {source_full} RENAME TO {stable_full}')
    else:
        assert spark.catalog.tableExists(stable_full)
        before=spark.table(stable_full).count()
    after=add_version(stable,version);assert after==before
    if stable=='availability_live_predictions':
        if 'model_version' not in spark.table(stable_full).columns:
            spark.sql(f'ALTER TABLE {stable_full} ADD COLUMNS (model_version STRING)')
        spark.sql(f'''UPDATE {stable_full} SET model_version='20261016T205658Z-53093e33'
            WHERE model_version IS NULL''')
        assert spark.table(stable_full).filter('model_version IS NULL').count()==0
    results.append({'source':source_full,'stable_table':stable_full,'dataset_version':version,'rows':after})

assert all(not spark.catalog.tableExists(schema+'.'+source) for source,_,_ in migrations+standalone)
assert all(spark.catalog.tableExists(schema+'.'+stable) for _,stable,_ in migrations+standalone)
version=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8]
report={'schema_version':'stable-catalog-migration-v1','status':'VERIFIED','migration_version':version,
    'completed_at_utc':datetime.now(timezone.utc).isoformat(),'migrated_tables':results,
    'stable_table_count':len(results),'immutable_volume_and_raid_copies_preserved':True}
root=Path('/Volumes/workspace/citibike_3h/exports/catalog_migrations')/version
root.mkdir(parents=True,exist_ok=False);(root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
display(spark.createDataFrame(results))
dbutils.notebook.exit(json.dumps(report))
