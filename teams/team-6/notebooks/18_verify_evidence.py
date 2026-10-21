# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Verify public evidence and temporal gates
# MAGIC Read-only checks. Empty prospective evaluation is expected until matching published outcomes are imported. No prediction or active model is changed.

# COMMAND ----------
from datetime import datetime,timezone
import hashlib,json
from pathlib import Path

dbutils.widgets.text('expected_active_pointer_sha256','')
active=Path('/Volumes/workspace/citibike_3h/models/borough_active.json')
assert hashlib.sha256(active.read_bytes()).hexdigest()==dbutils.widgets.get('expected_active_pointer_sha256')
rolling=json.loads(Path('/Volumes/workspace/citibike_3h/exports/rolling_backtest_latest.json').read_text())
for fold in rolling['folds']:
    cutoff=datetime.fromisoformat(fold['fit_cutoff_utc'])
    assert all(datetime.fromisoformat(value)<cutoff for value in fold['max_label_available_at_utc'].values())
    assert datetime.fromisoformat(fold['latest_refit_target_end_utc'])<=cutoff
    assert all(v['converged'] for v in fold['refit_convergence'].values())
    assert fold['validation_month']<fold['month']
checks={}
queries={
 'rolling_duplicate_keys':'''SELECT count(*) FROM (SELECT fold_month,model,origin_utc,horizon,count(*) AS n
    FROM workspace.citibike_3h.borough_rolling_predictions GROUP BY ALL HAVING n>1)''',
 'station_duplicate_keys':'''SELECT count(*) FROM (SELECT snapshot_run_id,station_id,_processing_version,count(*) AS n
    FROM workspace.citibike_3h.station_information_observations GROUP BY ALL HAVING n>1)''',
 'alert_duplicate_keys':'''SELECT count(*) FROM (SELECT snapshot_run_id,entity_id,_processing_version,count(*) AS n
    FROM workspace.citibike_3h.mta_alert_observations GROUP BY ALL HAVING n>1)''',
 'feature_duplicate_keys':'''SELECT count(*) FROM (SELECT snapshot_run_id,bike_station_id,radius_m,_processing_version,count(*) AS n
    FROM workspace.citibike_3h.station_subway_alert_features_available GROUP BY ALL HAVING n>1)''',
 'non_subway_selectors_resolved':'''SELECT count(*) FROM workspace.citibike_3h.mta_alert_selectors
    WHERE coalesce(agency_id,'')<>'MTASBWY' AND resolution LIKE 'resolved%' ''',
 'features_before_publication':'''SELECT count(*) FROM workspace.citibike_3h.station_subway_alert_features_available
    WHERE available_from_utc<publication_completed_at_utc OR available_from_utc<observed_at_utc
    OR available_from_utc<reference_available_at_utc''',
 'invalid_mapping_has_numeric_counts':'''SELECT count(*) FROM workspace.citibike_3h.station_subway_alert_features_available
    WHERE (NOT mapping_compatible OR NOT feed_fresh OR NOT gbfs_info_fresh) AND matched_active_alert_count IS NOT NULL''',
 'unverified_operation_promoted':'''SELECT count(*) FROM workspace.citibike_3h.station_information_observations
    WHERE historical_operation_verified OR effective_change_time_verified''',
 'late_forecasts_evaluated':'''SELECT count(*) FROM workspace.citibike_3h.borough_prospective_evaluation
    WHERE publication_completed_at_utc>=origin_utc'''}
for name,query in queries.items():
    checks[name]=spark.sql(query).first()[0]
    assert checks[name]==0,(name,checks[name])
history=spark.sql('''SELECT count(*) AS observations,count(DISTINCT station_id) AS station_ids,
    min(observed_at_utc) AS first_observed_utc,max(observed_at_utc) AS last_observed_utc
    FROM workspace.citibike_3h.station_information_observations''').first().asDict()
selectors=[r.asDict() for r in spark.sql('''SELECT agency_id,resolution,count(*) AS selectors
    FROM workspace.citibike_3h.mta_alert_selectors GROUP BY ALL ORDER BY agency_id,resolution''').collect()]
features=[r.asDict() for r in spark.sql('''SELECT radius_m,count(*) AS rows,
    count(matched_active_alert_count) AS usable_rows,sum(CASE WHEN matched_active_alert_count>0 THEN 1 ELSE 0 END) AS rows_with_matched_alerts
    FROM workspace.citibike_3h.station_subway_alert_features_available GROUP BY radius_m ORDER BY radius_m''').collect()]
report={'verified_at_utc':datetime.now(timezone.utc).isoformat(),'checks':checks,'station_history':history,
    'selector_resolution':selectors,'features':features,
    'matured_forecasts':spark.table('workspace.citibike_3h.borough_prospective_evaluation').count(),
    'active_pointer_unchanged':True,'rolling_release_gates_passed':True,
    'prospective_report':json.loads(Path('/Volumes/workspace/citibike_3h/exports/prospective_evidence_latest.json').read_text())}
Path('/Volumes/workspace/citibike_3h/exports/evidence_verification_latest.json').write_text(json.dumps(report,default=str,indent=2)+'\n')
dbutils.notebook.exit(json.dumps(report,default=str))
