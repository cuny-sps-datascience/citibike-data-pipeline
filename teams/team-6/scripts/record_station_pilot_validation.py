"""Write the small, reviewable station-pilot evidence receipt from measured runs."""
from __future__ import annotations
from datetime import datetime,timezone
import json,re
from pathlib import Path
import pyarrow.parquet as pq
from citibike_pipeline.forecast_sources import digest


if __name__=='__main__':
    validation=Path('docs/validation')
    def read(path):return json.loads(Path(path).read_text())
    source=read(validation/'station_pilot_sources_2026-10-16.json')
    inputs=read(validation/'station_pilot_package_2026-10-16.json')
    outputs=read(validation/'station_pilot_results_2026-10-16.json')
    cloud=read('.local/execution/station_pilot_result.json');report=cloud['report']
    before=read('.local/execution/station_pilot_active_before.json')
    package=Path(inputs['package_root']);manifest=read(package/'manifest.json')
    operation=read(package/'operation_audit.json');labels=pq.read_table(package/'supported_labels.parquet')
    assert source['all_sha256_checks_passed'] and inputs['passed'] and outputs['passed']
    assert cloud['status']['result_state']=='SUCCESS'
    assert before['sha256']==cloud['active_pointer_sha256']==report['active_pointer_sha256']
    assert report['save_reload_max_prediction_difference']==0
    tests=Path('.local/execution/station_pilot_tests.log').read_text()
    match=re.search(r'(\d+) passed in ([\d.]+)s',tests);assert match and int(match[1])>=256
    proposal=Path('docs/citibike_proposal_4_pages_for_revisions.docx')
    assert digest(proposal)=='fde305547639fb55442a84d0bb5f737ea5b4a4da7d24cc4988593a1231c2c90d'
    cohort=read(package/'cohort.json');features=pq.read_table(package/'features.parquet',columns=['station_id','origin_utc']).to_pylist()
    first=datetime(2026,9,1,4,tzinfo=timezone.utc);last=datetime(2026,10,1,4,tzinfo=timezone.utc)
    tested={r['station_id'] for r in features if first<=r['origin_utc']<last}
    receipt={'recorded_at_utc':datetime.now(timezone.utc).isoformat(),
        'scope':'Conditional historical 32-station calendar/weather pilot; exact alert ablation incomplete',
        'source_root':source['source_root'],'source_manifest_sha256':source['manifest_sha256'],
        'objects':source['objects'],'bytes':sum(source['bytes_by_source'].values()),
        'dates_per_source':178,'source_rows':source['rows_by_source'],
        'first_complete_alert_schema_date':source['first_complete_alert_schema_date'],
        'incomplete_alert_schema_dates':source['incomplete_alert_schema_dates'],
        'package_root':str(package),'package_manifest_sha256':inputs['manifest_sha256'],
        'result_root':cloud['result_root'],'cloud_output_root':cloud['cloud_output_root'],
        'cohort_stations':32,'test_stations':report['split_stations']['test'],
        'cohort_stations_without_test_support':[{'station_id':r['station_id'],'name':r['name']} for r in cohort if r['station_id'] not in tested],
        'bounded_supported_hours_full_archive':sum(operation['bounded_supported_hours'].values()),
        'unique_station_poll_rows':operation['unique_poll_rows'],
        'fresh_renting_empty_inventory_rows':operation['fresh_renting_empty_inventory_rows'],
        'unique_label_hours':len(labels),
        'unique_zero_label_hours':sum(v==0 for v in labels['departures'].to_pylist()),
        'features':manifest['rows'],'zero_label_forecast_rows':manifest['zero_labels'],
        'mapping_rows':pq.read_metadata(package/'station_subway_mapping.parquet').num_rows,
        'split_rows':report['split_rows'],'test':report['test'],
        'weather_mae_reduction_percent':100*(1-report['test']['weather']['mae']/report['test']['calendar']['mae']),
        'alert_ablation_ready':report['alert_ablation_ready'],'alert_ablation_gate':report['alert_ablation_gate'],
        'rich_schema_rows_by_split':report['rich_schema_rows_by_split'],
        'run_id':cloud['run_id'],'task_run_id':cloud['task_run_id'],'run_url':cloud['run_url'],
        'cloud_status':cloud['status'],'mlflow_run_id':report['mlflow_run_id'],
        'predictions_table':report['predictions_table'],'features_table':report['features_table'],
        'mapping_table':report['mapping_table'],'model_artifacts':cloud['artifacts'],
        'save_reload_checked_rows':report['save_reload_checked_rows'],
        'save_reload_max_prediction_difference':report['save_reload_max_prediction_difference'],
        'independent_checks':outputs['checks'],'local_tests_passed':int(match[1]),
        'local_tests_seconds':float(match[2]),'original_proposal_sha256':digest(proposal),
        'active_pointer_sha256':cloud['active_pointer_sha256'],'active_model_changed':False,
        'canonical_station_eligibility_promoted':False,'production_activation':False,
        'limitations':report['limitations']}
    path=validation/'station_pilot_2026-10-16.json';path.write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps({k:v for k,v in receipt.items() if k in ('cohort_stations_without_test_support',
        'features','test','weather_mae_reduction_percent','local_tests_passed','alert_ablation_ready')},indent=2))
