"""Independent read-only checks of portable station inputs and optional results."""
from __future__ import annotations
import argparse,json
from pathlib import Path
import duckdb
from citibike_pipeline.forecast_sources import digest
from citibike_pipeline.db import sql_literal


def audit(package,result=None):
    m=json.loads((package/'manifest.json').read_text())
    for a in m['artifacts']:
        p=package/a['path']
        if p.stat().st_size!=a['bytes'] or digest(p)!=a['sha256']:raise ValueError('Artifact integrity failed')
    con=duckdb.connect();con.execute("SET TimeZone='UTC';SET memory_limit='1GB';SET threads=2")
    p=str(package/'features.parquet')
    checks={
        'duplicate_feature_keys':con.execute('''SELECT count(*) FROM (SELECT station_id,origin_utc,horizon,
            count(*) n FROM read_parquet(?) GROUP BY ALL HAVING n<>1)''',[p]).fetchone()[0],
        'future_weather':con.execute('''SELECT count(*) FROM read_parquet(?) WHERE
            weather_assumed_available_at_utc>=issue_cutoff_utc''',[p]).fetchone()[0],
        'future_information':con.execute('''SELECT count(*) FROM read_parquet(?) WHERE
            information_assumed_available_at_utc>=issue_cutoff_utc''',[p]).fetchone()[0],
        'future_advisory':con.execute('''SELECT count(*) FROM read_parquet(?) WHERE advisory_covered AND
            (advisory_observed_at_utc>=issue_cutoff_utc OR gtfs_archive_available_at_utc>=issue_cutoff_utc)''',[p]).fetchone()[0],
        'stale_advisory':con.execute('''SELECT count(*) FROM read_parquet(?) WHERE advisory_covered AND
            (epoch(issue_cutoff_utc-advisory_observed_at_utc) NOT BETWEEN 0 AND 120
            OR advisory_feed_age_seconds NOT BETWEEN -300 AND 900)''',[p]).fetchone()[0],
        'unknown_advisory_encoded_zero':con.execute('''SELECT count(*) FROM read_parquet(?)
            WHERE NOT advisory_covered AND (advisory_active IS NOT NULL OR advisory_next3h IS NOT NULL
            OR advisory_unspecified IS NOT NULL)''',[p]).fetchone()[0],
        'negative_counts':con.execute('SELECT count(*) FROM read_parquet(?) WHERE departures<0',[p]).fetchone()[0],
        'wrong_horizon':con.execute('''SELECT count(*) FROM read_parquet(?) WHERE
            target_start_utc<>origin_utc+horizon*INTERVAL '1 hour'
            OR target_end_utc<>target_start_utc+INTERVAL '1 hour'
            OR issue_cutoff_utc<>origin_utc-INTERVAL '5 minutes' ''',[p]).fetchone()[0],
    }
    checks['unsupported_target_hours']=con.execute('''SELECT count(*) FROM read_parquet(?) f
        WHERE NOT EXISTS (SELECT 1 FROM read_parquet(?) i WHERE i.station_id=f.station_id
            AND to_timestamp(i.first)<=f.target_start_utc AND to_timestamp(i.last)>=f.target_end_utc)''',
        [p,str(package/'operation_intervals.parquet')]).fetchone()[0]
    trip_files=[str(Path(m['observed_root'])/r['path']) for r in m['trip_artifacts']]
    files=','.join(sql_literal(v) for v in trip_files)
    con.execute('''CREATE VIEW cohort_trips AS SELECT station_id,hour_utc,sum(departures) AS departures,
        max((last_day((nominal_release||'-01')::DATE)+45)::TIMESTAMPTZ) AS source_available_at_utc FROM read_parquet(['''+
        files+"]) WHERE id_scheme='short_name' GROUP BY station_id,hour_utc")
    checks['label_count_disagreement']=con.execute('''SELECT count(*) FROM read_parquet(?) f
        LEFT JOIN cohort_trips t ON f.station_id=t.station_id AND f.target_start_utc=t.hour_utc
        WHERE f.departures<>coalesce(t.departures,0)''',[p]).fetchone()[0]
    checks['label_release_disagreement']=con.execute('''SELECT count(*) FROM read_parquet(?) f
        LEFT JOIN cohort_trips t ON f.station_id=t.station_id AND f.target_start_utc=t.hour_utc
        WHERE f.label_available_at_utc<>greatest(t.source_available_at_utc,
            (last_day(timezone('America/New_York',f.target_start_utc)::DATE)+45)::TIMESTAMPTZ)''',[p]).fetchone()[0]
    n,zeros,covered=con.execute('''SELECT count(*),count(*) FILTER(WHERE departures=0),
        count(*) FILTER(WHERE advisory_covered) FROM read_parquet(?)''',[p]).fetchone()
    if n!=m['rows'] or zeros!=m['zero_labels']:raise ValueError('Manifest count mismatch')
    report={'package_root':str(package),'manifest_sha256':digest(package/'manifest.json'),
        'checks':checks,'rows':n,'zero_labels':zeros,'advisory_covered_rows':covered,
        'artifact_checks_passed':len(m['artifacts']),'canonical_station_eligibility_promoted':False,
        'active_model_changed':False}
    if result:
        r=json.loads((result/'report.json').read_text())
        prediction=str(result/'predictions.parquet')
        checks['duplicate_predictions']=con.execute('''SELECT count(*) FROM (SELECT model,station_id,origin_utc,horizon,
            count(*) n FROM read_parquet(?) GROUP BY ALL HAVING n<>1)''',[prediction]).fetchone()[0]
        checks['invalid_predictions']=con.execute('''SELECT count(*) FROM read_parquet(?)
            WHERE prediction<0 OR NOT isfinite(prediction) OR fit_cutoff_utc>=origin_utc''',[prediction]).fetchone()[0]
        differences=con.execute('''SELECT model,count(*),avg(abs(prediction-departures))
            FROM read_parquet(?) GROUP BY model ORDER BY model''',[prediction]).fetchall()
        for model,count,mae in differences:
            if count!=r['test'][model]['rows'] or abs(mae-r['test'][model]['mae'])>1e-9:raise ValueError('Metrics differ from persisted predictions')
        report['independently_recomputed_test_metrics']=[{'model':name,'rows':count,'mae':mae} for name,count,mae in differences]
        report['result_root']=str(result);report['alert_ablation_ready']=r['alert_ablation_ready']
    report['passed']=not any(checks.values());con.close()
    if not report['passed']:raise ValueError(json.dumps(report))
    return report


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--package',type=Path,required=True)
    p.add_argument('--result',type=Path);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();report=audit(args.package,args.result)
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
