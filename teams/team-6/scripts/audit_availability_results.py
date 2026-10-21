"""Independently recompute count, risk and interval scores from saved predictions."""
from __future__ import annotations
import argparse,json
from pathlib import Path
import duckdb
from citibike_pipeline.forecast_sources import digest
from citibike_pipeline.db import sql_literal


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--result',required=True,type=Path)
    p.add_argument('--output',required=True,type=Path);args=p.parse_args()
    r=json.loads((args.result/'report.json').read_text())
    con=duckdb.connect();con.execute("SET TimeZone='UTC';SET memory_limit='1GB';SET threads=2")
    con.execute('CREATE VIEW predictions AS SELECT * FROM read_parquet('+sql_literal(str(args.result/'predictions.parquet'))+')')
    checks={
        'duplicate_keys':con.execute('SELECT count(*) FROM (SELECT station_id,origin_utc,horizon_minutes,count(*) n FROM predictions GROUP BY ALL HAVING n>1)').fetchone()[0],
        'bad_horizon':con.execute("SELECT count(*) FROM predictions WHERE target_utc<>origin_utc+horizon_minutes*INTERVAL '1 minute' OR epoch(label_observed_at_utc-target_utc) NOT BETWEEN 0 AND 120").fetchone()[0],
    }
    recomputed=[]
    for target in ('bikes','docks'):
        checks['invalid_'+target+'_forecast']=con.execute(f'''SELECT count(*) FROM predictions WHERE
            {target}_prediction<0 OR NOT isfinite({target}_prediction)
            OR {target}_lower<0 OR {target}_lower>{target}_prediction OR {target}_upper<{target}_prediction
            OR {target}_p_zero NOT BETWEEN 0 AND 1 OR NOT isfinite({target}_p_zero)''').fetchone()[0]
        values=con.execute(f'''SELECT horizon_minutes,count(*) AS rows,
            avg(abs({target}_prediction-{target})) AS mae,
            sqrt(avg(pow({target}_prediction-{target},2))) AS rmse,
            avg(abs(current_{target}-{target})) AS persistence_mae,
            avg(CASE WHEN {target} BETWEEN {target}_lower AND {target}_upper THEN 1.0 ELSE 0.0 END) AS coverage,
            avg(pow({target}_p_zero-CASE WHEN {target}=0 THEN 1.0 ELSE 0.0 END,2)) AS brier
            FROM predictions GROUP BY horizon_minutes ORDER BY horizon_minutes''').fetchall()
        for h,n,mae,rmse,baseline,coverage,brier in values:
            expected=r['test_by_horizon'][str(h)][target]
            if n!=r['test_by_horizon'][str(h)]['rows']:raise ValueError('Prediction count mismatch')
            for a,b in [(mae,expected['model']['mae']),(rmse,expected['model']['rmse']),
                        (baseline,expected['persistence']['mae']),(coverage,expected['interval_coverage']),(brier,expected['brier'])]:
                if abs(a-b)>1e-9:raise ValueError('Independent metric differs')
            recomputed.append({'target':target,'horizon_minutes':h,'rows':n,'mae':mae,
                'rmse':rmse,'persistence_mae':baseline,'coverage':coverage,'brier':brier})
    result={'result_root':str(args.result),'report_sha256':digest(args.result/'report.json'),
        'checks':checks,'independently_recomputed':recomputed,'passed':not any(checks.values()),
        'save_reload_checked_rows':r['save_reload_checked_rows'],
        'save_reload_max_difference':r['save_reload_max_difference'],'active_departure_model_changed':False}
    args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
    if not result['passed']:raise ValueError('Availability result audit failed')
