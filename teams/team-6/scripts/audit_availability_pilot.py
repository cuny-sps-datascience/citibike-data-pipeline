"""Read-only validation of separated inventory predictors and future observations."""
from __future__ import annotations
import argparse,json
from pathlib import Path
import duckdb
from citibike_pipeline.db import sql_literal
from citibike_pipeline.forecast_sources import digest


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--package',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);args=p.parse_args();root=args.package
    m=json.loads((root/'manifest.json').read_text())
    for r in m['artifacts']:
        path=root/r['path']
        if path.stat().st_size!=r['bytes'] or digest(path)!=r['sha256']:raise ValueError('Checksum mismatch')
    con=duckdb.connect();con.execute("SET TimeZone='UTC';SET memory_limit='2GB';SET threads=2")
    con.execute('CREATE VIEW f AS SELECT * FROM read_parquet('+sql_literal(str(root/'features.parquet'))+')')
    con.execute('CREATE VIEW l AS SELECT * FROM read_parquet('+sql_literal(str(root/'labels.parquet'))+')')
    columns={r[0] for r in con.execute('DESCRIBE f').fetchall()}
    if {'bikes','docks','target_utc','label_observed_at_utc'}&columns:raise ValueError('Future labels in predictor table')
    checks={
        'duplicate_example_keys':con.execute('SELECT count(*) FROM (SELECT station_id,origin_utc,horizon_minutes,count(*) n FROM f GROUP BY ALL HAVING n>1)').fetchone()[0],
        'label_key_mismatch':con.execute('SELECT count(*) FROM f FULL OUTER JOIN l USING(example_id) WHERE f.example_id IS NULL OR l.example_id IS NULL').fetchone()[0],
        'future_or_stale_input':con.execute("SELECT count(*) FROM f WHERE input_observed_at_utc>=origin_utc OR epoch(origin_utc-input_observed_at_utc)>120").fetchone()[0],
        'future_information':con.execute('SELECT count(*) FROM f WHERE information_available_at_utc>=origin_utc').fetchone()[0],
        'bad_target_window':con.execute("SELECT count(*) FROM f JOIN l USING(example_id) WHERE target_utc<>origin_utc+horizon_minutes*INTERVAL '1 minute' OR epoch(label_observed_at_utc-target_utc) NOT BETWEEN 0 AND 120").fetchone()[0],
        'negative_inventory':con.execute('SELECT count(*) FROM f JOIN l USING(example_id) WHERE least(bikes,docks,current_bikes,current_docks)<0').fetchone()[0],
        'invalid_horizon':con.execute('SELECT count(*) FROM f WHERE horizon_minutes NOT IN (15,30,60,180)').fetchone()[0],
    }
    for lag in (15,30,60):
        checks['future_or_stale_lag_'+str(lag)]=con.execute(f'''SELECT count(*) FROM f WHERE lag_{lag}_observed_at_utc IS NOT NULL
            AND (lag_{lag}_observed_at_utc>=origin_utc-INTERVAL '{lag} minutes'
            OR epoch(origin_utc-INTERVAL '{lag} minutes'-lag_{lag}_observed_at_utc)>120)''').fetchone()[0]
    source=json.loads((root/'source_audit.json').read_text())
    files=','.join(sql_literal(str(root/r['path'])) for r in source['status_partitions'])
    con.execute('CREATE VIEW polls AS SELECT DISTINCT * FROM read_parquet(['+files+'])')
    checks['input_not_from_actual_poll']=con.execute('''SELECT count(*) FROM f LEFT JOIN polls p
        ON f.station_id=p.station_id AND epoch(f.input_observed_at_utc)=p.polled_at
        WHERE p.station_id IS NULL OR f.current_bikes<>p.num_bikes_available OR f.current_docks<>p.num_docks_available
        OR p.is_installed<>1 OR p.is_renting<>1 OR p.is_returning<>1
        OR p.polled_at-p.ts NOT BETWEEN -300 AND 120 OR p.polled_at-p.last_reported NOT BETWEEN -300 AND 600''').fetchone()[0]
    checks['label_not_from_actual_open_poll']=con.execute('''SELECT count(*) FROM f JOIN l USING(example_id)
        LEFT JOIN polls p ON f.station_id=p.station_id AND epoch(l.label_observed_at_utc)=p.polled_at
        WHERE p.station_id IS NULL OR l.bikes<>p.num_bikes_available OR l.docks<>p.num_docks_available
        OR p.is_installed<>1 OR p.is_renting<>1 OR p.is_returning<>1
        OR p.polled_at-p.ts NOT BETWEEN -300 AND 120 OR p.polled_at-p.last_reported NOT BETWEEN -300 AND 600''').fetchone()[0]
    rows=con.execute('SELECT count(*) FROM f').fetchone()[0]
    if rows!=m['rows']:raise ValueError('Row count mismatch')
    result={'package_root':str(root),'manifest_sha256':digest(root/'manifest.json'),'rows':rows,
        'checks':checks,'artifact_integrity_checks':len(m['artifacts']),'passed':not any(checks.values()),
        'canonical_eligibility_promoted':False,'original_models_changed':False}
    args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
    if not result['passed']:raise ValueError('Availability audit failed')
