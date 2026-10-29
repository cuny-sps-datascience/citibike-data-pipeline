from datetime import datetime,timedelta,timezone
from citibike_pipeline.rolling_backtest import label_availability,month_start,plan_fold,release_at

def row(month,day=1):
    dt=month_start(month)+timedelta(days=day-1)
    return dict(origin_utc=dt,target_start_utc=dt,target_end_utc=dt+timedelta(hours=1),weather_covered=True)

def test_label_zero_and_later_source_release_both_gate_training():
    zero=row('2026-03');spill=row('2026-02')
    result=label_availability([zero,spill],[dict(hour_utc=spill['target_start_utc'],nominal_release='2026-04')])
    assert result[0]['label_available_at_utc']==release_at('2026-03')
    assert result[1]['label_available_at_utc']==release_at('2026-04')

def test_validation_month_must_be_wholly_mature_and_training_earlier():
    rows=[row(m) for m in ('2026-01','2026-02','2026-03','2026-06')]
    cohorts=[dict(hour_utc=rows[2]['target_start_utc'],nominal_release='2026-04')]
    enriched=label_availability(rows,cohorts)
    fold=plan_fold(enriched,'2026-06')
    assert fold['validation_month']=='2026-02'
    assert len(fold['train'])==1
    assert all(r['label_available_at_utc']<fold['fit_cutoff_utc'] for r in fold['refit'])
    assert rows[2]['origin_utc'] not in {r['origin_utc'] for r in fold['refit']}
