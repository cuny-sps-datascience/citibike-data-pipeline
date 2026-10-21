from datetime import datetime, timedelta, timezone

import numpy as np

from citibike_pipeline.borough_mvp import label_assignable, release_at, split_origin, weather_initialization
from citibike_pipeline.model_runtime import FEATURES, numerical_features, select_profile, weather_inputs


def test_release_schedule_handles_february_and_year_boundary():
    assert release_at('2025-01')==datetime(2025,3,17,tzinfo=timezone.utc)
    assert release_at('2025-02')==datetime(2025,4,14,tzinfo=timezone.utc)
    assert release_at('2026-08')==datetime(2026,10,15,tzinfo=timezone.utc)


def test_weather_run_is_selected_before_five_minute_early_issue():
    # Nominal 07:00 has a 06:55 issue: the 00:00 run is not assumed available yet.
    issue=datetime(2025,3,1,6,55,tzinfo=timezone.utc)
    assert weather_initialization(issue)==datetime(2025,2,28,18,tzinfo=timezone.utc)
    assert weather_initialization(issue.replace(hour=7))==datetime(2025,3,1,0,tzinfo=timezone.utc)


def test_both_repeated_fall_hours_are_unassignable_but_adjacent_and_spring_hours_are_valid():
    assert not label_assignable(datetime(2025,11,2,5,tzinfo=timezone.utc))
    assert not label_assignable(datetime(2025,11,2,6,tzinfo=timezone.utc))
    assert label_assignable(datetime(2025,11,2,4,tzinfo=timezone.utc))
    assert label_assignable(datetime(2025,11,2,7,tzinfo=timezone.utc))
    assert label_assignable(datetime(2025,3,9,6,tzinfo=timezone.utc))
    assert label_assignable(datetime(2025,3,9,7,tzinfo=timezone.utc))


def test_split_purges_origins_crossing_target_boundary_and_retains_full_last_hour():
    assert split_origin(datetime(2026,4,1,1,tzinfo=timezone.utc))=='train'
    assert split_origin(datetime(2026,4,1,2,tzinfo=timezone.utc)) is None
    assert split_origin(datetime(2026,4,1,3,tzinfo=timezone.utc)) is None
    assert split_origin(datetime(2026,4,1,4,tzinfo=timezone.utc))=='validation'
    assert split_origin(datetime(2026,9,1,2,tzinfo=timezone.utc)) is None


def test_known_calendar_and_forecast_values_are_encoded_without_labels():
    target=datetime(2026,6,1,9,tzinfo=timezone.utc)
    values=dict(zip(FEATURES,numerical_features(100,80,target,3,12,.5,4)))
    assert values['log_profile']==np.log1p(100)
    assert values['horizon']==3
    assert values['temperature_squared']==144
    assert values['precipitation']==.5
    assert values['wet']==1


def test_precipitation_uses_end_of_target_interval_and_missing_is_not_zero():
    target=datetime(2026,6,1,9,tzinfo=timezone.utc)
    hourly={target:{'temperature_2m':15.,'wind_speed_10m':2.,'precipitation':9.},
            target+timedelta(hours=1):{'precipitation':.3}}
    assert weather_inputs(hourly,target)==([15.,.3,2.],True)
    hourly[target+timedelta(hours=1)]['precipitation']=None
    assert weather_inputs(hourly,target)==([15.,None,2.],False)
    hourly[target+timedelta(hours=1)]['precipitation']=0.
    assert weather_inputs(hourly,target)==([15.,0.,2.],True)


def test_live_profile_excludes_future_release_and_exact_cutoff():
    target=datetime(2026,6,1,9,tzinfo=timezone.utc)
    cutoff=datetime(2026,6,1,8,55,tzinfo=timezone.utc)
    old={'nominal_release':'2026-03','available_at_utc':datetime(2026,5,15,tzinfo=timezone.utc),
         'dow':0,'hour':9,'covered_hours':5,'departures':100,'rate':20.}
    future=old|{'nominal_release':'2026-04','available_at_utc':cutoff,'rate':999.}
    assert select_profile([old,future],cutoff,target)==(old,20.)
