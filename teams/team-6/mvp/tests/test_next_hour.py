import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from live_weather import collect_forecasts
from next_hour import WEATHER_FEATURES, demand_curves, issue_time, latest_forecasts, poisson_log_loss


def test_curves_preserve_zero_and_scale_by_station_hour():
    curve = demand_curves(pd.DataFrame({
        "station_id": pd.Categorical(["A", "A", "A", "B", "B"]),
        "t_dow": [0, 0, 0, 0, 0],
        "t_hour": [8, 8, 9, 8, 9],
        "departures": [0, 0, 20, 10, 30],
    })).set_index(["station_id", "t_hour"])
    assert curve.loc[("A", 8), "typical_departures"] == 0
    assert curve.loc[("A", 8), "observed_hours"] == 2
    assert curve.loc[("B", 9), "demand_index"] == 150
    assert curve.loc[("B", 9), "share_of_typical_day"] == 0.75
    assert curve.loc[("A", 8), "comparison_weight"] > curve.loc[("A", 9), "comparison_weight"]


def test_count_log_loss_includes_valid_zero_counts():
    np.testing.assert_allclose(poisson_log_loss([0, 2], [0, 2]), [0.1, 2 - np.log(2)])
    assert poisson_log_loss([8], [8])[0] < poisson_log_loss([8], [2])[0]


def test_forecast_selection_rejects_future_stale_wrong_target_and_missing():
    as_of = issue_time("2026-11-23T08:05:00-05:00")
    target = pd.Timestamp("2026-11-23T14:00:00Z")
    rows = [
        (19, "2026-11-23T13:04:00Z", target, 50),
        (19, "2026-11-23T13:06:00Z", target, 90),
        (19, "2026-11-23T13:05:00Z", target + pd.Timedelta(hours=1), 100),
        (21, "2026-11-23T10:04:00Z", target, 45),
        (24, "2026-11-23T13:04:00Z", target, np.nan),
    ]
    forecasts = pd.DataFrame([
        {"weather_request_class": weather_class, "received_at_utc": received,
         "target_hour_utc": hour, "weather_source": "test",
         **dict.fromkeys(WEATHER_FEATURES, 0), "forecast_temperature_f": temperature}
        for weather_class, received, hour, temperature in rows
    ])
    eligible = latest_forecasts(forecasts, as_of, target)
    assert eligible.weather_request_class.tolist() == [19]
    assert eligible.forecast_temperature_f.tolist() == [50]
    assert eligible.forecast_precipitation_in.tolist() == [0]
    with pytest.raises(ValueError, match="time zone"):
        issue_time("2026-11-23 08:05:00")


def test_live_weather_keeps_valid_dry_hours_and_omits_null(monkeypatch):
    import io
    import json
    from live_weather import FIELDS

    payload = {
        "utc_offset_seconds": 0,
        "hourly_units": {field: unit for field, (_, unit) in FIELDS.items()},
        "hourly": {"time": ["2026-11-23T14:00", "2026-11-23T15:00"],
                   **{field: [0, None] for field in FIELDS}},
    }
    monkeypatch.setattr("live_weather.urlopen", lambda *args, **kwargs: io.StringIO(json.dumps(payload)))
    forecasts, errors = collect_forecasts()
    assert errors == []
    assert sorted(forecasts.weather_request_class.tolist()) == [19, 21, 24]
    assert forecasts.forecast_precipitation_in.eq(0).all()
