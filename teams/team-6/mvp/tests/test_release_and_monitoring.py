import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from monitor_departures import evaluate_predictions
from poisson_challenger import glm_features
from release_backtest import label_maturity, mature_mask, month_cutoff


def test_release_gate_requires_completed_hour_and_receipt_strictly_before_fit():
    cutoff = month_cutoff("2026-03")
    targets = pd.DataFrame({"target_hour_utc": pd.to_datetime([
        "2026-01-15T15:00:00Z", "2026-02-20T15:00:00Z", "2026-03-01T03:00:00Z",
    ], utc=True)})
    receipts = pd.Series({
        "2026-01": cutoff - pd.Timedelta(minutes=1),
        "2026-02": cutoff,
    })
    observed = label_maturity(targets, receipts)
    assert observed.target_month.tolist() == ["2026-01", "2026-02", "2026-02"]
    assert mature_mask(observed, cutoff).tolist() == [True, False, False]
    assert not mature_mask(observed, cutoff + pd.Timedelta(minutes=1)).iloc[2]
    assert label_maturity(targets).label_available_at_utc.iloc[0] == pd.Timestamp("2026-03-17T00:00Z")


def test_monitor_separates_model_versions_and_keeps_unreleased_outcomes_pending():
    target = pd.Timestamp("2026-10-23T13:00:00Z")
    later = target + pd.Timedelta(days=31)
    rows = pd.DataFrame([
        ["A", target, target - pd.Timedelta(hours=1), "team6", "1", "both", 0.5, 1.0],
        ["A", target, target - pd.Timedelta(hours=1), "team6", "2", "both", 2.0, 1.0],
        ["A", later, later - pd.Timedelta(hours=1), "team6", "1", "calendar", 0.5, 1.0],
    ], columns=["station_id", "target_hour_utc", "issued_at_utc", "model_name", "model_version",
                "model_used", "predicted_departures", "comparison_weight"])
    actuals = pd.DataFrame({"station_id": ["A", "A"], "hour_utc": [target, later], "departures": [0, 9]})
    receipt = pd.Series({"2026-10": pd.Timestamp("2026-11-05T13:00:00Z")})
    as_of = pd.Timestamp("2026-11-24T13:00:00Z")
    evaluated, metrics = evaluate_predictions(rows, actuals, receipt, as_of)
    assert evaluated.status.tolist() == ["scored", "scored", "pending_release"]
    assert evaluated.actual_departures.iloc[:2].tolist() == [0, 0]
    assert pd.isna(evaluated.actual_departures.iloc[2])
    assert metrics.loc[metrics.evaluated_rows.eq(1), "poisson_log_loss"].tolist() == [0.5, 2.0]
    _, pending = evaluate_predictions(rows, actuals, pd.Series(dtype="datetime64[ns, UTC]"), as_of)
    assert pending.evaluated_rows.sum() == 0


def test_poisson_curves_have_station_and_weekend_interactions():
    frame = pd.DataFrame({
        "station_id": pd.Categorical(["A", "B", "A"]),
        "t_hour": [6, 6, 6], "t_dow": [0, 0, 6], "t_doy": [30, 30, 36],
        "t_holiday": [0, 0, 0], "t_weekend": [0, 0, 1],
    })
    features = glm_features(frame, "calendar")
    assert features.A_sin_1.tolist() == [1, 0, 1]
    assert features.B_sin_1.tolist() == [0, 1, 0]
    assert features.A_sin_1_weekend.tolist() == [0, 0, 1]
