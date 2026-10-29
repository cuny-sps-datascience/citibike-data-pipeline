from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import numpy as np
import pyarrow as pa

from citibike_pipeline import stockout_onset_runtime as runtime

UTC = timezone.utc


def split_tables():
    origins = [
        datetime(2026, 4, 8, 4, tzinfo=UTC),
        datetime(2026, 8, 1, 4, tzinfo=UTC),
        datetime(2026, 8, 15, 4, tzinfo=UTC),
        datetime(2026, 9, 1, 4, tzinfo=UTC),
    ]
    features, labels = [], []
    for index, origin in enumerate(origins):
        features.append(
            {
                "example_id": index,
                "origin_utc": origin,
                "input_observed_at_utc": origin - timedelta(seconds=30),
                "information_available_at_utc": origin - timedelta(days=1),
                "horizon_minutes": 30,
            }
        )
        labels.append(
            {
                "example_id": index,
                "target_utc": origin + timedelta(minutes=30),
                "label_observed_at_utc": origin + timedelta(minutes=31),
            }
        )
    return pa.Table.from_pylist(features), pa.Table.from_pylist(labels)


def test_partition_enforces_cutoffs_and_confirming_endpoint():
    protocol = json.loads(Path("docs/stockout_onset_protocol.json").read_text())
    features, labels = split_tables()
    masks = runtime.partition(features, labels, protocol)
    assert [masks[name].sum() for name in ("train", "validation", "refit", "calibration", "test")] == [1, 1, 2, 1, 1]
    broken = labels.to_pylist()
    broken[0]["label_observed_at_utc"] += timedelta(minutes=2)
    try:
        runtime.partition(features, pa.Table.from_pylist(broken), protocol)
    except ValueError as error:
        assert "endpoint" in str(error)
    else:
        raise AssertionError("late endpoint accepted")


def test_candidates_metrics_and_tie_break_are_frozen():
    assert runtime.candidate_names() == (
        "persistence_no_onset",
        "regularized_station_logistic",
        "histogram_boosting_15_leaves",
    )
    assert min(runtime.candidate_names(), key=lambda name: runtime.selection_key(name, 0.1)) == "persistence_no_onset"
    result = runtime.metrics(np.array([0, 1, 0, 1]), np.array([0.1, 0.8, 0.2, 0.7]))
    assert result["events"] == 2 and 0 < result["brier"] < 1


def test_constant_probability_and_calibration_are_finite():
    X = np.zeros((3, len(runtime.FEATURES)))
    assert np.allclose(runtime.probability(0.25, X), 0.25)
    calibrator = runtime.calibrate(np.array([0.1, 0.2, 0.3]), np.array([0, 0, 0]))
    values = runtime.calibrated(calibrator, np.array([0.1, 0.9]))
    assert np.isfinite(values).all() and np.all((0 < values) & (values < 1))
