"""Join saved forecasts to released, validated hourly outcome counts."""

import numpy as np
import pandas as pd

from mvp_data import ZONE
from next_hour import issue_time, poisson_log_loss
from release_backtest import label_maturity

OUTCOME_KEY = ["station_id", "target_hour_utc"]
FORECAST_KEY = [*OUTCOME_KEY, "issued_at_utc", "model_name", "model_version"]


def evaluate_predictions(predictions, actuals, receipts, as_of):
    as_of = issue_time(as_of)
    predictions = predictions.assign(
        station_id=predictions.station_id.astype(str),
        target_hour_utc=pd.to_datetime(predictions.target_hour_utc, utc=True),
        issued_at_utc=pd.to_datetime(predictions.issued_at_utc, utc=True),
    ).drop_duplicates()
    if predictions.duplicated(FORECAST_KEY).any():
        raise ValueError("The same forecast key has conflicting saved rows.")
    if not (predictions.issued_at_utc < predictions.target_hour_utc).all():
        raise ValueError("Each prediction must be issued before the target hour.")

    observed = actuals[["station_id", "hour_utc", "departures"]].rename(
        columns={"hour_utc": "target_hour_utc", "departures": "actual_departures"}
    )
    observed = observed.assign(station_id=observed.station_id.astype(str),
                               target_hour_utc=pd.to_datetime(observed.target_hour_utc, utc=True))
    counts = observed.actual_departures.dropna().astype(float)
    if not (np.isfinite(counts) & counts.ge(0) & counts.mod(1).eq(0)).all():
        raise ValueError("Validated departures must be nonnegative integer counts or null.")
    rows = label_maturity(predictions, receipts).merge(observed, on=OUTCOME_KEY, how="left",
                                                       validate="many_to_one")
    rows["local_hour"] = rows.target_hour_utc.dt.tz_convert(ZONE).dt.hour
    released = rows.label_available_at_utc.lt(as_of) & rows.target_end_utc.le(as_of)
    rows["status"] = np.select(
        [~rows.target_end_utc.le(as_of), ~released, rows.actual_departures.isna()],
        ["pending_hour", "pending_release", "awaiting_validated_count"], default="scored",
    )
    scored = rows.status.eq("scored")
    rows["poisson_log_loss"] = np.nan
    rows.loc[scored, "poisson_log_loss"] = poisson_log_loss(
        rows.loc[scored, "actual_departures"], rows.loc[scored, "predicted_departures"]
    )
    rows["absolute_error"] = (rows.actual_departures.astype(float) - rows.predicted_departures).abs().where(scored)
    rows["actual_departures"] = rows.actual_departures.where(scored)
    rows["evaluated_weight"] = rows.comparison_weight.where(scored)
    rows["weighted_loss"] = rows.poisson_log_loss * rows.evaluated_weight
    groups = ["model_name", "model_version", "model_used", "station_id", "local_hour"]
    metrics = rows.groupby(groups, observed=True, dropna=False).agg(
        forecast_rows=("status", "size"), evaluated_rows=("poisson_log_loss", "count"),
        poisson_log_loss=("poisson_log_loss", "mean"), mae=("absolute_error", "mean"),
        weighted_total=("weighted_loss", "sum"), weight=("evaluated_weight", "sum"),
    ).reset_index()
    metrics["weighted_poisson_log_loss"] = metrics.pop("weighted_total").div(metrics.pop("weight").replace(0, np.nan))
    return rows, metrics
