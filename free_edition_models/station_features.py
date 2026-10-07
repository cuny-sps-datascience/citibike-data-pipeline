# This module defines the functions which build the predictors and the targets.
# Feature definitions were developed and explored in 01-station-status-eda.
# These functions implement those definitions for training and prediction.

# To use them, put this file in the same folder as the notebook and run:
#     from station_features import build_predictors, build_targets
# (After editing this file, restart Python so notebooks pick up the change.)

from pyspark.sql import functions as F, Window
import math

def build_predictors(
    status,
    info,
    local_timezone="America/New_York",
    max_gap_minutes=10,
    min_history_minutes=20,
):
    """Calculate station predictors from status and historical metadata.

    Args:
        status: Spark DataFrame of station-status observations.
        info: Spark DataFrame of historical station-information snapshots.
        local_timezone: Timezone used for hour, weekend, and daily harmonics.
        max_gap_minutes: Largest observation gap allowed when calculating rates.
        min_history_minutes: Minimum history span required for the 30-minute rate.

    Returns:
        Spark DataFrame with historical metadata and predictor columns.

    Each row uses up to 30 minutes of preceding status observations.
    Insufficient history produces null rate features.
    """

    microseconds_per_minute = 60000000
    order = Window.partitionBy("station_id").orderBy("poll_microseconds")
    past = order.rangeBetween(-30 * microseconds_per_minute, 0)
    frame = status.withColumn("poll_microseconds", F.expr("unix_micros(fetched_at)"))

    for outcome, operation in [("bike", "is_renting"), ("dock", "is_returning")]:
        count = F.col(f"num_{outcome}s_available")
        eligible = (
            F.col("is_installed").cast("boolean")
            & F.col(operation).cast("boolean")
            & count.isNotNull() & (count >= 0)
        )
        frame = frame.withColumn(f"{outcome}_stockout", F.when(eligible, count == 0))

    frame = frame.withColumn(
        "gap_minutes",
        (F.col("poll_microseconds") - F.lag("poll_microseconds").over(order))
        / microseconds_per_minute,
    ).withColumn(
        "bad_gap",
        F.when(F.col("gap_minutes").between(.000001, max_gap_minutes), 0)
         .otherwise(1),
    )

    # Compute shared history summaries once for both outcomes.
    frame = frame.select(
        "*",
        F.min(F.struct("poll_microseconds", "num_bikes_available",
                       "num_docks_available", "bad_gap")).over(past).alias("history_start"),
        F.count("*").over(past).alias("history_count"),
        F.sum("bad_gap").over(past).alias("history_bad_gaps"),
        F.count("bike_stockout").over(past).alias("bike_history_count"),
        F.count("dock_stockout").over(past).alias("dock_history_count"),
    )
    elapsed = (
        F.col("poll_microseconds") - F.col("history_start.poll_microseconds")
    ) / microseconds_per_minute
    # Exclude the gap leading into the first observation in the history window.
    complete_history = (
        (elapsed >= min_history_minutes)
        & (F.col("history_bad_gaps") == F.col("history_start.bad_gap"))
    )

    for outcome in ["bike", "dock"]:
        available = F.when(
            F.col(f"{outcome}_stockout").isNotNull(),
            F.col(f"num_{outcome}s_available").cast("double"),
        )
        frame = frame.withColumn(
            f"{outcome}_change_per_minute",
            F.when(
                (F.col("gap_minutes") > 0)
                & (F.col("gap_minutes") <= max_gap_minutes),
                (available - F.lag(available).over(order)) / F.col("gap_minutes"),
            ),
        ).withColumn(
            f"{outcome}_change_30min_per_minute",
            F.when(
                complete_history
                & (F.col(f"{outcome}_history_count") == F.col("history_count")),
                (available - F.col(f"history_start.num_{outcome}s_available")) / elapsed,
            ),
        )

    frame = frame.drop("bad_gap", "history_start", "history_count", "history_bad_gaps",
                       "bike_history_count", "dock_history_count")

    # Match each poll to the most recent information snapshot.
    info_order = Window.partitionBy("station_id").orderBy("fetched_at")
    historical_info = (
        info.withColumn("next_info_at", F.lead("fetched_at").over(info_order))
        .withColumnRenamed("fetched_at", "info_at")
        .alias("info")
    )
    frame = frame.alias("status").join(
        historical_info,
        (F.col("status.station_id") == F.col("info.station_id"))
        & (F.col("status.fetched_at") >= F.col("info.info_at"))
        & (F.col("info.next_info_at").isNull()
           | (F.col("status.fetched_at") < F.col("info.next_info_at"))),
        "left",
    ).select(
        "status.*", "info.info_at", F.col("info.name").alias("station_name"),
        "info.capacity", "info.lat", "info.lon",
    )

    frame = (
        frame.withColumn("local_time", F.from_utc_timestamp("fetched_at", local_timezone))
        .withColumn("hour", F.hour("local_time"))
        .withColumn("weekend", F.dayofweek("local_time").isin(1, 7).cast("double"))
        .withColumn(
            "log_capacity",
            F.when(F.col("capacity") > 0, F.log("capacity")),
        )
        .withColumn("geographic_cell", F.when(
            F.col("lat").isNotNull() & F.col("lon").isNotNull(),
            F.concat_ws(":", F.floor((F.col("lon") + 74) * 84.3).cast("string"),
                        F.floor((F.col("lat") - 40.7) * 111.2).cast("string")),
        ).otherwise("unknown"))
    )
    hour = F.col("hour") + F.minute("local_time") / 60 + F.second("local_time") / 3600
    harmonics = [
        function(2 * math.pi * frequency * hour / 24).alias(f"hour_{name}_{frequency}")
        for frequency in [1, 2]
        for name, function in [("sin", F.sin), ("cos", F.cos)]
    ]
    frame = frame.select("*", *harmonics)
    capacity = F.when(F.col("capacity") > 0, F.col("capacity"))

    for outcome in ["bike", "dock"]:
        frame = frame.withColumn(
            f"{outcome}_available_fraction",
            F.when(F.col(f"{outcome}_stockout").isNotNull(),
                   F.col(f"num_{outcome}s_available").cast("double")) / capacity,
        )
        for rate in ["change_per_minute", "change_30min_per_minute"]:
            frame = frame.withColumn(
                f"{outcome}_{rate}_fraction", F.col(f"{outcome}_{rate}") / capacity,
            )
    return frame



def build_targets(
    predictors,
    horizon_minutes=30,
    max_gap_minutes=10,
):
    """Calculate future bike and dock stockout targets for each observation.

    Args:
        predictors: Spark DataFrame returned by build_predictors().
        horizon_minutes: Length of the prediction window.
        max_gap_minutes: Largest observation gap allowed inside the window,
            and the tolerance for the first poll after the horizon.

    Returns:
        Spark DataFrame containing station_id, fetched_at, bike_target,
        dock_target, label_end_microseconds, and horizon_minutes.

    Targets are 1 for an observed stockout within the horizon, 0 for none,
    and null for an existing stockout, ineligibility, or insufficient coverage.
    label_end_microseconds records the endpoint used to confirm coverage.
    """
    microseconds_per_minute = 60000000
    order = Window.partitionBy("station_id").orderBy("poll_microseconds")
    horizon = horizon_minutes * microseconds_per_minute
    maximum_gap = max_gap_minutes * microseconds_per_minute
    future = order.rangeBetween(1, horizon)
    endpoint_window = order.rangeBetween(horizon, horizon + maximum_gap)

    frame = predictors.select(
        "station_id", "fetched_at", "poll_microseconds", "gap_minutes",
        "bike_stockout", "dock_stockout",
    ).select(
        "*",
        F.count("*").over(future).alias("future_poll_count"),
        F.max("poll_microseconds").over(future).alias("last_future_poll"),
        F.max("gap_minutes").over(future).alias("largest_future_gap"),
        F.min(F.struct("poll_microseconds", "bike_stockout", "dock_stockout"))
         .over(endpoint_window).alias("endpoint"),
    )
    covered = (
        (F.col("future_poll_count") > 0)
        & (F.col("largest_future_gap") <= max_gap_minutes)
        & (F.col("endpoint.poll_microseconds") - F.col("last_future_poll") <= maximum_gap)
    )
    targets = []
    for outcome in ["bike", "dock"]:
        state = f"{outcome}_stockout"
        eligible = (
            (F.col(state) == False)
            & (F.count(state).over(future) == F.col("future_poll_count"))
            & F.col(f"endpoint.{state}").isNotNull()
        )
        targets.append(F.when(
            covered & eligible, F.max(F.col(state).cast("double")).over(future),
        ).alias(f"{outcome}_target"))

    return frame.select(
        "station_id", "fetched_at",
        F.col("endpoint.poll_microseconds").alias("label_end_microseconds"),
        *targets,
        F.lit(horizon_minutes).alias("horizon_minutes"),
    )
