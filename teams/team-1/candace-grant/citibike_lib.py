"""
Shared logic for the Citi Bike availability pipeline.

The SAME functions build features for training (historical snapshots) and for
live scoring (collector snapshots).  That is the main guard against
train/serve skew: never re-implement a feature inside a notebook.

Time semantics (all UTC):
  * snapshot_ts       when the feed was observed
  * prediction time t a point on a regular GRID_MIN grid
  * state at T        the latest snapshot with snapshot_ts in (T - tol, T]
                      ("as-of" join with a staleness tolerance)
  * features at t     use only states at t, t-15, t-30, t-60  -> no look-ahead
  * label at t        state at t + HORIZON_MIN
"""
from __future__ import annotations

import json
import math
import time
from typing import Iterable

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T_

import config as C

STD_STATUS_COLS = [
    "station_id", "snapshot_ts", "last_reported",
    "num_bikes_available", "num_ebikes_available", "num_docks_available",
    "num_bikes_disabled", "num_docks_disabled",
    "is_installed", "is_renting", "is_returning",
]
REQUIRED_STATUS = {"station_id", "snapshot_ts", "num_bikes_available", "num_docks_available"}

LIVE_STATUS_SCHEMA = T_.StructType([
    T_.StructField("station_id", T_.StringType(), False),
    T_.StructField("snapshot_ts", T_.TimestampType(), False),
    T_.StructField("last_reported", T_.TimestampType(), True),
    T_.StructField("num_bikes_available", T_.IntegerType(), True),
    T_.StructField("num_ebikes_available", T_.IntegerType(), True),
    T_.StructField("num_docks_available", T_.IntegerType(), True),
    T_.StructField("num_bikes_disabled", T_.IntegerType(), True),
    T_.StructField("num_docks_disabled", T_.IntegerType(), True),
    T_.StructField("is_installed", T_.IntegerType(), True),
    T_.StructField("is_renting", T_.IntegerType(), True),
    T_.StructField("is_returning", T_.IntegerType(), True),
    T_.StructField("feed_last_updated", T_.TimestampType(), True),
    T_.StructField("fetched_at", T_.TimestampType(), False),
    T_.StructField("source_url", T_.StringType(), True),
    T_.StructField("run_id", T_.StringType(), False),
])

LIVE_INFO_SCHEMA = T_.StructType([
    T_.StructField("station_id", T_.StringType(), False),
    T_.StructField("name", T_.StringType(), True),
    T_.StructField("capacity", T_.IntegerType(), True),
    T_.StructField("lat", T_.DoubleType(), True),
    T_.StructField("lon", T_.DoubleType(), True),
    T_.StructField("fetched_at", T_.TimestampType(), False),
])


# ===========================================================================
# Spark session hygiene
# ===========================================================================
def init_spark(spark: SparkSession) -> None:
    """All timestamp math in this project is UTC.

    PySpark converts naive Python datetimes using the *Python process* time zone in
    both directions (createDataFrame, F.lit, collect).  Pinning the process TZ and the
    Spark session TZ to UTC makes every naive datetime in this codebase mean UTC.
    """
    import os
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    spark.conf.set("spark.sql.session.timeZone", "UTC")


# ===========================================================================
# Live GBFS collection
# ===========================================================================
def fetch_json(urls: Iterable[str], timeout: int, retries: int):
    """Try each URL with exponential backoff. Returns (payload, url) or raises."""
    import requests

    errors = []
    for url in urls:
        for attempt in range(retries):
            try:
                r = requests.get(url, timeout=timeout, headers={"User-Agent": "cuny-data622-group1"})
                r.raise_for_status()
                payload = r.json()
                if "data" not in payload or "stations" not in payload["data"]:
                    raise ValueError("payload missing data.stations")
                return payload, url
            except Exception as e:  # noqa: BLE001 - we record and try the next attempt/host
                errors.append(f"{url} attempt {attempt + 1}: {type(e).__name__}: {e}")
                time.sleep(2 ** attempt)
    raise RuntimeError("All GBFS endpoints failed:\n" + "\n".join(errors))


def _epoch_to_ts(x):
    if x is None:
        return None
    try:
        return pd.Timestamp(int(x), unit="s", tz="UTC").tz_localize(None).to_pydatetime()
    except (TypeError, ValueError):
        return None


def _to_int(x):
    if x is None:
        return None
    if isinstance(x, bool):
        return int(x)
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def parse_status_payload(payload: dict, fetched_at, source_url: str, run_id: str) -> list[tuple]:
    """GBFS station_status -> rows matching LIVE_STATUS_SCHEMA.

    snapshot_ts is the feed's last_updated (when Lyft published the snapshot),
    falling back to fetched_at.  Booleans (GBFS 2.x/3.x) and ints (1.x) both map to 0/1.
    """
    feed_ts = _epoch_to_ts(payload.get("last_updated"))
    snap = feed_ts or fetched_at
    rows = []
    for s in payload["data"]["stations"]:
        sid = s.get("station_id")
        if sid is None:
            continue
        rows.append((
            str(sid), snap, _epoch_to_ts(s.get("last_reported")),
            _to_int(s.get("num_bikes_available")),
            _to_int(s.get("num_ebikes_available")),
            _to_int(s.get("num_docks_available")),
            _to_int(s.get("num_bikes_disabled")),
            _to_int(s.get("num_docks_disabled")),
            _to_int(s.get("is_installed")),
            _to_int(s.get("is_renting")),
            _to_int(s.get("is_returning")),
            feed_ts, fetched_at, source_url, run_id,
        ))
    return rows


def parse_info_payload(payload: dict, fetched_at) -> list[tuple]:
    rows = []
    for s in payload["data"]["stations"]:
        sid = s.get("station_id")
        if sid is None:
            continue
        lat, lon = s.get("lat"), s.get("lon")
        rows.append((str(sid), s.get("name"), _to_int(s.get("capacity")),
                     float(lat) if lat is not None else None,
                     float(lon) if lon is not None else None, fetched_at))
    return rows


# ===========================================================================
# Standardisation of the historical silver table
# ===========================================================================
def _to_timestamp_col(df: DataFrame, col: str):
    """Accept epoch seconds, epoch millis, timestamp, or string."""
    dt = dict(df.dtypes)[col]
    c = F.col(col)
    if dt in ("timestamp", "timestamp_ntz"):
        return c.cast("timestamp")
    if dt in ("bigint", "int", "double", "float", "decimal") or dt.startswith("decimal"):
        # millis if the value is implausibly large for seconds
        return F.when(c > F.lit(10_000_000_000), F.timestamp_seconds((c / 1000).cast("bigint"))) \
                .otherwise(F.timestamp_seconds(c.cast("bigint")))
    return F.to_timestamp(c)


def validate_colmap(df: DataFrame, colmap: dict, required: set[str], table: str) -> list[str]:
    """Raise with an actionable message if required source columns are missing."""
    present = set(df.columns)
    missing_req = [k for k in required if colmap.get(k) not in present]
    if missing_req:
        raise ValueError(
            f"{table}: required columns not found for {missing_req}. "
            f"Mapped names: { {k: colmap.get(k) for k in missing_req} }. "
            f"Available columns: {sorted(present)}. Fix STATUS_COLMAP / INFO_COLMAP in config.py."
        )
    return [k for k, v in colmap.items() if v is None or v not in present]


def standardize_status(df: DataFrame, colmap: dict, table: str = "status") -> tuple[DataFrame, list[str]]:
    missing_opt = validate_colmap(df, colmap, REQUIRED_STATUS, table)
    out = []
    for std in STD_STATUS_COLS:
        src = colmap.get(std)
        if std in missing_opt:
            default = {"is_installed": 1, "is_renting": 1, "is_returning": 1}.get(std)
            dtype = "timestamp" if std in ("snapshot_ts", "last_reported") else "int"
            out.append(F.lit(default).cast(dtype).alias(std))
        elif std in ("snapshot_ts", "last_reported"):
            out.append(_to_timestamp_col(df, src).alias(std))
        elif std == "station_id":
            out.append(F.col(src).cast("string").alias(std))
        else:
            # booleans and ints -> 0/1 / counts
            out.append(F.col(src).cast("int").alias(std))
    std_df = df.select(*out).where(F.col("station_id").isNotNull() & F.col("snapshot_ts").isNotNull())
    # one row per (station, snapshot)
    std_df = std_df.dropDuplicates(["station_id", "snapshot_ts"])
    return std_df, missing_opt


def standardize_info(df: DataFrame, colmap: dict) -> DataFrame:
    validate_colmap(df, colmap, {"station_id", "capacity"}, "station_info")
    cols = []
    for std in ["station_id", "name", "capacity", "lat", "lon"]:
        src = colmap.get(std)
        if src is None or src not in df.columns:
            cols.append(F.lit(None).cast("double" if std in ("lat", "lon") else "string").alias(std))
        else:
            cast = {"station_id": "string", "name": "string", "capacity": "int",
                    "lat": "double", "lon": "double"}[std]
            cols.append(F.col(src).cast(cast).alias(std))
    # keep the latest non-null capacity per station
    return df.select(*cols).groupBy("station_id").agg(
        F.max("name").alias("name"), F.max("capacity").alias("capacity"),
        F.avg("lat").alias("lat"), F.avg("lon").alias("lon"))


# ===========================================================================
# Grid, as-of state, features, labels
# ===========================================================================
def build_grid(status: DataFrame, grid_min: int, start=None, end=None) -> DataFrame:
    """Regular UTC grid per station between that station's first and last snapshot."""
    step = grid_min * 60
    bounds = status.groupBy("station_id").agg(
        F.min("snapshot_ts").alias("t0"), F.max("snapshot_ts").alias("t1"))
    if start is not None:
        bounds = bounds.withColumn("t0", F.greatest("t0", F.to_timestamp(F.lit(start))))
    if end is not None:
        bounds = bounds.withColumn("t1", F.least("t1", F.to_timestamp(F.lit(end))))
    # first grid point at or after t0
    # Stations whose snapshots all fall between two grid points get g0 > g1. Drop them
    # BEFORE sequence(): Databricks raises ILLEGAL_SEQUENCE_BOUNDARIES on reversed bounds.
    bounds = (bounds
              .withColumn("g0", F.timestamp_seconds((F.ceil(F.unix_timestamp("t0") / step) * step).cast("bigint")))
              .withColumn("g1", F.timestamp_seconds((F.floor(F.unix_timestamp("t1") / step) * step).cast("bigint")))
              .where(F.col("g0").isNotNull() & F.col("g1").isNotNull() & (F.col("g0") <= F.col("g1"))))
    return (bounds
            .select("station_id", F.explode(F.sequence("g0", "g1", F.expr(f"INTERVAL {grid_min} MINUTES"))).alias("t"))
            .where(F.col("t").isNotNull()))


def asof_state(status: DataFrame, grid: DataFrame, tol_min: int) -> DataFrame:
    """For every (station, t) on the grid: latest snapshot in (t - tol, t].

    Implemented as union + window `last(ignorenulls)` over a RANGE frame, which is
    exact point-in-time semantics (no future rows enter the frame) and avoids a
    range join.  Returns grid rows with the state struct flattened plus `state_age_min`.
    """
    tol_s = tol_min * 60
    payload_cols = [c for c in STD_STATUS_COLS if c not in ("station_id",)]
    snaps = status.select(
        "station_id",
        F.col("snapshot_ts").alias("ts"),
        F.struct(*[F.col(c) for c in payload_cols]).alias("s"),
        F.lit(0).alias("is_grid"),
    )
    grid_rows = grid.select(
        "station_id", F.col("t").alias("ts"),
        F.lit(None).cast(snaps.schema["s"].dataType).alias("s"),
        F.lit(1).alias("is_grid"),
    )
    w = (Window.partitionBy("station_id")
         .orderBy(F.unix_timestamp("ts"))
         .rangeBetween(-tol_s + 1, 0))           # (t - tol, t]
    # Snapshots must sort after nothing in the future: RANGE frames include peers with
    # the same ts, and grid rows carry null structs, so ties are safe.
    merged = snaps.unionByName(grid_rows).withColumn("st", F.last("s", ignorenulls=True).over(w))
    out = merged.where("is_grid = 1").select("station_id", F.col("ts").alias("t"), "st.*")
    return out.withColumn(
        "state_age_min",
        (F.unix_timestamp("t") - F.unix_timestamp("snapshot_ts")) / 60.0,
    )


def build_features_labels(status: DataFrame, info: DataFrame, cfg=C,
                          start=None, end=None, with_labels: bool = True) -> DataFrame:
    """Features at t and (optionally) labels at t + HORIZON_MIN, keyed on (station_id, t).

    Output columns: station_id, prediction_time_utc, feature columns, and if
    with_labels: empty_label, full_label, label_snapshot_ts, horizon_offset_min,
    eligible, exclusion_reason.
    """
    grid = build_grid(status, cfg.GRID_MIN, start, end)
    st = asof_state(status, grid, cfg.ASOF_TOLERANCE_MIN)

    w = Window.partitionBy("station_id").orderBy("t")
    step_s = cfg.GRID_MIN * 60
    df = st
    # Lags.  The grid is dense per station, so lag(k) is exactly t - k*GRID_MIN.
    for k in cfg.LAG_STEPS:
        m = k * cfg.GRID_MIN
        df = (df.withColumn(f"bikes_lag{m}", F.lag("num_bikes_available", k).over(w))
                .withColumn(f"docks_lag{m}", F.lag("num_docks_available", k).over(w))
                .withColumn(f"_tlag{m}", F.lag("t", k).over(w)))
        # guard against gaps: lag row must be exactly m minutes back
        df = (df.withColumn(f"bikes_lag{m}", F.when(F.unix_timestamp("t") - F.unix_timestamp(f"_tlag{m}") == k * step_s, F.col(f"bikes_lag{m}")))
                .withColumn(f"docks_lag{m}", F.when(F.unix_timestamp("t") - F.unix_timestamp(f"_tlag{m}") == k * step_s, F.col(f"docks_lag{m}")))
                .drop(f"_tlag{m}"))
        df = (df.withColumn(f"d_bikes_{m}", F.col("num_bikes_available") - F.col(f"bikes_lag{m}"))
                .withColumn(f"d_docks_{m}", F.col("num_docks_available") - F.col(f"docks_lag{m}")))

    if with_labels:
        h = cfg.HORIZON_MIN // cfg.GRID_MIN
        df = (df.withColumn("_t_lead", F.lead("t", h).over(w))
                .withColumn("_ok_lead", F.unix_timestamp("_t_lead") - F.unix_timestamp("t") == cfg.HORIZON_MIN * 60)
                .withColumn("y_bikes", F.when(F.col("_ok_lead"), F.lead("num_bikes_available", h).over(w)))
                .withColumn("y_docks", F.when(F.col("_ok_lead"), F.lead("num_docks_available", h).over(w)))
                .withColumn("y_snap", F.when(F.col("_ok_lead"), F.lead("snapshot_ts", h).over(w)))
                .withColumn("y_installed", F.when(F.col("_ok_lead"), F.lead("is_installed", h).over(w)))
                .withColumn("y_renting", F.when(F.col("_ok_lead"), F.lead("is_renting", h).over(w)))
                .withColumn("y_returning", F.when(F.col("_ok_lead"), F.lead("is_returning", h).over(w))))

    df = df.join(F.broadcast(info.select("station_id", "capacity")), "station_id", "left")

    local = F.from_utc_timestamp("t", cfg.LOCAL_TZ)
    minute_of_day = F.hour(local) * 60 + F.minute(local)
    total = F.col("num_bikes_available") + F.col("num_docks_available")
    df = (df
          .withColumn("hour_local", F.hour(local))
          .withColumn("dow_local", F.dayofweek(local))                     # 1=Sun..7=Sat
          .withColumn("is_weekend", F.col("dow_local").isin(1, 7).cast("int"))
          .withColumn("tod_sin", F.sin(2 * math.pi * minute_of_day / 1440))
          .withColumn("tod_cos", F.cos(2 * math.pi * minute_of_day / 1440))
          .withColumn("dow_sin", F.sin(2 * math.pi * (F.col("dow_local") - 1) / 7))
          .withColumn("dow_cos", F.cos(2 * math.pi * (F.col("dow_local") - 1) / 7))
          .withColumn("fill_ratio", F.when(total > 0, F.col("num_bikes_available") / total))
          .withColumn("is_empty_now", (F.col("num_bikes_available") == 0).cast("int"))
          .withColumn("is_full_now", (F.col("num_docks_available") == 0).cast("int"))
          .withColumn("ebike_share", F.when(F.col("num_bikes_available") > 0,
                                            F.col("num_ebikes_available") / F.col("num_bikes_available")))
          .withColumn("report_age_min", (F.unix_timestamp("t") - F.unix_timestamp("last_reported")) / 60.0))

    feature_ok = (F.col("snapshot_ts").isNotNull()
                  & (F.coalesce("is_installed", F.lit(1)) == 1)
                  & (F.coalesce("is_renting", F.lit(1)) == 1)
                  & (F.coalesce("is_returning", F.lit(1)) == 1))

    if with_labels:
        df = (df
              .withColumn("empty_label", (F.col("y_bikes") == 0).cast("int"))
              .withColumn("full_label", (F.col("y_docks") == 0).cast("int"))
              .withColumn("label_snapshot_ts", F.col("y_snap"))
              .withColumn("horizon_offset_min",
                          (F.unix_timestamp("y_snap") - F.unix_timestamp("t")) / 60.0)
              .withColumn("exclusion_reason", F.when(F.col("snapshot_ts").isNull(), "no_fresh_snapshot_at_t")
                          .when(~feature_ok, "station_not_operating_at_t")
                          .when(F.col("y_snap").isNull(), "no_fresh_snapshot_at_t_plus_h")
                          .when((F.coalesce("y_installed", F.lit(1)) != 1)
                                | (F.coalesce("y_renting", F.lit(1)) != 1)
                                | (F.coalesce("y_returning", F.lit(1)) != 1), "station_not_operating_at_t_plus_h"))
              .withColumn("eligible", F.col("exclusion_reason").isNull().cast("int")))
    else:
        df = (df.withColumn("exclusion_reason", F.when(F.col("snapshot_ts").isNull(), "no_fresh_snapshot_at_t")
                            .when(~feature_ok, "station_not_operating_at_t"))
                .withColumn("eligible", F.col("exclusion_reason").isNull().cast("int")))

    keep = ["station_id", F.col("t").alias("prediction_time_utc"), "snapshot_ts", "state_age_min",
            *FEATURE_COLS_SPARK]
    if with_labels:
        keep += ["empty_label", "full_label", "label_snapshot_ts", "horizon_offset_min"]
    keep += ["eligible", "exclusion_reason"]
    return df.select(*keep)


def _lag_feature_names(cfg=C) -> list[str]:
    names = []
    for k in cfg.LAG_STEPS:
        m = k * cfg.GRID_MIN
        names += [f"d_bikes_{m}", f"d_docks_{m}"]
    return names


FEATURE_COLS_SPARK = [
    "num_bikes_available", "num_ebikes_available", "num_docks_available",
    "num_bikes_disabled", "num_docks_disabled", "capacity",
    "fill_ratio", "is_empty_now", "is_full_now", "ebike_share", "report_age_min",
    *_lag_feature_names(),
    "hour_local", "dow_local", "is_weekend", "tod_sin", "tod_cos", "dow_sin", "dow_cos",
]

# Columns the models see (priors are joined in from the training split only).
PRIOR_COLS = ["prior_empty", "prior_full"]
MODEL_FEATURES = [c for c in FEATURE_COLS_SPARK if c not in ("hour_local", "dow_local")] + PRIOR_COLS


# ===========================================================================
# Station selection
# ===========================================================================
def station_quality(fl: DataFrame) -> DataFrame:
    """Coverage and event rates per station over the historical window."""
    return fl.groupBy("station_id").agg(
        F.count("*").alias("grid_points"),
        F.avg("eligible").alias("coverage"),
        F.avg(F.when(F.col("eligible") == 1, F.col("empty_label"))).alias("empty_rate"),
        F.avg(F.when(F.col("eligible") == 1, F.col("full_label"))).alias("full_rate"),
        F.avg("state_age_min").alias("mean_state_age_min"),
    )


def select_stations(quality: DataFrame, live_ids: DataFrame | None, cfg=C) -> DataFrame:
    """Keep all stations meeting the coverage requirement."""

    stations = quality.where(
        F.col("coverage") >= cfg.MIN_COVERAGE
    )

    if live_ids is not None:
        stations = stations.join(
            live_ids.select("station_id").distinct(),
            on="station_id",
            how="inner"
        )

    return (
        stations
        .withColumn("selection_stratum", F.lit("all_eligible"))
        .withColumn("selected_at", F.current_timestamp())
    )

# ===========================================================================
# Pandas-side modelling helpers (pure functions, unit-testable)
# ===========================================================================
def compute_split(times: pd.Series, cfg=C) -> dict:
    tmax = pd.Timestamp(times.max())
    test_start = pd.Timestamp(cfg.TEST_START_UTC) if cfg.TEST_START_UTC else (tmax - pd.Timedelta(days=cfg.TEST_DAYS)).floor("D")
    val_start = pd.Timestamp(cfg.VAL_START_UTC) if cfg.VAL_START_UTC else test_start - pd.Timedelta(days=cfg.VAL_DAYS)
    if not (pd.Timestamp(times.min()) < val_start < test_start <= tmax):
        raise ValueError(f"Invalid split: min={times.min()} val_start={val_start} test_start={test_start} max={tmax}. "
                         "Not enough history for VAL_DAYS + TEST_DAYS; shorten them in config.py.")
    return {"val_start_utc": val_start, "test_start_utc": test_start, "embargo_min": cfg.EMBARGO_MIN}


def assign_split(t: pd.Series, split: dict) -> pd.Series:
    """train / val / test with an embargo before each boundary (rows inside it -> 'embargo')."""
    emb = pd.Timedelta(minutes=split["embargo_min"])
    vs, ts = split["val_start_utc"], split["test_start_utc"]
    out = np.where(t >= ts, "test", np.where(t >= vs, "val", "train"))
    in_emb = ((t >= vs - emb) & (t < vs)) | ((t >= ts - emb) & (t < ts))
    return pd.Series(np.where(in_emb, "embargo", out), index=t.index)


def fit_priors(train: pd.DataFrame, m: float) -> pd.DataFrame:
    """Smoothed P(empty), P(full) by station x local hour x weekend, from TRAIN rows only."""
    g_empty, g_full = train["empty_label"].mean(), train["full_label"].mean()
    agg = (train.groupby(["station_id", "hour_local", "is_weekend"])
                .agg(n=("empty_label", "size"), e=("empty_label", "sum"), f=("full_label", "sum"))
                .reset_index())
    agg["prior_empty"] = (agg["e"] + m * g_empty) / (agg["n"] + m)
    agg["prior_full"] = (agg["f"] + m * g_full) / (agg["n"] + m)
    agg["global_empty"], agg["global_full"] = g_empty, g_full
    return agg[["station_id", "hour_local", "is_weekend", "n", "prior_empty", "prior_full", "global_empty", "global_full"]]


def fit_priors_oof(train: pd.DataFrame, m: float, fold_days: int = 7) -> tuple[np.ndarray, np.ndarray]:
    """Out-of-fold station x hour priors for TRAINING rows.

    Joining priors fit on all training rows back onto those same rows is in-sample
    target encoding: every row's own label is inside its prior, so a flexible model
    learns to over-trust the prior and fails to generalize.  Here training time is
    cut into consecutive `fold_days` blocks, and each block's priors are fit only on
    the OTHER blocks.  Validation, test, and live rows keep using priors fit on all
    training rows (fit_priors), which is leak-free for them.

    Returns (prior_empty, prior_full) as arrays aligned to `train`'s row order.
    """
    t = pd.to_datetime(train["prediction_time_utc"]).to_numpy()
    fold = ((t - t.min()) // np.timedelta64(fold_days, "D")).astype(int)
    keys = ["station_id", "hour_local", "is_weekend"]
    pe = np.full(len(train), np.nan)
    pf = np.full(len(train), np.nan)
    for k in np.unique(fold):
        inside = fold == k
        if (~inside).sum() == 0:
            raise ValueError("fit_priors_oof needs at least two folds; shorten fold_days")
        pri = fit_priors(train.loc[~inside], m)
        joined = join_priors(train.loc[inside, keys].reset_index(drop=True), pri)   # left merge keeps row order
        pe[inside] = joined["prior_empty"].to_numpy()
        pf[inside] = joined["prior_full"].to_numpy()
    return pe, pf


def join_priors(df: pd.DataFrame, priors: pd.DataFrame) -> pd.DataFrame:
    out = df.merge(priors[["station_id", "hour_local", "is_weekend", "prior_empty", "prior_full"]],
                   on=["station_id", "hour_local", "is_weekend"], how="left")
    g_e = priors["global_empty"].iloc[0] if len(priors) else 0.0
    g_f = priors["global_full"].iloc[0] if len(priors) else 0.0
    out["prior_empty"] = out["prior_empty"].fillna(g_e)
    out["prior_full"] = out["prior_full"].fillna(g_f)
    return out


def persistence_rates(train: pd.DataFrame, target: str) -> dict:
    """Calibrated persistence: P(y=1 at t+h | current state indicator) from train."""
    now = "is_empty_now" if target == "empty" else "is_full_now"
    y = f"{target}_label"
    r = train.groupby(now)[y].mean().to_dict()
    return {0: float(r.get(0, 0.0)), 1: float(r.get(1, 1.0))}


def classification_metrics(y: np.ndarray, p: np.ndarray, threshold: float) -> dict:
    from sklearn.metrics import (average_precision_score, brier_score_loss, f1_score,
                                 precision_score, recall_score, roc_auc_score)
    y = np.asarray(y).astype(int)
    p = np.clip(np.asarray(p, dtype=float), 0, 1)
    yhat = (p >= threshold).astype(int)
    pos = y.sum()
    return {
        "n": int(len(y)), "positives": int(pos), "prevalence": float(y.mean()) if len(y) else float("nan"),
        "pr_auc": float(average_precision_score(y, p)) if 0 < pos < len(y) else float("nan"),
        "roc_auc": float(roc_auc_score(y, p)) if 0 < pos < len(y) else float("nan"),
        "brier": float(brier_score_loss(y, p)),
        "precision": float(precision_score(y, yhat, zero_division=0)),
        "recall": float(recall_score(y, yhat, zero_division=0)),
        "f1": float(f1_score(y, yhat, zero_division=0)),
        "threshold": float(threshold),
    }


def best_f1_threshold(y: np.ndarray, p: np.ndarray) -> float:
    from sklearn.metrics import precision_recall_curve
    prec, rec, thr = precision_recall_curve(y, p)
    if len(thr) == 0:
        return 0.5
    f1 = 2 * prec[:-1] * rec[:-1] / np.clip(prec[:-1] + rec[:-1], 1e-12, None)
    return float(thr[int(np.nanargmax(f1))])


def calibration_table(y: np.ndarray, p: np.ndarray, n_bins: int = 10) -> pd.DataFrame:
    """Quantile-free fixed bins; includes counts so sparse bins are visible."""
    bins = np.linspace(0, 1, n_bins + 1)
    idx = np.clip(np.digitize(p, bins[1:-1], right=False), 0, n_bins - 1)
    df = pd.DataFrame({"bin": idx, "y": y, "p": p})
    out = df.groupby("bin").agg(n=("y", "size"), mean_pred=("p", "mean"), obs_rate=("y", "mean")).reset_index()
    out["bin_lo"], out["bin_hi"] = bins[out["bin"]], bins[out["bin"] + 1]
    return out


def make_models(random_state: int):
    """Logistic regression (scaled, imputed) and histogram GBM (native NaN handling)."""
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    lr = Pipeline([
        ("impute", SimpleImputer(strategy="median", add_indicator=True)),
        ("scale", StandardScaler()),
        ("clf", LogisticRegression(max_iter=2000, C=1.0)),
    ])
    gbm = HistGradientBoostingClassifier(
        learning_rate=0.06, max_iter=400, max_leaf_nodes=31, min_samples_leaf=200,
        l2_regularization=1.0, early_stopping=True, validation_fraction=0.1,
        n_iter_no_change=25, random_state=random_state)
    return {"logreg": lr, "hgb": gbm}


def to_pandas_features(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce feature dtypes to float64 for a stable MLflow signature."""
    X = df[MODEL_FEATURES].copy()
    for c in MODEL_FEATURES:
        X[c] = pd.to_numeric(X[c], errors="coerce").astype("float64")
    return X


def runtime_info() -> str:
    import sklearn
    return json.dumps({"sklearn": sklearn.__version__, "pandas": pd.__version__, "numpy": np.__version__})
