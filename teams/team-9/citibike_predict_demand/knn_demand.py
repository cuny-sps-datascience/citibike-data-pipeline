"""KNN demand model for new Citi Bike stations (Team 9).

Pure pandas / numpy / scipy / scikit-learn. No Spark, so it can be unit tested
anywhere and imported from any Databricks notebook in this folder:

    from knn_demand import KNNDemand, add_features, backtest, metrics

Target: average monthly trips that start at a station during its first
(up to) 12 months. For a new site, find the k most similar existing stations and
take a distance-weighted average of their demand. P10/P90 are weighted quantiles
of the neighbors' demand.
"""
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from scipy.stats import spearmanr
from sklearn.linear_model import Ridge

EARTH_R_M = 6_371_000.0
LAT0, LON0 = 40.73, -73.95
FEATURE_COLS = ["x_m", "y_m", "log_capacity", "log_subway_dist", "stations_400m"]
DEFAULT_WEIGHTS = {"location": 1.5, "log_capacity": 1.0,
                   "log_subway_dist": 1.0, "stations_400m": 0.5}
DENSITY_RADIUS_M = 400


# ---------------------------------------------------------------- geography
def to_xy(lat, lon):
    """Approximate local meters around NYC (fine at city scale)."""
    lat, lon = np.asarray(lat, float), np.asarray(lon, float)
    x = np.radians(lon - LON0) * EARTH_R_M * np.cos(np.radians(LAT0))
    y = np.radians(lat - LAT0) * EARTH_R_M
    return x, y


def _xy_array(lat, lon):
    x, y = to_xy(lat, lon)
    return np.column_stack([x, y])


def subway_distance_m(lat, lon, sub_lat, sub_lon):
    tree = cKDTree(_xy_array(sub_lat, sub_lon))
    d, _ = tree.query(_xy_array(lat, lon))
    return d


def count_within(lat, lon, st_lat, st_lon, radius=DENSITY_RADIUS_M):
    tree = cKDTree(_xy_array(st_lat, st_lon))
    pts = _xy_array(lat, lon)
    return np.array([len(tree.query_ball_point(p, radius)) for p in pts])


def add_features(stations, sub_lat, sub_lon):
    """stations needs lat, lon, capacity. Adds the model feature columns.
    Density excludes the station itself."""
    out = stations.copy()
    out["x_m"], out["y_m"] = to_xy(out["lat"], out["lon"])
    out["log_capacity"] = np.log1p(out["capacity"].clip(lower=0))
    out["log_subway_dist"] = np.log1p(
        subway_distance_m(out["lat"], out["lon"], sub_lat, sub_lon))
    out["stations_400m"] = count_within(out["lat"], out["lon"],
                                        out["lat"], out["lon"]) - 1
    return out


def site_features(lat, lon, capacity, stations, sub_lat, sub_lon):
    """One-row feature frame for a hypothetical new site."""
    row = pd.DataFrame({"lat": [lat], "lon": [lon], "capacity": [capacity]})
    row["x_m"], row["y_m"] = to_xy(row["lat"], row["lon"])
    row["log_capacity"] = np.log1p(max(capacity, 0))
    row["log_subway_dist"] = np.log1p(
        subway_distance_m(row["lat"], row["lon"], sub_lat, sub_lon))
    row["stations_400m"] = count_within(row["lat"], row["lon"],
                                        stations["lat"], stations["lon"])
    return row


# -------------------------------------------------------------------- model
def weighted_quantile(values, weights, q):
    order = np.argsort(values)
    v, w = np.asarray(values)[order], np.asarray(weights)[order]
    cum = (np.cumsum(w) - 0.5 * w) / w.sum()
    return float(np.interp(q, cum, v))


class KNNDemand:
    def __init__(self, k=10, weights=None):
        self.k = k
        self.weights = {**DEFAULT_WEIGHTS, **(weights or {})}

    def _design(self, X):
        Z = (X[FEATURE_COLS].to_numpy(float) - self.mu) / self.sd
        return Z * self.col_w

    def fit(self, X, y):
        A = X[FEATURE_COLS].to_numpy(float)
        self.mu = A.mean(axis=0)
        sd = A.std(axis=0)
        sd[sd == 0] = 1.0
        sd[:2] = sd[:2].mean()          # one shared scale keeps geography undistorted
        self.sd = sd
        w = self.weights
        self.col_w = np.array([w["location"], w["location"], w["log_capacity"],
                               w["log_subway_dist"], w["stations_400m"]])
        self.y = np.asarray(y, float)
        self.Z = self._design(X)
        return self

    def predict(self, X):
        Zq = self._design(X)
        k = min(self.k, len(self.y))
        rows = []
        for q in Zq:
            d = np.linalg.norm(self.Z - q, axis=1)
            idx = np.argpartition(d, k - 1)[:k]
            idx = idx[np.argsort(d[idx])]
            w = 1.0 / (d[idx] + 0.1)
            yy = self.y[idx]
            rows.append({"pred": float(np.average(yy, weights=w)),
                         "p10": weighted_quantile(yy, w, 0.10),
                         "p90": weighted_quantile(yy, w, 0.90),
                         "neighbor_idx": idx, "neighbor_dist": d[idx]})
        return pd.DataFrame(rows)


# --------------------------------------------------------------- evaluation
def asof_density(ctx, rows=None, radius=DENSITY_RADIUS_M):
    """For the given rows (default all), count stations within 400 m that opened strictly earlier."""
    ctx = ctx.reset_index(drop=True)
    fm = ctx["first_month"].to_numpy()
    xy = _xy_array(ctx["lat"], ctx["lon"])
    rows = ctx.index if rows is None else rows
    out = pd.Series(0.0, index=rows)
    for i in rows:
        pool = xy[fm < fm[i]]
        if len(pool):
            out[i] = len(cKDTree(pool).query_ball_point(xy[i], radius))
    return out


def test_stations(ctx, min_months=6):
    return ctx[(~ctx["is_pre_history"]) & (ctx["n_months"] >= min_months)]


def backtest(ctx, k=10, weights=None, min_pool=30, min_months=6, density=None, fast=False):
    """Pretend each station that opened during the history is new: hide it,
    use only stations that opened earlier, predict its first-year demand.
    fast=True skips the baseline and ridge (used while tuning)."""
    ctx = ctx.reset_index(drop=True)
    tests = test_stations(ctx, min_months)
    if density is None:
        density = asof_density(ctx, rows=tests.index)
    fm = ctx["first_month"].to_numpy()
    rows = []
    for i in tests.index:
        pool_idx = np.flatnonzero(fm < fm[i])
        if len(pool_idx) < max(min_pool, k):
            continue
        pool = ctx.iloc[pool_idx]
        q = ctx.loc[[i], FEATURE_COLS].astype(float).copy()
        q["stations_400m"] = float(density[i])
        model = KNNDemand(k, weights).fit(pool, pool["target"])
        p = model.predict(q).iloc[0]
        row = {"station_id": ctx.at[i, "station_id"], "actual": ctx.at[i, "target"],
               "knn": p["pred"], "p10": p["p10"], "p90": p["p90"]}
        if not fast:
            same = ((pool["x_m"] // 1500).to_numpy() == (q["x_m"].iloc[0] // 1500)) & \
                   ((pool["y_m"] // 1500).to_numpy() == (q["y_m"].iloc[0] // 1500))
            row["baseline_grid"] = pool.loc[same, "target"].mean() if same.any() else pool["target"].mean()
            Zp = (pool[FEATURE_COLS].to_numpy(float) - model.mu) / model.sd
            ridge = Ridge(alpha=1.0).fit(Zp, np.log1p(pool["target"]))
            Zq = (q[FEATURE_COLS].to_numpy(float) - model.mu) / model.sd
            row["ridge"] = float(np.expm1(ridge.predict(Zq)[0]))
        rows.append(row)
    return pd.DataFrame(rows)


def interval_factors(bt, lo=0.10, hi=0.90):
    """Calibrate the prediction range from the backtest: the 10th and 90th percentile of
    actual / predicted. Multiply a new prediction by these to get P10 and P90."""
    ratio = (bt["actual"] / bt["knn"].clip(lower=1.0)).to_numpy()
    return float(np.quantile(ratio, lo)), float(np.quantile(ratio, hi))


def metrics(bt):
    """MAE, WAPE, Spearman, and P10-P90 coverage for each model."""
    out = {}
    for m in ["knn", "baseline_grid", "ridge"]:
        err = (bt[m] - bt["actual"]).abs()
        out[m] = {"MAE": err.mean(), "WAPE": err.sum() / bt["actual"].sum(),
                  "Spearman": spearmanr(bt[m], bt["actual"])[0]}
    out["knn"]["P10_P90_coverage"] = ((bt["actual"] >= bt["p10"]) & (bt["actual"] <= bt["p90"])).mean()
    return pd.DataFrame(out).T


def tune(ctx, ks=(5, 10, 15, 20), loc_weights=(0.5, 1.0, 1.5, 2.5), min_months=6, min_pool=30):
    """Grid search over k and the location weight, using only numpy arrays (very fast).
    Same math as backtest(): for each pretend-new station, neighbors come from earlier stations."""
    ctx = ctx.reset_index(drop=True)
    tests = test_stations(ctx, min_months)
    density = asof_density(ctx, rows=tests.index)
    A = ctx[FEATURE_COLS].to_numpy(float)
    y = ctx["target"].to_numpy(float)
    fm = ctx["first_month"].to_numpy()
    w = DEFAULT_WEIGHTS
    combos = [(k, lw) for k in ks for lw in loc_weights]
    errors = {c: [] for c in combos}
    for i in tests.index:
        pool = np.flatnonzero(fm < fm[i])
        P = A[pool]
        mu, sd = P.mean(axis=0), P.std(axis=0)
        sd[sd == 0] = 1.0
        sd[:2] = sd[:2].mean()
        q = A[i].copy()
        q[4] = density[i]
        diff = (P - mu) / sd - (q - mu) / sd
        yp = y[pool]
        for k, lw in combos:
            if len(pool) < max(min_pool, k):
                continue
            colw = np.array([lw, lw, w["log_capacity"], w["log_subway_dist"], w["stations_400m"]])
            d = np.linalg.norm(diff * colw, axis=1)
            idx = np.argpartition(d, k - 1)[:k]
            wt = 1.0 / (d[idx] + 0.1)
            errors[(k, lw)].append(abs(np.average(yp[idx], weights=wt) - y[i]))
    rows = [{"k": k, "location_weight": lw, "n_test": len(e), "MAE": float(np.mean(e))}
            for (k, lw), e in errors.items() if e]
    return pd.DataFrame(rows).sort_values("MAE").reset_index(drop=True)


# ---------------------------------------------------- budget and crew hours
# Placeholder operating assumptions: Citi Bike cost data is not available to us.
TRIPS_PER_BIKE_CHECK = 200
HOURS_PER_BIKE_CHECK = 0.25
BASE_HOURS_PER_MONTH = 6.0


def seasonal_budget(avg_monthly_trips, season_share):
    """season_share: Series indexed 1..12 whose values sum to 1."""
    return pd.DataFrame({"month": season_share.index,
                         "trips": avg_monthly_trips * 12 * season_share.values})


def crew_plan(trips, trips_per_check=TRIPS_PER_BIKE_CHECK,
              hours_per_check=HOURS_PER_BIKE_CHECK, base_hours=BASE_HOURS_PER_MONTH):
    checks = np.asarray(trips, float) / trips_per_check
    return pd.DataFrame({"bike_checks": checks,
                         "crew_hours": base_hours + checks * hours_per_check})
