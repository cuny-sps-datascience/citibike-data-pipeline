"""Data access for the map app.

On Databricks Apps: reads Unity Catalog tables through a SQL warehouse
(DATABRICKS_WAREHOUSE_ID comes from the app's `sql-warehouse` resource).
Locally: reads demo parquet files from app/demo_data/ if they exist.
"""

import os
from pathlib import Path

import pandas as pd

SCHEMA = os.getenv("CB_SCHEMA", "workspace.default")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
DEMO_DIR = Path(__file__).parent / "demo_data"
TZ = "America/New_York"
ON_DATABRICKS = bool(WAREHOUSE_ID)

PLAYBACK = f"{SCHEMA}.team1_playback_15"
LIVE = f"{SCHEMA}.team1_live_predictions_15"
MODEL_INFO = f"{SCHEMA}.team1_model_info_15"
INFO_TABLES = [f"{SCHEMA}.silver_station_info", f"{SCHEMA}.live_station_info"]
COLUMNS = ("station_id, prediction_time, status, p_bike, p_dock, num_bikes_available, "
           "num_docks_available")


def _query(sql: str) -> pd.DataFrame:
    from databricks import sql as dbsql
    from databricks.sdk.core import Config
    cfg = Config()
    with dbsql.connect(server_hostname=cfg.host,
                       http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
                       credentials_provider=lambda: cfg.authenticate) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall_arrow().to_pandas()


def _exists(table: str) -> bool:
    catalog, schema, name = table.split(".")
    df = _query(f"SHOW TABLES IN {catalog}.{schema} LIKE '{name}'")
    return not df.empty


def stations() -> pd.DataFrame:
    """Newest name, location, and capacity for every station."""
    if not ON_DATABRICKS:
        return pd.read_parquet(DEMO_DIR / "stations.parquet")
    sources = [t for t in INFO_TABLES if _exists(t)]
    union = " UNION ALL ".join(
        f"SELECT station_id, fetched_at, name, lat, lon, capacity FROM {t}" for t in sources)
    return _query(f"""
        SELECT station_id, name, lat, lon, capacity FROM (
          SELECT *, row_number() OVER (PARTITION BY station_id ORDER BY fetched_at DESC) AS rn
          FROM ({union})) WHERE rn = 1 AND lat IS NOT NULL AND lon IS NOT NULL""")


def model_info() -> pd.DataFrame:
    if not ON_DATABRICKS:
        return pd.read_parquet(DEMO_DIR / "model_info.parquet")
    return _query(f"SELECT * FROM {MODEL_INFO}")


def playback_dates() -> list:
    """New York dates in the playback table."""
    if not ON_DATABRICKS:
        df = _demo("playback")
        return sorted({str(d) for d in df["prediction_time"].dt.tz_convert(TZ).dt.date})
    df = _query(f"""SELECT DISTINCT to_date(from_utc_timestamp(prediction_time, '{TZ}')) AS d
                    FROM {PLAYBACK} ORDER BY d""")
    return [str(d) for d in df["d"]]


def predictions(mode: str, date: str | None = None, live_hours: int = 6) -> pd.DataFrame:
    """Playback: one New York day, with outcomes. Live: the last `live_hours`."""
    if not ON_DATABRICKS:
        df = _demo(mode)
        if mode == "playback":
            df = df[df["prediction_time"].dt.tz_convert(TZ).dt.date.astype(str) == date]
        else:
            df = df[df["prediction_time"] >= df["prediction_time"].max() - pd.Timedelta(hours=live_hours)]
        return df
    if mode == "playback":
        pd.Timestamp(date)                       # validate before it goes into SQL
        df = _query(f"""SELECT {COLUMNS}, bike_target, dock_target FROM {PLAYBACK}
                        WHERE to_date(from_utc_timestamp(prediction_time, '{TZ}')) = DATE'{date}'""")
    else:
        if not _exists(LIVE):
            return pd.DataFrame()
        df = _query(f"""SELECT {COLUMNS} FROM {LIVE}
                        WHERE prediction_time >= (SELECT max(prediction_time) FROM {LIVE})
                              - INTERVAL {int(live_hours)} HOURS""")
    df["prediction_time"] = pd.to_datetime(df["prediction_time"], utc=True)
    return df


def _demo(mode):
    df = pd.read_parquet(DEMO_DIR / f"{mode}.parquet")
    df["prediction_time"] = pd.to_datetime(df["prediction_time"], utc=True)
    return df
