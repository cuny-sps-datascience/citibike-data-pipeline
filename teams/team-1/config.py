"""
Single source of configuration for the Citi Bike availability pipeline.

Every notebook imports this module. Change values here, never inside notebooks.
Record every non-default choice (and why) in ASSUMPTIONS.md.
"""

# ---------------------------------------------------------------------------
# Unity Catalog location (Free Edition default catalog is `workspace`)
# ---------------------------------------------------------------------------
import os

CATALOG = "workspace"
SCHEMA = "citibike"                      # project schema; created by 00_setup
VOLUME = "raw"                           # UC volume for raw GBFS JSON (bronze)

# ---------------------------------------------------------------------------
# Scope: which stations the modelling notebooks (02, 03, 06) work on.
#   "mvp"  -> the original 100-station MVP.  Outputs go to SCHEMA (the live system).
#   "full" -> every station with enough history that is in the live feed.
#             Outputs go to OUT_SCHEMA = citibike_full, with their own models,
#             so the live jobs (01, 04, 05) are never touched by a full-network run.
# Notebooks set it with a `scope` widget (env var CITIBIKE_SCOPE) BEFORE importing
# this module.  Jobs never set it, so they always run as "mvp".
# ---------------------------------------------------------------------------
SCOPE = os.environ.get("CITIBIKE_SCOPE", "mvp").strip().lower()
if SCOPE not in ("mvp", "full"):
    raise ValueError(f"CITIBIKE_SCOPE must be 'mvp' or 'full', got {SCOPE!r}")
OUT_SCHEMA = SCHEMA if SCOPE == "mvp" else f"{SCHEMA}_full"

def fq(name: str) -> str:
    """Fully qualified name in the project schema (inputs, live collector, production)."""
    return f"{CATALOG}.{SCHEMA}.{name}"

def fq_out(name: str) -> str:
    """Fully qualified name for modelling outputs of the current scope."""
    return f"{CATALOG}.{OUT_SCHEMA}.{name}"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# ---------------------------------------------------------------------------
# Professor's silver tables (historical source).  Set to where 1-download-silver-FE
# wrote them in YOUR workspace.  00_setup prints their schemas so you can verify
# the column map below.
# ---------------------------------------------------------------------------
SOURCE_STATUS_TABLE = "workspace.default.silver_station_status"
SOURCE_INFO_TABLE = "workspace.default.silver_station_info"

# Map from the standard column names this pipeline uses -> column names in the
# professor's silver_station_status.  `None` = column not present (filled with
# a neutral default and flagged).  00_setup validates this map and fails loudly.
STATUS_COLMAP = {
    "station_id": "station_id",
    "snapshot_ts": "feed_updated_at",        # feed's last_updated (timestamp) -- same meaning as the live collector
    "last_reported": "station_reported_at",  # parsed timestamp; the raw `last_reported` column is a string
    "num_bikes_available": "num_bikes_available",
    "num_ebikes_available": "num_ebikes_available",
    "num_docks_available": "num_docks_available",
    "num_bikes_disabled": "num_bikes_disabled",
    "num_docks_disabled": "num_docks_disabled",
    "is_installed": "is_installed",
    "is_renting": "is_renting",
    "is_returning": "is_returning",
}

# SQL filter applied to the professor's status table before anything else.
# Drops rows the professor's pipeline already flagged as bad. Columns that don't
# exist are skipped automatically (00_setup prints which flags it used).
SOURCE_STATUS_QUALITY_FLAGS = [          # rows flagged TRUE are dropped
    "_has_parse_error",
    "_required_value_missing",
    "_has_negative_count",
    "_ebikes_exceed_total",
]
# Rows flagged TRUE keep their counts, but last_reported is set to NULL
# (the station sent a dummy report time; counts are still valid).
SOURCE_LAST_REPORTED_PLACEHOLDER_FLAG = "_last_reported_placeholder"

INFO_COLMAP = {
    "station_id": "station_id",
    "name": "name",
    "capacity": "capacity",
    "lat": "lat",
    "lon": "lon",
}

# ---------------------------------------------------------------------------
# Live GBFS feed.  Free Edition restricts outbound domains; 01_live_collector
# tries each host in order and records which one worked.
# ---------------------------------------------------------------------------
GBFS_STATUS_URLS = [
    "https://gbfs.citibikenyc.com/gbfs/en/station_status.json",
    "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json",
]
GBFS_INFO_URLS = [
    "https://gbfs.citibikenyc.com/gbfs/en/station_information.json",
    "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json",
]
HTTP_TIMEOUT_S = 20
HTTP_RETRIES = 3
MIN_STATIONS_EXPECTED = 1000             # sanity floor for a valid payload
SAVE_RAW_JSON = True                     # keep bronze copies for replay/audit

# ---------------------------------------------------------------------------
# Prediction problem (proposal §1, §3)
# ---------------------------------------------------------------------------
LOCAL_TZ = "America/New_York"
GRID_MIN = 15                            # prediction times every 15 min (UTC :00/:15/:30/:45)
HORIZON_MIN = 15                         # predict state at t + 15 min
ASOF_TOLERANCE_MIN = 10                  # a state "at T" = latest snapshot in (T - tol, T]
LAG_STEPS = [1, 2, 4]                    # lags in GRID_MIN units -> t-15, t-30, t-60

# Station selection (proposal §6: 50–100 stations)
N_STATIONS = 100                         # mvp scope only; full scope keeps every eligible station
MIN_COVERAGE = 0.90                      # share of grid times with a fresh snapshot
SELECTION_SEED = 622                     # deterministic tie-break / random half

# Historical window used for labels/features.  None = use everything available.
HIST_START_UTC = None                    # e.g. "2026-08-01"
HIST_END_UTC = None                      # e.g. "2026-11-01"

# Chronological split.  If cutoffs are None, 03_train computes them from the data:
# last TEST_DAYS -> test, the VAL_DAYS before that -> validation, the rest -> train.
VAL_START_UTC = None
TEST_START_UTC = None
VAL_DAYS = 7
TEST_DAYS = 7
EMBARGO_MIN = 60                         # gap between splits so lag/label windows don't leak

# Training
TRAIN_MAX_ROWS = 1_500_000               # cap train rows pulled to pandas (serverless memory)
PRIOR_SMOOTHING_M = 20                   # Bayesian smoothing for station x hour priors
PRIOR_OOF_FOLD_DAYS = 7                  # training rows get out-of-fold priors from other 7-day blocks
RANDOM_STATE = 622

# Evaluation rows pulled to pandas per split (validation, test).  None = every row.
# Sampling keeps or drops whole station-days by hash, so it is deterministic and
# keeps empty/full episodes (and their onsets) intact.  The full network has about
# 1.3M rows per week; the cap is a memory guard for serverless, not a design choice.
EVAL_MAX_ROWS = None if SCOPE == "mvp" else 1_500_000

# Full scope: event-rate tiers used as strata in 06 (share of eligible stations per tier).
FULL_TIERS = 3                           # tertiles: low / mid / high event rate

# Weather:

WEATHER_FORECAST_TABLE = "workspace.default.weather_forecast_history"
WEATHER_MAPPING_TABLE = "workspace.default.station_forecast_mapping"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
TARGETS = ["empty", "full"]
MODEL_NAMES = {t: fq_out(f"citibike_{t}_15m") for t in TARGETS}
MODEL_ALIAS = "champion"
EXPERIMENT_NAME = "citibike_availability_15m" + ("" if SCOPE == "mvp" else "_full")   # under the user's home folder

# ---------------------------------------------------------------------------
# Monitoring thresholds
# ---------------------------------------------------------------------------
MAX_SNAPSHOT_AGE_MIN = 25                # alert if newest live snapshot is older
MIN_LIVE_STATIONS = 1000
MAX_PREDICTION_AGE_MIN = 30

# ---------------------------------------------------------------------------
# Table names
# ---------------------------------------------------------------------------
T = {
    "live_status": fq("live_station_status"),
    "live_info": fq("live_station_info"),
    "collector_runs": fq("collector_runs"),
    "std_status_hist": fq("std_station_status_hist"),
    "std_info": fq("std_station_info"),
    "mvp_stations": fq_out("mvp_stations"),
    "station_quality": fq_out("station_quality"),
    "features_labels": fq_out("features_labels_15m"),
    "split_config": fq_out("split_config"),
    "station_priors": fq_out("station_priors"),
    "test_predictions": fq_out("test_predictions"),
    "model_comparison": fq_out("model_comparison"),
    "calibration_bins": fq_out("calibration_bins"),
    "predictions": fq("predictions"),
    "live_outcomes": fq("live_outcomes"),
    "monitoring_log": fq("monitoring_log"),
    # full scope only: every station's features before selection, and step runtimes
    "features_labels_all": fq_out("features_labels_15m_all"),
    "run_timings": fq_out("run_timings"),
}
