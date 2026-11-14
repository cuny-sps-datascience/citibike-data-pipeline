"""Settings for Team 1's 15-minute MVP. Every notebook in this folder reads them.

Builds on the professor's Free Edition pipeline (free_edition_models/):
  1-download-silver-FE   -> silver_station_status, silver_station_info
  3-gold-features-FE     -> gold_station_predictors  (we reuse it as-is)
  0-live-station-poller  -> live_station_status, live_station_info

Our tables start with team1_ so they never overwrite the professor's.
After editing this file, run dbutils.library.restartPython() so notebooks see the change.
"""

SCHEMA = "workspace.default"
LOCAL_TZ = "America/New_York"

# --- Prediction problem ------------------------------------------------------
HORIZON_MIN = 15                 # our proposal: empty/full within 15 minutes
MAX_OBSERVATION_GAP_MIN = 10     # same coverage rule as the professor's targets

# --- Professor's tables (inputs) --------------------------------------------
SILVER_STATUS = f"{SCHEMA}.silver_station_status"
SILVER_INFO = f"{SCHEMA}.silver_station_info"
LIVE_STATUS = f"{SCHEMA}.live_station_status"
LIVE_INFO = f"{SCHEMA}.live_station_info"
PREDICTORS = f"{SCHEMA}.gold_station_predictors"

# --- Our tables (outputs) ----------------------------------------------------
TARGETS = f"{SCHEMA}.team1_gold_targets_15"
SPLIT = f"{SCHEMA}.team1_split_15"
EVAL = f"{SCHEMA}.team1_model_eval_15"
CALIBRATION = f"{SCHEMA}.team1_calibration_15"
MODEL_INFO = f"{SCHEMA}.team1_model_info_15"
STATION_HISTORY = f"{SCHEMA}.team1_station_hour_rates_15"
PLAYBACK = f"{SCHEMA}.team1_playback_15"
LIVE_PREDICTIONS = f"{SCHEMA}.team1_live_predictions_15"

MODEL_NAMES = {"bike": f"{SCHEMA}.team1_bike_stockout_15",
               "dock": f"{SCHEMA}.team1_dock_stockout_15"}
MODEL_ALIAS = "champion"

# --- Chronological split ------------------------------------------------------
# Test = the last TEST_DAYS of labeled data; validation = the VAL_DAYS before it;
# training = everything earlier. Rows whose label window crosses a cutoff are dropped.
VAL_DAYS = 7
TEST_DAYS = 7

# --- Training sample (Free Edition memory) -----------------------------------
TRAIN_SAMPLE = 0.05      # share of labeled training rows pulled into pandas
EVAL_SAMPLE = 0.25       # share of validation/test rows used for metrics
SEED = 622
C_GRID = [0.03, 0.3, 3.0]   # logistic regression penalty strengths to try
CV_TRAIN_DAYS = 21          # TimeGapSplit: first training fold length
CV_VALID_DAYS = 7

# --- Station x hour history baseline -----------------------------------------
HISTORY_SMOOTHING = 20   # pseudo-observations pulling a station-hour rate toward the overall rate

# --- Map ----------------------------------------------------------------------
PLAYBACK_STEP_MIN = 15   # one map frame every 15 minutes
WATCH_FRACTION = 0.5     # "watch" when probability >= half the warning threshold
LIVE_HISTORY_MIN = 45    # status history the live features need
MAX_STALENESS_MIN = 10   # live stations older than this are skipped
