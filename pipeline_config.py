"""Run mode and storage locations shared by the pipeline notebooks.

The purpose of this module is to help standardize the code that determines the
read and write paths at the beginning of each notebook. Typically, there will be
two ways to run each notebook, and they will change the base paths that the notebooks
use. The "dev" mode is the default mode for the notebook if it is run in interactive mode
The production mode is triggered by jobs, and will be activated if those jobs have a 
"run_mode" parameter with value "production"

If the job is in production, it will typically write to the citibike schema, and 
raed from the citibike volume.

If the job is dev, it will write to the scratch schema, though its read behavior may vary.

Usage at the top of a notebook (in the same folder as this file):
 
    import pipeline_config as cfg
 
    RUN_MODE = cfg.get_run_mode(dbutils)
    OUTPUT_SCHEMA = cfg.output_schema(RUN_MODE)
    SOURCE_TABLE = f"{OUTPUT_SCHEMA}.bronze_station_info"
    TARGET_TABLE = f"{OUTPUT_SCHEMA}.silver_station_info"
    CHECKPOINT_PATH = cfg.checkpoint_directory(RUN_MODE, "silver_station_info")

Jobs pass run_mode="production"; interactive runs default to "dev", which writes
to the scratch schema and _dev folders. If you are actively editing this file
while running interactive notebooks, you will need to run 
(dbutils.library.restartPython()) so notebooks pick up the change.
"""

PRODUCTION = "production"
RUN_MODES = ("dev", PRODUCTION)
 
CATALOG = "citibike_project"
PRODUCTION_SCHEMA = f"{CATALOG}.citibike"
DEV_SCHEMA = f"{CATALOG}.scratch"
 


# Raw GBFS files. Station status and station information share this folder for
# because initial decisions. The bronze notebooks distinguish them by file name.


RAW_STATION_FEEDS = f"/Volumes/{CATALOG}/citibike/raw/station_status"
 
 
def get_run_mode(dbutils, default="dev"):
    """The run_mode job parameter or widget, else the default. Rejects typos."""
    try:
        mode = dbutils.widgets.get("run_mode")
    except Exception:
        mode = default
    if mode not in RUN_MODES:
        raise ValueError(f"run_mode must be one of {RUN_MODES}, got {mode!r}")
    return mode
 
 
 # Again just being pedantic so no one ever misspells production probably this is silly
 # in a world with LLMs etc

def output_schema(mode):
    """Schema this run writes to: citibike in production, scratch in dev."""
    return PRODUCTION_SCHEMA if mode == PRODUCTION else DEV_SCHEMA
 
 
def table(mode, name):
    """Full name of a table this run writes."""
    return f"{output_schema(mode)}.{name}"



def volume_path(mode, volume, *parts):
    """Path inside a volume of this run's schema, e.g. volume_path(mode, "raw", "weather_forecast")."""
    catalog, schema_name = output_schema(mode).split(".")
    return "/".join([f"/Volumes/{catalog}/{schema_name}/{volume}", *parts])
 
 
def checkpoint_directory(mode, name):
    """Streaming checkpoint for one table."""
    return volume_path(mode, "checkpoints", name)
 
 
def raw_directory(mode, name):
    """File archive for one dataset, e.g. raw_directory(mode, "weather_forecast")."""
    return volume_path(mode, "raw", name)
 
 
def describe(**settings):
    """Print the resolved settings at the top of a run, for the job log."""
    width = max(len(name) for name in settings)
    for name, value in settings.items():
        print(f"{name:<{width}} = {value}")
 