# Databricks notebook source
# Team 3 reading copy with comments added (November 14, 2026). The code is unchanged from the shared pipeline original at the repository root, as saved in the workspace that day.
# This is a teammate-owned upstream notebook from the shared pipeline. Team 3 did not rerun it, and it does not affect the frozen model.
# The comments that were already here are the author's; the added ones follow the same top-to-bottom steps.
#
# The purpose of this notebook is to create the station info bronze table.
# Station info is like slowly changing metadata (or dimensions).
# It contains info such as the station name, location, capacity, and services.
# This notebook creates the bronze table by processing the archived station info
# jsons, which we downloaded once every 24 hours. 

# In dimensional modeling parlance, the grain is that each row describes the information
# about one station at the point in time the data was downloaded.
# Some basic information about the variables in the bronze table:

# station_id                   : Station identifier; used to join station status.
# external_id                  : Additional station identifier supplied by the feed.
# short_name                   : Short station code.
# name                         : Station name.
# region_id                    : Region identifier; missing for some stations.
# station_type                 : Station type reported by the feed.
# lat                          : Latitude in decimal degrees.
# lon                          : Longitude in decimal degrees.
# capacity                     : Reported station capacity; zero is retained.
# has_kiosk                    : Whether the station reports having a kiosk.
# electric_bike_surcharge_waiver: Whether the feed reports an e-bike surcharge waiver.
# eightd_has_key_dispenser      : Feed flag for a key dispenser.
# eightd_station_services      : Nested service information, retained as supplied.
# rental_methods               : List of supported rental methods.
# rental_uris                  : Links for opening the station in rental apps.

# fetched_at_raw               : Original collection timestamp string.
# fetched_at                   : Collection timestamp, interpreted in UTC.
# feed_ts                      : Original feed update time in Unix seconds.
# feed_updated_at              : Feed update time converted to a timestamp.
# snapshot_date                : UTC date of collection.
# poller_version               : Downloader version; missing in older files.
# git_sha                      : Downloader code reference; may contain manual_run.
# _source_file                 : Raw archive file from which the observation came.
# _rescued_data                : Data that did not fit the bronze reader's schema.
# _ingested_at                 : Time the observation was processed into bronze.
# _silver_processed_at         : Time the observation was processed into silver.

# _has_parse_error             : A supplied value could not be converted.
# _required_value_missing      : A required value is missing, or ID/name is blank.
# _has_negative_capacity       : Reported capacity is below zero.
# _has_invalid_coordinates     : A coordinate is NaN or outside geographic bounds.

# I did a short investigation of the dataset and identified a few unusual issues, which I summarize here:
# 1. Missing region_id: 13 stations had no region in all 35 daily polls.
# Their locations are normal, and nearby stations have regions.
# We don't know why these values are missing, so we leave them missing. If it was very
# important these values could be imputed
#
# 2. Zero station capacity: usually associated with stations reporting disabled service.
# Some appear to be awaiting activation; others have temporary interruptions
# or remain inactive throughout the archive. A few report active service before
# the next daily information download updates their capacity.
# Positive capacity does not necessarily mean a station is operating.



##

## Packages and run settings
from pyspark.sql import functions as F
import pipeline_config as cfg

# This boilerplate is identical to our station status notebooks
# We have a dev mode and a production mode which write to different places
# Resolve dev or production mode, then the target table and checkpoint folder.
RUN_MODE = cfg.get_run_mode(dbutils)

IS_PRODUCTION = RUN_MODE == "production"
OUTPUT_SCHEMA = cfg.output_schema(RUN_MODE)

TARGET_TABLE = f"{OUTPUT_SCHEMA}.bronze_station_info"

# The info downloader currently puts its files in the station_status directory.
# So we must filter to get just the station info

SOURCE_PATH = cfg.RAW_STATION_FEEDS + "/"
CHECKPOINT_PATH = cfg.checkpoint_directory(RUN_MODE, "bronze_station_info")

# Keep all times in UTC.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Display the resolved settings.
cfg.describe(RUN_MODE=RUN_MODE, SOURCE_PATH=SOURCE_PATH,
               TARGET_TABLE=TARGET_TABLE, CHECKPOINT_PATH=CHECKPOINT_PATH)

# We now read the raw files again using the autoloader.
# We will save everything as a string


## Read the raw station information files with Auto Loader (streaming, only new files)
raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("primitivesAsString", "true")
    .option("inferTimestamp", "false")
    .option("cloudFiles.schemaHints", "version STRING, git_sha STRING") # This accomodates the fact that not all raw files have the git-sha/version
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .option("multiLine", "true")
    .option("recursiveFileLookup", "true") 
    .option("pathGlobFilter", "info_*.json*") # This is where we get station info
    .load(SOURCE_PATH)
)

# Each raw file contains an array of stations. We need to unnest this array
# to get all the station info
# explode_outer creates the station rows, each one is an object containing
# station info. If the array is empty or null,
# it retains one row containing the poll metadata and null station fields.
# station.* in the select unnests the station information wider, creating
# the columns

## Build the bronze table: one row per station per downloaded file
bronze_df = (
    raw_df
    .select(
        F.col("fetched_at").alias("fetched_at_raw"),
        F.expr("try_cast(fetched_at AS TIMESTAMP)").alias("fetched_at"),
        F.col("feed_last_updated").alias("feed_ts"),
        F.col("version").alias("poller_version"),
        F.col("git_sha"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_rescued_data"),
        F.explode_outer("payload.data.stations").alias("station"), # This step is like unnest longer, payload data is an array of stations
    )
    .select(
        "fetched_at_raw",
        "fetched_at",
        "feed_ts",
        "poller_version",
        "git_sha",
        "station.*", # This is like unnest wider
        "_source_file",
        "_rescued_data",
    )
    .withColumns({
        "snapshot_date": F.to_date("fetched_at"),
        "_ingested_at": F.current_timestamp(),
    })
)

# As before, the first run processes the archive and later runs pick up
# new files. This checkpoint is separate from the station status checkpoint.
# New source columns can be added to bronze through schema evolution.

## Write the bronze table (append), process the files available now, then stop
bronze_stream = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append") # We are adding to the archive
    .option("checkpointLocation", CHECKPOINT_PATH) # This is where the log is stored
    .option("mergeSchema", "true") # This allows for schema evolution
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

# Wait for the write to finish, then display a confirmation line.
bronze_stream.awaitTermination() # We have to wait for the processes to finish
print(f"done -> {TARGET_TABLE}")