# Databricks notebook source
# Team 3 reading copy with comments added (November 14, 2026). The code is unchanged from the shared pipeline original at the repository root, as saved in the workspace that day.
# This is a teammate-owned upstream notebook from the shared pipeline. Team 3 did not rerun it, and it does not affect the frozen model.
# The comments that were already here are the author's; the added ones follow the same top-to-bottom steps.
#
# This notebook creates the silver station info table by casting
# the strings in the bronze table into the correct types.

# The grain is that each row contains information about a station at a point in time
# Here is a data dictionary for the silver table:

# station_id                   : Station identifier (string).
# external_id                  : Additional identifier supplied by the feed (string).
# short_name                   : Short station code (string).
# name                         : Station name (string).
# region_id                    : Region identifier; may be missing (string).
# station_type                 : Station type reported by the feed (string).
# lat                          : Latitude in decimal degrees (double).
# lon                          : Longitude in decimal degrees (double).
# capacity                     : Reported capacity; zero is retained (integer).
# has_kiosk                    : Whether the station reports a kiosk (boolean).
# electric_bike_surcharge_waiver: Reported e-bike surcharge waiver (boolean).
# eightd_has_key_dispenser      : Reported key dispenser flag (boolean).
# eightd_station_services      : Array of service records, unchanged from bronze.
# rental_methods               : Array of supported rental methods.
# rental_uris                  : Struct containing Android and iOS rental links.

# fetched_at_raw               : Original collection timestamp (string).
# fetched_at                   : Collection time (timestamp, displayed in UTC).
# feed_ts                      : Original feed update time in Unix seconds (string).
# feed_updated_at              : Converted feed update time (timestamp).
# snapshot_date                : UTC collection date (date).
# poller_version               : Downloader version; missing in older files.
# git_sha                      : Downloader code reference; may contain manual_run.
# _source_file                 : Original raw archive file path.
# _rescued_data                : Data that did not fit the bronze reader's schema.
# _ingested_at                 : Bronze processing time (timestamp).
# _silver_processed_at         : Silver processing time (timestamp).

# The following quality flags are booleans. Flagged rows are retained.
# _has_parse_error             : A supplied value failed type conversion.
# _required_value_missing      : A required value is missing, 
# _has_negative_capacity       : Capacity is below zero.
# _has_invalid_coordinates     : A coordinate is NaN or outside geographic bounds.


# Several properties of the data were investigated in the bronze table, I am repeating them here:

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



# Imports
## Packages and run settings
from pyspark.sql import functions as F

# Determining the run mode (see the station_status notebooks)

# Run mode and locations (see pipeline_config.py)
# Dev reads scratch bronze, production reads production bronze

import pipeline_config as cfg

RUN_MODE = cfg.get_run_mode(dbutils)
OUTPUT_SCHEMA = cfg.output_schema(RUN_MODE)
SOURCE_TABLE = f"{OUTPUT_SCHEMA}.bronze_station_info"
TARGET_TABLE = f"{OUTPUT_SCHEMA}.silver_station_info"
CHECKPOINT_PATH = cfg.checkpoint_directory(RUN_MODE, "silver_station_info")

spark.conf.set("spark.sql.session.timeZone", "UTC")

cfg.describe(RUN_MODE=RUN_MODE, SOURCE_TABLE=SOURCE_TABLE,
             TARGET_TABLE=TARGET_TABLE, CHECKPOINT_PATH=CHECKPOINT_PATH)

# Reading the bronze table

## Read the bronze table as a stream (only rows not processed before)
bronze_df = spark.readStream.table(SOURCE_TABLE)

# These boolean columns are unlikely to be used at the moment

## Define the type conversions (text to integer, double, boolean, timestamp)
BOOLEAN_COLUMNS = [
    "has_kiosk",
    "electric_bike_surcharge_waiver",
    "eightd_has_key_dispenser",
]

# Define the conversions. 
# fetched_at, snapshot_date, and _ingested_at already have the appropriate types

type_conversions = {
    "capacity": F.expr("try_cast(capacity AS INT)"),
    "lat": F.expr("try_cast(lat AS DOUBLE)"),
    "lon": F.expr("try_cast(lon AS DOUBLE)"),
    "feed_updated_at": F.expr(
        "try_cast(try_cast(feed_ts AS BIGINT) AS TIMESTAMP)"
    ),
}

# Add a boolean conversion for each flag column.
for column_name in BOOLEAN_COLUMNS:
    type_conversions[column_name] = F.expr(
        f"try_cast(`{column_name}` AS BOOLEAN)"
    )

# We will check to see if the conversions worked and then add a flag if
# they didn't. This is similar to what we do in the station_status silver
# notebook

## Flag rows where a value was present but failed conversion (the row is kept, not dropped)
parse_failure_conditions = [
    F.col(column_name).isNotNull()
    & type_conversions[column_name].isNull()
    for column_name in ["capacity", "lat", "lon"] + BOOLEAN_COLUMNS
] + [
    F.col("feed_ts").isNotNull()
    & type_conversions["feed_updated_at"].isNull(),
]

# Apply the conversions and add the parse error flag.
silver_df = (
    bronze_df
    .withColumn(
        "_has_parse_error", # Adding the parse error column
        F.coalesce(
            F.array_contains(F.array(*parse_failure_conditions), True),
            F.lit(False),
        ),
    )
    .withColumns(type_conversions)
)

# We will verify that columns with station location information and identify 
# are present, as well as the time the data was acquired

## Data quality flags: required values, negative capacity, impossible coordinates
REQUIRED_COLUMNS = [
    "station_id",
    "name",
    "lat",
    "lon",
    "fetched_at",
    "feed_updated_at",
]

missing_value_conditions = [
    F.col(column_name).isNull()
    for column_name in REQUIRED_COLUMNS
]

# Check for valid coordinates. Have seen weird lat and lon in NYC datasets before so 
# perhaps not too paranoid

invalid_coordinate_conditions = (
    F.isnan("lat")
    | F.isnan("lon")
    | ~F.col("lat").between(-90, 90)
    | ~F.col("lon").between(-180, 180)
)

# Add the data quality check columns

# Add the quality flag columns and the silver processing time.
silver_df = silver_df.withColumns({
    "_required_value_missing": F.coalesce(
        F.array_contains(F.array(*missing_value_conditions), True),
        F.lit(False),
    ),
    "_has_negative_capacity": F.coalesce(
        F.col("capacity") < 0,
        F.lit(False),
    ),
    "_has_invalid_coordinates": F.coalesce(
        invalid_coordinate_conditions,
        F.lit(False),
    ),
    "_silver_processed_at": F.current_timestamp(),
})

# Now we write the silver table. We did not change 
# station names, identifiers, rental_methods, rental_uris, or station service
# information 
# We don't remove any bad observations we just make a note of it in the
# data quality variables.

## Write the silver table (append), process the rows available now, then stop
silver_stream = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

silver_stream.awaitTermination()
print(f"done -> {TARGET_TABLE}")