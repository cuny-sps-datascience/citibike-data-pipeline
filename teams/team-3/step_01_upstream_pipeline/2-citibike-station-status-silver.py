# Databricks notebook source
# Team 3 reading copy with comments added (November 14, 2026). The code is unchanged from the shared pipeline original at the repository root, as saved in the workspace that day.
# This is a teammate-owned upstream notebook from the shared pipeline. Team 3 did not rerun it, and it does not affect the frozen model.
# The comments that were already here are the author's; the added ones follow the same top-to-bottom steps.
#
# The purpose of this notebook is to process the bronze table of station status, which contains strings for the fields, into a silver station status notebook. This notebook could theoretically be the basis of multiple gold tables specialized for a variety of
# different types of models and analyses
# This goal of the silver notebook is to:
# 1. One station per timestamp per row with no duplicates. We throw an error if this doesn't occur
# 2. All data fields are assigned an appropriate and consistent type
# 3. Missing and invalid data is flagged, consistent markers for missing/invalid data are used that don't have other meaning
# 4. Duplicates are not expected (Autoloader ingests each raw file once). If one appears, the check at the end stops the job rather than removing or flagging rows.
# 5. Keep information about the data origin/provenance

# The feature engineering/further processing will take place from this dataset to create the gold one(s)


# For those who are into "Kimball Dimensional Modeling" we could declare a "grain" for this table. The "grain" is the what is represented
# by a row in the table. Here the grain is the status of a single station in a single poll

# Overall, the dataset didn't require much cleaning and modification at least in my initial study of it, the only problem that was found was
# that inactive stations had a timestamp of 86400, which we will recode as missing

# AT the end, we will have a "silver table" with these variables

# SILVER DATA DICTIONARY
# One row per station per poll.
#
# station_id                  Primary station identifier
# legacy_id                   Older station identifier
# num_bikes_available         Available bikes, including e-bikes
# num_ebikes_available        Available e-bikes
# num_bikes_disabled          Disabled bikes, all types combined
# num_docks_available         Docks available for returns
# num_docks_disabled          Disabled docks
# num_scooters_available      Available scooters
# num_scooters_unavailable    Unavailable scooters
# is_installed                Station is physically installed
# is_renting                  Station permits rentals
# is_returning                Station permits returns
# eightd_has_available_keys   Vendor flag; apparently physical key availability
#
# fetched_at_raw              Original collection timestamp string
# fetched_at                  Collection timestamp
# feed_ts                     Original feed-update Unix seconds string
# feed_updated_at             Parsed feed-update timestamp
# last_reported               Original station-report Unix seconds string
# station_reported_at         Parsed station-report timestamp; null for 86400
# snapshot_date               UTC collection date
#
# poller_version              Downloader software version
# git_sha                     Downloader code-version marker
# _source_file                Source archive file path
# _rescued_data               Data that did not fit bronze's input schema
# _ingested_at                Bronze processing timestamp
# _silver_processed_at        Silver processing timestamp
#
# _has_parse_error            At least one type conversion failed
# _last_reported_placeholder  Original report time was 86400
# _required_value_missing     Required value is null or station ID is blank
# _has_negative_count         At least one count is negative
# _ebikes_exceed_total        Available e-bikes exceed available total bikes
#

# Imports, run_mode, and paths

## Packages and run settings
from pyspark.sql import functions as F

# This is the same idea as the code we use to process the raw json into bronze. We differentiate the behavior and the paths
# based on if the run is triggered from a scheduled job or if it is run interactively like it is here
# "dev" is the "interactive" mode
import pipeline_config as cfg
RUN_MODE = cfg.get_run_mode(dbutils)

# We read the production bronze by default
SOURCE_TABLE = "citibike_project.citibike.bronze_station_status"

# Deciding where we will put the output, in the main project for production
# or scratch otherwise

OUTPUT_SCHEMA = cfg.output_schema(RUN_MODE)

# This is the target that we will write at the end

TARGET_TABLE = f"{OUTPUT_SCHEMA}.silver_station_status"

# This is where we store the checkpoints. As usual the spark's streaming capabilities saves us
# by preventing us from redoing things

CHECKPOINT_PATH = cfg.checkpoint_directory(RUN_MODE, "silver_station_status")

# UTC is the databricks default (I think....) but not the Spark default so just set it

spark.conf.set("spark.sql.session.timeZone", "UTC")

# Just keeping a record of what paths were used for each run

cfg.describe(RUN_MODE=RUN_MODE, SOURCE_TABLE=SOURCE_TABLE,
               TARGET_TABLE=TARGET_TABLE, CHECKPOINT_PATH=CHECKPOINT_PATH)

# We begin reading the bronze table

## Read the bronze table as a stream
bronze_df = spark.readStream.table(SOURCE_TABLE)

# Our bronze dataset is divided into columns of different types.

# station_id and legacy_id are strings containing identifiers for each station
# num_bikes_available, num_bikes_disabled, num_docks_available, num_docks_disabled, num_ebikes_available
# num_scooters_available, and num_scooters_unavailable are cout variables at each time stamp. These will be
# converted into integers.

# There are some station status flags, which describe different aspects of the station_status, including
# is_installed, is_renting, is_returning, and "eightd_has_available_keys"

# There are also some rows with time information and origin information, such as fetched_at_raw, fetched_at, feed_ts, last_reported,
# snapshot_date, and _ingested_at with info variables like poller_version, git-sha, _source_file, and _rescued_data

# We define the following columns together to code as integers

## Column groups used by the conversions and quality checks
COUNT_COLUMNS = [
    "num_bikes_available", # this is total bikes
    "num_bikes_disabled", # this is total disabled bikes
    "num_docks_available", #number of functioning docks
    "num_docks_disabled", # the number of docks that are non-functional
    "num_ebikes_available", # total bikes includes ebikes
    "num_scooters_available", # scooters aren't being rented at the moment
    "num_scooters_unavailable", # same a useless variable for the moment
]

# These flag columns are boolean variables that describe the status of individual stations
# is_installed means that the station has been put in place physically
# is_renting means that the station is currently renting bikes
# is_returning means that the station is currently accepting returns
# I'm not sure what eightd_has_available_keys. It seems to be false in all our data.
# I think it refers to scooter keys and is not applicable. We keep it here in the silver
# because this is meant to be a complete record.

# These are the columns that we will use to determine if the station is operating or not.

BOOLEAN_COLUMNS = [
    "is_installed",
    "is_renting",
    "is_returning",
    "eightd_has_available_keys",
]

# We are going to be tracking data quality. We don't want to care as much about scooter data quality right now
# So we have a separate list of columns for which we will take missing values more seriously.

REQUIRED_COUNT_COLUMNS = [
    "num_bikes_available",
    "num_bikes_disabled",
    "num_docks_available",
    "num_docks_disabled",
    "num_ebikes_available",
]

# Again for now we are not going to worry about the mysterious 'eightd' column having missing data.

OPERATING_FLAG_COLUMNS = [
    "is_installed",
    "is_renting",
    "is_returning",
]


# Now it is time for use to do the type conversion

# We loop over the count columns and turn them into integers. Just note that this is spark/lazy
# this is actually just a list of spark column expressions that will get evaluated later

## Define the type conversions
type_conversions = {
    column_name: F.expr(f"try_cast(`{column_name}` AS INT)")
    for column_name in COUNT_COLUMNS
}

# Next we go through the boolean columns. try_cast in spark will
# attempt to look for the most typical strings that represent boolean
# values. In our data frame so far "1" and "0" are how booleans are encoded,
# except for the eightd keys variable which was false
# Again this is just building up the type conversion object 

for column_name in BOOLEAN_COLUMNS:
    type_conversions[column_name] = F.expr(
        f"try_cast(`{column_name}` AS BOOLEAN)"
    )

# These two timestamps need to be converetd. Others timestamps which we calcualted during processing steps
# begin life and remained life as times. We have to go to int first

type_conversions.update({
    "feed_updated_at":
        F.expr("try_cast(try_cast(feed_ts AS BIGINT) AS TIMESTAMP)"),
    "station_reported_at":
        F.expr("try_cast(try_cast(last_reported AS BIGINT) AS TIMESTAMP)"),
})

# Here we build a list of spark conditions. This just checks and see if the conversion process introduced a null
# value, i.e. if we started with a NotNull and ended with Null, in the columns that we converted. Again this is
# just a list of expressions nothing has been calculated yet

## Flag rows where a value was present but failed conversion
parse_failure_conditions = [
    F.col(column_name).isNotNull() & type_conversions[column_name].isNull()
    for column_name in COUNT_COLUMNS + BOOLEAN_COLUMNS
] + [
    F.col("feed_ts").isNotNull()
    & type_conversions["feed_updated_at"].isNull(),

    F.col("last_reported").isNotNull()
    & type_conversions["station_reported_at"].isNull(),
]

# Ok, now we apply our conversions and our condition. The .withColumns(type_conversions) applies our formulas,
# and the .with(column("has_parse_error")) evaluates our failure conditions (notice how the parse_failure_conditions contains
# the outcome of our type conversion as part of its definition). If there is a failure we replace that failed conversion row with a Null, but
# also add a _has_parse_error flag

# Apply the conversions and add the parse error flag.
silver_df = (
    bronze_df
    .withColumn(
        "_has_parse_error",
        F.coalesce(
            F.array_contains(F.array(*parse_failure_conditions), True),
            F.lit(False),
        ),
    )
    .withColumns(type_conversions)
)

# Now we handle the one weird missing value that we found in our initial exploration. Offline stations had a last_reported value of 86400. We are going to assume that this is a placeholder value and replace it with a Null

## Recode the 86400 placeholder report time (offline stations) as missing, and flag it
is_placeholder_report_time = F.coalesce(
    F.expr("try_cast(last_reported AS BIGINT)") == 86400,
    F.lit(False),
)

# We apply the replacement of 86400 with Null

silver_df = silver_df.withColumns({
    "_last_reported_placeholder": is_placeholder_report_time,
    "station_reported_at": (
        F.when(is_placeholder_report_time, F.lit(None).cast("timestamp"))
        .otherwise(F.col("station_reported_at"))
    ),
})


# Now we are going to do some data quality checks. We define a subset of columns (that are not related to scooters, do not fear scooters will be 
# banished from the gold level data) that we will be checking for missing values

## Data quality flags
required_columns = (
    REQUIRED_COUNT_COLUMNS
    + OPERATING_FLAG_COLUMNS
    + ["fetched_at", "feed_updated_at", "station_id"]
)

missing_value_conditions = [
    F.col(column_name).isNull()
    for column_name in required_columns
] + [
    F.trim(F.col("station_id")) == "",
]


# Now we put in a few data quality checks. It is hard to know which to use because actually the data is already of quite high quality

# Add the quality flag columns and the silver processing time.
silver_df = silver_df.withColumns({
    "_required_value_missing": F.coalesce(
        F.array_contains(F.array(*missing_value_conditions), True),
        F.lit(False),
    ),
    "_has_negative_count": F.coalesce(
        F.exists(
            F.array(*COUNT_COLUMNS),
            lambda count_value: count_value < 0,
        ),
        F.lit(False),
    ),
    "_ebikes_exceed_total": F.coalesce(
        F.col("num_ebikes_available") > F.col("num_bikes_available"),
        F.lit(False),
    ),
    "_silver_processed_at": F.current_timestamp(),
})

# Now we write the data

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


## Confirm no duplicate (station, poll) rows in the last two hours of the silver table
# This is just verifying that the silver table contains no duplicate rows. This line turned into a source of a large read of the table that is possibly unnecessary now that the cadence of updating silver increased. So I'm reducing this in an effort to cut costs

assert spark.table(TARGET_TABLE).filter("fetched_at >= current_timestamp() - INTERVAL 2 HOURS") \
    .groupBy("station_id", "fetched_at").count().filter("count > 1").isEmpty(), \
    f"Duplicate station/poll keys in {TARGET_TABLE}"

# COMMAND ----------

