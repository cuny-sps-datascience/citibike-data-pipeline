# Databricks notebook source
# Team 3 reading copy with comments added (November 14, 2026). The code is unchanged from the shared pipeline original at the repository root, as saved in the workspace that day.
# This is a teammate-owned upstream notebook from the shared pipeline. Team 3 did not rerun it, and it does not affect the frozen model.
# The comments that were already here are the author's; the added ones follow the same top-to-bottom steps.
#
# So this code at the very top is a test to see how this notebook was triggered. The idea is to make the notebook
# usable as a production job that might be scheduled regularly and also something that could be testable in a development
# environment which doesn't impact any of our "official" production datasets.

# Here are our imports:
## Packages and run settings
from pyspark.sql import functions as F


# The way this works is we first check and see if the run has been triggered by a schduled job. If in the scheduled job, we include the
# parameter "PROD", the the dbutils.widgets.get will successfully return the value "production". If not, we'll get an exception
# and we'll set the RUN_MODE to "dev"

import pipeline_config as cfg # This is how we handle the directories now
RUN_MODE = cfg.get_run_mode(dbutils)

# If we are in "production mode", we'll set the path to the production data. Otherwise, we'll set the path to the dev data

SCHEMA = cfg.output_schema(RUN_MODE) # We have a separate schema for production and development data

# The "CHECKPOINT" directory is where we keep track of the fraction of the raw data that we have already processed. This checkpoint 
# records which raw files have been processed, which is part of how we maintain our record of the set of transformations that have been applied to our raw data in order to get to our final production datasets. If you want to read more
# about the "philosophy" of why we are doing it this way check out this article: https://www.linkedin.com/blog/engineering/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying



# Now that we have established the locations for the checkpoints and the raw directories, we are going to setup the autoloader
# which will automatically check both directories, and find the files in raw which are not mentioned in the log. A complex
# directory structure/file structure will be created here by the autoloader, so as to keep track of the files we processed and some of
# their associated structure and metadata. Read more here: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/

## Source folder, checkpoint folder and target table
SRC = cfg.RAW_STATION_FEEDS + "/" # This is the location of the raw archiveCKPT = f"{CHECKPOINT_ROOT}/bronze_station_status" # This is where the logs for the station status go, created by autoloader
CKPT = cfg.checkpoint_directory(RUN_MODE, "bronze_station_status") # This is where the logs for the station status go, created by autoloader
TABLE = f"{SCHEMA}.bronze_station_status"  # This is the processed data table. 

# Let's keep a record of what each notebook thought it was doing:

cfg.describe(RUN_MODE=RUN_MODE, SRC=SRC, CKPT=CKPT, TABLE=TABLE)

# I think the default time zone is UTC but just in case

spark.conf.set("spark.sql.session.timeZone", "UTC")

# Next step is to define the operations for reading the data


## Read the raw station status files with Auto Loader (streaming, only new files)
raw = (spark.readStream.format("cloudFiles") #  This tells spark to use the autoloader
       .option("cloudFiles.format","json") # Self explanatory but this will work with the gzipped json files
       .option("cloudFiles.schemaLocation", CKPT) # This is where we store information on the schema of the raw data, which will be learned
       .option("multiLine", "true") # This helps if the target json goes over one line
       .option("cloudFiles.inferColumnTypes", "true") # We want to keep vectors, but load everything else as a string
       .option("primitivesAsString", "true")
       .option("cloudFiles.schemaEvolutionMode","addNewColumns") # If the data format changes and a new column is spotted, we add it to the schema and then crash (so this job will need to be restarted automatically)
       .option("rescuedDataColumn", "_rescued_data") # Even though we are storing everything as a string, this is needed because the autoloader will recognize things like vectors as arrays of strings, so the schema can still change at this level
       .option("recursiveFileLookup", "true") # These two lines scrape the raw directory folders for all the jsons
       .option("pathGlobFilter", "status_*.json*")
       .option("cloudFiles.schemaHints", "version STRING, git_sha STRING") # Need this for the first run only but that is life
       .load(SRC) #  This is the part that does the actual loading of data, though remember this is executed lazily
       )

# Now we create the bronze table, which means mapping each station in a given json to a row of the bronze

## Build the bronze table: one row per station per poll, all values kept as text
bronze = (
    raw
    .select(
        F.col("fetched_at").alias("fetched_at_raw"), 
        F.expr("try_cast(fetched_at AS TIMESTAMP)").alias("fetched_at"), # This is our metadata so we are going to cast it to a timestamp
        F.col("version").alias("poller_version"), # Our metadata
        F.col("git_sha"), # Our metadata, the git version of the poller script
        F.col("feed_last_updated").alias("feed_ts"), # When citibike says this data was last updated. We don't cast because we didn't create it
        F.col("_metadata.file_path").alias("_source_file"), # This is the file path to the raw json this row came from. Leading "_" in name is to identify this data as spark generated metadata, i.e. this is a naming convention
        F.col("_rescued_data"), # If we couldn't process something it is be stored here as a json string
        F.explode_outer("payload.data.stations").alias("station"), # This is doing a lot of work, it is expanding the json "station" field, putting all the station data for a given station into an array, so that we have row per station and a "station" column which has array data of all the specific station info. Think of this as a unnest_longer from DATA 607
    )
    .select( # This is like the select in R though can also be used like mutate
        "fetched_at_raw",
        "fetched_at", # We keep both just in case
        "poller_version",
        "git_sha",
        "feed_ts",
        "station.*", # This is the unnest wider. The names of the array fields in the station tabs get turned into columns
        "_source_file",
        "_rescued_data"
    )
    .withColumn("snapshot_date",F.to_date("fetched_at")) # These are mutates, this is the citibike time
    .withColumn("_ingested_at",F.current_timestamp()) # This is the time we processed, i.e. now
)

# Now we are finally going to actually write the table

## Write the bronze table (append), process the files available now, then stop
query = (
    bronze.writeStream
    .format("delta") # We are using the Delta Lake databricks format
    .outputMode("append") # We are appending to the table that already exists
    .option("checkpointLocation", CKPT) # This is where the logs for what we processed go
    .option("mergeSchema", "true") # This is needed in case a new column appears in the data. Will retroactively add NULL to files that lack it though I believe this is implicit
    .trigger(availableNow=True) # This means we are only going to read the unprocessed files available at this moment of time. The 
    # alternative would be to just keep this job going and process things indefinitely
    .toTable(TABLE) # Name of table we are writing
)

query.awaitTermination() # This prevents the code execution from continuing until the above job (which you can think of as running in 
# the background), completes

print(f"done -> {TABLE}")