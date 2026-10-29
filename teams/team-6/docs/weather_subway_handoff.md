# Team 6 Citi Bike weather and subway datasets

[Download the five-table dataset ZIP (69.09 MiB)](https://github.com/cuny-sps-datascience/citibike-data-pipeline/releases/download/team-6-weather-subway-2026-10-23/citibike_weather_subway_handoff_20261023T174914038037Z.zip)

We prepared these datasets to test whether forecast weather and recent subway alerts help predict Citi Bike departures in the next three hours. The weather archive stores horizons 1–6, so we can keep the first three for our proposed models without rebuilding the weather data.

This is Team 6 work. We reused weather downloads and subway processing from Farhod's ride-demand project, but the Citi Bike station mapping, tables and results stay seperate from that capstone.

The feature tables are saved and their construction checks passed. They have not been joined to the departure labels yet. Station identifier compatibility, target-hour definitions and row preservation still need checking when we build the modelling dataset.

## Period we will use

Our agreed modelling period starts March 1, 2025 and ends August 10, 2026, using New York local dates. We shortened the proposed August test window because the prepared subway archive does not support later dates.

| Boundary | New York time | UTC |
| --- | --- | --- |
| First included local day | March 1, 2025, 00:00 | March 1, 2025, 05:00 |
| Exclusive endpoint | August 11, 2026, 00:00 | August 11, 2026, 04:00 |

The stored archives cover more than this period:

| Archive | First hourly origin UTC | Last hourly origin UTC |
| --- | --- | --- |
| Weather v2 | August 10, 2024, 07:00 | September 1, 2026, 03:00 |
| Subway origin features | July 1, 2024, 04:00 | August 11, 2026, 03:00 |

Weather targets extend six hours past the final weather origin. The small September UTC weather partition represents the final hours of August in New York; it does not mean we downloaded a full month of September forecasts.

We kept the extra weather rows rather than deleting useful forecasts. For modelling, we will filter to the common period and remove origins whose three target hours cross a split or the study endpoint. We will not fill unsupported subway dates with zeros.

## Saved tables

| Table | Grain | Saved rows | Purpose |
| --- | --- | ---: | --- |
| `workspace.citibike_weather.archive_v2_weather_classes` | Weather class × origin × horizon | 3,031,560 | Compact forecast values, timing and source metadata |
| `workspace.citibike_weather.archive_v2_station_weather_mapping` | Citi Bike station | 2,519 | Assigning stations to the existing weather request classes |
| `workspace.citibike_subway.subway_complex_bike_bridge_200m_v1` | Subway complex × Citi Bike dock pair | 331 | Linking subway geography to 324 distinct docks and 309 complexes |
| `workspace.citibike_subway.archive_v1_station_origin_alert_features` | Dock × forecast origin | 5,995,296 | Recent published alert information for forecasting |
| `workspace.citibike_subway.archive_v1_station_hour_disruption_retrospective` | Dock × UTC hour | 5,995,296 | Retrospective measured exposure and quality flags |

The weather station view is `workspace.citibike_weather.archive_v2_station_forecast`. It joins the class table to the station mapping on `weather_request_class` and `dataset_build_id`.

We kept the weather as two compact tables because exporting the whole station view would repeat the same forecasts across many stations. The view implies 272,732,130 station–origin–horizon rows. We checked that the final origin returns all 15,114 station–horizon rows. The original weather `archive_v1` was retained unchanged.

## Weather features and selection

Each station is assigned to the nearest of the 28 archived request coordinates. This is a geographic proxy; we did not request a separate forecast at every Citi Bike dock. The accepted mapping is within 10 km, with the largest distance about 8.10 km. August run class IDs and request coordinates matched the earlier silver mapping.

| Weather column | Unit |
| --- | --- |
| `forecast_temperature_f` | Degrees Fahrenheit |
| `forecast_precipitation_in` | Inches |
| `forecast_rain_in` | Inches |
| `forecast_snowfall_in` | Inches |
| `forecast_wind_speed_mph` | Miles per hour |

The keys are `forecast_origin_utc`, `forecast_horizon` and `weather_request_class`. Other useful fields include:

- `target_hour_utc` and `valid_time_utc`.
- `selected_model_run_utc`, `selected_run_available_utc` and `selected_run_age_hours`.
- `weather_lead_hours`, `selected_forecast_hours` and `is_fallback_weather_run`.
- `weather_forecast_has_missing`.
- Request coordinates, returned model-grid coordinates and representative location ID.
- `selected_weather_source`, `selected_raw_json_relative_path`, package hashes and build IDs.

The station mapping retains station coordinates, distance, the retrospective geography policy and snapshot cutoff. The snapshot cutoff used for this mapping was October 18, 2026, at 02:42:08.995720 UTC. It does not establish which stations existed or where they were located at every historical origin.

We used the same assumed seven-hour delay after model initialization. Selected weather must cover the target hour and have an assumed availability time no later than the origin. The July–August extension added 1,494 origins and 250,992 class-level rows, with no missing values across the five weather variables. The earlier archive still retains its original missing values and fallback flags.

The early ECMWF archive includes documented hindcasts. The delay is an availability assumption, not verified historical publication time. We should describe the evaluation as using this retrospective archive rather than claiming it exactly reconstructs every operational forecast available at the time.

## Subway mapping

The bridge selects the nearest Citi Bike dock for each subway station location and accepts that link only within 200 m. It does **not** include every dock within a 200 m radius.

The bridge contains 331 distinct complex–dock pairs, 309 distinct subway complexes and 324 distinct Citi Bike docks. One dock can link to more than one complex. All 496 subway stop-to-complex assignments matched between the references used in the completed comparison.

The station references are retrospective snapshots. Historical station openings, closures and relocations remain limitations. The saved bridge is an input to the cleaned notebook; its original construction was not included in the uploaded weather notebook.

## Subway forecasting features

The forecasting table uses publications strictly before the hourly origin. For each event and origin, We kept its latest publication if it was less than three hours old. We converted structured status labels into indicators, mapped the publication's affected routes to docks, and counted each event once per dock and origin.

There was no text model. We did not use retrospective episode attributes or retrospective quality masks as forecasting features.

| Feature | Meaning |
| --- | --- |
| `recent_event_count` | Distinct recent events linked through affected routes |
| `recent_reroute_event_count` | Events labelled reroute |
| `recent_suspended_event_count` | Events labelled suspended |
| `recent_part_suspended_event_count` | Events labelled part-suspended |
| `recent_stops_skipped_event_count` | Events labelled stops-skipped |
| `recent_severe_delays_event_count` | Events labelled severe-delays |
| `recent_delays_event_count` | Events labelled delays |
| `recent_cancellations_event_count` | Events labelled cancellations |
| `recent_express_to_local_event_count` | Events labelled express-to-local |
| `recent_boarding_change_event_count` | Events labelled boarding-change |
| `recent_information_outage_event_count` | Events labelled information-outage |
| `latest_publication_age_minutes` | Age of the latest linked publication |
| `latest_publication_time_utc` | Timestamp of the latest linked publication |
| `has_recent_route_publication` | Whether a recent publication was linked to this dock |
| `has_ambiguous_shuttle_link` | Whether a linked publication used an ambiguous shuttle symbol |
| `archive_publications_last_3h` | System-wide archive publications in the preceding three hours |
| `archive_silence_last_3h` | No system-wide archive publication in that window |

Additional columns retain source-package provenance, publication-window length, the assumed zero-minute ingestion delay and the geography policy.

Some interpretation matters here:

- A publication exactly at the origin is excluded.
- The three-hour window measures recent publications, not incident duration.
- Route association is not confirmation of lost subway service at that specific dock.
- 3,097,558 dock-origins have recent linked publications.
- 278,316 dock-origins have no system-wide publication in the preceding three hours. Event counts remain null for these rows.
- When archive activity exists but no publication matches the dock, event counts are zero. Zero means no recent matching publication, not confirmed normal service.
- Archive silence does not prove a feed outage.
- Stations outside the 324 mapped docks remain unmapped. We dont assign them zero disruptions automatically.

## Retrospective exposure

We kept measured exposure in a separate table. It has 5,409,222 usable dock-hours out of 5,995,296, or 90.22%.

It contains `lost_minutes`, `partial_lost_minutes`, `access_lost_minutes`, `diverted_minutes` and `background_loss_minutes`. Corresponding `recorded_` columns preserve the measurements before quality screening.

Quality fields include `exposure_usable`, `has_recorded_exposure`, `alert_gap`, `alert_coverage_missing`, `train_data_missing`, `feed_problem`, `feed_unjudged`, `feed_status_missing`, `feed_clock_ambiguous`, `unmeasured_episode` and `censored_alert_state`.

These values support retrospective analysis. Target-hour measured exposure is not information known at a forecast origin and must not be included as a forecasting predictor.

## Transfer package

The verified package is `citibike_weather_subway_handoff_20261023T174914038037Z.zip` (69.09 MiB). It includes the five compact tables above as Parquet, together with a manifest. It does not export the expanded station weather view, raw trip records or the full raw weather and subway archives.

| Package path | Contents |
| --- | --- |
| `tables/weather_classes/` | Weather v2 class rows |
| `tables/station_weather_mapping/` | Weather v2 station mapping |
| `tables/subway_bridge/` | Validated complex–dock pairs |
| `tables/subway_alert_features/` | Origin-based alert features |
| `tables/subway_retrospective/` | Retrospective exposure |
| `transfer_manifest.json` | Source Delta versions, keys, row counts, schemas, file sizes and SHA-256 hashes |

The completed ZIP was copied to the Databricks Volume and its SHA-256 matched the temporary local ZIP. Compare the following hash after downloading or uploading the package:

```text
d750b7cc991d28c638a5bbfd44c4435d3de1666dec6ca92ad826ecffdb47ff41
```

All five exported table row counts matched the counts listed above. The per-file hashes and exact source schemas are in the manifest. Parquet preserves the data for transfer; Delta transaction history is not included.

The ZIP is published in the [Team 6 dataset release](https://github.com/cuny-sps-datascience/citibike-data-pipeline/releases/tag/team-6-weather-subway-2026-10-23). GitHub reports the same SHA-256 as our verified Databricks export. We kept the ZIP as a release attachment and the notebook and README in the repository. We will use a new version for later data updates so we can identify which inputs were used for each result.

## Reading the exported data

After extracting the ZIP to an accessible folder, the tables can be read independently:

```python
from pyspark.sql import functions as F

# Reading the extracted handoff in the receiving workspace
spark.conf.set("spark.sql.session.timeZone", "UTC")
package_root = "/Volumes/<catalog>/<schema>/<volume>/<extracted_package>"

weather_classes = spark.read.parquet(f"{package_root}/tables/weather_classes")
station_mapping = spark.read.parquet(f"{package_root}/tables/station_weather_mapping")
subway_bridge = spark.read.parquet(f"{package_root}/tables/subway_bridge")
subway_alert_features = spark.read.parquet(f"{package_root}/tables/subway_alert_features")
subway_retrospective = spark.read.parquet(f"{package_root}/tables/subway_retrospective")

# Reconstructing station weather from the two compact components
m = station_mapping.alias("m")
w = weather_classes.alias("w")
station_weather = m.join(
    w,
    (F.col("m.weather_request_class") == F.col("w.weather_request_class"))
    & (F.col("m.dataset_build_id") == F.col("w.dataset_build_id")),
    "inner",
).select(
    "m.station_id", "m.station_name", "m.weather_mapping_distance_km", "w.*"
)
```

This example reconstructs weather with a few useful station fields. The full Databricks view also includes the station coordinates, snapshot and mapping-policy fields. The import example creates DataFrames; it does not create persistent Delta tables or replace existing data.

## Joining later

Weather will join to modelling examples on `station_id`, `forecast_origin_utc` and `forecast_horizon`. Subway alert features will join on `station_id` and `forecast_origin_utc`; the same origin information can be used across the three prediction horizons.

Before joining, we need to agree on the station IDs and exact target intervals, use the common coverage, check unique keys and confirm the joins preserve the number of modelling rows. The comparisons with and without subway features also need the same eligible stations and origins.

For this export, Spark Connect used `.write.mode("error")`. ZIP creation was done in temporary local storage and the finished file was streamed to the Volume; writing the ZIP directly on the Volume failed.

The cleaned notebook loads saved tables by default. Its optional build switches start as `False`, so running it normally does not repeat completed extraction or replace the saved tables. The retained cell outputs are records of earlier runs, not evidence that a skipped build just ran. In a different workspace, import the handoff first or provide the stated build inputs.

## Sources

- [Open-Meteo Single Runs API](https://open-meteo.com/en/docs/single-runs-api) and [data licence](https://open-meteo.com/en/licence).
- MTA historical alerts and the pinned station reference retained in the subway source package; [Subway Data NYC](https://subwaydata.nyc/).
- [Citi Bike GBFS station information](https://gbfs.citibikenyc.com/gbfs/en/station_information.json).

The prepared tables are derived project data. Reusing our code does not replace the attribution and licence requirements of the original sources.
