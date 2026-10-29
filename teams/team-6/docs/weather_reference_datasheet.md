# Manhattan weather-run reference datasheet

This public external dataset supports leakage-aware three-hour departure forecasting and is portable to the DATA 622 shared catalog. Randy Howk collected the original benchmark window on October 15, 2026 and extended it on October 16 (New York dates), using the [Open-Meteo Single Runs API](https://open-meteo.com/en/docs/single-runs-api). It is independent of all class Citi Bike tables. The private Free Edition import does not itself constitute publication to the class catalog.

## Composition and collection

The requested point is latitude 40.75, longitude −73.98. Returned model-grid coordinates are retained in every row. The model is pinned to `ecmwf_ifs`, with four UTC initialization cycles daily and two forecast days per request. The extended window is February 28, 2025 through September 30, 2026 inclusive: **2,311 of 2,320 initialization requests succeeded**, including all 120 September runs. The original frozen benchmark stops August 31 and retains its original **2,191 of 2,200 runs and 105,168 rows**. Extending the contribution does not replace the active model's weather artifact or feature package.

Nine initialization requests remain unavailable: August 4 18Z; August 5 00Z/06Z; August 6 00Z; August 7 18Z; August 8 00Z/12Z; August 9 00Z, all in 2025; and June 10 12Z in 2026. Existing runs can also contain null hourly values. No missing record or null value establishes dry weather.

The extended normalized table contains **110,928 rows**, one per initialization and valid time; the handoff manifest states its SHA-256. The final September runs also contain forecast valid times extending into October. Requests explicitly select UTC and wind metres per second. Raw request/response JSON, retrieval time, returned units, listing of failures and hashes remain in the processed RAID's `modeling_exports/three_hour/source_candidates/weather`. Previous and new acquisition manifests are preserved under `weather/manifests/<sha256>.json` before replacing the current manifest. The portable handoff carries the normalized table, its checksum and lineage hashes, import notebook and this datasheet. It excludes credentials and machine-specific source paths. Exact extension counts, package paths and cloud checks are recorded in the [September refresh](september_refresh.md).

| Field | Type / unit | Meaning |
|---|---|---|
| `initialization_utc` | UTC timestamp | UTC model reference time; **not** public release time |
| `valid_time_utc` | UTC timestamp | Timestamp for each forecast value |
| `assumed_available_at_utc` | UTC timestamp | Initialization plus seven hours; modeling assumption |
| `availability_verified` | Boolean | False for every row; actual historical public availability unverified |
| `cell_latitude`, `cell_longitude` | Decimal degrees | Returned weather grid coordinates |
| `temperature_2m` | °C, nullable | Instantaneous temperature at valid time |
| `wind_speed_10m` | m/s, nullable | Instantaneous wind at valid time |
| `precipitation` | mm, nullable | Accumulation over the preceding hour |
| `precipitation_interval_start_utc` | UTC timestamp | Valid time minus one hour |
| `precipitation_interval_end_utc` | UTC timestamp | Valid time |

For target `[10:00,11:00)`, use instantaneous temperature/wind at 10:00 and precipitation ending at 11:00, all from a run available before the forecast cutoff. A single reference point is a borough proxy, and does not supply station-specific microclimate estimates. Early archived ECMWF runs include hindcasts and model versions change across the window. This dataset supports retrospective model development with disclosed timing assumptions; it does not reconstruct exact historic inputs available to a deployed system.

## Use, attribution and transfer

Credit **Weather data by Open-Meteo.com, ECMWF IFS**, under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). These normalized fields and assumed availability timestamps are project transformations. Preserve null values, interval bounds, attribution and the historical-availability caveat when deriving features. Other collected datasets have separate terms: CityBikes station history is not included in this weather handoff.

Extract the ZIP, upload its files to an authorized class volume, import `import_weather.py` as a Databricks notebook, and set `handoff_root`, `catalog`, `schema` and a new `table` name. The notebook verifies checksum, count, unique keys and interval/availability consistency, refuses an existing table, writes Delta and adds a source/units/limitations comment. Confirm that another team can read the class table before marking the sharing requirement complete. No class data or class connection is required to prepare this package.
