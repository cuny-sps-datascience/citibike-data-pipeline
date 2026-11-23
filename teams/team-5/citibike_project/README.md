# Citi Bike Demand Project

This project contains a structured analysis template for Citi Bike demand and
downloaders for archiving station information and status from Citi Bike's
official [GBFS feed](https://gbfs.citibikenyc.com/gbfs/gbfs.json).

## Project files

- `citibike_demand_project_proposal.qmd`: project analysis template with the
  preliminary EDA under `Analysis > Exploratory data analysis`.
- `download_station_info.py`: downloads and archives the current station list.
- `download_station_status.py`: downloads current bike and dock availability.
- `initialize_weather_locations.py`: maps stations to HRRR weather cells.
- `refresh_live_data.py`: runs the complete live-data refresh workflow.

The `data/` directory is generated locally and intentionally excluded from
Git. It does not need to exist in a fresh checkout.

## Start from a clean checkout

Run the refresh command from the repository's parent directory:

```bash
python citibike_project/refresh_live_data.py
```

Or run it from inside `citibike_project`:

```bash
python refresh_live_data.py
```

The script creates the complete generated-data hierarchy automatically:

```text
data/
└── raw/
    ├── station_information/
    ├── station_status/
    └── weather_forecast/
        └── reference/
```

It then downloads current station information and status and creates the
station-to-weather-cell reference files. Because `data/` is listed in
`.gitignore`, these generated files will not be added to the repository by a
normal `git add` command. Run the refresh before rendering the report.

## Download station information

The project requires Python 3 plus `requests`, `pandas`, and `matplotlib`.
Install the packages if they are not already available:

```bash
python -m pip install requests pandas matplotlib
```

From inside `citibike_project`, run:

```bash
python download_station_info.py
```

The script first reads the GBFS root document, resolves the current
`station_information` URL, and downloads that feed. Resolving the URL at run
time avoids hard-coding a provider URL that may change.

## Archive layout

Each run creates one gzip-compressed JSON snapshot under UTC-based date
partitions:

```text
data/raw/station_information/
└── year=YYYY/
    └── month=MM/
        └── day=DD/
            └── info_YYYYMMDDTHHMMSSZ.json.gz
```

For example:

```text
data/raw/station_information/year=2026/month=11/day=03/info_20261103T005722Z.json.gz
```

The date and time use UTC, so a snapshot collected late in the evening in New
York may appear under the following day's partition.

## Snapshot contents

Each archive contains retrieval metadata and the original GBFS response:

```json
{
  "fetched_at": "2026-11-03T00:57:22.962497+00:00",
  "feed_last_updated": 1793667348,
  "version": "1.2",
  "git_sha": "manual_run",
  "payload": {}
}
```

The station records are stored at `payload.data.stations`. Common station
fields include `station_id`, `name`, `lat`, `lon`, and `capacity`.

## Read a snapshot with Python

```python
import gzip
import json
from pathlib import Path

snapshots = sorted(
    Path("citibike_project/data/raw/station_information").rglob("*.json.gz")
)

with gzip.open(snapshots[-1], "rt", encoding="utf-8") as file:
    archive = json.load(file)

stations = archive["payload"]["data"]["stations"]
print(f"Stations: {len(stations):,}")
print(stations[0])
```

If running this code from inside `citibike_project`, change the search path to
`data/raw/station_information`.

## Optional settings

Use `--output-root` to save snapshots elsewhere:

```bash
python citibike_project/download_station_info.py \
  --output-root /path/to/station_information
```

Use `--git-sha` to record the source revision responsible for an automated
run:

```bash
python citibike_project/download_station_info.py --git-sha abc1234
```

The script also reads the `GIT_SHA` environment variable. If neither method is
used, the archive records `manual_run`.

## Refresh all live data

The recommended command downloads both live Citi Bike feeds and verifies the
weather mapping in one run:

```bash
python citibike_project/refresh_live_data.py
```

No manual directory setup is required. If `data/` does not exist, the command
creates it and all required subdirectories before downloading anything.

The weather mapping is reused when it already covers every current station
coordinate. Use `--force-weather-map` to rebuild it intentionally, or
`--git-sha` to record a source revision in the new GBFS archives. Full usage
instructions also appear in the analysis report under `Data and study design
> Refreshing the live data`.

## Download station status

Run the status downloader after downloading station information:

```bash
python citibike_project/download_station_status.py
```

It accepts the same `--output-root` and `--git-sha` options as the station
information downloader. By default, it writes snapshots to:

```text
data/raw/station_status/year=YYYY/month=MM/day=DD/status_YYYYMMDDTHHMMSSZ.json.gz
```

Station status changes frequently. Run the script again whenever a new
point-in-time snapshot is needed. A scheduler can run it every five minutes
to build a time series like the Databricks reference poller.

## Initialize weather locations

After downloading station information, create the station-to-weather lookup:

```bash
python citibike_project/initialize_weather_locations.py
```

The initializer sends station coordinates to Open-Meteo's
[GFS/HRRR API](https://open-meteo.com/en/docs/gfs-api) in batches of 50 and
pauses between requests. It writes:

- `hrrr_locations.json`: the `gfs_hrrr` model settings and one representative
  request coordinate per weather cell.
- `station_weather_lookup.csv`: each station ID and coordinate mapped to its
  weather `location_id`.

The command protects existing reference files. Use `--overwrite` only when the
station list has changed and the mapping should be replaced:

```bash
python citibike_project/initialize_weather_locations.py --overwrite
```

The lookup supplies join keys for weather data but contains no temperatures,
precipitation, wind, or other forecast values by itself.

## Render the analysis report

Download at least one information snapshot and one status snapshot before
rendering the Quarto document. From inside `citibike_project`, run:

```bash
quarto render citibike_demand_project_proposal.qmd
```

