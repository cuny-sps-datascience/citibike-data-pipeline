# Team weather handoff

1. Import both notebooks into the same Databricks folder. Run the station poller if `live_station_info` is absent, then run `11_collect_area_forecasts` and schedule it hourly. Each account creates its own data; GitHub does not share saved forecasts.
2. In a separate cell in the calling notebook, use `%run ./12_join_station_weather`. Then call `bike_features = add_weather(bike_features)` before selecting model columns. The collector and join use `workspace.default`.
3. Leave current model feature lists unchanged until matching training data are available. Join weather during both training and live scoring before fitting weather-enabled models. Historical backfill remains separate.

The collector replaces current location/mapping tables and appends forecast snapshots. The join preserves original feature columns for unique station/time keys and leaves unavailable weather null. Its horizon is 15 minutes, with hourly weather approximated by the target's containing hour. See notebook 12 for a short runnable example.

Checks: notebook format and Python syntax validated locally. Collector tested with simulated responses; join checked with local fixtures. The cleaned notebooks still require a Databricks run; no live tables, schedules, model files, or class repository were changed here.
