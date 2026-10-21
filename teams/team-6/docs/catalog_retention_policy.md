# Unity Catalog retention policy

`workspace.citibike_3h` keeps one managed Delta table for the current version of each imported logical dataset. The table has a stable name, such as `borough_actual_hourly` or `external_weather_reference`, and every row has a non-null `dataset_version` that identifies its immutable source package.

Future imports verify the package manifest and checksums, add `dataset_version`, and replace the stable table with `mode("overwrite")` and schema overwrite enabled. An import therefore changes the contents of an existing catalog object instead of creating another timestamped table and view. Consumers keep using the same table name. Operational event tables, publication logs, health histories, and raw prospective evidence remain append-only because their rows are the history.

The catalog is the current query layer, not the long-term archive. Every imported package remains immutable in its versioned Unity Catalog volume directory. The originating package, raw downloads, and processed outputs remain on the RAID under the paths in `config/local.yaml`. Reproducing or rolling back a catalog table means reimporting one of those immutable packages; no old version depends on a timestamped catalog table.

The one-time migration in `notebooks/23_migrate_stable_catalog.py` converts existing view/version-table pairs to stable managed tables without copying their data. It renames each current managed table, adds and fills `dataset_version`, validates row counts, and removes only the redundant pass-through view name. The two standalone timestamped tables are renamed in the same way. Its report is written under `/Volumes/workspace/citibike_3h/exports/catalog_migrations/<migration-version>/report.json`.

Use these checks after a future import:

```sql
SELECT dataset_version, count(*)
FROM workspace.citibike_3h.borough_actual_hourly
GROUP BY dataset_version;

DESCRIBE DETAIL workspace.citibike_3h.borough_actual_hourly;
```

The first query must return exactly one non-null current version for a replacement dataset. The immutable volume package and RAID copy must both exist before an obsolete catalog object is removed. Do not delete append-only operational tables through this policy.
