# Team 6 shared-repository migration

## Active location

Effective October 20, 2026, Team 6 development moved into the shared course repository:

- Repository: `https://github.com/cuny-sps-datascience/citibike-data-pipeline`
- Branch: `team-6`
- Project directory: `teams/team-6`
- Local project root: `/Users/randyhowk/Sites/CUNY/citibike-data-pipeline/teams/team-6`

Run the Team 6 CLI, scripts, tests and documentation builds from the project directory. Root-level files and notebooks belong to the shared pipeline; change them only when the result is meant for other teams as well. This boundary keeps Team 6 experiments and their locked Python environment independently reproducible while retaining direct access to common assets.

The former `cuny-sps-datascience/team6` repository is retained as a migration archive at commit `734af7a`. The former local checkout remains at `/Users/randyhowk/Sites/CUNY/CUNY622FinalProject`. Neither is the active working tree. The independent illustrative sample repository remains at `https://github.com/Vaene/CUNY622FinalProjectPublic`.

## What moved

The tracked Team 6 project moved intact: source code, locked environment, configuration, tests, scripts, notebooks, proposals, runbooks, validation receipts and generated presentation artifacts. Full source archives and processed/modeling datasets did not move. They remain at the configured RAID paths, so existing commands retain their data lineage and do not add bulk data to Git.

The October 11 consolidation record and manifest remain in `docs/` as historical evidence. Their old paths describe that completed migration and do not override this document.

## Shared data and code paths

The shared repository offers optional inputs that can extend Team 6 work without replacing its completed protocols:

- Shared Databricks tables and views use the `citibike_project.citibike` catalog/schema convention in the root notebooks.
- Public station exports are published under `s3://cuny-sps-citibike-archive/exports/station_status/`, with station metadata at `s3://cuny-sps-citibike-archive/exports/station_info.parquet`.
- Root notebooks provide examples for catalog setup, ingestion and checks that Team 6 can adapt behind its existing package and validation contracts.

The public S3 path was audited on October 20. It contained 59 daily station-status Parquet files from August 21 through October 18 (about 438 MB total). The October 18 file contained 725,472 unique station/time rows for 2,519 stations with a median five-minute interval. The station-information file contained 138,282 rows for 2,526 stations, but its newest `fetched_at` value was October 12; it therefore failed Team 6's 49-hour metadata-freshness requirement for later status dates. Recheck these mutable paths before each use and save a checksum, object listing, row/key audit and freshness result with any derived package.

Farhod's weather work remains available in shared-repository pull request 47. Treat code review and data delivery as separate steps: reviewed code can be adopted on this branch, while any uncommitted input data still requires a versioned, checksummed handoff.

## Adoption rules

Shared inputs enter Team 6 through a new immutable package or contribution rather than by overwriting a completed export. Record source object names, checksums, acquisition times, schema, duplicate-key checks and coverage. Preserve the existing chronological split, release-aware label policy, station eligibility, model routing and activation controls. A shared source can improve coverage or reduce duplicate collection work, but it does not by itself validate the station cohort or authorize deployment.

Use this sequence for future integration:

1. Audit the current shared objects and catalog permissions.
2. Write a narrow adapter that produces the existing Team 6 package contract.
3. Compare row counts, unique keys, timestamp coverage and freshness with the pinned Team 6 source.
4. Run the current package audit and model evaluation without changing active pointers or schedules.
5. Promote a source or model only after its dated validation evidence passes the existing gates.
