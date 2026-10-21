# Shared station source audit

## Decision

The shared public S3 station export is accepted as a separately versioned candidate source. It is suitable for expanding a future availability study after the existing Team 6 station-eligibility and metadata-freshness rules pass. It does not replace the completed ctbk.dev packages, establish continuous station operation, or change an active model.

The first read-only audit was executed October 20, 2026 and is recorded in [the source receipt](validation/shared_station_source_audit_2026-10-20.json) and [overlap comparison](validation/shared_station_overlap_2026-10-20.json). The immutable local acquisition is on the RAID at:

```text
/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/shared_pipeline_sources/20261021T005923Z-s3-overlap-v1
```

The UTC directory timestamp falls on October 21; the local execution date was October 20.

## Pinned acquisition

`scripts/audit_upstream_station_source.py` saved the public bucket listing, four overlapping status objects and the current station-information object. Each receipt records the S3 key, URL, version ID, ETag, last-modified time, byte size, SHA-256, row count, station count, duplicate-key count, invalid-row count, schema and timestamp coverage.

The listing contained 59 daily Parquet objects from August 21 through October 18. The pinned sample contains:

| Local date | Rows | Stations | Note |
|---|---:|---:|---|
| 2026-08-21 | 158,004 | 2,508 | Partial first day |
| 2026-08-22 | 722,304 | 2,508 | Full daily object |
| 2026-08-23 | 722,304 | 2,508 | Full daily object |
| 2026-08-24 | 722,416 | 2,509 | Full daily object |

The station-information object contains 138,282 rows and 2,526 stations. Downloaded data stays on the RAID and is not committed to Git.

## Frozen-cohort overlap

`scripts/compare_upstream_station_overlap.py` compared the S3 observations with the independently acquired ctbk.dev archive for Team 6's frozen 32-station cohort. Thirty-one stations were present in both sources. Every S3 row for those stations found a nearest ctbk.dev observation within 150 seconds. Median timestamp separation was 17–26 seconds and the daily 95th percentile was 27–33 seconds.

The sources poll independently, so inventory can change between matched timestamps. Exact agreement by day ranged from 92.1% to 95.9% for bikes, 92.7% to 96.7% for docks, 94.3% to 97.4% for e-bikes, and 97.8% to 98.5% for disabled counts plus service flags. The S3 cadence is about five minutes; ctbk.dev is about one minute. The August 24 metadata comparison matched all 31 common station UUIDs, short names and capacities, with zero coordinate difference.

This agreement supports adapter development. It does not make the sources interchangeable at an exact timestamp or justify filling gaps with zeros.

## Metadata-freshness resolution

The S3 station-information file was rewritten October 19, but its newest embedded `fetched_at` value is October 12 at 01:43:24 UTC. Under the already frozen 49-hour Team 6 rule, that metadata is valid only before October 14 at 02:43:24 UTC.

The integration therefore enforces this policy:

- Reject capacity, location, normalized-count and station-mapping predictors at or after that cutoff.
- Do not use the object's later S3 modification time as evidence that station records were freshly observed.
- Admit later status rows only after a newer station-information snapshot is independently acquired and audited, or for an analysis whose predeclared features do not require those metadata fields.
- Preserve missing or stale metadata as an exclusion state rather than forward-filling it.

The freshness predicate is implemented in `citibike_pipeline.upstream_source` and covered by unit tests.

## Reproduction

```sh
uv run --locked python scripts/audit_upstream_station_source.py \
  --root /absolute/versioned/raid/root \
  --dates 2026-08-21 2026-08-22 2026-08-23 2026-08-24

uv run --locked python scripts/compare_upstream_station_overlap.py \
  --audit-root /absolute/versioned/raid/root \
  --ctbk-root /absolute/pinned/ctbk/source/root \
  --cohort /absolute/frozen/package/cohort.json \
  --output /absolute/versioned/raid/root/overlap_comparison.json
```

Both commands are read-only with respect to upstream and ctbk.dev sources. The first refuses to overwrite a nonempty audit directory.
