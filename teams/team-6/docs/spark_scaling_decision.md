# Spark scaling decision

Randy Howk · October 20, 2026

## Decision

Do not migrate the current modeling workflow to Spark ML. Keep the validated single-node DuckDB/pandas/scikit-learn path for the present station and availability studies. Reconsider Spark first as a distributed data-preparation layer if later iterations outgrow the current feature-building budget; evaluate distributed model fitting only after data preparation becomes a demonstrated bottleneck.

Using Databricks does not by itself require Spark ML. The current notebooks use Databricks for reproducible execution, versioned artifacts and MLflow evidence while fitting compact prepared packages with scikit-learn.

## Evidence at the current scale

The completed ensemble experiments are small enough for one process:

| Workload | Prepared rows | Peak process RSS | Relevant timing |
|---|---:|---:|---:|
| Station ensemble package | 234,238 | 1,080.87 MB | Largest selected refit: 68.91 seconds |
| Availability ensemble package | 492,270 | 1,188.58 MB | Boosting fits: 2.44–3.96 seconds |

Both local and Databricks executions completed within the existing budgets, model reload checks passed and cloud metrics reproduced local results. The station artifact is about 10.3 MB and the routed availability artifact is under 0.8 MB. These measurements do not indicate a memory or distributed-training constraint.

Spark would add job scheduling, partition management, serialization and conversion overhead to datasets that already fit in memory. Reimplementing the feature and model paths would also require new parity tests without resolving the project's present evidence limits: historical station operation, advisory coverage, source-release timing and later-period confirmation.

The model evidence gives another reason not to migrate for training. Histogram boosting is the strongest station candidate, and the availability forests took 150.18–164.78 seconds per cloud fit while losing every outcome/horizon selection. Distributing random-forest training would not make an unselected model preferable. A Spark ML implementation would also be a different estimator whose behavior and persistence would need independent validation rather than being treated as an acceleration of the frozen scikit-learn model.

The raw archive remains on one local RAID. Running a multi-worker Spark job against that single storage device could move the bottleneck to disk contention, while moving the full archive to cloud storage would add a separate transfer, governance and reproducibility project. The current sequential archive reads and bounded DuckDB transformations are appropriate until measured preprocessing time or memory shows otherwise.

## Where Spark could help later

Spark becomes useful when a future iteration repeatedly processes data too large for the bounded single-node workflow or keeps the canonical inputs in cloud object storage or Delta tables. Its first useful responsibilities would be:

- reading many monthly trip and availability partitions in parallel;
- normalizing schema changes and deduplicating source rows;
- calculating station-partitioned lag and rolling features with chronological window functions;
- joining trip history to weather, station metadata, geography and advisory dimensions;
- refreshing only affected month partitions instead of rebuilding full feature tables;
- running completeness, uniqueness and temporal-cutoff checks across large tables; and
- materializing one versioned feature table that several model candidates can reuse.

Small weather, calendar and station metadata tables could use broadcast joins. Large outputs should be partitioned by coarse time periods such as month, avoiding tiny station/horizon partitions. Spark jobs must preserve the existing label-release cutoffs, predictor/label separation, station eligibility rules and repeated-clock handling.

## Reconsideration triggers

Benchmark a Spark feature pipeline when one or more of these conditions is observed:

- prepared training data no longer fits within the project's roughly 3 GB local memory cap;
- package construction repeatedly exceeds the ten-minute engineering budget because CPU work, rather than RAID throughput, is limiting it;
- minute-level, multi-year, multi-borough or multi-city coverage produces tens of millions of reusable feature rows;
- incremental refreshes require repeated scans of most of the raw archive; or
- canonical source partitions have moved to distributed cloud storage, removing the single-RAID input bottleneck.

Measure the candidate Spark pipeline on the same input manifest and output contract. Adoption requires matching row counts, keys, null policy, chronological cutoffs and feature values, plus a material improvement in elapsed time or capacity. Cluster startup time and data-transfer time belong in that comparison.

## Recommended migration sequence

1. Keep the existing model runtimes and versioned package contracts unchanged.
2. Prototype Spark SQL/DataFrame transformations for the most expensive measured preparation step and write the existing Parquet schema.
3. Compare end-to-end elapsed time, peak resources, file counts and exact output parity against DuckDB.
4. If the preparation benchmark wins, let Spark produce the versioned feature table and continue training the selected scikit-learn models from bounded extracts.
5. Consider Spark ML only when the training rows themselves exceed single-node capacity. Repeat chronological selection and later-period confirmation for every new estimator.

This sequence puts distributed compute where it can remove a measured constraint while preserving the tested models and audit trail.
