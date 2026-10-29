# Recent Modeling Input Audit

The read-only audit on October 11, 2026 scanned 126 selected published Parquet files for trips starting January 1, 2025 through August 31, 2026. It completed in 63.18 seconds with four DuckDB threads and a 3 GB memory cap. The [JSON report](model_input_audit_2026-10-11.json) contains the monthly results and live ledger totals.

| Measurement | Count |
|---|---:|
| Published trip rows in the period | 75,931,061 |
| Eligible departures | 75,893,264 |
| Eligible departures with missing derived duration | 0 |
| Eligible departures with nonpositive duration | 482 |
| Positive derived duration below 30 seconds | 3 |
| Positive derived duration below 60 seconds | 4 |
| Positive derived duration below 120 seconds | 2,150,766 |
| Eligible departures crossing a DST offset change | 798 |
| Positive under-60 departures without recorded DST transition flags | 3 |
| Rows with a non-null original source duration | 0 |

The positive-under-60 count is approximately 0.0000053% of eligible departures; the under-120 count is 2.8339%. These are cumulative positive-duration counts, not disjoint buckets. Nonpositive and missing durations are reported separately and would need an explicit policy in any thresholded target.

Three under-60 records occur in January 2026 and one in November 2025. One of the four has a DST-related flag. Counts alone do not establish that the records are false starts. Derived duration is the source wall-clock difference and can differ from elapsed time across DST; the current audit does not repair timestamps or approve a filtering rule.

All rows in this modern cohort have null `source_duration_seconds`, which is expected because modern source files lack the legacy duration field. This does not mean the calculated duration is missing.

The evidence supports keeping the published-departure baseline while inspecting invalid durations and comparing threshold alternatives. A 120-second cutoff would affect far more recorded rides than a 60-second cutoff. The audit does not yet measure changes to station rankings or model accuracy; those are required stages in the [modeling export plan](../modeling_export_plan.md).
