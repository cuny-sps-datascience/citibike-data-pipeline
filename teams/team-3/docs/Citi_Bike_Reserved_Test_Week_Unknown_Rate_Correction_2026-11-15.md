# Correction: unknown rates in the reserved test week group audit (November 15, 2026)

Applies to `Citi_Bike_Reserved_Test_Week_Result_2026-11-10.md`, which is kept unchanged. Its neighborhood audit says "Unknown rates were at most 0.3% in every group." That is true for one denominator but not the other. Read from the saved `06_Reserved_Test_Week_Result/test_summary.json` (audit section). No new scoring.

| Denominator | Largest rate (excluding the one-station missing group) | Where |
|---|---:|---|
| Eligible snapshots in the group | 0.26% | Poverty higher third, dock |
| Alerts sent in the group | 0.36% | No-vehicle lower third, bike, low-count rule |

Alert unknown rates above 0.30%:

| Grouping | Outcome | Method | Group | Alerts | Known alerts | Unknown rate |
|---|---|---|---|---:|---:|---:|
| No-vehicle share | Bike | Low-count | Lower | 2,782 | 2,772 | 0.36% |
| No-vehicle share | Bike | Logistic | Lower | 1,984 | 1,977 | 0.35% |
| Poverty share | Dock | Low-count | Higher | 3,521 | 3,510 | 0.31% |
| No-vehicle share | Dock | Low-count | Higher | 2,294 | 2,287 | 0.31% |
| No-vehicle share | Dock | Low-count | Middle | 3,592 | 3,581 | 0.31% |

Corrected sentence: "Unknown rates were at most 0.26% of eligible snapshots and at most 0.36% of alerts in every group."

None of the frozen candidate's alert unknown rates is above 0.30%. The headline catches and the group capture figures are unchanged. If a slide cites unknown rates, it should say which denominator it uses.
