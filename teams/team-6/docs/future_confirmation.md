# Frozen stockout-onset confirmation

The [confirmation protocol](stockout_onset_confirmation_protocol.json) was frozen on October 20 local time, before the source window begins. At freeze time the audited shared listing ended October 18. The untouched confirmation window is November 1 through November 30, 2026 local time, with evaluation no earlier than December 1 at 05:32 UTC.

The protocol fixes the existing 32-station cohort, 30-minute onset definition, source and metadata freshness, continuous-coverage rules, model bundle hash, candidates and metrics. The selected 15-leaf boosting models, logistic comparators and fixed no-onset baseline will not be retrained, recalibrated or threshold-tuned after future outcomes are observed.

Confirmation requires at least 20 eligible local dates, 25 stations, 50 bike-onset events and 200 dock-onset events. It remains inconclusive if any minimum or source-quality gate fails. In particular, every origin needs station metadata observed less than 49 hours earlier. The current shared metadata stops October 12 and cannot support this window; upstream station-information collection must be restored and independently audited first.

Automated acquisition may checksum and store source objects during November. Outcome summaries, event counts and model metrics stay unread until the full window and confirming endpoints close. The later report will compare all frozen candidates on identical keys, use Brier score as the primary metric and show paired differences by local date. This protocol does not authorize production activation.
