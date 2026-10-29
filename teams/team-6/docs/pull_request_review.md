# Team 6 pull request review layout

October 29, 2026

All Team 6 pull requests target the shared `team-6` integration branch. Work lives on descriptive feature branches; the feature name does not change the destination. Keep proposed changes off the integration branch until the user handles review and integration. Never merge or close a PR or enable auto-merge on the user's behalf.

## Replacement review groups

| Feature branch | Scope | Original PRs |
|---|---|---|
| `team-6-foundation-review` | Canonical project migration, locked environment, collection/processing code and shared-source audit | #54, #55 |
| `team-6-departure-review` | Historical Manhattan departures, original bounded integration receipts and feature/label/crosswalk preview | #58, #62 |
| `team-6-stockout-review` | Optional 30-minute stockout comparison and frozen November confirmation | #56, #57 |

Every replacement targets `team-6`. The integration branch does not yet contain the migrated Python project, so both feature branches include the identical foundation commit. Review the foundation first; until the user integrates it, its files also appear in each feature PR. Direct comparisons from `team-6-foundation-review` to either feature branch isolate the feature additions. The departure branch does not inherit the optional stockout/confirmation changes, and the optional branch does not inherit the historical departure panel or preview.

The earlier availability and ensemble work migrated as part of the foundation remains historical project material. Its presence does not make the new 30-minute experiment a requirement for departure forecasts. Existing external datasets, model pointers, source archives, schedules and frozen evidence are preserved.

## Review history and decisions

The original PRs remain open until the user chooses to close them. Their descriptions and head commits were archived locally before this reorganization. A GitHub audit of issue comments, reviews, inline comments and review threads found none on #54, #55, #56, #57, #58 or #62 on October 29. There were no posted review comments to copy; replacement descriptions retain source links and the applicable team decisions. Slack discussion supplied in the conversation is carried as attributed requirements and decisions, not presented as a new GitHub review by its original speaker.

The user can close #54/#55 after reviewing the foundation replacement, #58/#62 after reviewing the departure replacement, and #56/#57 after reviewing the optional replacement. PRs from other teams or shared-repository work are outside this reorganization.

The new midterm presentation, timed speaker script and professor preparation materials stay in ignored local directories and are excluded from these PRs.
