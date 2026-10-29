# Departure feature preview and next revision plan

October 27, 2026 preview · Branch separation updated October 29, 2026

The team's comments make the three separate hourly departure forecasts the main deliverable. This PR provides an inspectable sample and map demonstration, plus the plan for revising the full panel. The October 29 review reorganization combines the original PR 58 and PR 62 changes on a replacement branch targeting `team-6`, with the migrated foundation and without the optional 30-minute stockout/confirmation additions. It does not replace the already published dataset, change an existing protocol, fit a model, or activate a schedule.

## Start with the actual sample

Open [the preview](previews/departure_features/README.md), its [feature rows](previews/departure_features/features.csv), [label rows](previews/departure_features/labels.csv), and [station crosswalk](previews/departure_features/station_crosswalk.csv). These show W 31 St & 7 Ave (`6331.01`) at June 15, 2026, 10:00 New York across all three horizons. The observed departures are 17, 14 and 18. The display date/station are fixed examples, not chosen for model performance. Source hashes and column names are pinned in [the preview manifest](previews/departure_features/manifest.json).

![Three-hour map storyboard](previews/departure_features/map_storyboard.png)

The [animated GIF](previews/departure_features/map_preview.gif) is a bounded visual prototype using three actual departure-label frames for the 689-station snapshot cohort. Amber rings show the prior-origin publication signal, frozen across the three horizons. They identify linked signals, not measured incident impact or causal subway-to-bike substitution. The static storyboard is an alternative to animation.

## How the comments are addressed

| Team concern | Concrete preview / decision | Full-panel follow-up |
|---|---|---|
| Departure forecasting should remain the focus | Three hourly departure targets; no inventory input required | Keep stockout-onset work optional and outside the main dependency path |
| Missing counts should be zero during inferred active periods | A separate label helper assumes continuous operation from first observed local day through last observed local day, inclusive, when monthly trip files are complete | Version the label policy; rebuild the panel without requiring the 32-station inventory/pilot package |
| Profiles must use only released trips | An actual 56-day hour-of-week profile filters source release cohorts strictly before 09:55 and re-infers exposure from those released inputs only | Materialize/cache the same versioned profiles for all station-origins and score with identical definitions |
| Weather must cover the departure interval | Instantaneous temperature/wind at target start; precipitation/rain/snow at target end | Correct accumulated variables in a new full-panel version; current published #58 still needs this change |
| A prior snapshot can miss a 09:40 alert | The sample shows the legacy 09:00 source explicitly; a tested event-level cutoff prototype includes 09:40 and excludes 09:55/09:57 | Obtain raw publication versions and rebuild features strictly before 09:55; do not silently switch to the 10:00 aggregate |
| Make the departure work independent of optional stockout work | The replacement branch targets `team-6` and contains the shared foundation plus departure integration/preview; optional 30-minute stockout and confirmation files are absent | Finish the separately versioned full-panel label/timing revision below |

The proposed zero policy is an explicit **assumption**, not newly verified station operation. The preview exposes `label_basis`, `operation_assumed_24h`, `operation_verified=False`, `trip_files_complete`, and `inventory_required=False`. [Two real edge examples](previews/departure_features/zero_policy_examples.csv) distinguish an assumed zero inside the inferred period from unknown outside it. Long inactivity gaps inside the first/last day bracket remain a limitation; no arbitrary gap threshold or busy-hour selection is introduced. The missing fall-back offset still makes both repeated local hours unknown. The final inference rule and its sensitivity should be recorded before model comparison.

## Timing review before modeling

### Profiles

For the sample, the latest eligible complete nominal release is April 2026 under the existing **assumed**, unverified 45-day release delay. The profile window is March 6–April 30, with eight eligible Monday examples per target hour. Means are 10.625, 13.375 and 12.5 departures, using March/April release cohorts available by June 14 UTC. Later release cohorts, even when they contain trips in earlier months, are excluded until available. The test suite perturbs later-release records and verifies that neither numerator nor inferred-exposure denominator changes.

Label construction may infer retrospective active periods using the full declared study data. Predictor construction must **not** reuse that future-informed activity bracket: the preview recomputes it from eligible released records. A profile zero also requires a complete, released nominal month's trip files. These means are reconstructed baselines under the release and operation assumptions, not proof of verified real-time availability.

Model fitting needs its own label-maturity audit. Chronological feature construction alone does not make April/May validation outcomes available before a June test origin. The existing inspected periods remain retrospective development evidence. The later modeling revision must declare fit/refit/selection cutoffs separately and label any non-deployable historical selection scenario honestly.

### Weather

The [Open-Meteo variable contract](https://open-meteo.com/en/docs#hourly-parameter-definition) defines accumulated precipitation, rain and snowfall over the hour ending at the timestamp. Temperature and wind are instantaneous. The [Single Runs API](https://open-meteo.com/en/docs/single-runs-api) retains individual model-run structure using the Forecast API parameters.

For a 10:00 forecast, the three departure intervals are `[10,11)`, `[11,12)`, `[12,13)`. Temperature/wind stamps are 10, 11 and 12; accumulated-variable stamps are 11, 12 and 13. The preview reads source horizons 1–3 for instantaneous values and 2–4 for accumulations from the 09:00 weather source origin. It retains both run and availability timestamps, and checks both against 09:55. The sample is dry, so numerical rain values happen to agree; a nonzero synthetic test detects the one-hour shift. We do not present this as an exact historical operational weather reconstruction: early hindcasts, static geography and the seven-hour delay remain disclosed limitations.

### Subway publications

The prior-origin aggregate was a conservative response to the five-minute issue cutoff: the 10:00 aggregate can contain a 09:57 publication unavailable at 09:55. The team's 09:40 example is correct: the 09:00 aggregate unnecessarily loses that eligible information. Filtering only an aggregate's latest-publication timestamp cannot reconstruct every event/version contribution or the system-wide archive counts.

The desired contract is latest eligible version **per event** published and available strictly before 09:55, followed by the declared recent-publication window, route/dock mapping and event deduplication. A zero-minute ingestion delay remains an assumption unless source receipt times are supplied. Later clearing/update messages must not rewrite the earlier event state. Unmapped docks and archive silence stay missing; zero linked publications does not prove normal subway service.

The five-table handoff contains aggregates and retrospective exposure, not the required raw publication/version rows. The existing separate raw archive also needs UTC and version-semantics reconciliation. Request/pin event IDs, publication/update/availability timestamps, statuses, affected routes/selectors and source hashes before rebuilding. Until then, the preview's subway columns are explicitly the **legacy comparison input**, not a claim that the current-origin fix is complete. The timing unit test uses a synthetic 09:40 example, not a fabricated real alert.

## Making the departure branch independent

The October 29 replacement branch is based on the common foundation and targets `team-6` directly. The original stacked sequence (#54 → #55 → #56 → #57 → #58 → #62) is retained only in the original open PRs for review history. See [the review layout](pull_request_review.md).

The replacement carries collection/normalization, selected-file hourly aggregates, configuration, hashing, release rules, station identity and weather/subway contribution readers from the migrated foundation. It adds the historical integration and preview without the new 30-minute stockout runtime, result files, tests or confirmation protocol. No departure module imports those optional modules. The old bounded pilot and its receipts remain historical inputs under the published panel's existing policy; branch separation does not silently revise those labels.

The following data-contract changes remain a later, separately versioned revision:

1. Replace the full builder's mandatory legacy pilot-support input with the declared active-period/complete-file label contract, retaining imputation provenance and maintenance-gap sensitivity. The existing published panel is unchanged.
2. Materialize released-history profiles across the panel without future-informed exposure denominators.
3. Rebuild interval-end accumulated weather and eligible event-version subway features after the raw publication handoff is available.
4. Declare model fitting/selection availability cutoffs and rerun station comparisons on common eligible keys.

The optional stockout branch preserves its frozen protocol and original artifacts. No model or schedule is activated by the branch reorganization.

## Animated map roadmap

The committed GIF proves the three-horizon storyboard using real coordinates and a small, fixed example. The next interactive version should have:

- A fixed forecast-origin display, three horizon buttons, play/pause, a scrubber and reduced-motion/static mode. During horizon playback, origin-time alerts and the profile release version/cutoff stay fixed. Each horizon selects its own hour-of-week profile; labels and target-interval weather also change by horizon.
- Station details showing string trip/GBFS IDs, observed or assumed-zero label, target bounds, weather valid-time/run/availability, profile releases and alert cutoff. Unknowns remain distinct from zero.
- Toggleable departure-label, weather-proxy and mapped-publication layers. Draw bridge links or documented geometries only; do not invent a disruption polygon, weather resolution or causal affected area from a nearest-dock mapping.
- A later replay-over-origins mode that recomputes each frame's cutoff and source selection. Future alerts, updated incident endings and future trip profiles must not appear in an earlier frame. Forecast predictions should be a separate layer only after a model is validated.

Acceptance checks: three unique horizon rows per station-origin; matching feature/label keys; exact variable intervals; profile inputs strictly before cutoff; explicit imputation provenance; no optional inventory dependency; stable map scales; keyboard/reduced-motion controls for an interactive build. Unknown future operation and unknown alerts must remain visibly distinct. No website hosting or new recurring job is required for this preview.

## Reproduce and review

```sh
uv run --locked python scripts/build_departure_feature_preview.py
uv run --locked python scripts/run_tests.py -q tests/test_departure_preview.py
```

The builder checks source hashes, reads the RAID aggregates without changing them, pins the inventory completeness report, and commits only three feature/label rows, a single station crosswalk, tiny edge examples and rendered map artifacts. It never processes November confirmation outcomes. The published 26.18-million-row panel and its pointers remain unchanged.
