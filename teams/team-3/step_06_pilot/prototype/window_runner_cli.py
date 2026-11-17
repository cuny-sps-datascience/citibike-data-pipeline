"""Command-line entry for one window run (used by the interruption test and, later, a job).

Exit codes: 0 finished, 3 lock held by another writer, 1 other error. Prints one JSON line.
"""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Purpose: run one polling window from the command line, mainly for local
# tests. It also provides candidate_factory, which the Databricks job uses to
# load the ACS candidate model.

## Packages
import argparse, json, sys, zlib  # Command-line options, output, exit code, deterministic fake counts
from datetime import date, datetime, timedelta, timezone  # Window dates and times
from pathlib import Path  # Locate files next to this script

import pilot_state as ps  # Lock and checkpoint handling (for the LockHeld error)
import window_runner as wr  # The window runner itself


## Create a function that loads the ACS candidate scorer when called
def candidate_factory(candidate_dir, window, acs_file):
    """Return a no-argument function that loads the ACS candidate and refuses the fallback."""
    root = Path(__file__).resolve().parent  # Folder holding the cohort and baseline files
    def factory():
        from candidate_scorer_acs import load_scorer  # Loads candidate or fallback
        from batch_scorer import make_score_fn  # Wraps it for fast batch scoring
        scorer = load_scorer(candidate_dir, window, root / 'cohort_reconstructed.csv', root / 'model_parameters.json', acs_file)
        fn, version, mode = make_score_fn(scorer)
        # Here the fallback is treated as a failure, so the window runner can record it.
        if mode != 'acs_candidate':
            raise RuntimeError('candidate failed to load: ' + str(scorer['fallback_reason']))
        return fn, version, mode
    return factory


## Create a synthetic feed for local tests
def fake_get(cohort):
    """Synthetic feed on the real clock, for local tests only."""
    def get(kind):
        t = datetime.now(timezone.utc); ts = int(t.timestamp())  # Current time in seconds
        if kind == 'info':
            # Every cohort station with capacity 20.
            return {'last_updated': ts, 'data': {'stations': [{'station_id': s, 'capacity': 20} for s in cohort]}}
        # Bike counts change every 7 seconds and differ by station (crc32 of the ID), always between 0 and 20.
        return {'last_updated': ts, 'data': {'stations': [{'station_id': s, 'last_reported': ts - 5, 'num_bikes_available': (ts // 7 + zlib.crc32(s.encode())) % 21,
                'num_docks_available': 20 - (ts // 7 + zlib.crc32(s.encode())) % 21, 'is_installed': 1, 'is_renting': 1, 'is_returning': 1} for s in cohort]}}
    return get


## Run one window from the command line
def main(argv=None):
    """Parse options, run the window, print one JSON line, and return the exit code."""
    ## Read command-line options
    a = argparse.ArgumentParser()
    a.add_argument('--state', required=True); a.add_argument('--local', required=True); a.add_argument('--owner', required=True)  # State folder, local work folder, lock owner name
    a.add_argument('--start', help='custom window start, ISO UTC'); a.add_argument('--score-minutes', type=float); a.add_argument('--tail-minutes', type=float)  # Optional short test window
    a.add_argument('--cadence-seconds', type=float, default=300); a.add_argument('--day'); a.add_argument('--name', default='test')  # Poll every 5 minutes by default
    a.add_argument('--takeover'); a.add_argument('--stale-after-seconds', type=float, default=wr.STALE_AFTER_SECONDS)  # Lock recovery options
    a.add_argument('--candidate-dir'); a.add_argument('--candidate-window', default='train_before_Sep28'); a.add_argument('--acs-file')  # ACS candidate files
    a.add_argument('--fake-feed', action='store_true')  # Use the synthetic feed instead of the live one
    x = a.parse_args(argv)

    ## Build the optional custom window: (start, scoring end, outcome tail seconds, cadence seconds)
    custom = None
    if x.start:
        st = datetime.fromisoformat(x.start.replace('Z', '+00:00'))
        custom = (st, st + timedelta(minutes=x.score_minutes), x.tail_minutes * 60, x.cadence_seconds)
    day = date.fromisoformat(x.day) if x.day else datetime.now(timezone.utc).date()  # Default to today (UTC)
    kw = {}
    # Swap in the synthetic feed when requested.
    if x.fake_feed:
        from scoring import load_artifact, load_cohort
        root = Path(__file__).resolve().parent; art = load_artifact(root / 'model_parameters.json')
        kw['get'] = fake_get(sorted(load_cohort(root / 'cohort_reconstructed.csv', art['cohort_sha256_sorted_newline'])))
    factory = candidate_factory(x.candidate_dir, x.candidate_window, x.acs_file) if x.candidate_dir else None  # None = logistic baseline

    ## Run the window and report the result as one JSON line
    try:
        rep = wr.run_window(day=day, name=x.name, state_dir=x.state, local_dir=x.local, owner=x.owner, scorer_factory=factory,
                            takeover_confirmed_dead_owner=x.takeover, custom_window=custom, stale_after_seconds=x.stale_after_seconds, **kw)
    except ps.LockHeld as e:
        # Another runner holds the lock: write nothing and exit with code 3.
        print(json.dumps({'owner': x.owner, 'result': 'lock_held', 'info': e.info}, default=str)); return 3
    except Exception as e:
        print(json.dumps({'owner': x.owner, 'result': 'error', 'error': type(e).__name__ + ': ' + str(e)[:300]})); return 1
    # Display counts that confirm what the window did.
    print(json.dumps({'owner': x.owner, 'result': 'finished', 'polls': len(rep['polls']), 'skipped_late': len(rep['skipped_late']),
                      'already_recorded': len(rep['already_recorded']), 'errors': rep['errors'], 'scorer': rep['scorer']}, default=str))
    return 0


if __name__ == '__main__':
    sys.exit(main())
