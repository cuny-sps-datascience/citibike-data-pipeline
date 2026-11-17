"""Job entry point for the pilot (v18; v17 plus the S3 publisher step). One call runs the current New York window.

- Picks am (06:00-11:45 start) or pm (15:00-21:45 start) from New York time; uses the New York date.
- Checks the approved histogram gradient boosting model files, manifest and ACS table against their recorded SHA-256 before polling.
- State folder: <home>/Citi Bike - Pilot State. Candidate folder and ACS file are passed explicitly.
- Exit codes: 0 finished; 2 outside both windows; 3 lock held (see the recovery procedure);
  4 frozen files changed or missing; 5 checkpoint publish too slow; 1 any other failure.
v18: after a window finishes, publish_mode 'dry_run' stages the S3 files in the state folder and 'on' writes
them to S3 (s3_publisher.py). Publishing runs only after the window and its outcome tail are done, and a
publishing failure is logged but never changes the exit code or touches checkpoints. Default 'off'.
No alerts and no job creation.
"""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Purpose: this is what the scheduled Databricks job calls twice a day
# (06:25 and 15:25 New York). It works out which window it is, checks the
# frozen files, runs the window, and then publishes any finished windows.

## Packages
import hashlib, json, sys, tempfile, time  # Fingerprints, reports, exit code, scratch folders, clock
from datetime import datetime, time as dtime, timezone  # Window start times and UTC timestamps
from pathlib import Path  # Build file paths
from zoneinfo import ZoneInfo  # New York time zone

import pilot_loop as pl  # Live feed fetch and clock
import s3_publisher as pub  # Builds and writes the S3 files
import pilot_state as ps  # Checkpoints, locks and safe file replacement
import window_runner as wr  # Runs one polling window
from window_runner_cli import candidate_factory  # Loads the ACS candidate scorer

## Constants: folders and frozen file fingerprints
NY = ZoneInfo('America/New_York')
STATE_NAME = 'Citi Bike - Pilot State'  # Folder under home that holds checkpoints, reports and logs
CANDIDATE_FOLDER = 'Citi Bike - ACS Controls and Neighborhood Audit - ea8df268'  # Folder with the frozen ACS models
ACS_RELATIVE = 'Citi Bike - ACS and July Trip Development - e1eb6e26/acs_station_features.parquet'  # Frozen ACS table
CANDIDATE_WINDOW = 'train_before_Sep28'  # The approved model version (trained on data before Sep 28)
# Each frozen file and its approved SHA-256. If any differ, the job stops before polling (exit 4).
FROZEN = {
    CANDIDATE_FOLDER + '/bike_acs_candidate_train_before_Sep28.joblib': 'aa74b79e7c67276cc8d91895eefb56ec659c620182a6f021f41509eb5febaea4',  # Bike shortage model
    CANDIDATE_FOLDER + '/dock_acs_candidate_train_before_Sep28.joblib': 'd5ec6ce138da03d111ca912f9523bda5659939a6585400a772ec3c7c4c875dd8',  # Dock shortage model
    CANDIDATE_FOLDER + '/manifest.json': 'f130a24ae73e151826d184703f21a153508328a06bc5c7c019ab94ff0d77a196',  # Model manifest
    ACS_RELATIVE: '3332babcb605f43c85f60b979bcda82c66e1c5c1199167118f1a333dbc9b84f9',  # ACS station features
}
# A run may start a window only inside these New York time ranges.
STARTS = {'am': (dtime(6, 0), dtime(11, 45)), 'pm': (dtime(15, 0), dtime(21, 45))}


## Decide which window this run belongs to
def choose_window(now_utc):
    """Return ('am' or 'pm' or None, New York date) for a UTC time."""
    ny = now_utc.astimezone(NY)
    for name, (a, b) in STARTS.items():
        if a <= ny.time() < b:
            return name, ny.date()
    return None, ny.date()  # Outside both windows


## Check frozen files against their approved fingerprints
def check_frozen(home, frozen):
    """Return a list of problems ('missing: ...' or 'changed: ...'); empty means all match."""
    bad = []
    for rel, sha in frozen.items():
        p = Path(home) / rel
        if not p.exists():
            bad.append('missing: ' + rel)
        elif hashlib.sha256(p.read_bytes()).hexdigest() != sha:
            bad.append('changed: ' + rel)
    return bad


## Publishing limits
PUBLISH_BACKLOG_LIMIT = 12  # Look at no more than the 12 newest windows per run
PUBLISH_TIME_BUDGET_SECONDS = 600  # Stop starting new publishes after 10 minutes


## Find the final report for each window
def _final_reports(state):
    """Latest finished, publishable report per window (newest first)."""
    best = {}  # window_id -> newest report
    for p in sorted((state / 'window_reports').glob('*.json')) if (state / 'window_reports').exists() else []:
        try:
            r = json.loads(p.read_text())
        except ValueError:
            continue  # Skip an unreadable report
        prev = best.get(r['window_id'])
        # Keep the report that finished last (a window can have more than one report after a restart).
        if prev is None or r.get('finished_utc', '') > prev.get('finished_utc', ''):
            best[r['window_id']] = r
    return sorted(best.values(), key=lambda r: r['window_id'], reverse=True)


## Stage or publish every finished window that is not done yet
def publish_pending(state, *, mode, store=None, local=None, monotonic=time.monotonic):
    """Stage or publish every finished window not yet done in this mode. Never raises; returns a summary."""
    # mode 'dry_run' stages files locally; mode 'on' writes them to S3 through `store`.
    state = Path(state); logdir = state / 'publish_log'; logdir.mkdir(exist_ok=True)  # One log file per window
    done_key = 'published' if mode == 'on' else 'staged'  # Log key that marks the window as done
    summary = {'mode': mode, 'windows': {}}; t0 = monotonic()
    for rep in _final_reports(state)[:PUBLISH_BACKLOG_LIMIT]:
        wid = rep['window_id']; log_path = logdir / ('%s.json' % wid)
        log = json.loads(log_path.read_text()) if log_path.exists() else {}
        # Skip windows already done in this mode, so nothing is published twice.
        if log.get(done_key):
            continue
        # Leave the rest for the next run once the time budget is used.
        if monotonic() - t0 > PUBLISH_TIME_BUDGET_SECONDS:
            summary['windows'][wid] = 'deferred: time budget'; continue
        try:
            # Publish only complete windows: no fatal error, at least one poll, and no outcomes still pending.
            if rep.get('fatal') or not rep.get('polls') or (rep.get('health') or {}).get('pending_outcomes_at_end') != 0:
                summary['windows'][wid] = 'not ready (stopped early, no polls or pending outcomes)'; continue
            last = rep['polls'][-1]  # The last poll points to the final checkpoint
            src = state / 'checkpoints' / last['checkpoint']
            # Confirm the final checkpoint exists and matches the fingerprint in the report.
            # (This is the check that keeps refusing the 20261111_pm rehearsal window.)
            if not last.get('checkpoint_sha256') or ps.sha256_file(src) != last['checkpoint_sha256']:
                raise pub.PublishError('final checkpoint missing or fingerprint mismatch: ' + last['checkpoint'])
            # Work on a copy, so publishing can never change the checkpoint itself.
            work = Path(tempfile.mkdtemp(prefix='cb_pub_', dir=local)); db_path = work / 'final.sqlite'
            import shutil; shutil.copyfile(src, db_path)
            # Confirm the copy matches too.
            if ps.sha256_file(db_path) != last['checkpoint_sha256']:
                raise pub.PublishError('copied checkpoint fingerprint mismatch')
            rec = pub.publish_window(db_path=db_path, report=rep, store=store, mode=mode, staging_dir=state / 'publish_staging')  # Build and stage or write the 3 files
            rec['utc'] = datetime.now(timezone.utc).isoformat()
            # Mark the window done only when every file was complete.
            if rec.get('complete'):
                log[done_key] = rec
            log.setdefault('attempts', []).append({'mode': mode, 'utc': rec['utc'], 'ok': True, 'complete': rec.get('complete')})
            summary['windows'][wid] = '%s%s' % (done_key, '' if rec.get('complete') else ' (incomplete: ' + rec['notes'].get('outcomes_withheld', '') + ')')
        except Exception as e:
            # Record the failure in the window's log; the next run will try again.
            log.setdefault('attempts', []).append({'mode': mode, 'utc': datetime.now(timezone.utc).isoformat(), 'ok': False,
                                                   'error': type(e).__name__ + ': ' + str(e)[:300]})
            summary['windows'][wid] = 'FAILED: %s: %s' % (type(e).__name__, str(e)[:200])
        # Save the log by safe replacement (write a new file, then swap it in).
        try:
            ps._replace_text(log_path, json.dumps(log, indent=1, default=str))
        except Exception as e:
            summary['windows'][wid] = str(summary['windows'].get(wid)) + ' (log write failed: %s)' % type(e).__name__
    return summary


## Run the current window (the job's main step)
def run(*, home, local=None, now=pl.clock, sleep=time.sleep, get=pl.fetch, frozen=FROZEN, scorer_factory='candidate',
        retention_keep_last=None, takeover_confirmed_dead_owner=None, max_slots=None, publish_mode='off', store_factory=None):
    """Returns (exit_code, summary dict)."""
    # Arguments that tests replace: now (clock), sleep, get (feed fetch), frozen (fingerprints).
    home = Path(home); started = datetime.fromisoformat(now().replace('Z', '+00:00')).astimezone(timezone.utc)
    name, day = choose_window(started)
    summary = {'started_utc': started.isoformat(), 'new_york_date': day.isoformat(), 'window': name}
    # Stop if the run started outside both windows.
    if name is None:
        summary['message'] = 'Outside both pilot windows (am starts 06:00-11:45, pm 15:00-21:45 New York). Nothing done.'
        return 2, summary
    # Stop before polling if any frozen file is missing or changed.
    bad = check_frozen(home, frozen)
    if bad:
        summary['message'] = 'Frozen files failed their fingerprint check; not polling: ' + '; '.join(bad)
        return 4, summary
    # Use the approved ACS candidate unless a test passed a different scorer.
    if scorer_factory == 'candidate':
        scorer_factory = candidate_factory(str(home / CANDIDATE_FOLDER), CANDIDATE_WINDOW, str(home / ACS_RELATIVE))
    state = home / STATE_NAME
    local = Path(local or tempfile.mkdtemp(prefix='citibike_pilot_local_'))  # Fast local scratch folder
    ## Run the window
    try:
        rep = wr.run_window(day=day, name=name, state_dir=state, local_dir=local, scorer_factory=scorer_factory, get=get, now=now,
                            sleep=sleep, takeover_confirmed_dead_owner=takeover_confirmed_dead_owner, max_slots=max_slots,
                            retention_keep_last=retention_keep_last)
    except ps.LockHeld as e:
        summary.update({'message': 'Lock held by another writer; nothing written. Follow the manual recovery procedure.', 'lock': e.info})
        return 3, summary
    except wr.PublishTooSlow as e:
        summary['message'] = 'Stopped: ' + str(e) + '. State up to the last checkpoint is safe; see the window report.'
        return 5, summary
    except Exception as e:
        summary['message'] = 'Window failed: %s: %s' % (type(e).__name__, str(e)[:300])
        return 1, summary
    # Record what the window did (this is the summary printed at the end of each job run).
    summary.update({'window_id': rep['window_id'], 'scorer': rep['scorer'], 'polls': len(rep['polls']), 'errors': len(rep['errors']),
                    'skipped_late': len(rep['skipped_late']), 'not_attempted_before_start': len(rep['not_attempted_before_start']),
                    'health': rep['health'], 'orphans_moved': rep.get('orphans_moved', []), 'message': 'Window finished.'})
    ## Publish finished windows (only after the window itself is done)
    if publish_mode in ('dry_run', 'on'):
        try:
            store = store_factory() if publish_mode == 'on' else None  # S3 connection only in "on" mode
            summary['publish'] = publish_pending(state, mode=publish_mode, store=store, local=local)
        except Exception as e:  # a publishing problem never fails the window
            summary['publish'] = {'mode': publish_mode, 'error': type(e).__name__ + ': ' + str(e)[:300]}
    elif publish_mode != 'off':
        summary['publish'] = {'error': 'unknown publish_mode %r; nothing published' % publish_mode}
    return 0, summary


## Command-line entry point (local use)
if __name__ == '__main__':
    import argparse
    a = argparse.ArgumentParser(); a.add_argument('--home', required=True); a.add_argument('--retention-keep-last', type=int)
    a.add_argument('--takeover'); x = a.parse_args()
    code, summary = run(home=x.home, retention_keep_last=x.retention_keep_last, takeover_confirmed_dead_owner=x.takeover)
    print(json.dumps(summary, indent=1, default=str)); sys.exit(code)  # Display the summary and exit with its code
