"""Prototype v18 (from v16/v17): Option A window runner. One process per scoring window.

Windows (America/New_York): am 06:30-10:30 and pm 15:30-20:30, end-exclusive, scored every
5 minutes. Collection continues without scoring through 60 minutes after the window end so the
last predictions can receive outcomes. The scorer is loaded once per window, so a model change
only takes effect at a window boundary.

State: the working SQLite database lives on local disk. The single-writer lock, checkpoints,
raw response archives and window reports live in `state_dir` (a private workspace folder).
This module sends no alerts, writes nothing to S3 and creates no jobs.
"""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Purpose: run every poll of one window in order. For each 5-minute slot:
# wait until the slot time, poll (score or collect only), evaluate outcomes,
# save a checkpoint copy of the database, and record the result. At the end,
# write the window report that the publisher and the checks read.

## Packages
import json, sqlite3, time, uuid  # Reports, database, clock and sleep, owner IDs
from datetime import datetime, timedelta, timezone, time as dtime  # Slot times
from pathlib import Path  # Build file paths
from zoneinfo import ZoneInfo  # New York time zone

import pilot_loop as pl  # One poll, outcome evaluation, health summary
import pilot_state as ps  # Lock, checkpoints and safe file writes

## Constants
NY = ZoneInfo('America/New_York')
WINDOWS = {'am': (dtime(6, 30), dtime(10, 30)), 'pm': (dtime(15, 30), dtime(20, 30))}  # Scoring hours, New York time
CADENCE_SECONDS = 300  # One poll every 5 minutes
TAIL_SECONDS = 3600          # collect through window end + 60 minutes
LATE_TOLERANCE_SECONDS = 120  # a slot more than 2 minutes late is skipped and counted as a gap
STALE_AFTER_SECONDS = 1200  # A lock with no heartbeat for 20 minutes counts as stale
MAX_PUBLISH_SECONDS = 60     # a slower checkpoint publish stops the window loudly


## Error raised when saving a checkpoint takes too long
class PublishTooSlow(RuntimeError):
    pass


## Build the window ID, for example 20261114_pm
def window_id(day, name):
    return '%s_%s' % (day.strftime('%Y%m%d'), name)


## List the poll slots for a window
def window_slots(day, name, custom=None):
    """Return [(slot_utc, 'score' | 'collect')], plus window start and collection end in UTC.

    custom=(start_utc, end_utc, tail_seconds[, cadence_seconds]) defines a supervised test
    window instead of am/pm. A shorter cadence is only for fast local tests.
    """
    # Custom test window: confirm it is well formed, then build its slots.
    if custom is not None:
        start, end, tail = custom[:3]
        cadence = custom[3] if len(custom) > 3 else CADENCE_SECONDS
        if start.tzinfo is None or end <= start or tail < 0 or cadence <= 0:
            raise ValueError('invalid custom window')
        start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
        return _slots(start, end, end + timedelta(seconds=tail), cadence)
    # Standard am or pm window: convert New York times to UTC (this handles daylight saving).
    start_local, end_local = WINDOWS[name]
    start = datetime.combine(day, start_local, NY).astimezone(timezone.utc)
    end = datetime.combine(day, end_local, NY).astimezone(timezone.utc)
    return _slots(start, end, end + timedelta(seconds=TAIL_SECONDS))


## Step from start to stop, labeling each slot as scoring or collection only
def _slots(start, end, stop, cadence=CADENCE_SECONDS):
    slots, t = [], start
    while t <= stop:
        slots.append((t, 'score' if t < end else 'collect'))  # After the window end: collect outcomes only
        t += timedelta(seconds=cadence)
    return slots, start, stop


## Parse ISO text into a UTC datetime
def _utc(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)


## Run one whole window
def run_window(*, day, name, state_dir, local_dir, scorer_factory=None, get=pl.fetch, now=pl.clock,
               sleep=time.sleep, owner=None, takeover_confirmed_dead_owner=None, max_slots=None, custom_window=None,
               stale_after_seconds=STALE_AFTER_SECONDS, retention_keep_last=None, monotonic=time.monotonic):
    """Run one window. Returns the window report (also written to state_dir/window_reports/).

    scorer_factory() -> (score_fn, model_version, mode). If it raises, the logistic regression
    model is used and the reason is recorded. takeover_confirmed_dead_owner names the owner of a
    stale lock that the caller has confirmed is no longer running (for example, a supervised
    restart); only then is that lock moved aside and its unfinished runs resolved.

    v17: each window starts a fresh database. Only a checkpoint from the same window is restored
    (a restart inside a window). Nothing else is carried forward: the collection tail runs to 60
    minutes after the last scoring slot, so every prediction is finalized (known or unknown) before
    the window ends. retention_keep_last=N (approval required) keeps only the newest N checkpoints
    of this window; None keeps all. Checkpoint size and publish time are recorded for every poll.
    """
    state_dir, local_dir = Path(state_dir), Path(local_dir)
    owner = owner or uuid.uuid4().hex  # Unique name for this runner, used by the lock
    wid = window_id(day, name)
    slots, start, stop = window_slots(day, name, custom_window)

    ## Take the single-writer lock (only one runner may write the state folder)
    try:
        ps.acquire_lock(state_dir, owner=owner, window_id=wid, now=now())
        took_over = None
    except ps.LockHeld:
        # Without an explicit, confirmed takeover, stop: another runner may still be alive.
        if takeover_confirmed_dead_owner is None:
            raise
        took_over = ps.clear_stale_lock(state_dir, now=now(), stale_after_seconds=stale_after_seconds,
                                        previous_worker_confirmed_dead=True, expected_owner=takeover_confirmed_dead_owner)
        ps.acquire_lock(state_dir, owner=owner, window_id=wid, now=now())
    run_started = _utc(now())
    # Create the window report; each list is filled as slots run.
    report = {'window_id': wid, 'owner': owner, 'window_start_utc': start.isoformat(), 'collection_end_utc': stop.isoformat(),
              'run_started_utc': run_started.isoformat(), 'took_over_from': took_over, 'polls': [], 'skipped_late': [],
              'not_attempted_before_start': [], 'already_recorded': [], 'errors': [], 'retention_keep_last': retention_keep_last,
              'pruned_checkpoints': []}
    db = None
    try:
        ## Prepare the working database
        local_dir.mkdir(parents=True, exist_ok=True)
        local_db = local_dir / ('pilot_%s_%s.sqlite' % (wid, owner[:8]))  # Working copy on fast local disk
        report['orphans_moved'] = ps.quarantine_orphans(state_dir, owner=owner, window_id=wid, now=now())  # v18
        # Restore a checkpoint only if it belongs to this same window (a restart mid-window).
        latest = ps.peek_latest(state_dir)
        restored = ps.restore_latest(state_dir, local_db) if latest and latest.get('window_id') == wid else None
        report['restored_checkpoint'] = restored
        report['fresh_database'] = restored is None
        db = sqlite3.connect(str(local_db)); pl.init(db)
        # Unfinished runs in a restored database are allowed only after a confirmed takeover.
        unfinished = [r[0] for r in db.execute("SELECT run_id FROM runs WHERE state IN ('started','abandoned_unresolved')")]
        if unfinished:
            if took_over is None:
                raise RuntimeError('unfinished runs found without a confirmed takeover: %s' % unfinished)
            report['resolved_dead_runs'] = {rid: pl.resolve_dead_run(db, rid, worker_confirmed_dead=True, no_backfill=True, as_of=now()) for rid in unfinished}

        ## Load the scorer once for the whole window
        try:
            if scorer_factory is None:
                raise RuntimeError('no candidate scorer supplied')
            score_fn, version, mode = scorer_factory()
        except Exception as e:
            # Fall back to the logistic baseline and record why.
            score_fn, version, mode = None, None, 'logistic_fallback'
            report['scorer_fallback_reason'] = type(e).__name__ + ': ' + str(e)[:200]
        report['scorer'] = {'mode': mode, 'model_version': version}
        archive_dir = state_dir / 'raw' / wid; archive_dir.mkdir(parents=True, exist_ok=True)  # Raw response archives for this window
        seq = restored['sequence'] + 1 if restored and restored.get('window_id') == wid else 1  # Next checkpoint number
        recorded = {_utc(r[0]) for r in db.execute('SELECT slot FROM runs')}  # Slots already done before a restart
        done = 0

        ## Run each slot in order
        for slot, kind in slots:
            if max_slots is not None and done >= max_slots:
                break  # Test limit reached
            # Skip slots already recorded (after a restart).
            if slot in recorded:
                report['already_recorded'].append(slot.isoformat()); continue
            # Skip slots that were already past when this run started (for example the shadow run's late start).
            if (run_started - slot).total_seconds() > LATE_TOLERANCE_SECONDS:
                report['not_attempted_before_start'].append(slot.isoformat()); continue
            # Wait until the slot time.
            wait = (slot - _utc(now())).total_seconds()
            if wait > 0:
                sleep(wait)
            # Skip the slot if the wait overshot by more than 2 minutes.
            lateness = (_utc(now()) - slot).total_seconds()
            if lateness > LATE_TOLERANCE_SECONDS:
                report['skipped_late'].append(slot.isoformat()); continue
            entry = {'slot': slot.isoformat(), 'kind': kind}
            # Poll: score during the window, collect only during the tail.
            try:
                run_id, details = pl.run_poll(db, slot.isoformat(), get=get, now=now,
                                              score_fn=score_fn if kind == 'score' else None,
                                              model_version=version, collect_only=(kind == 'collect'),
                                              raw_archive_dir=archive_dir)
                entry.update({'run_id': run_id, 'state': 'success', 'score_counts': details.get('score_counts')})
            except Exception as e:
                # A failed poll is recorded and the window continues with the next slot.
                entry.update({'state': 'failed', 'error': type(e).__name__ + ': ' + str(e)[:200]})
                report['errors'].append(entry['error'])
            # Match any predictions whose target time is now covered.
            evaluate_at = now()
            watermark = pl.collection_watermark(evaluate_at, single_writer_verified=True, no_backfill=True)
            entry['evaluated'] = dict(pl.evaluate(db, as_of=evaluate_at, complete_through=watermark))
            ps.heartbeat(state_dir, owner=owner, now=now(), note=entry['slot'])  # Show the lock owner is alive
            # Save a fingerprinted checkpoint of the database to the state folder, and time it.
            t0 = monotonic()
            rec = ps.publish_checkpoint(db, state_dir, owner=owner, window_id=wid, sequence=seq, now=now())
            entry.update({'checkpoint': rec['name'], 'checkpoint_sha256': rec['sha256'], 'checkpoint_bytes': rec['bytes'], 'publish_seconds': round(monotonic() - t0, 3)})
            # Optionally delete older checkpoints of this window (the job uses retention 3).
            if retention_keep_last:
                report['pruned_checkpoints'] += ps.prune_window_checkpoints(state_dir, owner=owner, window_id=wid, keep_last=retention_keep_last)
            seq += 1; done += 1
            report['polls'].append(entry)
            # Stop the window if saving a checkpoint took over 60 seconds (polls would start running late).
            if entry['publish_seconds'] > MAX_PUBLISH_SECONDS:
                report['fatal'] = 'checkpoint publish took %.1f s (limit %d s)' % (entry['publish_seconds'], MAX_PUBLISH_SECONDS)
                break

        ## Summarize the window and write its report
        report['health'] = pl.run_health(db, start.isoformat(), (stop + timedelta(seconds=1)).isoformat())
        report['health'].update({
            'checkpoint_bytes_max': max([p['checkpoint_bytes'] for p in report['polls']], default=None),
            'publish_seconds_max': max([p['publish_seconds'] for p in report['polls']], default=None),
            # Predictions still without a final outcome; the publisher requires 0.
            'pending_outcomes_at_end': db.execute("SELECT count(*) FROM predictions p LEFT JOIN outcomes o ON p.run_id=o.run_id AND p.station_id=o.station_id AND p.outcome=o.outcome WHERE o.payload IS NULL OR json_extract(o.payload,'$.status')='pending'").fetchone()[0],
            'skipped_late': len(report['skipped_late']), 'not_attempted_before_start': len(report['not_attempted_before_start'])})
        report['finished_utc'] = _utc(now()).isoformat()
        rdir = state_dir / 'window_reports'; rdir.mkdir(exist_ok=True)
        ps._replace_text(rdir / ('%s_%s.json' % (wid, owner[:8])), json.dumps(report, indent=2, default=str))  # Save the report safely
        if report.get('fatal'):
            raise PublishTooSlow(report['fatal'])
        return report
    finally:
        # Always close the database and release the lock if this runner still holds it.
        if db is not None:
            db.close()
        if (state_dir / ps.LOCK).exists() and json.loads((state_dir / ps.LOCK).read_text())['owner'] == owner:
            ps.release_lock(state_dir, owner=owner, now=now())
