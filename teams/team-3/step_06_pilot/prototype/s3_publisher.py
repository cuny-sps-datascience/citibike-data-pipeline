"""P6 (v18): publish one finished pilot window to the approved S3 layout. Standard library only (boto3 only inside S3Store).

Layout under contributions/team-3/pilot/v1/ (approved November 11):
  predictions/date=YYYY-MM-DD/predictions_<window_id>.csv
  outcomes/date=YYYY-MM-DD/outcomes_<window_id>.csv
  run_health/date=YYYY-MM-DD/run_health_<window_id>.json
  README_schema.md (written once)
Columns are the same as the synthetic files (pilot-synthetic/v1/), with data_kind=PILOT.

Rules:
- Built only from a window's final verified checkpoint, after its one-hour outcome tail.
- Each file is written only if absent (S3 conditional put). An existing identical file is left alone.
  An existing file with different content is never overwritten: the new content goes to the next free
  name ending _v2, _v3 ... (a correction), and the log says so.
- Every write is read back and its SHA-256 compared.
- Outcomes are published only when no prediction is still pending.
- Keys outside the approved prefix are refused.
- Any publishing error is caught by the caller (pilot_job_entry) and logged; it never fails the window
  or touches pilot state. A later run retries windows whose publish did not finish.
Modes: 'off' (nothing), 'dry_run' (build and save the files to the private state folder for review; no S3),
'on' (write to S3; only after the publishing checklist passes and publishing is approved).
"""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Purpose: turn one window's final checkpoint database into the three shared
# files (predictions, outcomes, run health) that the dashboard reads, and
# write them to the class S3 bucket without ever overwriting anything.

## Packages
import csv, hashlib, io, json, sqlite3  # CSV output, fingerprints, in-memory text, JSON, checkpoint database
from datetime import datetime, timezone  # Time formatting
from pathlib import Path  # Staging folder paths

## Constants: where files go and which columns they have
APPROVED_PREFIX = 'contributions/team-3/pilot/v1/'  # The only S3 folder this code may write to
DATA_KIND = 'PILOT'  # Marks real pilot rows (the synthetic test files say SYNTHETIC)
# Prediction file columns (one row per poll, station and outcome).
PRED_COLS = ['data_kind', 'window_id', 'run_id', 'slot_utc', 'fetched_at_utc', 'target_at_utc', 'station_id', 'station_name', 'lat', 'lon',
             'outcome', 'model_version', 'current_count', 'capacity', 'probability', 'risk_rank', 'risk_tier', 'model_selected', 'low_count_selected']
# Outcome file columns (one row per prediction, joined on run_id, station_id and outcome).
OUT_COLS = ['data_kind', 'window_id', 'run_id', 'station_id', 'outcome', 'target_at_utc', 'status', 'label', 'matched_observation_utc',
            'evaluated_at_utc', 'model_selected', 'low_count_selected']
# Run health fields shared with the synthetic files.
HEALTH_KEYS = ['data_kind', 'window_id', 'window_start_utc', 'collection_end_utc', 'model_version', 'scorer_mode', 'polls_expected', 'polls_success',
               'polls_failed', 'missing_slots', 'skipped_late', 'scored_polls', 'predictions', 'outcomes_known', 'outcomes_unknown',
               'unknown_share_of_finalized', 'checkpoint_bytes_max', 'publish_seconds_max']


## Errors
class AlreadyExists(Exception):
    pass  # The S3 key already has an object


class PublishError(RuntimeError):
    pass  # Any publishing problem; logged by the caller


## Format a time as UTC text ending in Z
def iso(value):
    t = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if t.tzinfo is None:
        raise ValueError('timestamp lacks timezone')
    return t.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


## Build CSV bytes from a header and rows
def _csv(cols, rows):
    buf = io.StringIO(); w = csv.writer(buf, lineterminator='\n'); w.writerow(cols); w.writerows(rows)  # Unix line endings
    return buf.getvalue().encode()


## Build the three S3 keys for a window
def window_keys(window_id, prefix=APPROVED_PREFIX):
    """Return {'predictions', 'outcomes', 'run_health'} keys under date=YYYY-MM-DD folders."""
    d = '%s-%s-%s' % (window_id[:4], window_id[4:6], window_id[6:8])  # 20261114_pm -> 2026-11-14
    return {'predictions': prefix + 'predictions/date=%s/predictions_%s.csv' % (d, window_id),
            'outcomes': prefix + 'outcomes/date=%s/outcomes_%s.csv' % (d, window_id),
            'run_health': prefix + 'run_health/date=%s/run_health_%s.json' % (d, window_id)}


## Build the file contents for one window from its final checkpoint
def build_window_files(db_path, report, prefix=APPROVED_PREFIX):
    """Return ({key: bytes}, notes) for one finished window from its final checkpoint database."""
    wid = report['window_id']; keys = window_keys(wid, prefix); notes = {}

    ## Load what the files need from the checkpoint
    with sqlite3.connect(str(db_path)) as db:
        slot_of = {rid: slot for rid, slot in db.execute('SELECT run_id, slot FROM runs')}  # Poll -> scheduled slot time
        station_meta, capacity = {}, {}
        # Station name and location, taken from each poll's saved information feed.
        for rid, info_json in db.execute('SELECT run_id, info_json FROM raw'):
            try:
                for s in json.loads(info_json)['data']['stations']:
                    station_meta[(rid, s['station_id'])] = (s.get('name', ''), s.get('lat', ''), s.get('lon', ''))
            except (KeyError, TypeError, ValueError):
                pass  # Leave name and location blank if that poll's feed was malformed
        # Capacity per poll and station, from the observations.
        for rid, sid, payload in db.execute('SELECT run_id, station_id, payload FROM observations'):
            capacity[(rid, sid)] = json.loads(payload).get('capacity')
        preds = []
        for rid, sid, outcome, payload, alert in db.execute('SELECT run_id, station_id, outcome, payload, alert FROM predictions'):
            p = json.loads(payload)
            if not (isinstance(p.get('current_count'), (int, float)) and p['current_count'] > 0):
                continue  # only stations with a positive current count are candidates
            preds.append({'run_id': rid, 'station_id': sid, 'outcome': outcome, 'p': p, 'alert': int(alert)})
        outcomes = {(rid, sid, o): json.loads(payload) for rid, sid, o, payload in db.execute('SELECT run_id, station_id, outcome, payload FROM outcomes')}

    ## Rank stations within each poll and outcome, and mark the low-count comparison rule
    groups = {}
    for r in preds:
        groups.setdefault((r['run_id'], r['outcome']), []).append(r)
    for rows in groups.values():
        # Rank 1 = highest Risk Score; ties by station ID. Tiers: top 5 high, 6 to 15 medium, rest low.
        for rank, r in enumerate(sorted(rows, key=lambda r: (-r['p']['probability'], r['station_id'])), 1):
            r['rank'] = rank; r['tier'] = 'high' if rank <= 5 else 'medium' if rank <= 15 else 'low'
        # Low-count rule: the 5 stations with the fewest bikes (or docks) right now.
        low = {r['station_id'] for r in sorted(rows, key=lambda r: (r['p']['current_count'], r['station_id']))[:5]}
        for r in rows:
            r['low'] = int(r['station_id'] in low)
    preds.sort(key=lambda r: (slot_of[r['run_id']], r['outcome'], r['rank']))  # Order rows by time, outcome, rank

    ## Create prediction and outcome rows
    pred_rows, out_rows, statuses = [], [], {'known': 0, 'unknown': 0, 'pending': 0, 'missing': 0}
    for r in preds:
        p, rid, sid = r['p'], r['run_id'], r['station_id']
        name, lat, lon = station_meta.get((rid, sid), ('', '', ''))
        pred_rows.append([DATA_KIND, wid, rid, iso(slot_of[rid]), iso(p['fetched_at']), iso(p['target_at']), sid, name, lat, lon, r['outcome'],
                          p['model_version'], p['current_count'], capacity.get((rid, sid), ''), round(float(p['probability']), 6),
                          r['rank'], r['tier'], r['alert'], r['low']])
        o = outcomes.get((rid, sid, r['outcome']))
        status = o['status'] if o else 'missing'  # No outcome row at all counts as missing
        statuses[status] = statuses.get(status, 0) + 1
        # Only final outcomes (known or unknown) become rows; the label is blank when unknown.
        if status in ('known', 'unknown'):
            out_rows.append([DATA_KIND, wid, rid, sid, r['outcome'], iso(p['target_at']), status, o['label'] if status == 'known' else '',
                             iso(o['matched_at']) if o.get('matched_at') else '', iso(o['evaluated_at']), r['alert'], r['low']])
    files = {keys['predictions']: _csv(PRED_COLS, pred_rows)}
    # Withhold the outcomes file if any prediction is not final yet; otherwise include it.
    if statuses['pending'] or statuses['missing']:
        notes['outcomes_withheld'] = 'pending=%d missing=%d; outcomes are published only when every prediction is known or unknown' % (statuses['pending'], statuses['missing'])
    else:
        files[keys['outcomes']] = _csv(OUT_COLS, out_rows)

    ## Create the run health file
    h = report.get('health', {}); polls = report.get('polls', [])
    finalized = statuses['known'] + statuses['unknown']
    health = {'data_kind': DATA_KIND, 'window_id': wid, 'window_start_utc': iso(report['window_start_utc']),
              'collection_end_utc': iso(report['collection_end_utc']), 'model_version': (report.get('scorer') or {}).get('model_version'),
              'scorer_mode': (report.get('scorer') or {}).get('mode'), 'polls_expected': h.get('attempted', 0) + h.get('missing_slots', 0),
              'polls_success': h.get('states', {}).get('success', 0), 'polls_failed': h.get('failed', 0), 'missing_slots': h.get('missing_slots', 0),
              'skipped_late': h.get('skipped_late', 0), 'scored_polls': len({r['run_id'] for r in preds}), 'predictions': len(pred_rows),
              'outcomes_known': statuses['known'], 'outcomes_unknown': statuses['unknown'],
              'unknown_share_of_finalized': round(statuses['unknown'] / finalized, 4) if finalized else None,  # Share of final outcomes that are unknown
              'checkpoint_bytes_max': h.get('checkpoint_bytes_max'), 'publish_seconds_max': h.get('publish_seconds_max'),
              # extra fields in real files (not in the synthetic set)
              'not_attempted_before_start': h.get('not_attempted_before_start', 0), 'pending_outcomes_at_end': h.get('pending_outcomes_at_end'),
              'took_over_from_owner': ((report.get('took_over_from') or {}).get('lock') or {}).get('owner'),
              'scorer_fallback_reason': report.get('scorer_fallback_reason'), 'errors': len(report.get('errors', []))}
    files[keys['run_health']] = json.dumps(health, indent=2).encode()
    notes['rows'] = {'predictions': len(pred_rows), 'outcomes': len(out_rows) if keys['outcomes'] in files else None}
    return files, notes


## The README written once to pilot/v1/ (describes every column for readers of the shared files)
README_SCHEMA = """# Team 3 Citi Bike pilot files (real pilot)

**PILOT DATA. Live monitoring, not model evaluation.** Every row has data_kind=PILOT. Frozen candidate A (positive-count boosted tree, 7 station features plus 6 ACS measures) scores the fixed 100-station cohort every 5 minutes in two windows a day (am 06:30 to 10:30, pm 15:30 to 20:30 New York time); if it fails to load, the original logistic model is used and model_version says so. Files are written once per window after its one-hour outcome tail and are never overwritten; a correction appears as a new file ending _v2.

## predictions/date=YYYY-MM-DD/predictions_<window_id>.csv
One row per poll, station and outcome for stations with a positive current count.
data_kind; window_id; run_id (one per poll); slot_utc (scheduled poll time); fetched_at_utc; target_at_utc (fetched + 60 minutes); station_id; station_name; lat; lon; outcome (bike = will the station have zero bikes, dock = zero open docks); model_version; current_count; capacity; probability (for ranking only, not a calibrated chance); risk_rank (1 = highest within the poll and outcome); risk_tier (high = top 5, medium = 6 to 15, low = rest); model_selected (1 if among the model's 5 alerts); low_count_selected (1 if among the 5 lowest current counts).

## outcomes/date=YYYY-MM-DD/outcomes_<window_id>.csv
One row per prediction. Join to predictions on run_id, station_id and outcome.
data_kind; window_id; run_id; station_id; outcome; target_at_utc; status (known or unknown; pending rows are never published); label (1 = shortage happened, 0 = did not, blank if unknown); matched_observation_utc; evaluated_at_utc; model_selected; low_count_selected.

## run_health/date=YYYY-MM-DD/run_health_<window_id>.json
One file per window: polls expected, succeeded and failed, missing and late slots, scored polls, prediction and outcome counts, unknown share, maximum checkpoint size and publish time, plus slots not attempted because the run started late or restarted, any takeover, and any scorer fallback.

## Reminders
Times are UTC (ending in Z). Counts are 5-minute snapshots, not separate incidents. A prediction is unknown when no eligible observation was found near its target time, for example during an outage.
"""


## Test stand-in for S3
class FakeStore:
    """In-memory stand-in for S3 with failure injection, for tests."""
    def __init__(self):
        self.objects, self.puts, self.fail_on, self.corrupt_on = {}, [], set(), set()  # Stored objects, write log, keys to fail or corrupt
    def get(self, key):
        return self.objects[key]
    def exists(self, key):
        return key in self.objects
    def put_if_absent(self, key, body, content_type):
        self.puts.append(key)
        if key in self.fail_on:
            raise ConnectionError('simulated network failure for ' + key)
        if key in self.objects:
            raise AlreadyExists(key)
        self.objects[key] = body[:-1] + b'?' if key in self.corrupt_on else body  # Corrupt the last byte when asked, to test read-back


## Real S3 connection (used only in "on" mode)
class S3Store:
    """Conditional writes to the class bucket. Never overwrites (If-None-Match: *)."""
    def __init__(self, client, bucket='cuny-sps-citibike-archive'):
        self.c, self.bucket = client, bucket  # boto3 client created by the job from the secret scope
    def get(self, key):
        return self.c.get_object(Bucket=self.bucket, Key=key)['Body'].read()  # Download the object
    def exists(self, key):
        # Ask for the object's header only; a 404 means it does not exist.
        try:
            self.c.head_object(Bucket=self.bucket, Key=key); return True
        except Exception as e:
            code = str(getattr(e, 'response', {}).get('Error', {}).get('Code', ''))
            if code in ('404', 'NoSuchKey', 'NotFound'):
                return False
            raise
    def put_if_absent(self, key, body, content_type):
        # IfNoneMatch='*' tells S3 to refuse the write if the key already exists, so nothing is overwritten.
        try:
            self.c.put_object(Bucket=self.bucket, Key=key, Body=body, IfNoneMatch='*', ContentType=content_type, Metadata={'data-kind': DATA_KIND})
        except Exception as e:
            code = str(getattr(e, 'response', {}).get('Error', {}).get('Code', ''))
            if code in ('PreconditionFailed', '412', 'ConditionalRequestConflict'):
                raise AlreadyExists(key) from None
            raise


## Pick the content type from the file extension
def _ctype(key):
    return 'text/csv' if key.endswith('.csv') else 'application/json' if key.endswith('.json') else 'text/markdown'


## Build a correction name, for example predictions_20261114_pm_v2.csv
def _versioned(key, n):
    stem, dot, ext = key.rpartition('.')
    return '%s_v%d.%s' % (stem, n, ext)


## Write the files to S3, never overwriting
def write_files(files, store, *, prefix=APPROVED_PREFIX, max_versions=9):
    """Write each file only if absent; identical existing files are skipped; different ones go to _vN. Returns per-key results."""
    # Confirm every key is inside the approved folder before writing anything.
    for k in files:
        if not k.startswith(prefix) or '..' in k:
            raise PublishError('refusing key outside the approved prefix: ' + k)
    results = {}
    # Write order: README, predictions, outcomes, then run health.
    order = lambda k: (0 if k.endswith('README_schema.md') else 1 if '/predictions/' in k else 2 if '/outcomes/' in k else 3, k)
    for key, body in sorted(files.items(), key=lambda kv: order(kv[0])):  # run_health last: its presence means the window is complete
        want = hashlib.sha256(body).hexdigest()  # Fingerprint of what we intend to write
        target, action = key, 'written'
        # Try the plain name first, then _v2, _v3 ... up to the limit.
        for n in range(1, max_versions + 2):
            if n > max_versions:
                raise PublishError('too many versions for ' + key)
            target = key if n == 1 else _versioned(key, n)
            # If something is already there: identical means done; different means try the next version name.
            if store.exists(target):
                if hashlib.sha256(store.get(target)).hexdigest() == want:
                    action = 'already_present_identical' if n == 1 else 'already_present_identical_as_v%d' % n
                    break
                continue
            # Write only if still absent (another writer could have written in between).
            try:
                store.put_if_absent(target, body, _ctype(key))
            except AlreadyExists:
                if hashlib.sha256(store.get(target)).hexdigest() == want:
                    action = 'already_present_identical'; break
                continue
            # Read the object back and confirm its fingerprint matches.
            back = hashlib.sha256(store.get(target)).hexdigest()
            if back != want:
                raise PublishError('read-back fingerprint mismatch for ' + target)
            action = 'written' if n == 1 else 'written_correction_v%d' % n
            break
        results[key] = {'target': target, 'action': action, 'sha256': want, 'bytes': len(body)}  # Recorded in the publish log
    return results


## Stage or publish one window
def publish_window(*, db_path, report, store, mode, staging_dir=None, prefix=APPROVED_PREFIX):
    """Build and (depending on mode) stage or write one window. Returns a log record. Raises on failure."""
    files, notes = build_window_files(db_path, report, prefix)
    readme_key = prefix + 'README_schema.md'
    # Record each file's size, fingerprint and CSV row count (minus the header) for the log.
    record = {'window_id': report['window_id'], 'mode': mode, 'notes': notes,
              'files': {k: {'bytes': len(v), 'sha256': hashlib.sha256(v).hexdigest(),
                            'rows': (v.count(b'\n') - 1) if k.endswith('.csv') else None} for k, v in files.items()}}
    # Dry run: save the files into the private staging folder for review; nothing goes to S3.
    if mode == 'dry_run':
        if staging_dir is None:
            raise PublishError('dry_run needs a staging folder')
        out = Path(staging_dir) / report['window_id']; out.mkdir(parents=True, exist_ok=True)
        for k, v in files.items():
            (out / k.rsplit('/', 1)[1]).write_bytes(v)  # Save under the file name only
        (out / 'README_schema.md').write_text(README_SCHEMA)
        record['staged_to'] = str(out); record['complete'] = 'outcomes_withheld' not in notes
        return record
    if mode != 'on':
        raise PublishError('unknown publish mode: ' + str(mode))
    # Real publish: add the README the first time, then write everything.
    if not store.exists(readme_key):
        files[readme_key] = README_SCHEMA.encode()
    record['results'] = write_files(files, store, prefix=prefix)
    record['complete'] = 'outcomes_withheld' not in notes
    return record
