"""Prototype v16: single-writer lock and checkpoint publication for the pilot. Standard library only.

Design (see Citi_Bike_Storage_Probe_and_Pilot_State_Design_2026-11-11.md):
- The lock is a file created with exclusive create ('x'). Its content never changes.
  A separate heartbeat file is rewritten by temp file and rename.
- Checkpoints are SQLite backups published under new numbered names by temp file and
  rename, verified by SHA-256, then pointed to by LATEST (also temp file and rename).
- Nothing is deleted except by the approved retention rule. A stale lock is moved aside into locks_cleared/, never removed.

The November 11 probe verified these file operations on Databricks workspace storage
across two machines, one session after the other. Two sessions racing for the lock at
the same instant are not verified. v18: kills during a poll and at four points of a
checkpoint publish are tested locally with real SIGKILL (kill_test.py); a killed publish
can leave a temp file or an unreferenced checkpoint, which quarantine_orphans() moves aside.
"""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Purpose: keep the pilot's saved state safe if a run is killed part way.
# Two ideas do most of the work:
#   1. Write to a temporary file, then rename it into place. A rename is all
#      or nothing, so readers never see a half-written file.
#   2. Fingerprint every checkpoint (SHA-256) and check it on every copy.
#
# Files in the state folder:
#   writer.lock       -> who is currently allowed to write (one runner at a time)
#   writer.heartbeat  -> last time the lock owner showed it was alive
#   LATEST.json       -> which checkpoint is the current, verified one
#   checkpoints/      -> numbered database copies, for example checkpoint_20261114_pm_0073.sqlite

## Packages
import hashlib, json, os, shutil, sqlite3, tempfile, uuid  # Fingerprints, records, rename and fsync, copies, backup, scratch, unique names
from datetime import datetime, timezone  # Time stamps
from pathlib import Path  # Build file paths

## Constants: file names in the state folder
LOCK = 'writer.lock'
HEARTBEAT = 'writer.heartbeat'
LATEST = 'LATEST.json'


## Error raised when another runner holds the lock
class LockHeld(RuntimeError):
    def __init__(self, info):
        self.info = info  # Lock and heartbeat details, for the report
        super().__init__('another writer holds the lock: ' + json.dumps(info))


## Parse ISO text into UTC; refuse times without a time zone
def _utc(value):
    t = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if t.tzinfo is None:
        raise ValueError('timestamp lacks timezone')
    return t.astimezone(timezone.utc)


## Calculate the SHA-256 fingerprint of a file
def sha256_file(path):
    """Return the SHA-256 hex digest of a file, read in 1 MB blocks."""
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):  # 1 << 20 bytes = 1 MB
            h.update(block)
    return h.hexdigest()


## Replace a text file safely (write a temporary file, then rename it into place)
def _replace_text(path, text):
    """Write text to a new temporary file, force it to disk, then swap it in with one rename."""
    path = Path(path)
    tmp = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex[:8])  # Unique temporary name next to the target
    with open(tmp, 'x', encoding='utf-8') as f:
        f.write(text); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)  # All-or-nothing swap


## Take the lock
def acquire_lock(state_dir, *, owner, window_id, now):
    """Create the lock or raise LockHeld. Returns the lock record."""
    state_dir = Path(state_dir); state_dir.mkdir(parents=True, exist_ok=True)
    record = {'owner': owner, 'window_id': window_id, 'acquired_utc': _utc(now).isoformat()}
    # Mode 'x' creates the file only if it does not exist, so only one runner can succeed.
    try:
        with open(state_dir / LOCK, 'x', encoding='utf-8') as f:
            f.write(json.dumps(record)); f.flush(); os.fsync(f.fileno())
    except FileExistsError:
        raise LockHeld(lock_status(state_dir, now=now)) from None
    heartbeat(state_dir, owner=owner, now=now)  # Record the first heartbeat
    return record


## Describe the current lock
def lock_status(state_dir, *, now):
    """Describe the current lock and heartbeat age; None if unlocked."""
    state_dir = Path(state_dir)
    if not (state_dir / LOCK).exists():
        return None
    info = {'lock': json.loads((state_dir / LOCK).read_text(encoding='utf-8'))}
    hb = state_dir / HEARTBEAT
    if hb.exists():
        info['heartbeat'] = json.loads(hb.read_text(encoding='utf-8'))
        info['heartbeat_age_seconds'] = (_utc(now) - _utc(info['heartbeat']['utc'])).total_seconds()  # Seconds since the owner last checked in
    return info


## Confirm this runner owns the lock before it writes anything
def _require_owner(state_dir, owner):
    current = json.loads((Path(state_dir) / LOCK).read_text(encoding='utf-8'))
    if current['owner'] != owner:
        raise LockHeld({'lock': current})


## Record that the lock owner is still alive
def heartbeat(state_dir, *, owner, now, note=None):
    _require_owner(state_dir, owner)
    _replace_text(Path(state_dir) / HEARTBEAT, json.dumps({'owner': owner, 'utc': _utc(now).isoformat(), 'note': note}))


## Give up the lock at the end of a window
def release_lock(state_dir, *, owner, now):
    """Move our own lock aside into locks_released/ (no deletion)."""
    state_dir = Path(state_dir); _require_owner(state_dir, owner)
    dest = state_dir / 'locks_released'; dest.mkdir(exist_ok=True)
    os.replace(state_dir / LOCK, dest / ('%s_%s.lock' % (_utc(now).strftime('%Y%m%dT%H%M%SZ'), owner)))  # Keep it, renamed with the time


## Move a dead runner's lock aside (supervised recovery only)
def clear_stale_lock(state_dir, *, now, stale_after_seconds, previous_worker_confirmed_dead, expected_owner):
    """Move a stale lock aside so a new run can start. Requires explicit confirmation.

    The caller must establish that the previous worker is no longer running (for a job,
    that its run has ended). A recent heartbeat always blocks clearing.
    """
    # Confirm the caller explicitly vouched that the old runner is dead.
    if previous_worker_confirmed_dead is not True:
        raise ValueError('previous worker must be confirmed dead')
    info = lock_status(state_dir, now=now)
    if info is None:
        return None  # Nothing to clear
    # Confirm the lock belongs to the runner the caller named.
    if info['lock']['owner'] != expected_owner:
        raise LockHeld(info)
    # Refuse if the owner checked in recently; it may still be running.
    age = info.get('heartbeat_age_seconds')
    if age is not None and age < stale_after_seconds:
        raise LockHeld(info)
    dest = Path(state_dir) / 'locks_cleared'; dest.mkdir(exist_ok=True)
    os.replace(Path(state_dir) / LOCK, dest / ('%s_%s.lock' % (_utc(now).strftime('%Y%m%dT%H%M%SZ'), expected_owner)))
    return info


## Save a verified checkpoint copy of the working database
def publish_checkpoint(db, state_dir, *, owner, window_id, sequence, now):
    """Back up the live SQLite DB and publish it as a new, verified checkpoint."""
    state_dir = Path(state_dir); _require_owner(state_dir, owner)
    cdir = state_dir / 'checkpoints'; cdir.mkdir(exist_ok=True)
    name = 'checkpoint_%s_%04d.sqlite' % (window_id, sequence)  # Numbered name, for example ..._0073.sqlite
    final = cdir / name
    # Never overwrite an existing checkpoint.
    if final.exists():
        raise FileExistsError('checkpoint already published: ' + name)
    with tempfile.TemporaryDirectory(prefix='cb_ckpt_') as work:
        # Take a consistent snapshot with SQLite's backup function, then fingerprint it.
        snap = Path(work) / 'snap.sqlite'
        with sqlite3.connect(str(snap)) as dst:
            db.backup(dst)
        expected = sha256_file(snap)
        # Copy to a temporary name in the state folder and confirm the copy matches.
        tmp = cdir / (name + '.tmp-' + uuid.uuid4().hex[:8])
        shutil.copyfile(snap, tmp)
        if sha256_file(tmp) != expected:
            raise IOError('checkpoint copy hash mismatch')
        os.replace(tmp, final)  # Rename into its final name
    # Confirm the final file still matches after the rename.
    if sha256_file(final) != expected:
        raise IOError('published checkpoint hash mismatch')
    record = {'name': name, 'sha256': expected, 'bytes': final.stat().st_size, 'window_id': window_id,
              'sequence': sequence, 'owner': owner, 'utc': _utc(now).isoformat()}
    _replace_text(state_dir / LATEST, json.dumps(record))  # Point LATEST at the new checkpoint last
    return record


## Copy the latest checkpoint to local disk (restart within a window)
def restore_latest(state_dir, local_path):
    """Copy the latest verified checkpoint to local disk. Returns its record, or None if none exists."""
    state_dir = Path(state_dir)
    if not (state_dir / LATEST).exists():
        return None
    record = json.loads((state_dir / LATEST).read_text(encoding='utf-8'))
    src = state_dir / 'checkpoints' / record['name']
    # Confirm LATEST names a plain file and that file still has its recorded fingerprint.
    if Path(record['name']).name != record['name'] or sha256_file(src) != record['sha256']:
        raise IOError('latest checkpoint missing or hash mismatch: ' + record['name'])
    local_path = Path(local_path)
    # Never overwrite a local database.
    if local_path.exists():
        raise FileExistsError('local database already exists: ' + str(local_path))
    shutil.copyfile(src, local_path)
    # Confirm the local copy matches and that SQLite reports the database as intact.
    if sha256_file(local_path) != record['sha256']:
        raise IOError('restored copy hash mismatch')
    with sqlite3.connect(str(local_path)) as db:
        if db.execute('pragma integrity_check').fetchone()[0] != 'ok':
            raise IOError('restored checkpoint failed integrity check')
    return record


## Read LATEST without restoring anything
def peek_latest(state_dir):
    """Return the LATEST record without restoring, or None."""
    p = Path(state_dir) / LATEST
    return json.loads(p.read_text(encoding='utf-8')) if p.exists() else None


## Move leftovers from a killed checkpoint save out of the way
def quarantine_orphans(state_dir, *, owner, window_id, now):
    """v18: move leftovers of a killed publish aside before this window publishes again. Nothing is deleted.

    A run killed during publish_checkpoint can leave (a) a temporary checkpoint copy, (b) a complete
    checkpoint that LATEST never pointed to, or (c) a temporary LATEST file. LATEST stays the only source
    of truth, so these are never restored; but (b) would block the next publish of the same sequence
    number. Only this window's files are considered. The caller must hold the lock.
    Returns the names moved into checkpoints_orphaned/.
    """
    _require_owner(state_dir, owner)
    state_dir = Path(state_dir); cdir = state_dir / 'checkpoints'
    latest = peek_latest(state_dir) or {}
    latest_seq = latest.get('sequence') if latest.get('window_id') == window_id else 0  # Highest trusted number for this window
    prefix = 'checkpoint_%s_' % window_id
    victims = []
    if cdir.exists():
        for p in sorted(cdir.iterdir()):
            if not p.name.startswith(prefix):
                continue  # Other windows are never touched
            # (a) Temporary copies.
            if '.tmp-' in p.name:
                victims.append(p); continue
            # (b) Complete checkpoints numbered after the one LATEST points to.
            num = p.name[len(prefix):-7] if p.name.endswith('.sqlite') else ''  # -7 strips ".sqlite"
            if num.isdigit() and int(num) > latest_seq:
                victims.append(p)
    victims += sorted(state_dir.glob(LATEST + '.tmp-*'))  # (c) Temporary LATEST files
    if not victims:
        return []
    dest = state_dir / 'checkpoints_orphaned'; dest.mkdir(exist_ok=True)
    stamp = _utc(now).strftime('%Y%m%dT%H%M%SZ')
    for p in victims:
        os.replace(p, dest / ('%s_%s' % (stamp, p.name)))  # Move aside, renamed with the time
    return [p.name for p in victims]


## Delete older checkpoints of this window (approved retention rule)
def prune_window_checkpoints(state_dir, *, owner, window_id, keep_last):
    """Delete this window's checkpoints except the newest keep_last (v17; automatic retention requires the approved configuration).

    Only files named checkpoint_<window_id>_NNNN.sqlite are considered, so other windows are never
    touched. The file LATEST points to is always kept. Returns the names removed.
    """
    if keep_last < 1:
        raise ValueError('keep_last must be at least 1')
    _require_owner(state_dir, owner)
    cdir = Path(state_dir) / 'checkpoints'
    latest = (peek_latest(state_dir) or {}).get('name')
    prefix = 'checkpoint_%s_' % window_id
    # This window's numbered checkpoints, oldest first.
    mine = sorted(p for p in cdir.glob(prefix + '*.sqlite') if p.name[len(prefix):-7].isdigit())
    removed = []
    # Remove all but the newest keep_last, never the one LATEST points to.
    for p in mine[:-keep_last]:
        if p.name != latest:
            os.remove(p); removed.append(p.name)
    return removed
