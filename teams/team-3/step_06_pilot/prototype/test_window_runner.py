"""Synthetic tests for the v16 lock, checkpoints and Option A window runner (fake clock and feed)."""
import json, sqlite3, tempfile, unittest, zlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import pilot_state as ps
import window_runner as wr
from scoring import load_artifact, load_cohort

R = Path(__file__).parent
A = load_artifact(R / 'model_parameters.json')
COHORT = sorted(load_cohort(R / 'cohort_reconstructed.csv', A['cohort_sha256_sorted_newline']))
DAY = date(2026, 11, 12)


class Clock:
    def __init__(self, t): self.t = t
    def now(self): return self.t.isoformat()
    def sleep(self, s): self.t += timedelta(seconds=s)


def feed_for(clock, crash_after=None):
    calls = {'n': 0}
    def get(kind):
        if kind == 'status':
            calls['n'] += 1
            if crash_after is not None and calls['n'] > crash_after:
                raise KeyboardInterrupt('simulated kill')
        t = clock.t; ts = int(t.timestamp()); m = t.hour * 60 + t.minute
        if kind == 'info':
            return {'last_updated': ts, 'data': {'stations': [{'station_id': s, 'capacity': 20} for s in COHORT]}}
        rows = []
        for s in COHORT:
            b = (m // 5 + zlib.crc32(s.encode())) % 21
            rows.append({'station_id': s, 'last_reported': ts - 20, 'num_bikes_available': b, 'num_docks_available': 20 - b,
                         'is_installed': 1, 'is_renting': 1, 'is_returning': 1})
        return {'last_updated': ts, 'data': {'stations': rows}}
    return get


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); base = Path(self.tmp.name)
        self.state, self.local = base / 'state', base / 'local'
        slots, self.start, self.stop = wr.window_slots(DAY, 'am')
        self.clock = Clock(self.start - timedelta(seconds=30))

    def tearDown(self): self.tmp.cleanup()

    def run_w(self, **kw):
        kw.setdefault('get', feed_for(self.clock))
        return wr.run_window(day=DAY, name='am', state_dir=self.state, local_dir=self.local, now=self.clock.now, sleep=self.clock.sleep, **kw)

    def test_slots_cover_window_and_tail(self):
        slots, start, stop = wr.window_slots(DAY, 'am')
        self.assertEqual(start, datetime(2026, 11, 12, 11, 30, tzinfo=timezone.utc))  # 06:30 EST
        self.assertEqual(sum(k == 'score' for _, k in slots), 48)
        self.assertEqual(sum(k == 'collect' for _, k in slots), 13)
        self.assertEqual(slots[-1][0] - slots[0][0], timedelta(hours=5))

    def test_full_window(self):
        rep = self.run_w(owner='a' * 32)
        self.assertEqual(len(rep['polls']), 61); self.assertEqual(rep['errors'], []); self.assertEqual(rep['skipped_late'], [])
        self.assertEqual(rep['scorer']['mode'], 'logistic_fallback')
        self.assertEqual(rep['health']['missing_slots'], 0); self.assertEqual(rep['health']['failed'], 0)
        latest = json.loads((self.state / ps.LATEST).read_text()); self.assertEqual(latest['sequence'], 61)
        self.assertFalse((self.state / ps.LOCK).exists()); self.assertEqual(len(list((self.state / 'locks_released').iterdir())), 1)
        self.assertEqual(len(list((self.state / 'raw' / rep['window_id']).iterdir())), 61)
        local = self.local / 'check.sqlite'; ps.restore_latest(self.state, local)
        with sqlite3.connect(local) as db:
            states = dict(db.execute("select json_extract(o.payload,'$.status'),count(*) from outcomes o group by 1").fetchall())
            self.assertGreater(states.get('known', 0), 0); self.assertEqual(states.get('pending', 0), 0)
            raw_len = db.execute('select max(length(status_json)) from raw').fetchone()[0]
            self.assertLess(raw_len, 40000)  # only cohort stations kept in the database

    def test_lock_conflict_writes_nothing(self):
        ps.acquire_lock(self.state, owner='other', window_id='x', now=self.clock.now())
        with self.assertRaises(ps.LockHeld): self.run_w(owner='b' * 32)
        self.assertFalse((self.state / 'checkpoints').exists())

    def test_crash_then_takeover_restart(self):
        real_release = ps.release_lock; ps.release_lock = lambda *a, **k: None  # a killed process does not release
        try:
            with self.assertRaises(KeyboardInterrupt): self.run_w(owner='c' * 32, get=feed_for(self.clock, crash_after=5))
        finally:
            ps.release_lock = real_release
        self.assertTrue((self.state / ps.LOCK).exists())
        with self.assertRaises(ps.LockHeld): self.run_w(owner='d' * 32)
        with self.assertRaises(ps.LockHeld): self.run_w(owner='d' * 32, takeover_confirmed_dead_owner='c' * 32)  # heartbeat too recent
        self.clock.t += timedelta(minutes=25)
        rep = self.run_w(owner='d' * 32, takeover_confirmed_dead_owner='c' * 32)
        self.assertEqual(rep['took_over_from']['lock']['owner'], 'c' * 32)
        self.assertEqual(rep['restored_checkpoint']['sequence'], 5)
        self.assertEqual(len(rep['already_recorded']), 5); self.assertGreater(len(rep['not_attempted_before_start']), 0)
        self.assertEqual(len(rep['already_recorded']) + len(rep['not_attempted_before_start']) + len(rep['skipped_late']) + len(rep['polls']), 61)
        local = self.local / 'check.sqlite'; ps.restore_latest(self.state, local)
        with sqlite3.connect(local) as db:
            self.assertEqual(db.execute('select count(*) from runs').fetchone()[0], db.execute('select count(distinct slot) from runs').fetchone()[0])
        self.assertEqual(len(list((self.state / 'locks_cleared').iterdir())), 1)

    def test_scorer_factory_failure_falls_back(self):
        def bad(): raise ValueError('ACS table checksum mismatch')
        rep = self.run_w(owner='e' * 32, scorer_factory=bad, max_slots=2)
        self.assertEqual(rep['scorer']['mode'], 'logistic_fallback'); self.assertIn('checksum', rep['scorer_fallback_reason'])
        self.assertEqual([p['state'] for p in rep['polls']], ['success', 'success'])

    def test_custom_score_fn_used_for_scoring_slots_only(self):
        from scoring import score_snapshot
        calls = []
        def fn(rows, at):
            calls.append(at); return score_snapshot(rows, A, set(COHORT), at, positive_only=True, model_version='custom')
        rep = self.run_w(owner='f' * 32, scorer_factory=lambda: (fn, 'custom', 'acs_candidate'), max_slots=3)
        self.assertEqual(len(calls), 3); self.assertEqual(rep['scorer']['mode'], 'acs_candidate')

    def test_tampered_checkpoint_refused(self):
        self.run_w(owner='g' * 32, max_slots=2)
        latest = json.loads((self.state / ps.LATEST).read_text()); p = self.state / 'checkpoints' / latest['name']
        p.write_bytes(p.read_bytes() + b'x')
        with self.assertRaises(IOError): ps.restore_latest(self.state, self.local / 'x.sqlite')

    def test_non_owner_cannot_release_or_publish(self):
        ps.acquire_lock(self.state, owner='h', window_id='x', now=self.clock.now())
        with self.assertRaises(ps.LockHeld): ps.release_lock(self.state, owner='i', now=self.clock.now())
        with sqlite3.connect(':memory:') as db:
            with self.assertRaises(ps.LockHeld): ps.publish_checkpoint(db, self.state, owner='i', window_id='x', sequence=1, now=self.clock.now())


    def test_custom_test_window(self):
        st = self.clock.t + timedelta(minutes=1)
        st = st.replace(second=0, microsecond=0)
        rep = wr.run_window(day=DAY, name='test', state_dir=self.state, local_dir=self.local, now=self.clock.now, sleep=self.clock.sleep,
                            get=feed_for(self.clock), owner='j' * 32, custom_window=(st, st + timedelta(minutes=15), 600))
        self.assertEqual([p['kind'] for p in rep['polls']], ['score'] * 3 + ['collect'] * 3)
        self.assertEqual(rep['errors'], [])


    def test_fresh_database_per_window_and_metrics(self):
        self.run_w(owner='k' * 32, max_slots=2)
        pm_slots, pm_start, _ = wr.window_slots(DAY, 'pm'); self.clock.t = pm_start - timedelta(seconds=10)
        rep = wr.run_window(day=DAY, name='pm', state_dir=self.state, local_dir=self.local, now=self.clock.now, sleep=self.clock.sleep,
                            get=feed_for(self.clock), owner='l' * 32, max_slots=2)
        self.assertTrue(rep['fresh_database']); self.assertIsNone(rep['restored_checkpoint'])
        local = self.local / 'pm.sqlite'; ps.restore_latest(self.state, local)
        with sqlite3.connect(local) as db:
            self.assertEqual(db.execute('select count(*) from runs').fetchone()[0], 2)  # am runs not carried into pm
        self.assertTrue(all('checkpoint_bytes' in p and 'publish_seconds' in p for p in rep['polls']))
        self.assertIsNotNone(rep['health']['checkpoint_bytes_max'])

    def test_full_window_finalizes_everything(self):
        rep = self.run_w(owner='m' * 32)
        self.assertEqual(rep['health']['pending_outcomes_at_end'], 0)

    def test_late_start_counts_before_start_not_late(self):
        self.clock.t = self.start + timedelta(minutes=37)
        rep = self.run_w(owner='n' * 32, max_slots=1)
        self.assertEqual(len(rep['not_attempted_before_start']), 7); self.assertEqual(rep['skipped_late'], [])

    def test_retention_keeps_last_n_of_this_window_only(self):
        self.run_w(owner='o' * 32, max_slots=2)  # am window keeps all
        pm_slots, pm_start, _ = wr.window_slots(DAY, 'pm'); self.clock.t = pm_start - timedelta(seconds=10)
        rep = wr.run_window(day=DAY, name='pm', state_dir=self.state, local_dir=self.local, now=self.clock.now, sleep=self.clock.sleep,
                            get=feed_for(self.clock), owner='p' * 32, max_slots=6, retention_keep_last=3)
        names = sorted(p.name for p in (self.state / 'checkpoints').iterdir())
        self.assertEqual(sum('_am_' in n for n in names), 2)
        self.assertEqual([n[-11:-7] for n in names if '_pm_' in n], ['0004', '0005', '0006'])
        self.assertEqual(len(rep['pruned_checkpoints']), 3)
        ps.restore_latest(self.state, self.local / 'r.sqlite')

    def test_slow_publish_stops_window_loudly(self):
        ticks = iter([0, 61, 100, 200, 300, 400])
        with self.assertRaises(wr.PublishTooSlow):
            self.run_w(owner='q' * 32, max_slots=3, monotonic=lambda: next(ticks))
        self.assertFalse((self.state / ps.LOCK).exists())
        self.assertEqual(len(list((self.state / 'window_reports').iterdir())), 1)


if __name__ == '__main__':
    unittest.main()
