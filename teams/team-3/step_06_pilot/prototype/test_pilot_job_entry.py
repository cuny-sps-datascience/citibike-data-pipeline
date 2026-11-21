import hashlib, tempfile, unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pilot_job_entry as je
import pilot_state as ps
from test_window_runner import Clock, feed_for

class EntryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.home = Path(self.tmp.name)
        self.frozen = {}
        for rel in je.FROZEN:
            p = self.home / rel; p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(rel.encode())
            self.frozen[rel] = hashlib.sha256(rel.encode()).hexdigest()
    def tearDown(self): self.tmp.cleanup()
    def go(self, clock, **kw):
        return je.run(home=self.home, local=self.home / 'local', now=clock.now, sleep=clock.sleep, get=feed_for(clock),
                      frozen=self.frozen, scorer_factory=None, **kw)
    def test_window_choice_uses_new_york_date(self):
        # 2026-11-13 01:00 UTC is still Nov 12 at 20:00 in New York (pm window)
        name, day = je.choose_window(datetime(2026, 11, 13, 1, 0, tzinfo=timezone.utc))
        self.assertEqual((name, day.isoformat()), ('pm', '2026-11-12'))
        self.assertEqual(je.choose_window(datetime(2026, 11, 12, 11, 20, tzinfo=timezone.utc))[0], 'am')  # 06:20 New York
        self.assertIsNone(je.choose_window(datetime(2026, 11, 12, 18, 0, tzinfo=timezone.utc))[0])        # 13:00 New York
    def test_outside_windows_exit_2(self):
        code, s = self.go(Clock(datetime(2026, 11, 12, 18, 0, tzinfo=timezone.utc))); self.assertEqual(code, 2)
    def test_frozen_change_exit_4(self):
        rel = next(iter(self.frozen)); (self.home / rel).write_bytes(b'tampered')
        code, s = self.go(Clock(datetime(2026, 11, 12, 20, 29, 50, tzinfo=timezone.utc))); self.assertEqual(code, 4); self.assertIn('changed', s['message'])
        self.assertFalse((self.home / je.STATE_NAME).exists())
    def test_pm_run_and_lock_held(self):
        c = Clock(datetime(2026, 11, 12, 20, 29, 50, tzinfo=timezone.utc))  # 15:29:50 New York
        code, s = self.go(c, max_slots=2)
        self.assertEqual(code, 0); self.assertEqual(s['window_id'], '20261112_pm'); self.assertEqual(s['polls'], 2)
        self.assertEqual(s['scorer']['mode'], 'logistic_fallback')
        ps.acquire_lock(self.home / je.STATE_NAME, owner='stuck', window_id='x', now=c.now())
        code, s = self.go(c, max_slots=1); self.assertEqual(code, 3); self.assertEqual(s['lock']['lock']['owner'], 'stuck')

if __name__ == '__main__':
    unittest.main()
