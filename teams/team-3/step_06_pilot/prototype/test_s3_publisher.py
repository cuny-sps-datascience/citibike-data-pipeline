"""P6 tests: S3 publisher against a fake store (no network, no S3)."""
import csv, hashlib, io, json, shutil, sqlite3, tempfile, unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pilot_job_entry as je
import pilot_state as ps
import s3_publisher as pub
import window_runner as wr
from test_window_runner import Clock, feed_for, DAY

SYN_PRED = ['data_kind', 'window_id', 'run_id', 'slot_utc', 'fetched_at_utc', 'target_at_utc', 'station_id', 'station_name', 'lat', 'lon',
            'outcome', 'model_version', 'current_count', 'capacity', 'probability', 'risk_rank', 'risk_tier', 'model_selected', 'low_count_selected']
SYN_OUT = ['data_kind', 'window_id', 'run_id', 'station_id', 'outcome', 'target_at_utc', 'status', 'label', 'matched_observation_utc',
           'evaluated_at_utc', 'model_selected', 'low_count_selected']
SYN_HEALTH = ['data_kind', 'window_id', 'window_start_utc', 'collection_end_utc', 'model_version', 'scorer_mode', 'polls_expected', 'polls_success',
              'polls_failed', 'missing_slots', 'skipped_late', 'scored_polls', 'predictions', 'outcomes_known', 'outcomes_unknown',
              'unknown_share_of_finalized', 'checkpoint_bytes_max', 'publish_seconds_max']


def rows(b):
    return list(csv.DictReader(io.StringIO(b.decode())))


class PublisherTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(); base = Path(cls.tmp.name)
        cls.state, cls.local = base / 'state', base / 'local'
        _, start, _ = wr.window_slots(DAY, 'am'); clock = Clock(start - timedelta(seconds=30))
        cls.rep = wr.run_window(day=DAY, name='am', state_dir=cls.state, local_dir=cls.local, owner='p' * 32, now=clock.now, sleep=clock.sleep,
                                get=feed_for(clock), retention_keep_last=3)
        cls.db = base / 'final.sqlite'; ps.restore_latest(cls.state, cls.db)

    @classmethod
    def tearDownClass(cls): cls.tmp.cleanup()

    def build(self):
        return pub.build_window_files(self.db, self.rep)

    def test_layout_columns_and_counts(self):
        files, notes = self.build()
        k = pub.window_keys('20261112_am')
        self.assertEqual(set(files), set(k.values()))
        self.assertEqual(k['predictions'], 'contributions/team-3/pilot/v1/predictions/date=2026-11-12/predictions_20261112_am.csv')
        p, o = rows(files[k['predictions']]), rows(files[k['outcomes']])
        self.assertEqual(list(p[0].keys()), SYN_PRED); self.assertEqual(list(o[0].keys()), SYN_OUT)
        self.assertTrue(all(r['data_kind'] == 'PILOT' for r in p + o))
        self.assertTrue(all(int(r['current_count']) > 0 for r in p))
        self.assertEqual(len(p), len(o))
        self.assertTrue(all(r['status'] in ('known', 'unknown') for r in o))
        h = json.loads(files[k['run_health']])
        self.assertTrue(set(SYN_HEALTH) <= set(h)); self.assertEqual(h['polls_expected'], 61); self.assertEqual(h['scored_polls'], 48)
        self.assertEqual(h['predictions'], len(p)); self.assertEqual(h['outcomes_known'] + h['outcomes_unknown'], len(o))
        with sqlite3.connect(self.db) as db:
            n = db.execute("select count(*) from predictions where json_extract(payload,'$.current_count')>0").fetchone()[0]
        self.assertEqual(len(p), n)
        self.assertTrue(all(r['slot_utc'].endswith('Z') and r['target_at_utc'].endswith('Z') for r in p))

    def test_ranks_tiers_and_selections(self):
        files, _ = self.build(); p = rows(files[pub.window_keys('20261112_am')['predictions']])
        by = {}
        for r in p: by.setdefault((r['run_id'], r['outcome']), []).append(r)
        for grp in by.values():
            self.assertEqual(sorted(int(r['risk_rank']) for r in grp), list(range(1, len(grp) + 1)))
            self.assertEqual(sum(int(r['model_selected']) for r in grp), min(5, len(grp)))
            self.assertEqual({r['station_id'] for r in grp if r['model_selected'] == '1'}, {r['station_id'] for r in grp if int(r['risk_rank']) <= 5})
            low = sorted(grp, key=lambda r: (int(r['current_count']), r['station_id']))[:5]
            self.assertEqual({r['station_id'] for r in low}, {r['station_id'] for r in grp if r['low_count_selected'] == '1'})
            self.assertTrue(all(r['risk_tier'] == ('high' if int(r['risk_rank']) <= 5 else 'medium' if int(r['risk_rank']) <= 15 else 'low') for r in grp))

    def test_write_once_readme_once_and_rerun_is_noop(self):
        store = pub.FakeStore()
        r1 = pub.publish_window(db_path=self.db, report=self.rep, store=store, mode='on')
        self.assertEqual(len(store.objects), 4)
        self.assertTrue(all(v['action'] == 'written' for v in r1['results'].values()))
        r2 = pub.publish_window(db_path=self.db, report=self.rep, store=store, mode='on')
        self.assertEqual(len(store.objects), 4)
        self.assertTrue(all(v['action'] == 'already_present_identical' for v in r2['results'].values()))
        self.assertNotIn('contributions/team-3/pilot/v1/README_schema.md', r2['results'])

    def test_changed_content_becomes_v2_never_overwrites(self):
        store = pub.FakeStore(); pub.publish_window(db_path=self.db, report=self.rep, store=store, mode='on')
        key = pub.window_keys('20261112_am')['run_health']; original = store.objects[key]
        rep2 = json.loads(json.dumps(self.rep)); rep2['errors'] = ['late correction']
        r = pub.publish_window(db_path=self.db, report=rep2, store=store, mode='on')
        self.assertEqual(store.objects[key], original)
        self.assertEqual(r['results'][key]['target'], key.replace('.json', '_v2.json'))
        self.assertEqual(r['results'][key]['action'], 'written_correction_v2')
        rep3 = json.loads(json.dumps(self.rep)); rep3['errors'] = ['second correction', 'third']
        r = pub.publish_window(db_path=self.db, report=rep3, store=store, mode='on')
        self.assertEqual(r['results'][key]['target'], key.replace('.json', '_v3.json'))

    def test_prefix_guard(self):
        with self.assertRaises(pub.PublishError):
            pub.write_files({'contributions/team-3/pilot-synthetic/v1/x.csv': b'a'}, pub.FakeStore())
        with self.assertRaises(pub.PublishError):
            pub.write_files({'contributions/team-3/pilot/v1/../x.csv': b'a'}, pub.FakeStore())

    def test_readback_mismatch_is_an_error(self):
        store = pub.FakeStore(); store.corrupt_on.add(pub.window_keys('20261112_am')['outcomes'])
        with self.assertRaises(pub.PublishError):
            pub.publish_window(db_path=self.db, report=self.rep, store=store, mode='on')

    def test_pending_outcomes_withheld(self):
        d = Path(self.tmp.name) / 'pending.sqlite'; shutil.copyfile(self.db, d)
        with sqlite3.connect(d) as db:
            db.execute("update outcomes set payload=json_set(payload,'$.status','pending') where rowid=(select min(rowid) from outcomes)")
        files, notes = pub.build_window_files(d, self.rep)
        self.assertNotIn(pub.window_keys('20261112_am')['outcomes'], files); self.assertIn('outcomes_withheld', notes)

    def test_dry_run_writes_nothing_to_store(self):
        store = pub.FakeStore(); stage = Path(self.tmp.name) / 'stage'
        rec = pub.publish_window(db_path=self.db, report=self.rep, store=store, mode='dry_run', staging_dir=stage)
        self.assertEqual(store.puts, []); self.assertTrue(rec['complete'])
        self.assertEqual(sorted(p.name for p in (stage / '20261112_am').iterdir()),
                         ['README_schema.md', 'outcomes_20261112_am.csv', 'predictions_20261112_am.csv', 'run_health_20261112_am.json'])


class EntryPublishTest(unittest.TestCase):
    """Publishing inside the job entry point: failure never fails the window; backlog retried; state untouched."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.home = Path(self.tmp.name); self.frozen = {}
        for rel in je.FROZEN:
            p = self.home / rel; p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(rel.encode())
            self.frozen[rel] = hashlib.sha256(rel.encode()).hexdigest()
        self.state = self.home / je.STATE_NAME
    def tearDown(self): self.tmp.cleanup()

    def go(self, clock, **kw):
        return je.run(home=self.home, local=self.home / 'local', now=clock.now, sleep=clock.sleep, get=feed_for(clock),
                      frozen=self.frozen, scorer_factory=None, retention_keep_last=3, **kw)

    def snapshot(self):
        return {p.relative_to(self.state).as_posix(): ps.sha256_file(p) for p in self.state.rglob('*')
                if p.is_file() and p.parts[len(self.state.parts)] in ('checkpoints', 'raw') or p.name == ps.LATEST}

    def test_failure_then_backlog_retry(self):
        _, start, _ = wr.window_slots(DAY, 'am'); clock = Clock(start - timedelta(seconds=30))
        store = pub.FakeStore(); store.fail_on.add(pub.window_keys('20261112_am')['outcomes'])
        code, s = self.go(clock, publish_mode='on', store_factory=lambda: store)
        self.assertEqual(code, 0); self.assertTrue(s['publish']['windows']['20261112_am'].startswith('FAILED'))
        before = self.snapshot()
        # README and predictions were written; outcomes failed; run health (written last) is absent until the retry
        self.assertIn(pub.window_keys('20261112_am')['predictions'], store.objects)
        self.assertNotIn(pub.window_keys('20261112_am')['run_health'], store.objects)
        # pm window: publisher retries the am backlog and publishes pm
        _, pm_start, _ = wr.window_slots(DAY, 'pm'); clock.t = pm_start - timedelta(seconds=30); store.fail_on.clear()
        code, s = self.go(clock, publish_mode='on', store_factory=lambda: store)
        self.assertEqual(code, 0)
        self.assertEqual(s['publish']['windows'], {'20261112_pm': 'published', '20261112_am': 'published'})
        self.assertEqual(len([k for k in store.objects if 'README' in k]), 1)
        self.assertEqual(len(store.objects), 7)
        after = self.snapshot()
        self.assertTrue(all(after.get(k) == v for k, v in before.items() if k.startswith('checkpoints/checkpoint_20261112_am') or k.startswith('raw/20261112_am')))
        log = json.loads((self.state / 'publish_log' / '20261112_am.json').read_text())
        self.assertEqual([a['ok'] for a in log['attempts']], [False, True])
        # a third run publishes nothing new
        code, s = self.go(Clock(pm_start + timedelta(hours=7)), publish_mode='on', store_factory=lambda: store)
        self.assertEqual(code, 2)  # outside windows: no window, no publishing

    def test_store_factory_crash_does_not_fail_window(self):
        _, start, _ = wr.window_slots(DAY, 'am'); clock = Clock(start - timedelta(seconds=30))
        def boom(): raise RuntimeError('secret scope unavailable')
        code, s = self.go(clock, publish_mode='on', store_factory=boom, max_slots=3)
        self.assertEqual(code, 0); self.assertIn('secret scope', s['publish']['error'])

    def test_off_by_default_and_dry_run(self):
        _, start, _ = wr.window_slots(DAY, 'am'); clock = Clock(start - timedelta(seconds=30))
        code, s = self.go(clock)
        self.assertEqual(code, 0); self.assertNotIn('publish', s); self.assertFalse((self.state / 'publish_staging').exists())
        code, s = self.go(Clock(wr.window_slots(DAY, 'pm')[1] - timedelta(seconds=30)), publish_mode='dry_run')
        self.assertEqual(s['publish']['windows'], {'20261112_pm': 'staged', '20261112_am': 'staged'})
        self.assertEqual(len(list((self.state / 'publish_staging').iterdir())), 2)

    def test_partial_window_not_published(self):
        _, start, _ = wr.window_slots(DAY, 'am'); clock = Clock(start - timedelta(seconds=30))
        code, s = self.go(clock, publish_mode='dry_run', max_slots=3)  # stopped early: outcomes still pending
        self.assertEqual(code, 0); self.assertIn('not ready', s['publish']['windows']['20261112_am'])


if __name__ == '__main__':
    unittest.main()
