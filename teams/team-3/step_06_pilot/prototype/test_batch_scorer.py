import unittest, math
from datetime import timedelta
import numpy as np
import test_candidate_scorer_acs as base
from candidate_scorer_acs import score
from batch_scorer import make_score_fn

class BatchTest(base.AcsCandidateTest):
    def test_batch_equals_single_row_path(self):
        s = self.load(); rng = np.random.default_rng(3)
        recs = [base.record(sid, bikes=int(rng.integers(0, 15)), docks=int(rng.integers(0, 15)), capacity=20) for sid in base.IDS]
        at = base.iso(base.T + timedelta(seconds=10))
        single = score(recs, s, at); fn, version, mode = make_score_fn(s); batch = fn(recs, at)
        self.assertEqual(mode, 'acs_candidate')
        a = {(r['station_id'], r['outcome']): r['probability'] for r in single[0]}
        b = {(r['station_id'], r['outcome']): r['probability'] for r in batch[0]}
        self.assertEqual(a.keys(), b.keys())
        self.assertLess(max(abs(a[k] - b[k]) for k in a), 1e-12)
        self.assertEqual({o: [x['station_id'] for x in v] for o, v in single[1].items()}, {o: [x['station_id'] for x in v] for o, v in batch[1].items()})
        self.assertEqual(single[2], batch[2])

    def test_batch_missing_station_rejected(self):
        s = self.load(); del s['acs']['s003']; fn, _, _ = make_score_fn(s)
        out, _, reasons = fn([base.record('s003'), base.record('s004')], base.iso(base.T + timedelta(seconds=10)))
        self.assertEqual({r['station_id'] for r in out}, {'s004'}); self.assertEqual(reasons['bike_station_features_missing'], 1)

    def test_fallback_passthrough(self):
        (self.d / 'acs.csv').write_text((self.d / 'acs.csv').read_text() + '\n')
        s = self.load(); fn, version, mode = make_score_fn(s)
        self.assertEqual(mode, 'logistic_fallback')
        out, _, _ = fn([base.record('s001')], base.iso(base.T + timedelta(seconds=10)))
        self.assertTrue(all(r['model_version'] == 'logistic-test' for r in out))

if __name__ == '__main__':
    unittest.main()
