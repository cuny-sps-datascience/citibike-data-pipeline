"""Synthetic tests for the v14 ACS candidate scorer and its logistic fallback."""
import copy
import hashlib
import json
import math
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import HistGradientBoostingClassifier

from candidate_scorer_acs import ACS_FEATURES, CANDIDATE_FEATURES, load_scorer, score
from scoring import FEATURES

T = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
IDS = ['s%03d' % i for i in range(100)]
COHORT_SHA = hashlib.sha256('\n'.join(sorted(IDS)).encode()).hexdigest()


def iso(t):
    return t.isoformat()


def record(sid, bikes=3, docks=12, capacity=20, renting=1, returning=1):
    return {'station_id': sid, 'fetched_at': iso(T), 'available_at': iso(T + timedelta(seconds=5)),
            'station_reported_at': iso(T - timedelta(seconds=30)), 'metadata_available_at': iso(T - timedelta(days=1)),
            'num_bikes_available': bikes, 'num_docks_available': docks, 'capacity': capacity,
            'is_installed': 1, 'is_renting': renting, 'is_returning': returning}


def baseline_artifact():
    m = {'feature_order': FEATURES, 'mean': [0.0] * 7, 'scale': [1.0] * 7, 'coef': [-0.1] * 7,
         'intercept': -1.0, 'classes': [0, 1]}
    return {'version': 'logistic-test', 'cohort_sha256_sorted_newline': COHORT_SHA, 'horizon_minutes': 60,
            'models': {'bike': m, 'dock': copy.deepcopy(m)}}


class AcsCandidateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); d = Path(self.tmp.name); self.d = d
        (d / 'cohort.csv').write_text('selection_order,station_id\n' + '\n'.join('%d,%s' % (i + 1, s) for i, s in enumerate(IDS)) + '\n')
        (d / 'baseline.json').write_text(json.dumps(baseline_artifact()))
        rng = np.random.default_rng(7)
        acs = pd.DataFrame({'station_id': IDS, **{c: rng.random(100) * 100 for c in ACS_FEATURES}})
        acs.loc[acs.station_id == 's005', ACS_FEATURES[1:]] = np.nan  # one station with missing percentage fields
        acs.to_csv(d / 'acs.csv', index=False)
        self.acs = acs
        x = pd.DataFrame(rng.random((600, len(CANDIDATE_FEATURES))) * 20, columns=CANDIDATE_FEATURES)
        x.loc[::7, ACS_FEATURES[1:]] = np.nan
        y = (x.iloc[:, 0] + x.iloc[:, 8].fillna(5) / 10 > 11).astype(int)
        self.folder = d / 'cand'; self.folder.mkdir()
        manifest = {'run_id': 'test', 'sklearn_version': sklearn.__version__, 'params': {'early_stopping': True},
                    'feature_order': CANDIDATE_FEATURES, 'cohort_sha256': COHORT_SHA,
                    'acs_table': {'sha256': hashlib.sha256((d / 'acs.csv').read_bytes()).hexdigest()}, 'models': {}}
        self.models = {}
        for o in ('bike', 'dock'):
            m = HistGradientBoostingClassifier(max_iter=10, min_samples_leaf=5, early_stopping=True, random_state=42).fit(x, y)
            name = '%s_acs_candidate_w.joblib' % o; joblib.dump(m, self.folder / name)
            manifest['models'][name] = {'features': CANDIDATE_FEATURES, 'sha256': hashlib.sha256((self.folder / name).read_bytes()).hexdigest()}
            self.models[o] = m
        self.manifest = manifest; self.write_manifest()

    def tearDown(self):
        self.tmp.cleanup()

    def write_manifest(self):
        (self.folder / 'manifest.json').write_text(json.dumps(self.manifest))

    def load(self):
        return load_scorer(self.folder, 'w', self.d / 'cohort.csv', self.d / 'baseline.json', self.d / 'acs.csv')

    def expected(self, outcome, sid, rec):
        b, k, c = rec['num_bikes_available'], rec['num_docks_available'], rec['capacity']
        local = T.astimezone(__import__('zoneinfo').ZoneInfo('America/New_York'))
        ph = (local.hour + local.minute / 60 + local.second / 3600) * 2 * math.pi / 24
        row = [b, k, c, b / c, k / c, math.sin(ph), math.cos(ph)] + self.acs.set_index('station_id').loc[sid, ACS_FEATURES].tolist()
        return self.models[outcome].predict_proba(pd.DataFrame([row], columns=CANDIDATE_FEATURES))[0, 1]

    def test_joins_acs_by_station_and_matches_direct_prediction(self):
        s = self.load(); self.assertEqual(s['mode'], 'acs_candidate')
        recs = [record('s001'), record('s002', bikes=1)]
        out, alerts, _ = score(recs, s, iso(T + timedelta(seconds=10)))
        for r in out:
            rec = next(x for x in recs if x['station_id'] == r['station_id'])
            self.assertLess(abs(r['probability'] - self.expected(r['outcome'], r['station_id'], rec)), 1e-12)
        self.assertTrue(all(r['model_version'] == 'acs_boosted_positive_test_w' for r in out))

    def test_missing_acs_values_pass_through_as_nan(self):
        s = self.load(); rec = record('s005')
        out, _, _ = score([rec], s, iso(T + timedelta(seconds=10)))
        self.assertTrue(math.isnan(s['acs']['s005'][1]))
        self.assertLess(abs(out[0]['probability'] - self.expected(out[0]['outcome'], 's005', rec)), 1e-12)

    def test_station_absent_from_table_is_rejected_not_imputed(self):
        s = self.load(); del s['acs']['s003']
        out, _, reasons = score([record('s003'), record('s004')], s, iso(T + timedelta(seconds=10)))
        self.assertEqual({r['station_id'] for r in out}, {'s004'})
        self.assertEqual(reasons['bike_station_features_missing'], 1); self.assertEqual(reasons['dock_station_features_missing'], 1)

    def test_already_empty_outcome_excluded(self):
        s = self.load(); out, _, reasons = score([record('s001', bikes=0)], s, iso(T + timedelta(seconds=10)))
        self.assertEqual(reasons['bike_already_empty'], 1); self.assertEqual([r['outcome'] for r in out], ['dock'])

    def assertFallback(self, text):
        s = self.load(); self.assertEqual(s['mode'], 'logistic_fallback'); self.assertIn(text, s['fallback_reason'])
        out, _, _ = score([record('s001')], s, iso(T + timedelta(seconds=10)))
        self.assertTrue(all(r['model_version'] == 'logistic-test' for r in out))

    def test_acs_hash_mismatch_falls_back(self):
        (self.d / 'acs.csv').write_text((self.d / 'acs.csv').read_text() + '\n'); self.assertFallback('ACS table checksum')

    def test_acs_table_missing_cohort_station_falls_back(self):
        self.acs[self.acs.station_id != 's009'].to_csv(self.d / 'acs.csv', index=False)
        self.manifest['acs_table']['sha256'] = hashlib.sha256((self.d / 'acs.csv').read_bytes()).hexdigest(); self.write_manifest()
        self.assertFallback('do not match the cohort')

    def test_model_hash_mismatch_falls_back(self):
        p = self.folder / 'dock_acs_candidate_w.joblib'; p.write_bytes(p.read_bytes() + b'x'); self.assertFallback('checksum')

    def test_feature_order_mismatch_falls_back(self):
        self.manifest['feature_order'] = list(reversed(CANDIDATE_FEATURES)); self.write_manifest(); self.assertFallback('feature order')

    def test_sklearn_version_mismatch_falls_back(self):
        self.manifest['sklearn_version'] = '0.0'; self.write_manifest(); self.assertFallback('version')

    def test_early_stopping_not_recorded_falls_back(self):
        self.manifest['params'] = {'early_stopping': 'auto'}; self.write_manifest(); self.assertFallback('early_stopping')

    def test_missing_artifact_folder_falls_back(self):
        (self.folder / 'manifest.json').unlink(); self.assertFallback('FileNotFoundError')


if __name__ == '__main__':
    unittest.main()
