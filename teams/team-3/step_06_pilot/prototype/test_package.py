import unittest, io, json, contextlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
import live_demo
from scoring import load_artifact, load_cohort, score_snapshot, ZeroScoreError
from outcomes import match_outcomes
from normalization import flag, integer
from replay_guard import guard_interval

ROOT=Path(__file__).parent
A=load_artifact(ROOT/'model_parameters.json')
C=load_cohort(ROOT/'cohort_reconstructed.csv',A['cohort_sha256_sorted_newline'])
SID=sorted(C)[0]
T=datetime(2026,9,28,12,tzinfo=timezone.utc)
def iso(t): return t.isoformat()
def row(t=T):
 return dict(station_id=SID, fetched_at=iso(t),available_at=iso(t),station_reported_at=iso(t),metadata_available_at=iso(t-timedelta(days=1)),num_bikes_available=3,num_docks_available=12,capacity=20.0,is_installed=1,is_renting=1,is_returning=0)
def pred(): return dict(station_id=SID,outcome='bike',fetched_at=iso(T),target_at=iso(T+timimedelta()),model_version='test')
def timimedelta(): return timedelta(hours=1)
class PackageTests(unittest.TestCase):
 def test_score_and_reasons(self):
  r=row(); scores, alerts, reasons=score_snapshot([r],A,C,iso(T))
  self.assertEqual(len(scores),1); self.assertEqual(len(alerts['bike']),1); self.assertEqual(reasons['dock_service_disabled'],1)
  self.assertTrue(0 <= scores[0]['probability'] <= 1)
 def test_zero_scores_and_skew(self):
  r=row();r['station_reported_at']=iso(T+timedelta(microseconds=500000))
  with self.assertRaises(ZeroScoreError) as err: score_snapshot([r],A,C,iso(T))
  self.assertEqual(err.exception.reasons['report_after_poll_start_clock_skew'],1)
 def test_strict_values(self):
  self.assertEqual(integer(20.0),20);self.assertTrue(flag(1))
  for v in ('1',2,None):
   with self.assertRaises(ValueError): flag(v)
  for v in (True,1.5,float('nan')):
   with self.assertRaises(ValueError): integer(v)
  try: import numpy as np
  except ImportError: return
  self.assertEqual(integer(np.int64(2)),2);self.assertTrue(flag(np.bool_(True)))
 def test_invalid_nearest_is_not_replaced(self):
  near=row(T+timedelta(hours=1));near['is_renting']=0
  far=row(T+timedelta(hours=1,seconds=20));far['num_bikes_available']=0
  result=match_outcomes([pred()],[near,far],as_of=iso(T+timedelta(hours=2)),complete_through=iso(T+timedelta(hours=2)))
  self.assertEqual((result[0]['status'],result[0]['reason']),('unknown','nearest_observation_ineligible'))
 def test_duplicate_conflict_fails(self):
  a=row(T+timedelta(hours=1)); b=dict(a,num_bikes_available=0)
  with self.assertRaisesRegex(ValueError,'duplicate'): match_outcomes([pred()],[a,b],as_of=iso(T+timedelta(hours=2)),complete_through=iso(T+timedelta(hours=2)))
 def test_simulated_feed(self):
  now=datetime.now(timezone.utc); info=[dict(station_id=SID,capacity=20)]
  status=[dict(station_id=SID,last_reported=int(now.timestamp()),num_bikes_available=3,num_docks_available=12,is_installed=1,is_renting=1,is_returning=0)]
  old=live_demo.get_feed
  try:
   live_demo.get_feed=lambda kind: info if kind=='info' else status
   out=io.StringIO()
   with contextlib.redirect_stdout(out): live_demo.main()
   result=json.loads(out.getvalue());self.assertEqual(result['score_counts'],{'bike':1,'dock':0});self.assertEqual(result['cohort_missing'],99)
   live_demo.get_feed=lambda kind: info+info if kind=='info' else status
   with self.assertRaisesRegex(ValueError,'duplicate'): live_demo.main()
  finally: live_demo.get_feed=old
 def test_guard(self):
  guard_interval('2026-09-28T04:00:00Z','2026-10-05T04:00:00Z')
  for a,b in [('2026-10-05T04:00:00Z','2026-10-05T05:00:00Z'),('2026-10-05T03:59:59Z','2026-10-05T04:00:01Z')]:
   with self.assertRaises(ValueError): guard_interval(a,b)
if __name__=='__main__': unittest.main()
