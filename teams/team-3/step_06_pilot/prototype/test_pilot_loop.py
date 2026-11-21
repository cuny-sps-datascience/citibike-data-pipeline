import json,sqlite3,unittest,tempfile
from datetime import datetime,timedelta,timezone
from pathlib import Path
from scoring import load_artifact,load_cohort,ZeroScoreError
from pilot_loop import init,run_poll,evaluate,expected_slot_gaps,mark_abandoned,resolve_dead_run,collection_watermark,run_health
R=Path(__file__).parent; A=load_artifact(R/'model_parameters.json'); SID=sorted(load_cohort(R/'cohort_reconstructed.csv',A['cohort_sha256_sorted_newline']))[0]
T=datetime(2026,9,28,12,tzinfo=timezone.utc)
def feed(t,installed=1):
 return {'info':{'last_updated':int(t.timestamp()),'data':{'stations':[{'station_id':SID,'capacity':20}]}},'status':{'last_updated':int(t.timestamp()),'data':{'stations':[{'station_id':SID,'last_reported':int(t.timestamp()),'num_bikes_available':3,'num_docks_available':0,'is_installed':installed,'is_renting':1,'is_returning':1}]}}}
class Pilot(unittest.TestCase):
 def setUp(self):self.db=sqlite3.connect(':memory:')
 def tearDown(self):self.db.close()
 def test_raw_scores_and_pending_then_known(self):
  for i in (0,1,2):
   t=T+timedelta(minutes=60*i); data=feed(t)
   run_poll(self.db,t.isoformat(),get=lambda k:data[k],now=lambda:t.isoformat())
  self.assertEqual(self.db.execute('select count(*) from raw').fetchone()[0],3)
  self.assertEqual(self.db.execute('select count(*) from predictions').fetchone()[0],6)
  pending=evaluate(self.db,as_of=(T+timedelta(hours=3)).isoformat());self.assertEqual(pending['pending'],6)
  out=evaluate(self.db,as_of=(T+timedelta(hours=3)).isoformat(),complete_through=(T+timedelta(hours=2,minutes=3)).isoformat())
  self.assertEqual(out['known'],4);self.assertEqual(out['pending'],2)
 def test_zero_scores_recorded_and_raw_retained(self):
  d=feed(T,0)
  with self.assertRaises(ZeroScoreError):run_poll(self.db,T.isoformat(),get=lambda k:d[k],now=lambda:T.isoformat())
  self.assertEqual(self.db.execute('select state from runs').fetchone()[0],'failed_observations_saved')
  self.assertEqual(self.db.execute('select count(*) from raw').fetchone()[0],1)
  self.assertEqual(self.db.execute('select count(*) from observations').fetchone()[0],1)
 def test_duplicate_slot_fails_closed(self):
  d=feed(T);run_poll(self.db,T.isoformat(),get=lambda k:d[k],now=lambda:T.isoformat())
  with self.assertRaises(sqlite3.IntegrityError):run_poll(self.db,T.isoformat(),get=lambda k:d[k],now=lambda:T.isoformat())
 def test_normalized_slots(self):
  d=feed(T); run_poll(self.db,T.isoformat(),get=lambda k:d[k],now=lambda:T.isoformat())
  with self.assertRaises(sqlite3.IntegrityError): run_poll(self.db,T.isoformat().replace('+00:00','Z'),get=lambda k:d[k],now=lambda:T.isoformat())
  with self.assertRaises(ValueError): run_poll(self.db,'invalid',get=lambda k:d[k],now=lambda:T.isoformat())
 def test_final_outcome_not_downgraded(self):
  for i in (0,1):
   t=T+timedelta(hours=i); d=feed(t); run_poll(self.db,t.isoformat(),get=lambda k:d[k],now=lambda:t.isoformat())
  evaluate(self.db,as_of=(T+timedelta(hours=3)).isoformat(),complete_through=(T+timedelta(hours=2)).isoformat())
  before=self.db.execute("select payload from outcomes order by run_id,outcome").fetchall()
  evaluate(self.db,as_of=(T+timedelta(hours=3)).isoformat())
  self.assertEqual(before,self.db.execute("select payload from outcomes order by run_id,outcome").fetchall())
 def test_missing_response_does_not_block_later(self):
  d=feed(T);run_poll(self.db,T.isoformat(),get=lambda k:d[k],now=lambda:T.isoformat())
  bad=T+timedelta(minutes=5)
  def fail(k): raise TimeoutError()
  with self.assertRaises(TimeoutError):run_poll(self.db,bad.isoformat(),get=fail,now=lambda:bad.isoformat())
  t=T+timedelta(hours=1); d=feed(t);run_poll(self.db,t.isoformat(),get=lambda k:d[k],now=lambda:t.isoformat())
  out=evaluate(self.db,as_of=(T+timedelta(hours=2)).isoformat(),complete_through=(T+timedelta(hours=1,minutes=3)).isoformat())
  self.assertEqual(out['known'],2)
  self.assertEqual(len(expected_slot_gaps(self.db,T.isoformat(),(T+timedelta(minutes=15)).isoformat())),1)
 def test_file_restart_and_advancing_clock(self):
  with tempfile.TemporaryDirectory() as directory:
   path=Path(directory)/'pilot.sqlite'
   with sqlite3.connect(path) as db:
    for i in range(14):
     t=T+timedelta(minutes=5*i); d=feed(t); ticks=iter([t,t+timedelta(milliseconds=10),t+timedelta(milliseconds=20),t+timedelta(milliseconds=70),t+timedelta(milliseconds=80),t+timedelta(milliseconds=90)])
     run_poll(db,t.isoformat(),get=lambda k:d[k],now=lambda:next(ticks).isoformat())
   with sqlite3.connect(path) as db:
    self.assertEqual(db.execute('select count(*) from runs').fetchone()[0],14)
    actual=evaluate(db,as_of=(T+timedelta(hours=2)).isoformat(),complete_through=(T+timedelta(hours=1,minutes=5)).isoformat())
    self.assertGreater(actual['known'],0)
    raw=db.execute('select metadata_ready,poll_start,poll_ready from raw order by poll_start limit 1').fetchone()
    self.assertLess(raw[0],raw[1]);self.assertLess(raw[1],raw[2])
 def test_unparseable_received_response_blocks_its_window(self):
  d=feed(T);run_poll(self.db,T.isoformat(),get=lambda k:d[k],now=lambda:T.isoformat())
  t=T+timedelta(hours=1); data=feed(t);data['status']['data']['stations']*=2
  with self.assertRaises(ValueError):run_poll(self.db,t.isoformat(),get=lambda k:data[k],now=lambda:t.isoformat())
  out=evaluate(self.db,as_of=(T+timedelta(hours=2)).isoformat(),complete_through=(T+timedelta(hours=1,minutes=3)).isoformat())
  self.assertEqual(out['pending'],2)
 def test_abandoned_start_is_not_silently_missing(self):
  d=feed(T);run_poll(self.db,T.isoformat(),get=lambda k:d[k],now=lambda:T.isoformat())
  t=T+timedelta(hours=1);self.db.execute('insert into runs values(?,?,?,?,?,?)',('stuck',t.isoformat(),t.isoformat(),None,'started','{}'));self.db.commit()
  self.assertEqual(mark_abandoned(self.db,as_of=(t+timedelta(minutes=10)).isoformat(),timeout_seconds=300),1)
  out=evaluate(self.db,as_of=(t+timedelta(hours=1)).isoformat(),complete_through=(t+timedelta(minutes=3)).isoformat())
  self.assertEqual(out['pending'],2)
 def test_dead_worker_without_saved_raw_becomes_collection_gap(self):
  init(self.db); t=T+timedelta(hours=1)
  self.db.execute('insert into runs values(?,?,?,?,?,?)',('stuck',t.isoformat(),t.isoformat(),None,'started','{}'));self.db.commit()
  with self.assertRaises(ValueError): resolve_dead_run(self.db,'stuck',worker_confirmed_dead=False,no_backfill=True,as_of=(t+timedelta(minutes=10)).isoformat())
  self.assertEqual(resolve_dead_run(self.db,'stuck',worker_confirmed_dead=True,no_backfill=True,as_of=(t+timedelta(minutes=10)).isoformat()),'failed_missing')
  self.assertIn('no persisted response',self.db.execute('select details from runs').fetchone()[0])
 def test_dead_worker_raw_without_observations_blocks_only_its_window(self):
  d=feed(T);run_poll(self.db,T.isoformat(),get=lambda k:d[k],now=lambda:T.isoformat())
  t=T+timedelta(hours=1); data=feed(t);self.db.execute('insert into runs values(?,?,?,?,?,?)',('stuck',t.isoformat(),t.isoformat(),None,'started','{}'))
  self.db.execute('insert into raw values(?,?,?,?,?,?)',('stuck',t.isoformat(),t.isoformat(),t.isoformat(),json.dumps(data['info']),json.dumps(data['status'])));self.db.commit()
  self.assertEqual(resolve_dead_run(self.db,'stuck',worker_confirmed_dead=True,no_backfill=True,as_of=(t+timedelta(minutes=10)).isoformat()),'failed_unresolved')
  out=evaluate(self.db,as_of=(t+timedelta(hours=1)).isoformat(),complete_through=(t+timedelta(minutes=3)).isoformat())
  self.assertEqual(out['pending'],2)
  later=T+timedelta(hours=2);d=feed(later);run_poll(self.db,later.isoformat(),get=lambda k:d[k],now=lambda:later.isoformat())
  out=evaluate(self.db,as_of=(later+timedelta(hours=2)).isoformat(),complete_through=(later+timedelta(hours=1,minutes=3)).isoformat())
  self.assertEqual(out['pending'],2); self.assertGreater(out['unknown'],0)
 def test_dead_worker_with_saved_observation_does_not_block(self):
  d=feed(T);run_poll(self.db,T.isoformat(),get=lambda k:d[k],now=lambda:T.isoformat())
  t=T+timedelta(hours=1);d=feed(t);rid,_=run_poll(self.db,t.isoformat(),get=lambda k:d[k],now=lambda:t.isoformat())
  self.db.execute("update runs set state='started' where run_id=?",(rid,));self.db.commit()
  self.assertEqual(resolve_dead_run(self.db,rid,worker_confirmed_dead=True,no_backfill=True,as_of=(t+timedelta(minutes=10)).isoformat()),'failed_observations_saved')
  out=evaluate(self.db,as_of=(t+timedelta(hours=1)).isoformat(),complete_through=(t+timedelta(minutes=3)).isoformat())
  self.assertEqual(out['known'],2)
 def test_collection_watermark_requires_external_evidence(self):
  with self.assertRaises(ValueError):collection_watermark(T.isoformat(),single_writer_verified=False,no_backfill=True)
  with self.assertRaises(ValueError):collection_watermark(T.isoformat(),single_writer_verified=True,no_backfill=False)
  self.assertEqual(collection_watermark(T.isoformat(),single_writer_verified=True,no_backfill=True),T.isoformat())
 def test_window_health_counts_failure_and_missing_separately(self):
  data=feed(T);run_poll(self.db,T.isoformat(),get=lambda k:data[k],now=lambda:T.isoformat())
  t=T+timedelta(minutes=5)
  def fail(k):raise TimeoutError()
  with self.assertRaises(TimeoutError):run_poll(self.db,t.isoformat(),get=fail,now=lambda:t.isoformat())
  health=run_health(self.db,T.isoformat(),(T+timedelta(minutes=15)).isoformat())
  self.assertEqual((health['attempted'],health['failed'],health['missing_slots']),(2,1,1))
  self.assertEqual(health['feed_age_seconds']['status']['max'],0)
  self.assertIsNone(health['unknown_share_of_finalized'])
if __name__=='__main__': unittest.main()
