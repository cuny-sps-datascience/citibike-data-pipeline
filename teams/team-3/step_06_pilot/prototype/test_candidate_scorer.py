import hashlib,json,tempfile,unittest
from pathlib import Path
import joblib,numpy as np,pandas as pd,sklearn
from sklearn.ensemble import HistGradientBoostingClassifier
from scoring import FEATURES,ZeroScoreError
from candidate_scorer import load_candidate,score_candidate
from test_package import ROOT,A,C,row,T,iso

class CandidateTest(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name)
  rng=np.random.default_rng(42);x=pd.DataFrame(rng.random((160,7)),columns=FEATURES);y=(x.iloc[:,0]>.5).astype(int)
  self.model=HistGradientBoostingClassifier(max_iter=5,min_samples_leaf=5,random_state=42).fit(x,y)
  meta={'sklearn_version':sklearn.__version__,'cohort_sha256':A['cohort_sha256_sorted_newline'],'models':{}}
  for outcome in ('bike','dock'):
   name=outcome+'_boosted_positive.joblib';joblib.dump(self.model,self.folder/name)
   meta['models'][name]={'features':FEATURES,'sha256':hashlib.sha256((self.folder/name).read_bytes()).hexdigest()}
  (self.folder/'manifest.json').write_text(json.dumps(meta));self.meta=meta
 def tearDown(self):self.temp.cleanup()
 def test_reference_prediction_and_candidate_filters(self):
  a,c,models=load_candidate(self.folder,ROOT/'cohort_reconstructed.csv',ROOT/'model_parameters.json')
  r=row();out,alerts,reasons=score_candidate([r],a,c,models,iso(T))
  import math
  x=[3,12,20,3/20,12/20,math.sin(2*math.pi*8/24),math.cos(2*math.pi*8/24)]
  expected=self.model.predict_proba(pd.DataFrame([x],columns=FEATURES))[0,1]
  self.assertAlmostEqual(out[0]['probability'],expected);self.assertEqual(out[0]['model_version'],'experimental_boosted_positive_2026-11-10')
  self.assertEqual(reasons['dock_service_disabled'],1);self.assertEqual(len(alerts['bike']),1)
  r['num_bikes_available']=0
  with self.assertRaises(ZeroScoreError) as e:score_candidate([r],a,c,models,iso(T))
  self.assertEqual(e.exception.reasons['bike_already_empty'],1)
 def test_artifact_tampering_fails_before_load(self):
  p=self.folder/'bike_boosted_positive.joblib';p.write_bytes(p.read_bytes()+b'x')
  with self.assertRaisesRegex(ValueError,'checksum'):load_candidate(self.folder,ROOT/'cohort_reconstructed.csv',ROOT/'model_parameters.json')
 def test_version_mismatch_fails(self):
  self.meta['sklearn_version']='invalid';(self.folder/'manifest.json').write_text(json.dumps(self.meta))
  with self.assertRaisesRegex(ValueError,'version'):load_candidate(self.folder,ROOT/'cohort_reconstructed.csv',ROOT/'model_parameters.json')
if __name__=='__main__':unittest.main()
