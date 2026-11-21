# Databricks notebook source
# Team 3 commented copy (step 02). Comments were added on November 16, 2026; the code is unchanged.
# Terms used in these comments (they match the v06 reports and the v06.1 deck):
#   Poll: one fetch of the public Citi Bike station feed.
#   Risk Score: a model's predicted probability of a shortage; the five highest Risk Scores per poll become alerts.
#   True positive: an alert (a flagged station observation) with a confirmed shortage at the target time.
#   Recall = true positives / all confirmed shortages among eligible stations.
#   Precision = true positives / alerts with known outcomes.
#   Methods: histogram gradient boosting (scikit-learn HistGradientBoostingClassifier), logistic regression,
#   and the low-count rule (the stations with the fewest bikes or open docks right now).
#   Code names such as `caught`, `probability` and `acs_candidate` are kept so saved results still load.
# This is the development notebook where the model type was chosen. It was run interactively, cell by cell,
# over several sessions, so some cells only inspect or reload results after a restart; their labels say so.
# Main path: cells 0 to 13 (data, labels, model comparison, intervals, calibration) and cells 17 to 19 (ACS).
# Cells 20 to 39 test July trip and weather features, which were not kept. Cells 40 to 55 audit results by
# station-area group and check trip coverage. It reads only Aug 24 to Oct 4 data, never the Oct 5 to 11 test week.
# The ACS table embedded in cell 17 is the copy step 03 compares its ACS file against.
## Cell 0: packages, S3 connection and date bounds
import io, boto3, pandas as pd, numpy as np, pyarrow.parquet as pq
from botocore.config import Config
from datetime import timedelta
## S3 connection (keys are read at run time from the Databricks secret scope, never stored here)
scope='citibike-team3-secrets'; bucket='cuny-sps-citibike-archive'
client=boto3.client('s3',aws_access_key_id=dbutils.secrets.get(scope,'aws_access_key_id'),aws_secret_access_key=dbutils.secrets.get(scope,'aws_secret_access_key'),region_name='us-west-2',config=Config(connect_timeout=10,read_timeout=30,retries={'max_attempts':2}))
## Date bounds: training Aug 24 to Sep 27, validation Sep 28 to Oct 4 (New York midnight in UTC)
START=pd.Timestamp('2026-08-24T04:00:00Z'); VALID=pd.Timestamp('2026-09-28T04:00:00Z'); END=pd.Timestamp('2026-10-05T04:00:00Z')
print('TRAIN_VALIDATION_ONLY',START.isoformat(),VALID.isoformat(),END.isoformat())


# COMMAND ----------

## Cell 1: rebuild the 100-station cohort and read Aug 24 to Oct 4 station status
# Reconstruct the fixed cohort and read only dated training/validation partitions.
import hashlib
from pyspark.sql import functions as F, Window
spark.conf.set('spark.sql.session.timeZone','UTC')
# Station info as of the training start; keep stations with positive capacity
info=pq.read_table(io.BytesIO(client.get_object(Bucket=bucket,Key='exports/station_info.parquet')['Body'].read()),columns=['station_id','fetched_at','capacity']).to_pandas()
info['fetched_at']=pd.to_datetime(info.fetched_at,utc=True)
pre=info.loc[info.fetched_at<=START].sort_values('fetched_at').drop_duplicates('station_id',keep='last')
pre=pre.loc[pre.capacity>0,['station_id']]
ids_df=spark.createDataFrame(pre.astype({'station_id':'str'}))
# The fixed cohort: the first 100 stations in a hash order (the same method is used in every later step)
C={r.station_id for r in ids_df.orderBy(F.xxhash64('station_id'),'station_id').limit(100).collect()}
# Confirm the cohort matches its fingerprint
assert len(C)==100 and hashlib.sha256('\n'.join(sorted(C)).encode()).hexdigest()=='3d5bf5cd0594981fb60b995a08f6c305b2927122ae1299021e413a1b35414ff3'
info=info.loc[info.station_id.isin(C)].rename(columns={'fetched_at':'metadata_time'}).sort_values('metadata_time')
columns=['station_id','fetched_at','station_reported_at','num_bikes_available','num_docks_available','is_installed','is_renting','is_returning']
parts=[]
# Read one status file per day, Aug 24 to Oct 4, keeping cohort stations only
for day in pd.date_range('2026-08-24','2026-10-04',freq='D'):
    prefix='exports/station_status/nyc_date='+day.strftime('%Y-%m-%d')+'/'
    items=client.list_objects_v2(Bucket=bucket,Prefix=prefix).get('Contents',[])
    keys=[o['Key'] for o in items if o['Key'].endswith('.parquet')]
    if len(keys)!=1: raise ValueError('Unexpected number of files for '+day.strftime('%Y-%m-%d'))
    frame=pq.read_table(io.BytesIO(client.get_object(Bucket=bucket,Key=keys[0])['Body'].read()),columns=columns).to_pandas()
    parts.append(frame[frame.station_id.isin(C)])
    if len(parts)%7==0: print('LOADED_DAYS',len(parts),flush=True)
raw=pd.concat(parts,ignore_index=True)
for col in ['fetched_at','station_reported_at']: raw[col]=pd.to_datetime(raw[col],utc=True)
# Keep the development period; confirm one row per station per poll
raw=raw.loc[(raw.fetched_at>=START)&(raw.fetched_at<END)].sort_values('fetched_at')
assert not raw.duplicated(['station_id','fetched_at']).any()
print('READ_COMPLETE',len(raw),'cohort',len(C),'test_partition_read',False,flush=True)


# COMMAND ----------

## Cell 2: usable snapshots, 60-minute labels and the seven station features
# Point-in-time metadata and closest later observation; historical availability is simulated.
# Attach the station info that was known at each poll (no later metadata)
df=pd.merge_asof(raw,info,by='station_id',left_on='fetched_at',right_on='metadata_time',direction='backward')
# Usable snapshot: report at most 5 minutes old, positive capacity, installed; the outcome must also be in service
age=(df.fetched_at-df.station_reported_at).dt.total_seconds()
base=age.between(0,300)&(df.capacity>0)&(df.num_bikes_available>=0)&(df.num_docks_available>=0)&df.is_installed.fillna(False).astype(bool)
df['bike_ok']=base&df.is_renting.fillna(False).astype(bool)
df['dock_ok']=base&df.is_returning.fillna(False).astype(bool)
df['both']=df.bike_ok&df.dock_ok
# Also require usable snapshots 15 and 30 minutes earlier (within 5.5 minutes)
for m in (15,30):
    left=df[['station_id','fetched_at']].copy(); left['cutoff']=left.fetched_at-pd.Timedelta(minutes=m)
    right=df[['station_id','fetched_at','bike_ok','dock_ok']].rename(columns={'fetched_at':'lagtime','bike_ok':'lagbike','dock_ok':'lagdock'})
    lag=pd.merge_asof(left.sort_values('cutoff'),right.sort_values('lagtime'),by='station_id',left_on='cutoff',right_on='lagtime',direction='backward')
    gap=(lag.cutoff-lag.lagtime).dt.total_seconds();lag['valid']=gap.between(0,330)&lag.lagbike.fillna(False)&lag.lagdock.fillna(False)
    df=df.merge(lag[['station_id','fetched_at','valid']],on=['station_id','fetched_at'],how='left',validate='one_to_one')
    df['both'] &= df.pop('valid').fillna(False)
    print('LAG_DONE',m,flush=True)
# Leave out the last 62.5 minutes of each period so no label crosses into the next one
end_train=VALID-pd.Timedelta(seconds=3750); end_val=END-pd.Timedelta(seconds=3750)
within=((df.fetched_at<end_train)|((df.fetched_at>=VALID)&(df.fetched_at<end_val)))
examples=df.loc[within & df.both].copy()
## Labels: is the station at zero bikes (or zero docks) about 60 minutes later? Unknown if that later snapshot is unusable
examples['target_at']=examples.fetched_at+pd.Timedelta(minutes=60)
future=df[['station_id','fetched_at','bike_ok','dock_ok','num_bikes_available','num_docks_available']].rename(columns={'fetched_at':'future_at','bike_ok':'future_bike_ok','dock_ok':'future_dock_ok','num_bikes_available':'future_bikes','num_docks_available':'future_docks'})
examples=pd.merge_asof(examples.sort_values('target_at'),future.sort_values('future_at'),by='station_id',left_on='target_at',right_on='future_at',direction='nearest',tolerance=pd.Timedelta(seconds=150))
examples['split']=np.where(examples.fetched_at<VALID,'train','validation')
for o in ['bike','dock']:
    label=np.where(examples[o+'_ok']&examples['future_'+o+'_ok'].fillna(False), (examples['future_'+o+'s']==0).astype(float), np.nan)
    examples[o+'_label']=label
## Station features: counts, capacity, fractions, and time of day as a circle (so 23:59 sits next to 00:00)
examples['bike_fraction']=examples.num_bikes_available/examples.capacity
examples['dock_fraction']=examples.num_docks_available/examples.capacity
local=examples.fetched_at.dt.tz_convert('America/New_York')
phase=(local.dt.hour+local.dt.minute/60+local.dt.second/3600)*2*np.pi/24
examples['hour_sin']=np.sin(phase);examples['hour_cos']=np.cos(phase)
assert not examples.duplicated(['station_id','fetched_at']).any()
# Display row and positive counts for comparison with earlier runs
for o,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
    tr=examples[(examples.split=='train')&examples[o+'_label'].notna()]
    val=examples[examples.split=='validation']; known=val[val[o+'_label'].notna()]; pool=val[val[o+'_ok']&(val[count]>0)]
    print('MATCH_COUNTS',o,'train',len(tr),'train_positive',int(tr[o+'_label'].sum()),'val_known',len(known),'val_positive',int(known[o+'_label'].sum()),'candidates',len(pool),'candidate_positive',int(pool[o+'_label'].sum()),flush=True)


# COMMAND ----------

## Cell 3: compare three models and the low-count rule on the Sep 28 to Oct 4 validation week
# Exploratory comparison only. All selection uses the same validation candidates.
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import average_precision_score,log_loss
## The seven station features
features=['num_bikes_available','num_docks_available','capacity','bike_fraction','dock_fraction','hour_sin','hour_cos']
assert np.isfinite(examples[features].to_numpy()).all()
comparison=[]; fitted={}; selected={}
for o,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
    lab=o+'_label';train=examples[(examples.split=='train')&examples[lab].notna()]
    val=examples[(examples.split=='validation')&examples[o+'_ok']&(examples[count]>0)].copy()
    known=val[lab].notna(); print('FIT_START',o,'all',len(train),'positive_count',int((train[count]>0).sum()),flush=True)
    # Models: logistic on all rows (original style), logistic on rows with a positive count, boosted tree on rows with a positive count
    plans=[('existing_style',train,make_pipeline(StandardScaler(),LogisticRegression(solver='lbfgs',max_iter=300))),('positive_count',train[train[count]>0],make_pipeline(StandardScaler(),LogisticRegression(solver='lbfgs',max_iter=300))),('boosted_positive',train[train[count]>0],HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=15,min_samples_leaf=200,learning_rate=0.1,random_state=42))]
    for name,fit_rows,model in plans:
        model.fit(fit_rows[features],fit_rows[lab].astype(int)); fitted[o,name]=model
        if name!='boosted_positive': assert model.named_steps['logisticregression'].n_iter_.max()<300
        risk=model.predict_proba(val[features])[:,1];val['risk']=risk
        # Top five per poll by Risk Score; count true positives, known non-shortages and unknown labels
        picks=val.sort_values(['fetched_at','risk','station_id'],ascending=[True,False,True]).groupby('fetched_at',sort=False).head(5).copy()
        n_known=int(picks[lab].notna().sum()); caught=int(picks[lab].sum()); unknown=len(picks)-n_known
        ap=average_precision_score(val.loc[known,lab].astype(int),risk[known.to_numpy()]); loss=log_loss(val.loc[known,lab].astype(int),risk[known.to_numpy()],labels=[0,1])
        selected[o,name]=picks[['station_id','fetched_at',lab]].copy()
        row=(o,name,len(fit_rows),len(val),len(picks),caught,n_known-caught,unknown,round(float(ap),6),round(float(loss),6));comparison.append(row)
        print('MODEL_RESULT',row,flush=True)
    # Low-count rule: the five stations with the fewest bikes (or docks) per poll
    picks=val.sort_values(['fetched_at',count,'station_id']).groupby('fetched_at',sort=False).head(5).copy()
    n_known=int(picks[lab].notna().sum()); caught=int(picks[lab].sum()); selected[o,'low_count']=picks[['station_id','fetched_at',lab]].copy()
    row=(o,'low_count',0,len(val),len(picks),caught,n_known-caught,len(picks)-n_known,None,None);comparison.append(row);print('MODEL_RESULT',row,flush=True)
print('EXPLORATORY_VALIDATION_ONLY: historical availability simulated; no test data; one already-used validation week',flush=True)


# COMMAND ----------

## Cell 4: display the validation comparison
display(pd.DataFrame(comparison,columns=['outcome','model','train_rows','candidates','alerts','caught','nonempty','unknown','eligible_average_precision','eligible_log_loss']))


# COMMAND ----------

## Cell 5: repeat the comparison on two earlier weeks (Sep 14 to 20 and Sep 21 to 27)
# Two earlier forward-in-time checks; labels cannot cross either fold boundary.
folds=[('Sep14-20',pd.Timestamp('2026-09-14T04:00:00Z'),pd.Timestamp('2026-09-21T04:00:00Z')),('Sep21-27',pd.Timestamp('2026-09-21T04:00:00Z'),pd.Timestamp('2026-09-28T04:00:00Z'))]
fold_results=[]; fold_selected={}
for fold,start,end in folds:
    for o,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
        lab=o+'_label'
        train=examples[(examples.fetched_at<start-pd.Timedelta(seconds=3750))&examples[lab].notna()]
        val=examples[(examples.fetched_at>=start)&(examples.fetched_at<end-pd.Timedelta(seconds=3750))&examples[o+'_ok']&(examples[count]>0)].copy()
        assert len(val)>0 and val.fetched_at.min()>=start and val.target_at.max()+pd.Timedelta(seconds=150)<end
        plans=[('existing_style',train,make_pipeline(StandardScaler(),LogisticRegression(solver='lbfgs',max_iter=300))),('positive_count',train[train[count]>0],make_pipeline(StandardScaler(),LogisticRegression(solver='lbfgs',max_iter=300))),('boosted_positive',train[train[count]>0],HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=15,min_samples_leaf=200,learning_rate=0.1,random_state=42))]
        for name,fit_rows,model in plans:
            model.fit(fit_rows[features],fit_rows[lab].astype(int))
            if name!='boosted_positive': assert model.named_steps['logisticregression'].n_iter_.max()<300
            val['risk']=model.predict_proba(val[features])[:,1]
            picks=val.sort_values(['fetched_at','risk','station_id'],ascending=[True,False,True]).groupby('fetched_at',sort=False).head(5)
            caught=int(picks[lab].sum()); known=int(picks[lab].notna().sum())
            row=(fold,o,name,len(fit_rows),len(val),len(picks),caught,known-caught,len(picks)-known)
            fold_results.append(row);fold_selected[fold,o,name]=picks[['station_id','fetched_at',lab]].copy();print('FOLD_RESULT',row,flush=True)
        picks=val.sort_values(['fetched_at',count,'station_id']).groupby('fetched_at',sort=False).head(5)
        caught=int(picks[lab].sum());known=int(picks[lab].notna().sum())
        row=(fold,o,'low_count',0,len(val),len(picks),caught,known-caught,len(picks)-known);fold_results.append(row);fold_selected[fold,o,'low_count']=picks[['station_id','fetched_at',lab]].copy();print('FOLD_RESULT',row,flush=True)
print('EARLIER_FOLDS_COMPLETE; training ends before each held-out week; no test-period reads',flush=True)


# COMMAND ----------

## Cell 6: display the earlier-week results
print('EARLIER VALIDATION: caught counts, not independent test results')
display(pd.DataFrame(fold_results,columns=['week','outcome','model','train_rows','candidates','alerts','caught','nonempty','unknown']))


# COMMAND ----------

## Cell 7: paired station-day intervals, histogram gradient boosting versus logistic regression (first version)
# Exploratory paired station-day bootstrap for observed true-positive count differences.
# A station-day cluster does not remove cross-station or cross-day dependence.
rng=np.random.default_rng(20261110); intervals=[]
for week in ['Sep14-20','Sep21-27','Sep28-Oct04']:
    for o in ['bike','dock']:
        lab=o+'_label'
        a=selected[o,'boosted_positive'] if week=='Sep28-Oct04' else fold_selected[week,o,'boosted_positive']
        b=selected[o,'existing_style'] if week=='Sep28-Oct04' else fold_selected[week,o,'existing_style']
        def clusters(frame,name):
            x=frame[['station_id','fetched_at',lab]].copy(); x['day']=x.fetched_at.dt.tz_convert('America/New_York').dt.date
            x[name]=x[lab].fillna(0); return x.groupby(['station_id','day'])[name].sum()
        paired=pd.concat([clusters(a,'boosted'),clusters(b,'existing')],axis=1).fillna(0)
        delta=(paired.boosted-paired.existing).to_numpy(); n=len(delta)
        samples=delta[rng.integers(0,n,size=(1000,n))].sum(axis=1)
        row=(week,o,int(delta.sum()),round(float(np.quantile(samples,.025)),1),round(float(np.quantile(samples,.975)),1),n,int(a[lab].isna().sum()),int(b[lab].isna().sum()))
        intervals.append(row);print('STATION_DAY_BOOTSTRAP',row,flush=True)
print('Conditional exploratory station-day resampling; unknown labels contribute no confirmed catch; this is not a test-set estimate.',flush=True)


# COMMAND ----------

## Cell 8: experimental history features (earlier same-station availability at the same weekday and hour)
# Experimental features based on strictly earlier station observations, never future labels.
hist=df[['station_id','fetched_at','bike_ok','dock_ok','num_bikes_available','num_docks_available','capacity']].sort_values(['station_id','fetched_at']).copy()
ny=hist.fetched_at.dt.tz_convert('America/New_York'); hist['dow']=ny.dt.dayofweek; hist['hr']=ny.dt.hour
keys=['station_id','dow','hr']
for o,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
    valid=hist[o+'_ok'] & hist.capacity.gt(0)
    contribution=(hist[count]/hist.capacity).where(valid,0.0)
    seen=valid.astype('int64')
    prior_sum=contribution.groupby([hist[k] for k in keys],sort=False).cumsum()-contribution
    prior_count=seen.groupby([hist[k] for k in keys],sort=False).cumsum()-seen
    hist[o+'_history']=np.where(prior_count>0,prior_sum/prior_count.where(prior_count>0),0.5)
    hist[o+'_history_count']=prior_count
hist['weekend']=(hist.dow>=5).astype(int)
assert not hist.duplicated(['station_id','fetched_at']).any()
extra=['weekend','bike_history','dock_history','bike_history_count','dock_history_count']
examples=examples.merge(hist[['station_id','fetched_at']+extra],on=['station_id','fetched_at'],how='left',validate='one_to_one')
assert np.isfinite(examples[features+extra].to_numpy()).all()
print('HISTORY_READY',len(examples),'first_observation_defaults',int((examples.bike_history_count==0).sum()),flush=True)
print('History uses earlier same-station local weekday/hour observations, with collection time as simulated availability; no outcome labels.',flush=True)


# COMMAND ----------

## Cell 9: boosted tree with the history features on three weeks (not kept)
history_results=[];history_fitted={};history_selected={}
for week,start,end in folds+[('Sep28-Oct04',VALID,END)]:
    for o,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
        lab=o+'_label'
        train=examples[(examples.fetched_at<start-pd.Timedelta(seconds=3750))&examples[lab].notna()&(examples[count]>0)]
        val=examples[(examples.fetched_at>=start)&(examples.fetched_at<end-pd.Timedelta(seconds=3750))&examples[o+'_ok']&(examples[count]>0)].copy()
        model=HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=15,min_samples_leaf=200,learning_rate=0.1,random_state=42)
        model.fit(train[features+extra],train[lab].astype(int))
        val['risk']=model.predict_proba(val[features+extra])[:,1]
        picks=val.sort_values(['fetched_at','risk','station_id'],ascending=[True,False,True]).groupby('fetched_at',sort=False).head(5)
        caught=int(picks[lab].sum());known=int(picks[lab].notna().sum())
        row=(week,o,'boosted_with_history',len(train),len(val),len(picks),caught,known-caught,len(picks)-known)
        history_results.append(row);history_fitted[week,o]=model;history_selected[week,o]=picks[['station_id','fetched_at',lab]].copy();print('HISTORY_RESULT',row,flush=True)
print('EXPLORATORY: history uses simulated availability; no held-out test data; not a production scoring implementation.',flush=True)


# COMMAND ----------

## Cell 10: intervals again, now including every eligible station-day (replaces cell 7)
# Supersedes cell 8: include every eligible station-day, even when neither model selected it.
rng=np.random.default_rng(20261110); intervals_full=[]
for week,start,end in folds+[('Sep28-Oct04',VALID,END)]:
 for o,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
    lab=o+'_label';a=selected[o,'boosted_positive'] if week=='Sep28-Oct04' else fold_selected[week,o,'boosted_positive']; b=selected[o,'existing_style'] if week=='Sep28-Oct04' else fold_selected[week,o,'existing_style']
    universe=examples[(examples.fetched_at>=start)&(examples.fetched_at<end-pd.Timedelta(seconds=3750))&examples[o+'_ok']&(examples[count]>0)][['station_id','fetched_at']].copy()
    universe['day']=universe.fetched_at.dt.tz_convert('America/New_York').dt.date
    index=universe[['station_id','day']].drop_duplicates().set_index(['station_id','day']).index
    def cluster_sum(frame):
        x=frame[['station_id','fetched_at',lab]].copy();x['day']=x.fetched_at.dt.tz_convert('America/New_York').dt.date
        x['positive']=x[lab].fillna(0);return x.groupby(['station_id','day']).positive.sum().reindex(index,fill_value=0)
    delta=(cluster_sum(a)-cluster_sum(b)).to_numpy();n=len(delta); draws=delta[rng.integers(0,n,size=(1000,n))].sum(axis=1)
    row=(week,o,int(delta.sum()),round(float(np.quantile(draws,.025)),1),round(float(np.quantile(draws,.975)),1),n,int(a[lab].isna().sum()),int(b[lab].isna().sum()));intervals_full.append(row);print('FULL_STATION_DAY_BOOTSTRAP',row,flush=True)
print('Exploratory paired station-day resampling; dependence across stations/days and training/model-selection uncertainty are not captured.',flush=True)


# COMMAND ----------

## Cell 11: save the experimental models to a private folder, with fingerprints
# Save private experimental model artifacts, without altering the pilot scorer.
from pathlib import Path
import tempfile,shutil,uuid,json,hashlib,joblib,sklearn
user=spark.sql('select current_user() as u').first()['u']
# New private folder in the current user's workspace
folder=Path('/Workspace/Users')/user/('citibike_experimental_models_'+uuid.uuid4().hex)
folder.mkdir()
manifest={'purpose':'exploratory research; not frozen or deployed','sklearn_version':sklearn.__version__,'training_start_utc':START.isoformat(),'training_end_exclusive_utc':(VALID-pd.Timedelta(seconds=3750)).isoformat(),'validation_start_utc':VALID.isoformat(),'validation_end_utc':END.isoformat(),'cohort_sha256':'3d5bf5cd0594981fb60b995a08f6c305b2927122ae1299021e413a1b35414ff3','availability':'historical collection time used as simulated availability','models':{}}
with tempfile.TemporaryDirectory(prefix='citibike_models_') as t:
 for o in ['bike','dock']:
  for name in ['existing_style','positive_count','boosted_positive','boosted_with_history']:
   model=history_fitted['Sep28-Oct04',o] if name=='boosted_with_history' else fitted[o,name]
   filename=o+'_'+name+'.joblib'; localfile=Path(t)/filename;joblib.dump(model,localfile)
   data=localfile.read_bytes(); destination=folder/filename
   with destination.open('xb') as f:f.write(data)
   digest=hashlib.sha256(data).hexdigest();assert hashlib.sha256(destination.read_bytes()).hexdigest()==digest
   manifest['models'][filename]={'sha256':digest,'features':features+extra if name=='boosted_with_history' else features,'bytes':len(data)}
 with (folder/'manifest.json').open('x') as f:json.dump(manifest,f,indent=2)
print('PRIVATE_EXPERIMENTS_SAVED',folder.name,'model_files',len(manifest['models']),flush=True)


# COMMAND ----------

## Cell 12: confirm the saved copies predict identically and the history features use earlier data only
# Verify saved copies and check strictly earlier history ordering.
prev=hist.groupby(['station_id','dow','hr'],sort=False).fetched_at.shift(1)
assert (prev.isna() | (prev<hist.fetched_at)).all()
assert (hist.bike_history_count>=0).all() and (hist.dock_history_count>=0).all()
checks=[]
with tempfile.TemporaryDirectory(prefix='citibike_verify_') as t:
 for o in ['bike','dock']:
  for name in ['existing_style','positive_count','boosted_positive','boosted_with_history']:
   filename=o+'_'+name+'.joblib';source=folder/filename;localfile=Path(t)/filename;shutil.copyfile(source,localfile)
   assert hashlib.sha256(localfile.read_bytes()).hexdigest()==manifest['models'][filename]['sha256']
   restored=joblib.load(localfile);original=history_fitted['Sep28-Oct04',o] if name=='boosted_with_history' else fitted[o,name]
   cols=manifest['models'][filename]['features']; sample=examples[examples.split=='validation'][cols].head(100)
   delta=float(np.max(np.abs(restored.predict_proba(sample)-original.predict_proba(sample))));assert delta==0
   checks.append((o,name,delta))
print('SAVED_MODEL_COPY_CHECKS',checks)
print('HISTORY_ORDER_CHECK',True,'scope','prior same-station weekday-hour timestamps strictly earlier')
for row in comparison: print('VALIDATION_METRICS',row)


# COMMAND ----------

## Cell 13: descriptive calibration on the validation week (ten Risk Score bands)
# Descriptive calibration on known-label, eligible validation rows only.
from sklearn.metrics import brier_score_loss
calibration=[]
for o,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
 lab=o+'_label'; v=examples[(examples.split=='validation')&examples[o+'_ok']&(examples[count]>0)&examples[lab].notna()].copy()
 for name in ['existing_style','positive_count','boosted_positive']:
  p=fitted[o,name].predict_proba(v[features])[:,1]; y=v[lab].astype(int)
  print('CALIBRATION_SCORE',o,name,'known',len(v),'AP',round(float(average_precision_score(y,p)),6),'log_loss',round(float(log_loss(y,p,labels=[0,1])),6),'brier',round(float(brier_score_loss(y,p)),6),flush=True)
  band=pd.cut(p,bins=np.linspace(0,1,11),include_lowest=True,labels=False)
  grouped=pd.DataFrame({'band':band,'pred':p,'observed':y.to_numpy()}).groupby('band').agg(n=('pred','size'),predicted=('pred','mean'),observed=('observed','mean'))
  for band,r in grouped.iterrows():
   if r['n']>=100: calibration.append((o,name,int(band),int(r['n']),round(float(r['predicted']),4),round(float(r['observed']),4)))
print('CALIBRATION_BANDS',calibration,flush=True)
print('One previously used validation week; uncalibrated estimates; unknown outcomes excluded.',flush=True)


# COMMAND ----------

## Cell 14: replay one saved live poll through the embedded scorer package and compare with the models
import base64,io,zipfile,sys,tempfile,sqlite3,json,shutil,hashlib,math
from pathlib import Path
from datetime import datetime
import numpy as np,pandas as pd
from zoneinfo import ZoneInfo
package=Path(tempfile.mkdtemp(prefix='candidate_package_'))
# Embedded scorer package (scoring, normalization and candidate scorer modules, compressed)
with zipfile.ZipFile(io.BytesIO(base64.b64decode('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'))) as z:z.extractall(package)
for name in ['scoring','normalization','candidate_scorer']:sys.modules.pop(name,None)
sys.path.insert(0,str(package))
from candidate_scorer import load_candidate,score_candidate
from scoring import FEATURES
user=spark.sql('select current_user() as u').first()['u']
parent=Path('/Workspace/Users')/user
candidate_folder=parent/'citibike_experimental_models_c4e388f77d614002a3512d0478d40037'
source=parent/'citibike_manual_trial_3ad737d357c8403e9b27611ebb926b8b'/'outcomes_final.sqlite'
localdb=package/'trial.sqlite';shutil.copyfile(source,localdb)
baseline,cohort,models=load_candidate(candidate_folder,package/'cohort_reconstructed.csv',package/'model_parameters.json')
with sqlite3.connect(localdb) as db:
 run=db.execute('select run_id from runs where state="success" order by slot limit 1').fetchone()[0]
 records=[json.loads(r[0]) for r in db.execute('select payload from observations where run_id=?',(run,))]
 score_time=json.loads(db.execute('select payload from predictions where run_id=? limit 1',(run,)).fetchone()[0])['scored_at']
scored,alerts,rejected=score_candidate(records,baseline,cohort,models,score_time)
byid={r['station_id']:r for r in records};details=[]
for outcome in ['bike','dock']:
 subset=[s for s in scored if s['outcome']==outcome]; ref=[]
 for s in subset:
  r=byid[s['station_id']];b=float(r['num_bikes_available']);d=float(r['num_docks_available']);c=float(r['capacity']);t=datetime.fromisoformat(r['fetched_at']).astimezone(ZoneInfo('America/New_York'));angle=(t.hour+t.minute/60+t.second/3600)*2*math.pi/24
  ref.append([b,d,c,b/c,d/c,math.sin(angle),math.cos(angle)])
 expected=models[outcome].predict_proba(pd.DataFrame(ref,columns=FEATURES))[:,1]
 maximum=max((abs(s['probability']-p) for s,p in zip(subset,expected)),default=0.)
 ordered=sorted(zip(subset,expected),key=lambda x:(-x[1],x[0]['station_id']))[:5]
 reference=[s['station_id'] for s,p in ordered];actual=[s['station_id'] for s in alerts[outcome]]
 assert maximum<1e-12 and actual==reference
 details.append((outcome,len(subset),len(actual),maximum,actual==reference))
print('SAVED_LIVE_SNAPSHOT_REPLAY',details,'rejections',rejected,flush=True)
print('LIMITS: one previously saved poll; same runtime and model implementation; no new feed collection or test-week scoring.',flush=True)


# COMMAND ----------

## Cell 15: one fresh read-only check of the public station feed (no alerts sent)
# One fresh read-only public-feed check; no persistence, scheduling, or alert delivery.
from urllib.request import Request,urlopen
from datetime import datetime,timezone
from time import monotonic
urls={'info':'https://gbfs.citibikenyc.com/gbfs/en/station_information.json','status':'https://gbfs.citibikenyc.com/gbfs/en/station_status.json'}
def public_feed(kind):
    with urlopen(Request(urls[kind],headers={'User-Agent':'DATA622-Team3-Development-Demo/1.0'}),timeout=15) as response:return json.load(response)['data']['stations']
metadata=public_feed('info');metadata_ready=datetime.now(timezone.utc);lookup={r['station_id']:r for r in metadata};assert len(lookup)==len(metadata)
poll_start=datetime.now(timezone.utc);statuses=public_feed('status');available=datetime.now(timezone.utc);assert len({r['station_id'] for r in statuses})==len(statuses)
rows=[]
for r in statuses:
 sid=r['station_id']
 if sid not in cohort:continue
 try: reported=datetime.fromtimestamp(int(r['last_reported']),timezone.utc).isoformat()
 except (KeyError,ValueError,TypeError,OverflowError,OSError):reported=None
 rows.append({'station_id':sid,'fetched_at':poll_start.isoformat(),'available_at':available.isoformat(),'station_reported_at':reported,'metadata_available_at':metadata_ready.isoformat(),'num_bikes_available':r.get('num_bikes_available'),'num_docks_available':r.get('num_docks_available'),'capacity':lookup.get(sid,{}).get('capacity'),'is_installed':r.get('is_installed'),'is_renting':r.get('is_renting'),'is_returning':r.get('is_returning')})
score_at=datetime.now(timezone.utc);begin=monotonic();fresh_scores,fresh_alerts,fresh_rejections=score_candidate(rows,baseline,cohort,models,score_at.isoformat());elapsed=monotonic()-begin
print('FRESH_CHECK',{'cohort_seen':len(rows),'missing':len(cohort)-len(rows),'score_counts':{o:sum(s['outcome']==o for s in fresh_scores) for o in ['bike','dock']},'selected_counts':{o:len(fresh_alerts[o]) for o in ['bike','dock']},'rejections':fresh_rejections,'score_seconds':round(elapsed,3)},flush=True)
print('One snapshot, unscheduled, no alerts sent; not a model-performance evaluation.',flush=True)


# COMMAND ----------

## Cell 16: inspection: confirm the session still holds the development data after a restart
print('DEVELOPMENT_DATA_READY', {k:(k in globals()) for k in ['examples','df','folds','features','fitted','client','bucket']})


# COMMAND ----------

## Cell 17: load the ACS station table (embedded below) and check station coordinates
# ACS 2020–2024 estimates, already released before the 2026 development period.
import base64,gzip,json,hashlib,io
import pandas as pd,numpy as np,pyarrow.parquet as pq
from sklearn.ensemble import HistGradientBoostingClassifier
# ACS table for the 100 stations, embedded as compressed text; its fingerprint is checked on the next line
blob='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'
raw_acs=gzip.decompress(base64.b64decode(blob));assert hashlib.sha256(raw_acs).hexdigest()=='18f55c17cbf88dfe07dc4b7f3152aca23c002ba5afb011307f52491dae14da83'
acs=pd.DataFrame(json.loads(raw_acs));acs_features=['estimated_population_500m','bachelors_plus_pct_500m','households_no_vehicle_pct_500m','poverty_pct_500m','bicycle_commute_pct_500m','school_enrollment_pct_500m']
for k in ['lat','lon']+acs_features:acs[k]=pd.to_numeric(acs[k],errors='coerce')
assert len(acs)==100 and not acs.station_id.duplicated().any() and set(acs.station_id)==set(C)
# Check that mapped station coordinates agree with metadata known by the training start.
coord=pq.read_table(io.BytesIO(client.get_object(Bucket=bucket,Key='exports/station_info.parquet')['Body'].read()),columns=['station_id','fetched_at','lat','lon']).to_pandas()
coord=coord[coord.station_id.isin(C)];coord['fetched_at']=pd.to_datetime(coord.fetched_at,utc=True)
coord=coord[coord.fetched_at<=START].sort_values('fetched_at').drop_duplicates('station_id',keep='last')
verify=acs[['station_id','lat','lon']].merge(coord,on='station_id',how='left',suffixes=('_acs','_asof'),validate='one_to_one')
assert len(verify)==100 and verify.fetched_at.notna().all()
assert np.allclose(verify.lat_acs,verify.lat_asof,atol=5e-9,rtol=0) and np.allclose(verify.lon_acs,verify.lon_asof,atol=5e-9,rtol=0)
acs_examples=examples.merge(acs[['station_id']+acs_features],on='station_id',how='left',validate='many_to_one')
assert len(acs_examples)==len(examples)
print('ACS_COVERAGE',len(acs),'stations', 'missing_station_values',acs[acs_features].isna().sum().to_dict(),'asof_coordinate_matches',len(verify),flush=True)
print('ACS_SOURCE 2020-2024 ACS five-year estimates, 500m area-weighted; tract income excluded; association only.',flush=True)


# COMMAND ----------

## Cell 18: boosted tree with and without the six ACS measures on three weeks
# Paired exploratory comparison: unchanged boosted specification, with or without ACS inputs.
from sklearn.metrics import average_precision_score,log_loss
assert examples[['station_id','fetched_at']].reset_index(drop=True).equals(acs_examples[['station_id','fetched_at']].reset_index(drop=True))
acs_results=[];acs_selected={};acs_models={}
for week,start,end in folds+[('Sep28-Oct04',VALID,END)]:
 for o,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
  lab=o+'_label'; train=acs_examples[(acs_examples.fetched_at<start-pd.Timedelta(seconds=3750))&acs_examples[lab].notna()&(acs_examples[count]>0)]
  val=acs_examples[(acs_examples.fetched_at>=start)&(acs_examples.fetched_at<end-pd.Timedelta(seconds=3750))&acs_examples[o+'_ok']&(acs_examples[count]>0)].copy()
  model=HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=15,min_samples_leaf=200,learning_rate=0.1,random_state=42)
  model.fit(train[features+acs_features],train[lab].astype(int));val['risk']=model.predict_proba(val[features+acs_features])[:,1]
  picks=val.sort_values(['fetched_at','risk','station_id'],ascending=[True,False,True]).groupby('fetched_at',sort=False).head(5)
  nknown=int(picks[lab].notna().sum());caught=int(picks[lab].sum());known=val[lab].notna()
  row=(week,o,len(train),len(val),len(picks),caught,nknown-caught,len(picks)-nknown,round(float(average_precision_score(val.loc[known,lab].astype(int),val.loc[known,'risk'])),6),round(float(log_loss(val.loc[known,lab].astype(int),val.loc[known,'risk'],labels=[0,1])),6))
  acs_results.append(row);acs_selected[week,o]=picks[['station_id','fetched_at',lab]].copy();acs_models[week,o]=model;print('ACS_RESULT',row,flush=True)
print('ACS_EXPERIMENT_COMPLETE: validation-only, same candidates, no S3 writes, no reserved test scoring.',flush=True)


# COMMAND ----------

## Cell 19: ACS versus no-ACS paired station-day intervals
# Exploratory ACS vs no-ACS paired station-day resampling.
rng=np.random.default_rng(20261110); acs_intervals=[]
for week,start,end in folds+[('Sep28-Oct04',VALID,END)]:
 for o,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
  lab=o+'_label';a=acs_selected[week,o];b=selected[o,'boosted_positive'] if week=='Sep28-Oct04' else fold_selected[week,o,'boosted_positive']
  eligible=examples[(examples.fetched_at>=start)&(examples.fetched_at<end-pd.Timedelta(seconds=3750))&examples[o+'_ok']&(examples[count]>0)][['station_id','fetched_at']].copy()
  eligible['day']=eligible.fetched_at.dt.tz_convert('America/New_York').dt.date
  idx=eligible[['station_id','day']].drop_duplicates().set_index(['station_id','day']).index
  def grouped(frame):
   x=frame[['station_id','fetched_at',lab]].copy();x['day']=x.fetched_at.dt.tz_convert('America/New_York').dt.date;x['confirmed']=x[lab].fillna(0)
   return x.groupby(['station_id','day']).confirmed.sum().reindex(idx,fill_value=0)
  delta=(grouped(a)-grouped(b)); n=len(delta); draws=delta.to_numpy()[rng.integers(0,n,size=(1000,n))].sum(axis=1)
  station=delta.groupby(level='station_id').sum();most=int(station.max()); least=int(station.min())
  row=(week,o,int(delta.sum()),round(float(np.quantile(draws,.025)),1),round(float(np.quantile(draws,.975)),1),n,most,least,int(a[lab].isna().sum()),int(b[lab].isna().sum()))
  acs_intervals.append(row);print('ACS_PAIRED_BOOTSTRAP',row,flush=True)
print('Conditional station-day intervals; cross-station/day dependence, model selection, and missing outcomes are not fully captured.',flush=True)


# COMMAND ----------

## Cell 20: read-only inventory of class S3 folders and public trip archives
# Read-only inventory of existing class paths and public trip archives; no data writes.
from urllib.request import Request,urlopen
from urllib.error import HTTPError,URLError
for prefix in ['exports/','contributions/team-3/']:
 try:
  listed=client.list_objects_v2(Bucket=bucket,Prefix=prefix,Delimiter='/',MaxKeys=1000)
  print('FOLDER_INVENTORY',prefix,[x['Prefix'] for x in listed.get('CommonPrefixes',[])],flush=True)
 except Exception as e:print('INVENTORY_UNAVAILABLE',prefix,type(e).__name__,flush=True)
for month in ['202606','202607','202608','202609']:
 try:
  req=Request('https://s3.amazonaws.com/tripdata/'+month+'-citibike-tripdata.zip',method='HEAD',headers={'User-Agent':'DATA622-Team3-Development-Demo/1.0'})
  with urlopen(req,timeout=15) as r: print('PUBLIC_TRIP_ARCHIVE',month,'last_modified',r.headers.get('Last-Modified'),'bytes',r.headers.get('Content-Length'),flush=True)
 except HTTPError as e:print('PUBLIC_TRIP_ARCHIVE_HTTP',month,e.code,flush=True)
 except Exception as e:print('PUBLIC_TRIP_ARCHIVE_UNAVAILABLE',month,type(e).__name__,flush=True)
print('Last-modified describes current object version; it is not proof of the first publication date.',flush=True)


# COMMAND ----------

## Cell 21: inventory weather files and the station short-name mapping at the training start
# Inventory weather object names and point-in-time station short-name mappings.
for prefix in ['contributions/team-3/weather_data/','exports/live/']:
 try:
  obj=[]
  for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix=prefix,PaginationConfig={'MaxItems':1000}): obj.extend(page.get('Contents',[]))
  print('SOURCE_OBJECTS',prefix,'count',len(obj),'sample',[(x['Key'],x['Size']) for x in obj[:8]],flush=True)
 except Exception as e: print('SOURCE_UNAVAILABLE',prefix,type(e).__name__,flush=True)
station_meta=pq.read_table(io.BytesIO(client.get_object(Bucket=bucket,Key='exports/station_info.parquet')['Body'].read()),columns=['station_id','fetched_at','short_name','name','lat','lon']).to_pandas()
station_meta=station_meta[station_meta.station_id.isin(C)];station_meta['fetched_at']=pd.to_datetime(station_meta.fetched_at,utc=True)
station_start=station_meta[station_meta.fetched_at<=START].sort_values('fetched_at').drop_duplicates('station_id',keep='last')
print('TRIP_ID_MAPPING_AT_TRAIN_START','rows',len(station_start),'null_short_names',int(station_start.short_name.isna().sum()),'duplicated_short_names',int(station_start.short_name.duplicated().sum()),'short_name_changes_over_archive',int(station_meta.groupby('station_id').short_name.nunique().gt(1).sum()),flush=True)


# COMMAND ----------

## Cell 22: download the July trip archive and build station weekday-hour trip profiles
# July trip archive: last-modified prior to training, no reserved-test data.
import tempfile,zipfile,hashlib,collections,shutil,email.utils
from urllib.request import Request,urlopen
url='https://s3.amazonaws.com/tripdata/202607-citibike-tripdata.zip'
with urlopen(Request(url,method='HEAD'),timeout=30) as r:
    trip_last_modified=pd.Timestamp(email.utils.parsedate_to_datetime(r.headers['Last-Modified']))
    expected_bytes=int(r.headers['Content-Length'])
assert trip_last_modified<START and expected_bytes<1100000000
allmeta=pq.read_table(io.BytesIO(client.get_object(Bucket=bucket,Key='exports/station_info.parquet')['Body'].read()),columns=['station_id','fetched_at','short_name','name']).to_pandas()
allmeta['fetched_at']=pd.to_datetime(allmeta.fetched_at,utc=True)
allstart=allmeta[allmeta.fetched_at<=START].sort_values('fetched_at').drop_duplicates('station_id',keep='last')
allstart=allstart[allstart.short_name.notna()]; ambiguous=set(allstart.loc[allstart.short_name.duplicated(keep=False),'short_name'])
validmap=station_start[~station_start.short_name.isin(ambiguous)].copy()
short_to_id=dict(zip(validmap.short_name.astype(str),validmap.station_id.astype(str)))
short_to_name=dict(zip(validmap.short_name.astype(str),validmap.name.astype(str)))
print('TRIP_MAP',len(short_to_id),'of',len(C),'cohort IDs uniquely map across metadata',flush=True)
assert len(short_to_id)>0
counts=collections.Counter(); matched=collections.Counter(); mismatches=collections.Counter();total_rows=0;file_count=0
with tempfile.TemporaryDirectory(prefix='citibike_july_trips_') as t:
 path=Path(t)/'july.zip';h=hashlib.sha256();downloaded=0;last_report=0
 with urlopen(Request(url,headers={'User-Agent':'DATA622-Team3-Development-Demo/1.0'}),timeout=60) as source,path.open('wb') as target:
  while True:
   data=source.read(8*1024*1024)
   if not data:break
   target.write(data);h.update(data);downloaded+=len(data)
   if downloaded-last_report>=250000000:print('TRIP_DOWNLOAD_MB',round(downloaded/1e6),flush=True);last_report=downloaded
 assert downloaded==expected_bytes
 print('TRIP_DOWNLOAD_COMPLETE','bytes',downloaded,'sha256',h.hexdigest(),flush=True)
 with zipfile.ZipFile(path) as z:
  files=[n for n in z.namelist() if n.lower().endswith('.csv')];assert files
  for name in files:
   with z.open(name) as source:
    for chunk in pd.read_csv(source,usecols=['started_at','ended_at','start_station_id','end_station_id','start_station_name','end_station_name'],dtype='string',chunksize=200000):
     total_rows+=len(chunk)
     for typ,date_col,id_col,name_col in [('depart','started_at','start_station_id','start_station_name'),('arrive','ended_at','end_station_id','end_station_name')]:
      x=chunk[chunk[id_col].isin(short_to_id)].copy()
      if x.empty:continue
      dt=pd.to_datetime(x[date_col],errors='coerce');good=(dt>=pd.Timestamp('2026-07-01'))&(dt<pd.Timestamp('2026-08-01'));x=x[good];dt=dt[good]
      if x.empty:continue
      x['sid']=x[id_col].map(short_to_id);x['dow']=dt.dt.dayofweek;x['hour']=dt.dt.hour
      matched[typ]+=len(x);mismatches[typ]+=int((x[name_col].fillna('')!=x[id_col].map(short_to_name)).sum())
      g=x.groupby(['sid','dow','hour']).size()
      for (sid,dow,hour),n in g.items():counts[(sid,int(dow),int(hour),typ)]+=int(n)
   file_count+=1;print('TRIP_FILES_COMPLETE',file_count,'rows_seen',total_rows,flush=True)
observed={sid for sid,dow,hour,typ in counts}
days=collections.Counter(pd.date_range('2026-07-01','2026-07-31').dayofweek)
trip_features=[]
for sid in sorted(C):
 for dow in range(7):
  for hour in range(24):
   trip_features.append((sid,dow,hour,counts[(sid,dow,hour,'depart')]/days[dow] if sid in observed else np.nan,counts[(sid,dow,hour,'arrive')]/days[dow] if sid in observed else np.nan))
trip_profiles=pd.DataFrame(trip_features,columns=['station_id','trip_dow','trip_hour','july_departures_per_weekday_hour','july_arrivals_per_weekday_hour'])
print('TRIP_PROFILE_READY','input_rows',total_rows,'files',file_count,'stations_observed',len(observed),'mapped',len(short_to_id),'matched',dict(matched),'station_name_mismatches',dict(mismatches),flush=True)
print('LIMITS: naive trip timestamps treated as New York local provisionally; ID mapping as of August 24; July station operating periods not verified; staff trips excluded.',flush=True)


# COMMAND ----------

## Cell 23: inspection: column lists and the weather schema
print('TRIP_PROFILE_COLUMNS',trip_profiles.columns.tolist(),'rows',len(trip_profiles),'missing',trip_profiles.isna().sum().to_dict(),flush=True)
print('TRIP_DIAGNOSTICS', 'matched',dict(matched),'station_name_mismatches',dict(mismatches),flush=True)
print('MODEL_FEATURES',features,'ACS_FEATURES',acs_features,flush=True)
print('EXAMPLE_COLUMNS',examples.columns.tolist(),flush=True)
print('ACS_RESULT_EXAMPLE',acs_results[:2],flush=True)
try:
    weather = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket,Key='contributions/team-3/weather_data/weather_forecast_history.parquet')['Body'].read())).to_pandas()
    print('WEATHER_SCHEMA',weather.dtypes.astype(str).to_dict(),'rows',len(weather),flush=True)
    for col in weather.columns:
        if 'time' in col.lower() or 'date' in col.lower() or 'fetch' in col.lower() or 'ingest' in col.lower():
            print('WEATHER_TIME_FIELD',col,'non_null',int(weather[col].notna().sum()),'min',str(weather[col].min()),'max',str(weather[col].max()),flush=True)
except Exception as e:
    print('WEATHER_INSPECTION_ERROR',type(e).__name__,str(e)[:300],flush=True)


# COMMAND ----------

## Cell 24: inspection: weather columns and time range
print('WX',weather.dtypes.astype(str).to_dict(), 'rows',len(weather),flush=True)
print('WX_RANGE',[(c,str(weather[c].min()),str(weather[c].max())) for c in weather.columns if 'utc' in c.lower()],flush=True)
print('EXAMPLE_COLS',examples.columns.tolist(),flush=True)
print('FOLDS',folds,flush=True)


# COMMAND ----------

## Cell 25: test July trip features on top of ACS (not kept)
# Exploratory July trips: use only an archive with a current object timestamp before training.
trip_cols=['july_departures_per_weekday_hour','july_arrivals_per_weekday_hour']
trip_examples=acs_examples.copy()
local_time=trip_examples.fetched_at.dt.tz_convert('America/New_York')
trip_examples['trip_dow']=local_time.dt.dayofweek
trip_examples['trip_hour']=local_time.dt.hour
trip_examples=trip_examples.merge(trip_profiles,on=['station_id','trip_dow','trip_hour'],how='left',validate='many_to_one',sort=False)
assert len(trip_examples)==len(examples) and trip_examples[['station_id','fetched_at']].reset_index(drop=True).equals(examples[['station_id','fetched_at']].reset_index(drop=True))
trip_results=[];trip_selected={};trip_models={}
for week,start,end in folds+[('Sep28-Oct04',VALID,END)]:
    for outcome,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
        lab=outcome+'_label'
        train=trip_examples[(trip_examples.fetched_at<start-pd.Timedelta(seconds=3750)) & trip_examples[lab].notna() & (trip_examples[count]>0)]
        valid=trip_examples[(trip_examples.fetched_at>=start) & (trip_examples.fetched_at<end-pd.Timedelta(seconds=3750)) & trip_examples[outcome+'_ok'] & (trip_examples[count]>0)].copy()
        for variant,cols in [('trips',features+trip_cols),('acs_and_trips',features+acs_features+trip_cols)]:
            model=HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=15,min_samples_leaf=200,learning_rate=0.1,random_state=42)
            model.fit(train[cols],train[lab].astype(int)); valid['risk']=model.predict_proba(valid[cols])[:,1]
            picks=valid.sort_values(['fetched_at','risk','station_id'],ascending=[True,False,True]).groupby('fetched_at',sort=False).head(5)
            nknown=int(picks[lab].notna().sum()); caught=int(picks[lab].sum()); known=valid[lab].notna()
            row=(week,outcome,variant,len(train),len(valid),len(picks),caught,nknown-caught,len(picks)-nknown,round(float(average_precision_score(valid.loc[known,lab].astype(int),valid.loc[known,'risk'])),6),round(float(log_loss(valid.loc[known,lab].astype(int),valid.loc[known,'risk'],labels=[0,1])),6))
            trip_results.append(row);trip_selected[week,outcome,variant]=picks[['station_id','fetched_at',lab]].copy();trip_models[week,outcome,variant]=model
            print('TRIP_RESULT',row,flush=True)
print('TRIP_TEST_COMPLETE: development only; timestamps provisional; missing July stations remain missing; no reserved test scoring.',flush=True)


# COMMAND ----------

## Cell 26: display the trip results and the save folder
print('TRIP_COUNTS',[(r[0],r[1],r[2],r[6],r[8]) for r in trip_results],flush=True)
print('PRIVATE_PARENT',str(folder.parent),flush=True)


# COMMAND ----------

## Cell 27: display the final-week trip results
print('FINAL_TRIP',[(r[1],r[2],r[6],r[8]) for r in trip_results if r[0]=='Sep28-Oct04'],flush=True)
print('SAVE_PARENT',str(folder.parent),flush=True)


# COMMAND ----------

## Cell 28: save the ACS table, trip profiles and models to a new private folder
# Save reviewable development artifacts in the user's workspace only.
import joblib, json, uuid
# Later steps read the ACS table from the folder this cell created in the recorded run (ID e1eb6e26)
experiment_dir=folder.parent/('Citi Bike - ACS and July Trip Development - '+uuid.uuid4().hex[:8])
experiment_dir.mkdir(exist_ok=False)
acs[['station_id']+acs_features].to_parquet(experiment_dir/'acs_station_features.parquet',index=False)
trip_profiles.to_parquet(experiment_dir/'july_station_weekday_hour_profiles.parquet',index=False)
saved_models=[]
for outcome in ['bike','dock']:
    for label,model in [('acs',acs_models['Sep28-Oct04',outcome]),('july_trips',trip_models['Sep28-Oct04',outcome,'trips']),('acs_and_july_trips',trip_models['Sep28-Oct04',outcome,'acs_and_trips'])]:
        name=outcome+'_'+label+'.joblib';joblib.dump(model,experiment_dir/name);saved_models.append(name)
manifest={'purpose':'Exploratory development only; not approved for pilot or production', 'validation_weeks':['Sep14-20','Sep21-27','Sep28-Oct04'], 'reserved_test_scored':False,'feature_order':{'base':features,'acs':acs_features,'trips':trip_cols},'models':saved_models,'model_training_end':'before 2026-09-28 04:00 UTC less 3750 seconds','trip_archive':'202607-citibike-tripdata.zip','trip_archive_current_last_modified':'2026-08-12 16:03:43 UTC','trip_archive_sha256':'9aeb4f2a1744ce4a81ab5a2bc999bb3e7794fc0a010415e64aeb80283a0c21c0','trip_assumptions':'Naive trip timestamps provisionally New York local; July station operating times and historical mapping not independently verified; no trips for six cohort stations treated as missing','acs_assumptions':'2020-2024 ACS provisional 500m area-weighted estimates, not rider characteristics; same stations in train and validation','results_acs':acs_results,'results_trip':trip_results}
with (experiment_dir/'manifest.json').open('x') as fh: json.dump(manifest,fh,indent=2,default=str)
print('SAVED_PRIVATE_EXPERIMENT',str(experiment_dir),'models',len(saved_models),'feature_tables',2,flush=True)


# COMMAND ----------

## Cell 29: paired station-day intervals for the trip models
# Paired station-day resampling is descriptive and conditional on these fitted models.
rng=np.random.default_rng(42); trip_intervals=[]
for week,start,end in folds+[('Sep28-Oct04',VALID,END)]:
    for outcome,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
        lab=outcome+'_label';a=trip_selected[week,outcome,'acs_and_trips'];b=acs_selected[week,outcome]
        elig=examples[(examples.fetched_at>=start)&(examples.fetched_at<end-pd.Timedelta(seconds=3750))&examples[outcome+'_ok']&(examples[count]>0)][['station_id','fetched_at']].copy()
        elig['day']=elig.fetched_at.dt.tz_convert('America/New_York').dt.date; idx=elig[['station_id','day']].drop_duplicates().set_index(['station_id','day']).index
        def count_by_day(x):
            z=x.copy();z['day']=z.fetched_at.dt.tz_convert('America/New_York').dt.date;z['confirmed']=z[lab].fillna(0);return z.groupby(['station_id','day']).confirmed.sum().reindex(idx,fill_value=0)
        delta=count_by_day(a)-count_by_day(b);draws=delta.to_numpy()[rng.integers(0,len(delta),size=(1000,len(delta)))].sum(axis=1)
        result=(week,outcome,int(delta.sum()),round(float(np.quantile(draws,.025)),1),round(float(np.quantile(draws,.975)),1));trip_intervals.append(result);print('TRIP_VS_ACS',result,flush=True)
print('Uncertainty excludes dependence across stations and days and model selection; unknown labels do not count as confirmed catches.',flush=True)


# COMMAND ----------

## Cell 30: inspection: session state
print('SESSION_STATE', {x:(x in globals()) for x in ['examples','acs_examples','trip_profiles','trip_selected','acs_selected','fold_selected','client','bucket']},flush=True)


# COMMAND ----------

## Cell 31: reload the saved ACS and trip files after a restart
from pathlib import Path
import json, joblib, pandas as pd, numpy as np
personal=Path('/Workspace/Users')/spark.sql('select current_user() as u').first()['u']
exp=personal/'Citi Bike - ACS and July Trip Development - e1eb6e26'
manifest=json.loads((exp/'manifest.json').read_text())
acs_saved=pd.read_parquet(exp/'acs_station_features.parquet'); trip_saved=pd.read_parquet(exp/'july_station_weekday_hour_profiles.parquet')
print('SAVED_RECOVERY',len(acs_saved),len(trip_saved),'models',manifest['models'],flush=True)
print('EXAMPLE_COLS',examples.columns.tolist(),flush=True)
print('PERSONAL_EXPERIMENTS',[p.name for p in personal.iterdir() if 'Model' in p.name or 'model' in p.name or 'Experiment' in p.name],flush=True)


# COMMAND ----------

## Cell 32: inspection: saved folders and the ACS feature order
print('DIRS',[(p.name,p.is_dir()) for p in personal.iterdir() if 'model' in p.name.lower() or 'experiment' in p.name.lower()],flush=True)
print('ACS_FIELDS',manifest['feature_order'],flush=True)


# COMMAND ----------

## Cell 33: request previous-day weather forecasts for the development period from a public weather service
import urllib.request, urllib.parse
# Previous-day forecasts for one Midtown point, so each value was issued before the hour it describes
params={'latitude':40.75,'longitude':-73.98,'start_date':'2026-08-23','end_date':'2026-10-04','hourly':'temperature_2m_previous_day1,precipitation_previous_day1,wind_speed_10m_previous_day1','models':'gfs_seamless','timezone':'UTC'}
url='https://previous-runs-api.open-meteo.com/v1/forecast?'+urllib.parse.urlencode(params)
try:
    with urllib.request.urlopen(url,timeout=45) as response: wx_json=json.load(response)
    wx=pd.DataFrame(wx_json['hourly']);print('WX_AVAILABLE',len(wx),'columns',wx.columns.tolist(),'non_null',wx.notna().sum().to_dict(),'first_last',(wx.time.iloc[0],wx.time.iloc[-1]),flush=True)
except Exception as e:
    print('WX_REQUEST_FAILED',type(e).__name__,str(e)[:300],flush=True)
print('FEATURES_MISSING_FROM_EXAMPLES',[x for x in manifest['feature_order']['base'] if x not in examples.columns],flush=True)
base_dir=personal/'citibike_experimental_models_c4e388f77d614002a3512d0478d40037'
print('BASE_MANIFEST',json.loads((base_dir/'manifest.json').read_text()),flush=True)


# COMMAND ----------

## Cell 34: test weather features on top of ACS and trips (not kept)
# Conditional exploratory weather test. A one-day-old forecast is NOT proof of actual receipt time.
params['end_date']='2026-10-05'
with urllib.request.urlopen('https://previous-runs-api.open-meteo.com/v1/forecast?'+urllib.parse.urlencode(params),timeout=45) as response: wx_json=json.load(response)
wx=pd.DataFrame(wx_json['hourly']);wx['weather_hour']=pd.to_datetime(wx.pop('time'),utc=True)
wx=wx[wx.weather_hour<pd.Timestamp('2026-10-05T04:00:00Z')].copy()
wx_cols=['temperature_2m_previous_day1','precipitation_previous_day1','wind_speed_10m_previous_day1']
assert not wx.weather_hour.duplicated().any() and wx[wx_cols].notna().all().all()
features=manifest['feature_order']['base'];acs_features=manifest['feature_order']['acs'];trip_cols=manifest['feature_order']['trips']
workx=examples.merge(acs_saved,on='station_id',how='left',validate='many_to_one',sort=False)
local=workx.fetched_at.dt.tz_convert('America/New_York');workx['trip_dow']=local.dt.dayofweek;workx['trip_hour']=local.dt.hour
workx=workx.merge(trip_saved,on=['station_id','trip_dow','trip_hour'],how='left',validate='many_to_one',sort=False)
workx['weather_hour']=workx.fetched_at.dt.floor('h')+pd.Timedelta(hours=1)
workx=workx.merge(wx[['weather_hour']+wx_cols],on='weather_hour',how='left',validate='many_to_one',sort=False)
assert len(workx)==len(examples) and workx[['station_id','fetched_at']].reset_index(drop=True).equals(examples[['station_id','fetched_at']].reset_index(drop=True))
print('WEATHER_MATCH',len(workx),'missing',workx[wx_cols].isna().sum().to_dict(),flush=True)
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score,log_loss
folds=[('Sep14-20',pd.Timestamp('2026-09-14T04:00Z'),pd.Timestamp('2026-09-21T04:00Z')),('Sep21-27',pd.Timestamp('2026-09-21T04:00Z'),pd.Timestamp('2026-09-28T04:00Z')),('Sep28-Oct04',pd.Timestamp('2026-09-28T04:00Z'),pd.Timestamp('2026-10-05T04:00Z'))]
weather_results=[];weather_selected={};weather_models={}
for week,start,end in folds:
    for outcome,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
        lab=outcome+'_label';cols=features+acs_features+trip_cols+wx_cols
        train=workx[(workx.fetched_at<start-pd.Timedelta(seconds=3750))&workx[lab].notna()&(workx[count]>0)]
        val=workx[(workx.fetched_at>=start)&(workx.fetched_at<end-pd.Timedelta(seconds=3750))&workx[outcome+'_ok']&(workx[count]>0)].copy()
        model=HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=15,min_samples_leaf=200,learning_rate=0.1,random_state=42)
        model.fit(train[cols],train[lab].astype(int));val['risk']=model.predict_proba(val[cols])[:,1]
        picks=val.sort_values(['fetched_at','risk','station_id'],ascending=[True,False,True]).groupby('fetched_at',sort=False).head(5)
        known=val[lab].notna(); row=(week,outcome,len(train),len(val),len(picks),int(picks[lab].sum()),int(picks[lab].isna().sum()),round(float(average_precision_score(val.loc[known,lab].astype(int),val.loc[known,'risk'])),6),round(float(log_loss(val.loc[known,lab].astype(int),val.loc[known,'risk'],labels=[0,1])),6))
        weather_results.append(row);weather_selected[week,outcome]=picks[['station_id','fetched_at',lab]].copy();weather_models[week,outcome]=model;print('WEATHER_RESULT',row,flush=True)
print('CONDITIONAL_WEATHER_TEST_COMPLETE. Forecast issuance and receipt are distinct; no test-week scoring.',flush=True)


# COMMAND ----------

## Cell 35: display the weather results, then compare the last development week by poverty third
print('WEATHER_COUNTS',[(r[0],r[1],r[5],r[6]) for r in weather_results],flush=True)
# Last development week only: reconstruct saved ACS+trip predictions and compare by cohort poverty tercile.
start,end=folds[-1][1:]
poverty='poverty_pct_500m'; groups=acs_saved[['station_id',poverty]].copy()
groups['group']='missing';known_station=groups[poverty].notna();groups.loc[known_station,'group']=pd.qcut(groups.loc[known_station,poverty].rank(method='first'),3,labels=['lower','middle','higher']).astype(str)
print('GROUP_STATIONS',groups.group.value_counts().to_dict(),flush=True)
equity_rows=[]
for outcome,count,expected in [('bike','num_bikes_available',1826),('dock','num_docks_available',2756)]:
    lab=outcome+'_label';val=workx[(workx.fetched_at>=start)&(workx.fetched_at<end-pd.Timedelta(seconds=3750))&workx[outcome+'_ok']&(workx[count]>0)].copy()
    m=joblib.load(exp/(outcome+'_acs_and_july_trips.joblib'));val['risk']=m.predict_proba(val[features+acs_features+trip_cols])[:,1]
    old=val.sort_values(['fetched_at','risk','station_id'],ascending=[True,False,True]).groupby('fetched_at',sort=False).head(5)[['station_id','fetched_at',lab]]
    assert int(old[lab].sum())==expected
    eligible=val[['station_id',lab]].merge(groups[['station_id','group']],on='station_id',validate='many_to_one')
    for variant,pick in [('acs_trips',old),('acs_trips_weather',weather_selected['Sep28-Oct04',outcome])]:
        selected_group=pick.merge(groups[['station_id','group']],on='station_id',validate='many_to_one')
        for g in ['lower','middle','higher','missing']:
            eg=eligible[eligible.group==g];pg=selected_group[selected_group.group==g];positive=int(eg[lab].sum());caught=int(pg[lab].sum());nknown=int(pg[lab].notna().sum())
            row=(outcome,variant,g,int(len(eg)),positive,len(pg),caught,len(pg)-nknown,round(caught/positive,4) if positive else None,round(caught/nknown,4) if nknown else None,int(pg.station_id.nunique()))
            equity_rows.append(row);print('GROUP_RESULT',row,flush=True)
print('GROUP_LIMITS: observational station snapshot analysis; cohort-specific poverty tertiles; not a measure of causal impact or rider demographics.',flush=True)


# COMMAND ----------

## Cell 36: display the group results and save the weather files privately
print('BIKE_GROUPS',[(r[1],r[2],r[5],r[6],r[8]) for r in equity_rows if r[0]=='bike'],flush=True)
print('DOCK_GROUPS',[(r[1],r[2],r[5],r[6],r[8]) for r in equity_rows if r[0]=='dock'],flush=True)
print('WX_UNITS',wx_json.get('hourly_units'),flush=True)
import uuid
weather_dir=personal/('Citi Bike - Historical Weather Development - '+uuid.uuid4().hex[:8]);weather_dir.mkdir(exist_ok=False)
wx.to_parquet(weather_dir/'development_day_ahead_forecasts.parquet',index=False)
for outcome in ['bike','dock']: joblib.dump(weather_models['Sep28-Oct04',outcome],weather_dir/(outcome+'_acs_trips_weather.joblib'))
record={'purpose':'conditional exploratory development only; no pilot deployment','source':'Open-Meteo Previous Runs API','request_parameters':params,'response_coordinates':{'latitude':wx_json.get('latitude'),'longitude':wx_json.get('longitude')},'hourly_units':wx_json.get('hourly_units'),'forecast_availability':'previous_day1 indicates 24-hour-ahead forecast; historical actual delivery and receipt times not verified','join':'forecast valid hour is floor(prediction timestamp)+1 hour; source forecast nominally 24 hours before valid hour','feature_order':features+acs_features+trip_cols+wx_cols,'weather_results':weather_results,'group_result_columns':['outcome','model','poverty_group','eligible_rows','eligible_known_positive','alerts','confirmed_positive','unknown_alerts','confirmed_capture_fraction','confirmed_precision','distinct_stations_alerted'],'group_results':equity_rows,'reserved_test_scored':False}
with (weather_dir/'manifest.json').open('x') as fh:json.dump(record,fh,indent=2,default=str)
print('SAVED_WEATHER',str(weather_dir),flush=True)


# COMMAND ----------

## Cell 37: display results and confirm the saved weather files reload
print('DOCK_GROUPS',[(r[1],r[2],r[5],r[6],r[8]) for r in equity_rows if r[0]=='dock'],flush=True)
print('SAVED_DIR',str(weather_dir),'units',wx_json['hourly_units'],flush=True)
check_model=joblib.load(weather_dir/'bike_acs_trips_weather.joblib');check_wx=pd.read_parquet(weather_dir/'development_day_ahead_forecasts.parquet')
print('RESTORE_CHECK',len(check_wx),len(check_model.feature_names_in_), 'expected_features',len(features+acs_features+trip_cols+wx_cols),flush=True)


# COMMAND ----------

## Cell 38: inspection: reload check and higher-poverty bike details
print('RECOVERY',len(check_wx),len(check_model.feature_names_in_),len(features+acs_features+trip_cols+wx_cols),'last',str(check_wx.weather_hour.max()),flush=True)
print('HIGH_POVERTY_BIKE_DETAILS',[(r[1],r[4],r[5],r[6],r[7],r[8],r[9],r[10]) for r in equity_rows if r[0]=='bike' and r[2]=='higher'],flush=True)


# COMMAND ----------

## Cell 39: inspection: session state
print('STATE', {k:k in globals() for k in ['examples','workx','folds','acs_saved','trip_saved','features','weather_selected','groups']},flush=True)


# COMMAND ----------

## Cell 40: three-week group audit (same pools, same models, same alert budget)
# Three-week exploratory group audit; same candidate pools, model specification, and alert budget.
from sklearn.ensemble import HistGradientBoostingClassifier
import numpy as np,pandas as pd
audit_rows=[];audit_station=[];audit_picks={}
for week,start,end in folds:
    for outcome,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
        lab=outcome+'_label'
        train=workx[(workx.fetched_at<start-pd.Timedelta(seconds=3750)) & workx[lab].notna() & (workx[count]>0)]
        pool=workx[(workx.fetched_at>=start)&(workx.fetched_at<end-pd.Timedelta(seconds=3750))&workx[outcome+'_ok']&(workx[count]>0)].copy()
        base=pool[['station_id','fetched_at',lab]].merge(groups[['station_id','group']],on='station_id',validate='many_to_one')
        for variant,cols in [('station_only',features),('acs_trips',features+acs_features+trip_cols)]:
            model=HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=15,min_samples_leaf=200,learning_rate=0.1,random_state=42)
            model.fit(train[cols],train[lab].astype(int));pool['risk']=model.predict_proba(pool[cols])[:,1]
            picks=pool.sort_values(['fetched_at','risk','station_id'],ascending=[True,False,True]).groupby('fetched_at',sort=False).head(5)[['station_id','fetched_at',lab]]
            audit_picks[week,outcome,variant]=picks.copy()
            pg=picks.merge(groups[['station_id','group']],on='station_id',validate='many_to_one')
            for g in ['lower','middle','higher','missing']:
                b=base[base.group==g];p=pg[pg.group==g];known=int(b[lab].notna().sum());pos=int(b[lab].sum());caught=int(p[lab].sum());nunk=int(p[lab].isna().sum())
                row=(week,outcome,variant,g,len(b),known,pos,int(b[lab].isna().sum()),len(p),caught,nunk,int(p.station_id.nunique()),round(caught/pos,4) if pos else None,round(caught/(len(p)-nunk),4) if len(p)>nunk else None)
                audit_rows.append(row)
            bs=base.groupby('station_id',observed=True)[lab].agg(positive='sum',eligible='size',eligible_known='count').reset_index()
            ps=pg.groupby('station_id',observed=True)[lab].agg(caught='sum',alerts='size',alert_known='count').reset_index()
            st=groups[['station_id','group']].merge(bs,on='station_id',how='left').merge(ps,on='station_id',how='left').fillna({'positive':0,'eligible':0,'eligible_known':0,'caught':0,'alerts':0,'alert_known':0})
            st['week']=week;st['outcome']=outcome;st['model']=variant;audit_station.append(st)
            print('AUDIT_COMPLETED',week,outcome,variant,'alerts',len(picks),'caught',int(picks[lab].sum()),flush=True)
audit_station=pd.concat(audit_station,ignore_index=True)
print('AUDIT_DONE','rows',len(audit_rows),'station_rows',len(audit_station),'reserved_test_scored',False,flush=True)


# COMMAND ----------

## Cell 41: group capture tables and the stations missing trip profiles
cols=['week','outcome','model','group','eligible','eligible_known','eligible_positive','eligible_unknown','alerts','caught','alert_unknown','stations_alerted','capture','precision']
audf=pd.DataFrame(audit_rows,columns=cols)
for outcome in ['bike','dock']:
    for model in ['station_only','acs_trips']:
        sub=audf[(audf.outcome==outcome)&(audf.model==model)&audf.group.isin(['lower','middle','higher'])]
        print('GROUP_CAPTURE',outcome,model,[(r.week,r.group,r.eligible_positive,r.caught,r.capture,r.alerts,r.alert_unknown,r.stations_alerted) for r in sub.itertuples()],flush=True)
missing_ids=set(trip_saved.loc[trip_saved[trip_cols[0]].isna(),'station_id'])
print('TRIP_MISSING_IDS',len(missing_ids),sorted(missing_ids),flush=True)
print('INFO_FIELDS',info.columns.tolist(),flush=True)


# COMMAND ----------

## Cell 42: display capture by week
for week,_,_ in folds:
    for outcome in ['bike','dock']:
        print('CAPTURE',week,outcome,[(m,[round(float(x),3) for x in audf[(audf.week==week)&(audf.outcome==outcome)&(audf.model==m)&audf.group.isin(['lower','middle','higher'])].capture]) for m in ['station_only','acs_trips']],flush=True)
print('INFO_COLUMNS',info.columns.tolist(),flush=True)
print('MISSING_PROFILE_IDS',sorted(missing_ids),flush=True)


# COMMAND ----------

## Cell 43: station resampling intervals within groups
print('INFO_COLUMNS',info.columns.tolist(),flush=True)
print('TRIP_MISSING_IDS',sorted(missing_ids),flush=True)
print('FINAL_DOCK',[(m,[round(float(x),3) for x in audf[(audf.week=='Sep28-Oct04')&(audf.outcome=='dock')&(audf.model==m)&audf.group.isin(['lower','middle','higher'])].capture]) for m in ['station_only','acs_trips']],flush=True)
rng=np.random.default_rng(20261110);cluster_intervals=[]
for week,_,_ in folds:
    for outcome in ['bike','dock']:
        for model in ['station_only','acs_trips']:
            st=audit_station[(audit_station.week==week)&(audit_station.outcome==outcome)&(audit_station.model==model)]
            distributions=[]
            for g in ['lower','higher']:
                arr=st[st.group==g][['caught','positive']].to_numpy(float);samples=arr[rng.integers(0,len(arr),size=(3000,len(arr)))].sum(axis=1)
                distributions.append(np.divide(samples[:,0],samples[:,1],out=np.full(len(samples),np.nan),where=samples[:,1]>0))
            dif=distributions[1]-distributions[0];actual=audf[(audf.week==week)&(audf.outcome==outcome)&(audf.model==model)].set_index('group')
            row=(week,outcome,model,round(float(actual.loc['higher','capture']-actual.loc['lower','capture']),3),*[round(float(x),3) for x in np.nanquantile(dif,[.025,.975])]);cluster_intervals.append(row)
            print('STATION_BOOTSTRAP',row,flush=True)
print('Intervals resample stations within groups; conditional on fitted models and group assignments, not causal.',flush=True)


# COMMAND ----------

## Cell 44: display intervals, shortage concentration, and current metadata for stations missing trip data
print('BIKE_INTERVALS',[r for r in cluster_intervals if r[1]=='bike'],flush=True)
print('DOCK_INTERVALS',[r for r in cluster_intervals if r[1]=='dock'],flush=True)
# Describe concentration of known positive snapshots and true positives among stations.
for week,_,_ in folds:
    x=audit_station[(audit_station.week==week)&(audit_station.outcome=='bike')&(audit_station.model=='acs_trips')&(audit_station.group=='higher')].sort_values('positive',ascending=False)
    print('HIGH_GROUP_CONCENTRATION',week,'positive_top3',int(x.head(3).positive.sum()),'positive_total',int(x.positive.sum()),'caught_top3',int(x.head(3).caught.sum()),'caught_total',int(x.caught.sum()),'zero_alert_stations',int((x.alerts==0).sum()),flush=True)
# Current public metadata is only a present-day clue, not July 2026 historical station evidence.
try:
    with urllib.request.urlopen('https://gbfs.citibikenyc.com/gbfs/en/station_information.json',timeout=20) as f:live_meta=json.load(f)
    live_stations=pd.DataFrame(live_meta['data']['stations']);print('LIVE_META_COLUMNS',live_stations.columns.tolist(),flush=True)
    print('MISSING_CURRENT',live_stations[live_stations.station_id.isin(missing_ids)][['station_id','name','short_name','lat','lon']].to_dict('records'),flush=True)
except Exception as e:print('LIVE_METADATA_ERROR',type(e).__name__,str(e)[:200],flush=True)


# COMMAND ----------

## Cell 45: display the missing stations' names and concentration by week
print('MISSING_CURRENT_NAMES',live_stations[live_stations.station_id.isin(missing_ids)][['name','short_name','lat','lon']].to_dict('records'),flush=True)
for week,_,_ in folds:
    x=audit_station[(audit_station.week==week)&(audit_station.outcome=='bike')&(audit_station.model=='acs_trips')&(audit_station.group=='higher')].sort_values('positive',ascending=False)
    print('HIGH_CONCENTRATION',week,'top3_positive',int(x.head(3).positive.sum()),'all_positive',int(x.positive.sum()),'top3_caught',int(x.head(3).caught.sum()),'all_caught',int(x.caught.sum()),'zero_alert',int((x.alerts==0).sum()),flush=True)


# COMMAND ----------

## Cell 46: check whether the Jersey City July trip archive exists
from urllib.request import Request,urlopen
for name in ['JC-202607-citibike-tripdata.csv.zip','JC-202607-citibike-tripdata.zip']:
    try:
        with urlopen(Request('https://s3.amazonaws.com/tripdata/'+name,method='HEAD'),timeout=20) as r:print('JC_ARCHIVE',name,r.headers.get('Last-Modified'),r.headers.get('Content-Length'),flush=True)
    except Exception as e:print('JC_ARCHIVE_UNAVAILABLE',name,type(e).__name__,str(e)[:100],flush=True)
print('MISSING_ASOF',station_start[station_start.station_id.isin(missing_ids)][['station_id','name','short_name']].to_dict('records'),flush=True)
for week,_,_ in folds:
    x=audit_station[(audit_station.week==week)&(audit_station.outcome=='bike')&(audit_station.model=='acs_trips')&(audit_station.group=='higher')].sort_values('positive',ascending=False)
    print('CONCENTRATION',week,int(x.head(3).positive.sum()),int(x.positive.sum()),int(x.head(3).caught.sum()),int(x.caught.sum()),int((x.alerts==0).sum()),flush=True)


# COMMAND ----------

## Cell 47: read the Jersey City and Hoboken July archive for the missing stations
# Inspect separate Jersey City/Hoboken July archive only for the six formerly missing stations.
import zipfile,io,hashlib,collections,calendar
url_jc='https://s3.amazonaws.com/tripdata/JC-202607-citibike-tripdata.csv.zip'
with urlopen(url_jc,timeout=45) as response: jc_bytes=response.read()
print('JC_BYTES_SHA',len(jc_bytes),hashlib.sha256(jc_bytes).hexdigest(),flush=True)
short_to_station=dict(zip(station_start.short_name.astype(str),station_start.station_id.astype(str)))
missing_short={k:v for k,v in short_to_station.items() if v in missing_ids};jc_counts=collections.Counter();jc_observed=set();jc_name_mismatch=collections.Counter();jc_rows=0
name_by_station=dict(zip(station_start.station_id.astype(str),station_start.name.astype(str)))
with zipfile.ZipFile(io.BytesIO(jc_bytes)) as z:
    files=[n for n in z.namelist() if n.lower().endswith('.csv')];print('JC_FILES',files,flush=True)
    for n in files:
        with z.open(n) as f:
            for chunk in pd.read_csv(f,usecols=['started_at','ended_at','start_station_id','end_station_id','start_station_name','end_station_name'],dtype='string',chunksize=100000):
                jc_rows+=len(chunk)
                for direction,dtcol,idcol,namecol in [('depart','started_at','start_station_id','start_station_name'),('arrive','ended_at','end_station_id','end_station_name')]:
                    x=chunk[chunk[idcol].isin(missing_short)].copy();dt=pd.to_datetime(x[dtcol],errors='coerce'); good=(dt>=pd.Timestamp('2026-07-01'))&(dt<pd.Timestamp('2026-08-01'));x=x[good];dt=dt[good]
                    if x.empty:continue
                    x['station_id']=x[idcol].map(missing_short);x['dow']=dt.dt.dayofweek;x['hour']=dt.dt.hour
                    for key,nrows in x.groupby(['station_id','dow','hour']).size().items():jc_counts[(direction,)+key]+=int(nrows)
                    jc_observed.update(x.station_id);jc_name_mismatch[direction]+=int((x[namecol].astype(str)!=x.station_id.map(name_by_station)).sum())
print('JC_MATCH',jc_rows,'matched_stations',len(jc_observed),'station_names',station_start[station_start.station_id.isin(jc_observed)][['name','short_name']].to_dict('records'),'name_mismatch',dict(jc_name_mismatch),flush=True)
trip_plus=trip_saved.copy();day_counts=pd.Series(pd.date_range('2026-07-01','2026-07-31').dayofweek).value_counts().to_dict()
for i,row in trip_plus[trip_plus.station_id.isin(jc_observed)].iterrows():
    for direction,col in [('depart',trip_cols[0]),('arrive',trip_cols[1])]:trip_plus.at[i,col]=jc_counts[(direction,row.station_id,row.trip_dow,row.trip_hour)]/day_counts[row.trip_dow]
print('COVERAGE_AFTER',100-trip_plus[trip_cols[0]].isna().groupby(trip_plus.station_id).all().sum(),'remaining_missing',int(trip_plus[trip_cols[0]].isna().groupby(trip_plus.station_id).all().sum()),flush=True)


# COMMAND ----------

## Cell 48: inspection: archive contents
print('ZIP_CONTENTS',files,flush=True)
with zipfile.ZipFile(io.BytesIO(jc_bytes)) as z:
    for n in files: print('ZIP_PREFIX',n,repr(z.read(n)[:80]),flush=True)


# COMMAND ----------

## Cell 49: count Jersey City trips for the missing stations
# The ZIP also contains a macOS metadata file ending in .csv; ignore that entry.
jc_counts=collections.Counter();jc_observed=set();jc_name_mismatch=collections.Counter();jc_rows=0
with zipfile.ZipFile(io.BytesIO(jc_bytes)) as z:
    csvs=[n for n in z.namelist() if n.lower().endswith('.csv') and not n.startswith('__MACOSX/') and not n.split('/')[-1].startswith('._')]
    for n in csvs:
        with z.open(n) as f:
            for chunk in pd.read_csv(f,usecols=['started_at','ended_at','start_station_id','end_station_id','start_station_name','end_station_name'],dtype='string',chunksize=100000):
                jc_rows+=len(chunk)
                for direction,dtcol,idcol,namecol in [('depart','started_at','start_station_id','start_station_name'),('arrive','ended_at','end_station_id','end_station_name')]:
                    x=chunk[chunk[idcol].isin(missing_short)].copy();dt=pd.to_datetime(x[dtcol],errors='coerce');good=(dt>=pd.Timestamp('2026-07-01'))&(dt<pd.Timestamp('2026-08-01'));x=x[good];dt=dt[good]
                    if x.empty:continue
                    x['station_id']=x[idcol].map(missing_short);x['dow']=dt.dt.dayofweek;x['hour']=dt.dt.hour
                    for key,nrows in x.groupby(['station_id','dow','hour']).size().items():jc_counts[(direction,)+key]+=int(nrows)
                    jc_observed.update(x.station_id);jc_name_mismatch[direction]+=int((x[namecol].astype(str)!=x.station_id.map(name_by_station)).sum())
trip_plus=trip_saved.copy();day_counts=pd.Series(pd.date_range('2026-07-01','2026-07-31').dayofweek).value_counts().to_dict()
for i,row in trip_plus[trip_plus.station_id.isin(jc_observed)].iterrows():
    for direction,col in [('depart',trip_cols[0]),('arrive',trip_cols[1])]:trip_plus.at[i,col]=jc_counts[(direction,row.station_id,row.trip_dow,row.trip_hour)]/day_counts[row.trip_dow]
remaining=set(trip_plus.loc[trip_plus[trip_cols[0]].isna(),'station_id'])
print('JC_FOUND',jc_rows,'stations',station_start[station_start.station_id.isin(jc_observed)][['name','short_name']].to_dict('records'),'mismatches',dict(jc_name_mismatch),'coverage',100-len(remaining),'missing',len(remaining),flush=True)


# COMMAND ----------

## Cell 50: refit the ACS-plus-trips model with the expanded July coverage
# Refit the same ACS-plus-trips specification using the expanded July coverage.
expanded=workx.drop(columns=trip_cols).merge(trip_plus,on=['station_id','trip_dow','trip_hour'],how='left',validate='many_to_one',sort=False)
assert len(expanded)==len(workx) and expanded[['station_id','fetched_at']].reset_index(drop=True).equals(workx[['station_id','fetched_at']].reset_index(drop=True))
expanded_results=[];expanded_models={};expanded_picks={}
for week,start,end in folds:
    for outcome,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
        lab=outcome+'_label';cols=features+acs_features+trip_cols
        tr=expanded[(expanded.fetched_at<start-pd.Timedelta(seconds=3750)) & expanded[lab].notna() & (expanded[count]>0)]
        val=expanded[(expanded.fetched_at>=start)&(expanded.fetched_at<end-pd.Timedelta(seconds=3750))&expanded[outcome+'_ok']&(expanded[count]>0)].copy()
        model=HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=15,min_samples_leaf=200,learning_rate=0.1,random_state=42)
        model.fit(tr[cols],tr[lab].astype(int));val['risk']=model.predict_proba(val[cols])[:,1]
        p=val.sort_values(['fetched_at','risk','station_id'],ascending=[True,False,True]).groupby('fetched_at',sort=False).head(5)[['station_id','fetched_at',lab]]
        prior=audit_picks[week,outcome,'acs_trips'];row=(week,outcome,int(p[lab].sum()),int(prior[lab].sum()),int(p[lab].isna().sum()),int(prior[lab].isna().sum()))
        expanded_results.append(row);expanded_models[week,outcome]=model;expanded_picks[week,outcome]=p;print('EXPANDED_RESULT',row,flush=True)
print('EXPANDED_TEST_COMPLETE',flush=True)


# COMMAND ----------

## Cell 51: remaining missing stations and expanded group results
print('REMAINING_STATIONS',station_start[station_start.station_id.isin(remaining)][['station_id','name','short_name']].to_dict('records'),flush=True)
for week,_,_ in folds:
    x=audit_station[(audit_station.week==week)&(audit_station.outcome=='bike')&(audit_station.model=='acs_trips')&(audit_station.group=='higher')].sort_values('positive',ascending=False)
    print('CONCENTRATION',week,'top3',int(x.head(3).positive.sum()),'all',int(x.positive.sum()),'top3_caught',int(x.head(3).caught.sum()),'all_caught',int(x.caught.sum()),'zero_alert_stations',int((x.alerts==0).sum()),flush=True)
for week,start,end in folds:
    for outcome,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
        lab=outcome+'_label';pool=expanded[(expanded.fetched_at>=start)&(expanded.fetched_at<end-pd.Timedelta(seconds=3750))&expanded[outcome+'_ok']&(expanded[count]>0)][['station_id',lab]].merge(groups[['station_id','group']],on='station_id',validate='many_to_one')
        p=expanded_picks[week,outcome].merge(groups[['station_id','group']],on='station_id',validate='many_to_one')
        print('EXPANDED_GROUP',week,outcome,[(g,int(pool.loc[pool.group==g,lab].sum()),int(p.loc[p.group==g,lab].sum()),round(p.loc[p.group==g,lab].sum()/pool.loc[pool.group==g,lab].sum(),3)) for g in ['lower','middle','higher']],flush=True)


# COMMAND ----------

## Cell 52: read-only search of the New York July records for the three remaining stations
# Read-only diagnostic: search NYC July records for the three remaining stations by name and numeric-equivalent short ID.
import tempfile,zipfile,hashlib,collections
from decimal import Decimal,InvalidOperation
missing_details=station_start[station_start.station_id.isin(remaining)][['station_id','name','short_name']].copy()
missing_names=set(missing_details.name.astype(str));missing_codes=set(Decimal(str(x)) for x in missing_details.short_name)
name_hits=collections.Counter();code_hits=collections.Counter();hit_ranges={}; total=0
with tempfile.TemporaryDirectory(prefix='citibike_july_audit_') as d:
    tempzip=Path(d)/'trips.zip';digest=hashlib.sha256()
    with urlopen('https://s3.amazonaws.com/tripdata/202607-citibike-tripdata.zip',timeout=60) as source,tempzip.open('wb') as target:
        while True:
            block=source.read(8*1024*1024)
            if not block:break
            digest.update(block);target.write(block)
    assert digest.hexdigest()=='9aeb4f2a1744ce4a81ab5a2bc999bb3e7794fc0a010415e64aeb80283a0c21c0'
    with zipfile.ZipFile(tempzip) as z:
        files=[n for n in z.namelist() if n.lower().endswith('.csv') and not n.startswith('__MACOSX/') and not n.split('/')[-1].startswith('._')]
        for name in files:
            with z.open(name) as f:
                for chunk in pd.read_csv(f,usecols=['started_at','ended_at','start_station_id','end_station_id','start_station_name','end_station_name'],dtype='string',chunksize=200000):
                    total+=len(chunk)
                    for side,dtcol,idcol,namecol in [('start','started_at','start_station_id','start_station_name'),('end','ended_at','end_station_id','end_station_name')]:
                        # IDs are matched textually after trimming decimal trailing zeros only for diagnosis, never as an automatic join.
                        ids=chunk[idcol].str.rstrip('0').str.rstrip('.'); targetcodes={format(x,'f').rstrip('0').rstrip('.') if '.' in format(x,'f') else format(x,'f') for x in missing_codes}
                        names=chunk[namecol].isin(missing_names); codes=ids.isin(targetcodes);q=chunk[names|codes]
                        if q.empty:continue
                        dt=pd.to_datetime(q[dtcol],errors='coerce');q=q[(dt>=pd.Timestamp('2026-07-01'))&(dt<pd.Timestamp('2026-08-01'))]
                        for (rawid,rawname),n in q.groupby([idcol,namecol],dropna=False).size().items():
                            key=(side,str(rawid),str(rawname));name_hits[key]+=int(n)
print('NYC_DIAGNOSTIC',total,'hits',dict(name_hits),'sha_verified',True,flush=True)


# COMMAND ----------

## Cell 53: save a new, versioned audit folder
# Persist a new, versioned development audit without changing earlier artifacts.
import uuid,json,joblib
audit_dir=personal/('Citi Bike - Neighborhood and Expanded Trip Audit - '+uuid.uuid4().hex[:8]);audit_dir.mkdir(exist_ok=False)
audf.to_parquet(audit_dir/'three_week_group_comparison.parquet',index=False)
audit_station.to_parquet(audit_dir/'three_week_station_comparison.parquet',index=False)
trip_plus.to_parquet(audit_dir/'july_trip_profiles_nyc_and_jc.parquet',index=False)
for outcome in ['bike','dock']:joblib.dump(expanded_models['Sep28-Oct04',outcome],audit_dir/(outcome+'_acs_expanded_trips.joblib'))
audit_manifest={'purpose':'Exploratory development audit; no test scoring or pilot deployment','features':features+acs_features+trip_cols,'expanded_trip_coverage':97,'remaining_unmatched_stations':missing_details.to_dict('records'),'nyc_archive_sha256':'9aeb4f2a1744ce4a81ab5a2bc999bb3e7794fc0a010415e64aeb80283a0c21c0','jc_archive_sha256':hashlib.sha256(jc_bytes).hexdigest(),'jc_current_last_modified':'2026-08-05T20:50:16Z','jc_profile_method':'fill only stations previously missing in NYC archive; no overlapping station contributions; July weekday-hour counts divided by number of weekdays in July; operating periods not verified','source_limitations':'July trip timestamps provisionally interpreted as New York local; ID mapping as of August 24; no proof of station operation all July or actual trip-file first-publication date; no cross-source record deduplication was needed for filled stations','expanded_results':expanded_results,'station_cluster_intervals_higher_minus_lower':cluster_intervals,'nyc_missing_name_or_numeric_equivalent_id_hits':dict(name_hits),'reserved_test_scored':False}
with (audit_dir/'manifest.json').open('x') as f:json.dump(audit_manifest,f,indent=2,default=str)
print('AUDIT_SAVED',str(audit_dir),'group_rows',len(audf),'station_rows',len(audit_station),'profiles',len(trip_plus),flush=True)
print('EXPANDED_RESULTS',expanded_results,flush=True)


# COMMAND ----------

## Cell 54: weekly audit rates and a reload check
for week,start,end in folds:
    out=[]
    for outcome,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
        lab=outcome+'_label';p=expanded_picks[week,outcome].merge(groups[['station_id','group']],on='station_id',validate='many_to_one')
        old=audf[(audf.week==week)&(audf.outcome==outcome)&(audf.model=='acs_trips')].set_index('group')
        out.append((outcome,[round(float(p.loc[p.group==g,lab].sum()/old.loc[g,'eligible_positive']),3) for g in ['lower','middle','higher']]))
    x=audit_station[(audit_station.week==week)&(audit_station.outcome=='bike')&(audit_station.model=='acs_trips')&(audit_station.group=='higher')].sort_values('positive',ascending=False)
    print('WEEK_AUDIT',week,'rates',out,'top3_positive',int(x.head(3).positive.sum()),'group_positive',int(x.positive.sum()),'zero_alert_stations',int((x.alerts==0).sum()),flush=True)
restored_model=joblib.load(audit_dir/'bike_acs_expanded_trips.joblib');restored_profiles=pd.read_parquet(audit_dir/'july_trip_profiles_nyc_and_jc.parquet')
assert list(restored_model.feature_names_in_)==features+acs_features+trip_cols and len(restored_profiles)==16800
print('AUDIT_RESTORE_OK',len(restored_profiles),'model_features',len(restored_model.feature_names_in_),flush=True)


# COMMAND ----------

## Cell 55: station-cluster resampling for the expanded-trip model; save the group table
# Station-cluster resampling for the expanded-trip model, including stations with no alerts.
expanded_groups=[];expanded_intervals=[];rng=np.random.default_rng(20261111)
for week,start,end in folds:
    for outcome,count in [('bike','num_bikes_available'),('dock','num_docks_available')]:
        lab=outcome+'_label';pool=expanded[(expanded.fetched_at>=start)&(expanded.fetched_at<end-pd.Timedelta(seconds=3750))&expanded[outcome+'_ok']&(expanded[count]>0)][['station_id',lab]].merge(groups[['station_id','group']],on='station_id',validate='many_to_one')
        picks=expanded_picks[week,outcome].merge(groups[['station_id','group']],on='station_id',validate='many_to_one')
        bs=pool.groupby('station_id')[lab].agg(positive='sum',eligible='size',eligible_known='count').reset_index();ps=picks.groupby('station_id')[lab].agg(caught='sum',alerts='size',alert_known='count').reset_index()
        st=groups[['station_id','group']].merge(bs,on='station_id',how='left').merge(ps,on='station_id',how='left').fillna({'positive':0,'eligible':0,'eligible_known':0,'caught':0,'alerts':0,'alert_known':0})
        distributions=[];rates=[]
        for g in ['lower','middle','higher','missing']:
            x=st[st.group==g];pos=float(x.positive.sum());caught=float(x.caught.sum());row=(week,outcome,g,int(x.eligible.sum()),int(x.eligible_known.sum()),int(pos),int(x.alerts.sum()),int(caught),int((x.alerts-x.alert_known).sum()),int((x.alerts>0).sum()),round(caught/pos,4) if pos else None);expanded_groups.append(row)
            if g in ['lower','higher']:
                arr=x[['caught','positive']].to_numpy(float);s=arr[rng.integers(0,len(arr),size=(3000,len(arr)))].sum(axis=1);distributions.append(np.divide(s[:,0],s[:,1],out=np.full(len(s),np.nan),where=s[:,1]>0));rates.append(caught/pos)
        diff=distributions[1]-distributions[0];rec=(week,outcome,round(rates[1]-rates[0],3),*[round(float(z),3) for z in np.nanquantile(diff,[.025,.975])]);expanded_intervals.append(rec);print('EXPANDED_INTERVAL',rec,flush=True)
expanded_group_cols=['week','outcome','group','eligible','eligible_known','positive','alerts','caught','alert_unknown','stations_alerted','capture']
pd.DataFrame(expanded_groups,columns=expanded_group_cols).to_parquet(audit_dir/'expanded_trip_group_comparison.parquet',index=False)
with (audit_dir/'expanded_trip_intervals.json').open('x') as f:json.dump({'method':'3000 independent station resamples within lower and higher groups; fixed fitted models','higher_minus_lower':expanded_intervals},f,indent=2)
print('EXPANDED_GROUP_AUDIT_SAVED',len(expanded_groups),flush=True)


# COMMAND ----------

