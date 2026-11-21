"""Unscheduled single-process pilot. SQLite storage; caller supplies completeness.

Not a distributed scheduler, a dashboard publisher, or an ingestion watermark source.
"""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Purpose: the core of one pilot poll. run_poll downloads the feed, saves the
# raw responses and observations, scores the cohort, and saves predictions.
# evaluate later matches predictions to outcomes. run_health summarizes a
# window. Everything is stored in one SQLite database (the checkpoint).
#
# Tables in the database:
#   runs          -> one row per poll slot, with its final state
#   raw           -> the feed responses as received (cohort stations only when archived)
#   observations  -> one row per cohort station per poll
#   predictions   -> one row per station, outcome and poll, with the alert flag
#   outcomes      -> the matched result for each prediction

## Packages
import json, sqlite3, uuid  # Payloads, database, unique run IDs
from collections import Counter  # Count states and reasons
from datetime import datetime, timedelta, timezone  # Times and spans
from pathlib import Path  # Locate files next to this script
from urllib.request import Request, urlopen  # Download the public feed
from scoring import load_artifact, load_cohort, score_snapshot, ZeroScoreError, timestamp  # Scoring path
from normalization import flag, integer  # Value validation
from outcomes import match_outcomes  # Outcome matching
from private_artifacts import write_feed_archive  # Compressed raw response archive

## Setup
ROOT=Path(__file__).resolve().parent  # Folder holding the model and cohort files
URLS={'info':'https://gbfs.citibikenyc.com/gbfs/en/station_information.json',  # Station list with capacity
      'status':'https://gbfs.citibikenyc.com/gbfs/en/station_status.json'}  # Live bikes and docks


## Return the current time in UTC as text
def clock(): return datetime.now(timezone.utc).isoformat()


## Download one feed
def fetch(kind):
    """Return the decoded JSON of the information or status feed (15-second timeout)."""
    req=Request(URLS[kind],headers={'User-Agent':'DATA622-Team3-Pilot/1.0'})  # Identify the project to the feed
    with urlopen(req,timeout=15) as r: return json.load(r)


## Create the database tables if they do not exist
def init(db):
    """Create the five pilot tables and the observation index."""
    # PRIMARY KEY and UNIQUE constraints make duplicate slots or rows fail instead of being added twice.
    db.executescript('''CREATE TABLE IF NOT EXISTS runs(run_id TEXT PRIMARY KEY, slot TEXT NOT NULL UNIQUE, started TEXT NOT NULL, finished TEXT, state TEXT NOT NULL, details TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS raw(run_id TEXT PRIMARY KEY, metadata_ready TEXT, poll_start TEXT, poll_ready TEXT, info_json TEXT NOT NULL, status_json TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS observations(run_id TEXT, station_id TEXT, fetched_at TEXT, payload TEXT NOT NULL, PRIMARY KEY(run_id,station_id));
    CREATE INDEX IF NOT EXISTS observations_station_time ON observations(station_id,fetched_at);
    CREATE TABLE IF NOT EXISTS predictions(run_id TEXT, station_id TEXT, outcome TEXT, payload TEXT NOT NULL, alert INTEGER NOT NULL, PRIMARY KEY(run_id,station_id,outcome));
    CREATE TABLE IF NOT EXISTS outcomes(run_id TEXT, station_id TEXT, outcome TEXT, payload TEXT NOT NULL, PRIMARY KEY(run_id,station_id,outcome));''')
    db.commit()


## Extract the station list from a feed
def stations(feed):
    """Return feed['data']['stations'] after confirming it is a list."""
    x=feed['data']['stations']
    if not isinstance(x,list): raise ValueError('feed stations must be a list')
    return x


## Run one poll: download, save, score, save predictions
def run_poll(db,slot,*,get=fetch,now=clock,score_fn=None,model_version=None,collect_only=False,raw_archive_dir=None):
    """A slot is never silently retried: an existing slot must be investigated.

    Record raw payloads before parsing/scoring. Observations commit before scoring.
    The caller owns process concurrency; no completeness is inferred from a run.

    v16 options (defaults keep v15 behavior): score_fn(rows, scored_at) returns
    (scores, alerts, rejections) for another model, such as the ACS candidate;
    collect_only stores observations without scoring (used for the collection
    tail after a scoring window); raw_archive_dir writes the full compressed
    responses to a new per-run file first and keeps only cohort stations in the
    raw table, so checkpoints stay small.
    """
    slot=timestamp(slot).isoformat()  # Normalize the slot time to UTC text
    # Record the run as "started" first. `stage` tracks how far it got, so a failure is labeled precisely.
    init(db); run_id=str(uuid.uuid4()); started=now(); stage='no_status_response'
    db.execute('INSERT INTO runs VALUES(?,?,?,?,?,?)',(run_id,slot,started,None,'started','{}'));db.commit()  # Fails if the slot already exists
    try:
        ## Load the baseline and cohort, then download both feeds
        a=load_artifact(ROOT/'model_parameters.json'); cohort=load_cohort(ROOT/'cohort_reconstructed.csv',a['cohort_sha256_sorted_newline'])
        info_feed=get('info'); metadata_ready=now(); poll_start=now()  # Information first; poll time starts after it
        status_feed=get('status'); poll_ready=now(); stage='status_received_unresolved'

        ## Save the raw responses before any parsing
        if raw_archive_dir is not None:
            # Write the full responses to a compressed archive file, then keep only cohort stations in the database.
            archive=write_feed_archive(raw_archive_dir,run_id,info_feed,status_feed)
            def keep(feed):
                out={k:v for k,v in feed.items() if k!='data'}  # Keep feed headers such as last_updated
                try: out['data']={'stations':[x for x in feed['data']['stations'] if isinstance(x,dict) and x.get('station_id') in cohort]}
                except (KeyError,TypeError): out['data']=feed.get('data') if isinstance(feed,dict) else None  # Keep a malformed feed as received
                out['archive']=archive; return out  # Point to the full archive file and its fingerprint
            db.execute('INSERT INTO raw VALUES(?,?,?,?,?,?)',(run_id,metadata_ready,poll_start,poll_ready,json.dumps(keep(info_feed)),json.dumps(keep(status_feed))));db.commit()
        else:
            db.execute('INSERT INTO raw VALUES(?,?,?,?,?,?)',(run_id,metadata_ready,poll_start,poll_ready,json.dumps(info_feed),json.dumps(status_feed)));db.commit()
        metadata=stations(info_feed); status=stations(status_feed)

        ## Index each feed by station ID, refusing blank or duplicate IDs
        def index(rows):
            out={}
            for x in rows:
                sid=x['station_id']
                if not isinstance(sid,str) or not sid or sid in out: raise ValueError('invalid or duplicate station ID')
                out[sid]=x
            return out
        meta=index(metadata); observations=index(status)

        ## Build one observation row per cohort station
        rows=[]; rejected=Counter()
        for sid,s in observations.items():
            if sid not in cohort: continue  # Keep only the 100 cohort stations
            # Keep an observation even when its eligibility fields cannot be parsed.
            row={'station_id':sid,'fetched_at':poll_start,'available_at':poll_ready,
                 'metadata_available_at':metadata_ready}
            # Convert last_reported (seconds since 1970) to a UTC time; None if unusable.
            try: row['station_reported_at']=datetime.fromtimestamp(integer(s['last_reported']),timezone.utc).isoformat()
            except (KeyError,ValueError,TypeError,OverflowError,OSError): row['station_reported_at']=None
            for k in ('num_bikes_available','num_docks_available','is_installed','is_renting','is_returning'): row[k]=s.get(k)  # Copy values as received
            row['capacity']=meta.get(sid,{}).get('capacity')  # Capacity comes from the information feed
            if sid not in meta: rejected['missing_metadata']+=1
            rows.append(row)
        # Save observations in one transaction before scoring, so they survive a scoring failure.
        with db:
            db.executemany('INSERT INTO observations VALUES(?,?,?,?)',[(run_id,r['station_id'],r['fetched_at'],json.dumps(r)) for r in rows])
        stage='observations_saved'

        ## Collection-only polls (the 60-minute tail) stop here
        if collect_only:
            details={'collect_only':True,'cohort_seen':len(set(observations)&cohort),'cohort_missing':len(cohort-set(observations)),
                     'adapter_counts':dict(rejected),'info_last_updated':info_feed.get('last_updated'),'status_last_updated':status_feed.get('last_updated')}
            with db: db.execute('UPDATE runs SET finished=?, state=?, details=? WHERE run_id=?',(now(),'success',json.dumps(details),run_id))
            return run_id,details

        ## Score the poll and save predictions
        scored_at=now()
        # Use the supplied model (ACS candidate) or the logistic baseline.
        scores,alerts,score_rejected=score_fn(rows,scored_at) if score_fn is not None else score_snapshot(rows,a,cohort,scored_at)
        alertkeys={(p['station_id'],o) for o,ps in alerts.items() for p in ps}  # (station, outcome) pairs chosen as alerts
        details={'cohort_seen':len(set(observations)&cohort),'cohort_missing':len(cohort-set(observations)),
                 'adapter_counts':dict(rejected),'score_rejections':score_rejected,
                 'score_counts':dict(Counter(p['outcome'] for p in scores)),
                 'info_last_updated':info_feed.get('last_updated'),'status_last_updated':status_feed.get('last_updated'),
                 'model_version':model_version or a['version']}
        # Save predictions and mark the run successful in one transaction.
        with db:
            db.executemany('INSERT INTO predictions VALUES(?,?,?,?,?)',[(run_id,p['station_id'],p['outcome'],json.dumps(p),int((p['station_id'],p['outcome']) in alertkeys)) for p in scores])
            db.execute('UPDATE runs SET finished=?, state=?, details=? WHERE run_id=?',(now(),'success',json.dumps(details),run_id))
        return run_id,details
    except Exception as e:
        # Record the failure with the stage it reached, then re-raise so the caller sees it.
        detail={'error_type':type(e).__name__,'failure_stage':stage}
        if isinstance(e,ZeroScoreError):detail['score_rejections']=e.reasons
        # Stage -> failed state: no response saved / response saved but unparsed / observations saved.
        with db: db.execute('UPDATE runs SET finished=?,state=?,details=? WHERE run_id=?',(now(), {'no_status_response':'failed_missing','status_received_unresolved':'failed_unresolved','observations_saved':'failed_observations_saved'}[stage],json.dumps(detail),run_id))
        raise


## Match waiting predictions to outcomes
def evaluate(db, *, as_of, complete_through=None):
    """Match unresolved predictions only; final outcomes are immutable.

    An external caller must prove the watermark and account for expected slots,
    retries and late arrivals. A missing response is a recorded collection gap.
    A received but unparseable response blocks overlapping outcome windows.
    """
    from datetime import timedelta
    init(db); now=timestamp(as_of)
    watermark=timestamp(complete_through) if complete_through is not None else None
    # Confirm the watermark is not in the future.
    if watermark is not None and watermark>now: raise ValueError('watermark cannot be later than evaluation time')
    # Load predictions with no outcome yet, or whose outcome is still pending.
    waiting=db.execute("""SELECT p.run_id,p.payload FROM predictions p LEFT JOIN outcomes o
        ON p.run_id=o.run_id AND p.station_id=o.station_id AND p.outcome=o.outcome
        WHERE o.payload IS NULL OR json_extract(o.payload,'$.status')='pending'""").fetchall()
    # List polls whose observations might still be missing (still running, or failed after a response arrived).
    unresolved=[]
    for state,started,poll_start in db.execute("""SELECT r.state,r.started,raw.poll_start FROM runs r LEFT JOIN raw ON raw.run_id=r.run_id
        WHERE r.state IN ('started','failed_unresolved','abandoned_unresolved')"""):
        unresolved.append((state,timestamp(poll_start or started),poll_start is None))  # (state, time, time unknown?)
    counts=Counter()
    for rid,payload in waiting:
        # Matching window: target time plus or minus 150 seconds.
        p=json.loads(payload); target=timestamp(p['target_at']); left=target-timedelta(seconds=150); right=target+timedelta(seconds=150)
        # Stay pending if observations are not complete through the window, or an unresolved poll could fall inside it.
        if watermark is None or watermark<right or any(t<=right for state,t,unknown_time in unresolved if state in ('started','abandoned_unresolved') or unknown_time) or any(left<=t<=right for state,t,unknown_time in unresolved if state=='failed_unresolved' and not unknown_time):
            counts['pending']+=1; continue
        # Load this station's observations inside the matching window and match them.
        rows=db.execute('SELECT payload FROM observations WHERE station_id=? AND fetched_at>=? AND fetched_at<=?',(p['station_id'],left.isoformat(),right.isoformat())).fetchall()
        result=match_outcomes([p],[json.loads(r[0]) for r in rows],as_of=as_of,complete_through=complete_through)[0]
        # Insert the outcome, or update it only if it was still pending (final outcomes never change).
        with db:
            db.execute('INSERT OR IGNORE INTO outcomes VALUES(?,?,?,?)',(rid,p['station_id'],p['outcome'],json.dumps(result)))
            db.execute("UPDATE outcomes SET payload=? WHERE run_id=? AND station_id=? AND outcome=? AND json_extract(payload,'$.status')='pending'",(json.dumps(result),rid,p['station_id'],p['outcome']))
        counts[result['status']]+=1
    return counts  # Counts by status for this evaluation pass


## Mark polls that have been "started" too long as abandoned
def mark_abandoned(db, *, as_of, timeout_seconds):
    """Mark timed-out starts unresolved; never assert a response was absent.

    Call only after proving the old worker is no longer running. An abandoned
    run remains a completeness blocker until its possible observation is resolved.
    """
    init(db); now=timestamp(as_of)
    if timeout_seconds<=0: raise ValueError('timeout must be positive')
    stale=[rid for rid,started in db.execute("SELECT run_id,started FROM runs WHERE state='started'") if now-timestamp(started)>=timedelta(seconds=timeout_seconds)]
    with db:
        db.executemany("UPDATE runs SET state='abandoned_unresolved',finished=? WHERE run_id=? AND state='started'",[(now.isoformat(),rid) for rid in stale])
    return len(stale)  # Number of runs marked


## Close out one run from a worker known to be dead
def resolve_dead_run(db, run_id, *, worker_confirmed_dead, no_backfill, as_of):
    """Resolve one abandoned worker, using only evidence persisted in this DB.

    A missing raw row is a gap in *our saved collection*, not proof that no
    response reached the worker. Existing raw without observations remains
    unresolved in its recorded poll window. At least one saved observation
    proves the observation transaction committed. Empty batches remain
    unresolved because the old schema has no explicit commit marker.
    The caller is responsible for establishing both preconditions externally.
    """
    # Confirm the caller explicitly vouched for both conditions.
    if worker_confirmed_dead is not True or no_backfill is not True:
        raise ValueError('confirmed dead worker and no backfill required')
    init(db); finished=timestamp(as_of).isoformat()
    row=db.execute('SELECT state FROM runs WHERE run_id=?',(run_id,)).fetchone()
    if not row or row[0] not in ('started','abandoned_unresolved'):
        raise ValueError('run is not unresolved')
    # Decide the final state from what was saved: observations, raw only, or nothing.
    raw=db.execute('SELECT 1 FROM raw WHERE run_id=?',(run_id,)).fetchone()
    saved=db.execute('SELECT 1 FROM observations WHERE run_id=? LIMIT 1',(run_id,)).fetchone()
    state='failed_observations_saved' if saved else 'failed_unresolved' if raw else 'failed_missing'
    detail=json.dumps({'recovery':'confirmed_dead','saved_raw':bool(raw),'saved_observations':bool(saved),
                       'missing_means':'no persisted response in our collection' if not raw else None})
    with db:
        changed=db.execute("UPDATE runs SET state=?,finished=?,details=? WHERE run_id=? AND state IN ('started','abandoned_unresolved')",(state,finished,detail,run_id)).rowcount
    # Confirm exactly one row changed; otherwise something else touched the run.
    if changed != 1: raise RuntimeError('run changed during recovery')
    return state


## Return the collection watermark (time through which observations are complete)
def collection_watermark(current_run_start, *, single_writer_verified, no_backfill):
    """Conditional collection watermark; caller must establish both conditions.

    This is complete only for observations saved by earlier collection runs;
    evaluate separately checks unresolved runs. It is not a lock or proof of
    external feed completeness, and it excludes the current run's response.
    """
    if single_writer_verified is not True or no_backfill is not True:
        raise ValueError('verified single writer and no backfill required')
    return timestamp(current_run_start).isoformat()


## List expected poll slots with no recorded run
def expected_slot_gaps(db, start, end, *, cadence_seconds=300):
    """Count unrecorded UTC slots in [start,end); this is not a watermark."""
    init(db); a=timestamp(start); b=timestamp(end)
    if a>=b or cadence_seconds<=0: raise ValueError('invalid slot interval')
    recorded={timestamp(row[0]) for row in db.execute('SELECT slot FROM runs')}
    gaps=[]; t=a
    # Step through the window every 5 minutes and note slots with no run.
    while t<b:
        if t not in recorded: gaps.append(t.isoformat())
        t+=timedelta(seconds=cadence_seconds)
    return gaps


## Summarize a window's health (written into the window report and run_health file)
def run_health(db, start, end, *, cadence_seconds=300):
    """Summarize a caller-supplied window; failed and absent slots are distinct."""
    init(db); a=timestamp(start); b=timestamp(end)
    gaps=expected_slot_gaps(db,a.isoformat(),b.isoformat(),cadence_seconds=cadence_seconds)
    # Runs whose slot falls inside the window.
    rows=[(rid,started,state,json.loads(details)) for rid,started,state,details,slot in db.execute('SELECT run_id,started,state,details,slot FROM runs') if a<=timestamp(slot)<b]
    states=Counter(state for _,_,state,_ in rows); rejections=Counter(); ages={'info':[],'status':[]}
    ids={rid for rid,_,_,_ in rows}
    for _,started,_,details in rows:
        rejections.update(details.get('score_rejections',{}))  # Add up rejection reasons across polls
        # Feed age = poll start minus the feed's own last_updated time.
        for kind in ages:
            v=details.get(kind+'_last_updated')
            if isinstance(v,(int,float)) and not isinstance(v,bool):
                ages[kind].append((timestamp(started)-datetime.fromtimestamp(v,timezone.utc)).total_seconds())
    # Count outcomes by status for this window's runs.
    counts=Counter()
    for rid,payload in db.execute('SELECT run_id,payload FROM outcomes'):
        if rid in ids: counts[json.loads(payload)['status']]+=1
    finalized=counts['known']+counts['unknown']
    # Return the summary; unknown share is the main data-quality number for the window.
    return {'attempted':len(rows),'states':dict(states),'failed':sum(n for s,n in states.items() if s.startswith('failed')),
            'missing_slots':len(gaps),'rejections':dict(rejections),
            'feed_age_seconds':{kind:{'min':min(v),'max':max(v)} if v else None for kind,v in ages.items()},
            'known':counts['known'],'unknown':counts['unknown'],
            'unknown_share_of_finalized':counts['unknown']/finalized if finalized else None}


## Command-line entry point: run one poll into a given database
def main():
    import argparse
    parser=argparse.ArgumentParser(description='Unscheduled pilot: explicitly supply a private SQLite path and poll slot.')
    parser.add_argument('--db',required=True); parser.add_argument('--slot',required=True,help='timezone-aware ISO poll slot')
    args=parser.parse_args(); timestamp(args.slot)  # Confirm the slot parses before touching the database
    with sqlite3.connect(args.db) as db:
        run_id,details=run_poll(db,args.slot)
        print(json.dumps({'run_id':run_id,'state':'success','details':details},indent=2))
if __name__=='__main__':main()
