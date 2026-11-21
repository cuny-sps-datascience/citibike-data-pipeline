# Reading copy with comments added. The code is unchanged from the original module (this file is not embedded in the deployed notebook).
#
# Purpose: a self-check for outcomes.match_outcomes, the function that decides
# whether a prediction made at time T came true one hour later. Each assert
# below tests one rule. If every rule holds, the script prints PASS.

## Packages and setup
import sys  # Edit the import path
from pathlib import Path  # Locate this file's folder
sys.path.insert(0, str(Path(__file__).resolve().parent))  # Let Python find outcomes.py next to this file
from outcomes import match_outcomes  # The function under test

## Create one example prediction: station "a" will run out of bikes by 01:00
p={'station_id':'a','outcome':'bike','fetched_at':'2026-09-28T00:00:00Z','target_at':'2026-09-28T01:00:00Z','model_version':'v'}


## Create one example observation of station "a" at a given time
def obs(at,bikes=0,renting=True,reported=None):
 # Default: 0 bikes (empty), renting on, station report time equal to the poll time.
 return {'station_id':'a','fetched_at':f'2026-09-28T{at}Z','available_at':f'2026-09-28T{at}Z',
 'station_reported_at':f'2026-09-28T{reported or at}Z','metadata_available_at':'2026-09-27T00:00:00Z',
 'num_bikes_available':bikes,'num_docks_available':5,'capacity':20,'is_installed':True,'is_renting':renting,'is_returning':True}


## Match one prediction against observations and return its single result
def run(pred, rows, watermark='2026-09-28T01:02:30Z', now='2026-09-28T01:05:00Z'):
 # watermark = the latest time for which all observations are known to be in.
 return match_outcomes([pred],rows,as_of=now,complete_through=watermark)[0]


## Confirm each matching rule
# No watermark yet: the outcome must stay pending.
assert run(p,[obs('01:00:00')],None)['status']=='pending'
# Watermark one second short of target + 2.5 minutes: still pending.
assert run(p,[obs('01:00:00')],'2026-09-28T01:02:29Z')['status']=='pending'
# Two observations equally far from the target (tie): the earlier one wins.
r=run(p,[obs('01:02:30',0),obs('00:57:30',2)])
assert r['label']==0 and r['matched_at'].endswith('00:57:30+00:00')
# An observation more than 2.5 minutes from the target is not used.
assert run(p,[obs('01:02:31')])['reason']=='no_observation_within_tolerance'
# The closest observation is chosen first, then checked for eligibility (not the other way round).
assert run(p,[obs('01:00:00',0,renting=False),obs('01:00:01',0)])['reason']=='nearest_observation_ineligible'
# A station report older than 5 minutes is stale, so the outcome is unknown.
assert run(p,[obs('01:00:00',0,reported='00:54:59')])['status']=='unknown'
# A dock outcome ignores the renting flag (renting only matters for bikes).
assert run(dict(p,outcome='dock'),[obs('01:00:00',0,renting=False)])['label']==0
# Zero bikes at the target time means the shortage happened (label 1).
assert run(p,[obs('01:00:00')])['label']==1
# An observation that only became available after "now" cannot be used.
late=obs('01:00:00');late['available_at']='2026-09-28T01:06:00Z'
assert run(p,[late])['reason']=='no_observation_within_tolerance'
# Duplicate observations must be refused.
try: run(p,[obs('01:00:00'),obs('01:00:00')]); raise AssertionError('duplicate observation accepted')
except ValueError: pass
# A watermark later than "now" must be refused.
try: run(p,[], '2026-09-28T01:06:00Z'); raise AssertionError('future watermark accepted')
except ValueError: pass

## Display the result
print('PASS: window watermark, boundary and tie, closest-before-eligibility, stale and unknown, outcome-specific flags, label, late availability, duplicates')
