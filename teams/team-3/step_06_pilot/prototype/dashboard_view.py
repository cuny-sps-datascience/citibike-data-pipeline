"""Render a compact, offline monitoring view from a pilot SQLite checkpoint."""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
# (This file uses one-space indentation in the original; it is kept as is.)
#
# Purpose: read the pilot's SQLite checkpoint (predictions plus matched
# outcomes) and write one HTML page that compares two alert rules:
#   model     -> the 5 stations per outcome with the highest Risk Score (predicted probability)
#   low count -> the 5 stations per outcome with the fewest bikes (or docks) right now
# Optionally it also shows capture by neighborhood group (ACS thirds).

## Packages
import argparse, csv, html, json, sqlite3  # Command line, group file, HTML escaping, payloads, checkpoint database
from collections import defaultdict,Counter  # Group rows and count totals
from pathlib import Path  # Resolve file paths

## Escape a value for safe display in HTML
def esc(v):return html.escape(str(v))

## Build the full monitoring page
def report(db, groups=None):
 """groups: optional {grouping name: {station_id: group label}} for the per-group capture panel."""
 # Load every prediction with its matched outcome (if one exists yet).
 rows=db.execute('''SELECT p.run_id,p.station_id,p.outcome,p.payload,p.alert,o.payload FROM predictions p LEFT JOIN outcomes o ON o.run_id=p.run_id AND o.station_id=p.station_id AND o.outcome=p.outcome''').fetchall()
 grouped=defaultdict(list)  # (poll run, outcome) -> list of (prediction, alerted?, outcome)
 run_states=Counter(state for (state,) in db.execute('SELECT state FROM runs'))  # Count polls by final state (success, error...)
 for run,sid,outcome,payload,alert,outcome_json in rows:
  # Predictions with no outcome row yet are treated as pending.
  p=json.loads(payload);o=json.loads(outcome_json) if outcome_json else {'status':'pending','label':None}
  grouped[(run,outcome)].append((p,bool(alert),o))
 totals=Counter();recent=[];gtot=Counter()  # Overall totals, recent model alerts, per-group totals

 ## Tally results for each poll and outcome
 for (run,outcome),items in grouped.items():
  # Low-count rule: among stations not already empty, the 5 with the lowest count (ties by station ID).
  eligible=[x for x in items if x[0]['current_count']>0]
  baseline={x[0]['station_id'] for x in sorted(eligible,key=lambda x:(x[0]['current_count'],x[0]['station_id']))[:5]}
  for p,alert,o in items:
   # Count alerts and their later results under each rule.
   for rule,chosen in [('model',alert),('low count',p['station_id'] in baseline)]:
    if chosen:
     totals[(outcome,rule,'alerts')]+=1
     status=o['status'];label=o.get('label')
     # positive = later empty; negative = later not empty; otherwise unknown or pending.
     totals[(outcome,rule,'positive' if status=='known' and label==1 else 'negative' if status=='known' and label==0 else status)]+=1
   # Keep model alerts for the "recent alerts" table.
   if alert:recent.append((p['fetched_at'],outcome,p['station_id'],p['current_count'],o['status']))
   # Per-group tallies, only for stations that could become new shortages.
   if groups and p['current_count']>0:
    known_pos=o['status']=='known' and o.get('label')==1  # This snapshot was later confirmed empty
    for gname,gmap in groups.items():
     g=gmap.get(p['station_id'],'missing')  # Group label (lower, middle, higher) or missing
     gtot[(gname,outcome,'eligible',g)]+=1
     if known_pos:gtot[(gname,outcome,'positives',g)]+=1
     for rule,chosen in [('model',alert),('low count',p['station_id'] in baseline)]:
      if chosen:
       gtot[(gname,outcome,rule,'alerts',g)]+=1
       if known_pos:gtot[(gname,outcome,rule,'caught',g)]+=1  # Alert that was later confirmed
       if o['status'] in ('unknown','pending'):gtot[(gname,outcome,rule,o['status'],g)]+=1
 recent.sort(reverse=True)  # Newest alerts first

 ## Assemble the HTML page
 parts=['<!doctype html><meta charset="utf-8"><title>Citi Bike Pilot Monitoring</title>',
 '<style>body{font:16px system-ui;max-width:1000px;margin:3em auto;padding:0 1em;color:#18324a}table{border-collapse:collapse;width:100%;margin:1em 0}td,th{border-bottom:1px solid #ddd;text-align:left;padding:.55em}p{line-height:1.5}</style>',
 '<h1>Citi Bike pilot monitoring</h1><p>Development monitoring for the fixed working cohort. Counts represent station snapshots, not distinct incidents. Model and low-count rules select up to five candidates per outcome per update; currently empty stations are not new-shortage candidates. Outcomes can remain pending or unknown.</p>',
 '<h2>Recorded poll health</h2><p>'+esc(', '.join(f'{state}: {count}' for state,count in sorted(run_states.items())) if run_states else 'No recorded polls')+'. Missing expected slots require a separate schedule check.</p>',
 '<h2>Alert outcomes</h2><table><tr><th>Outcome</th><th>Rule</th><th>Alerts</th><th>Later empty</th><th>Later nonempty</th><th>Unknown</th><th>Pending</th></tr>']
 # One table row per outcome and rule.
 for outcome in ['bike','dock']:
  for rule in ['model','low count']:
   parts.append('<tr>'+''.join('<td>'+esc(x)+'</td>' for x in [outcome,rule,*[totals[(outcome,rule,k)] for k in ['alerts','positive','negative','unknown','pending']]])+'</tr>')
 if groups:parts.append(group_panel(gtot,groups))  # Add the neighborhood group panel when groups are given
 parts.append('</table><h2>Recent model alerts</h2><table><tr><th>Poll time (UTC)</th><th>Outcome</th><th>Station ID</th><th>Current count</th><th>Outcome status</th></tr>')
 parts.extend('<tr>'+''.join('<td>'+esc(v)+'</td>' for v in row)+'</tr>' for row in recent[:30])  # Show the 30 most recent
 parts.append('</table><p>Results depend on the supplied checkpoint and its completeness policy. No live alerts are sent by this view.</p>')
 return ''.join(parts)  # Return the page as one string

## Format a percentage, or n/a when the denominator is zero
def pct(n,d):return f'{100*n/d:.1f}%' if d else 'n/a'

## Build the capture-by-neighborhood-group panel
def group_panel(gtot,groups):
 """Recall by neighborhood group (shown as capture): true positives / known shortage snapshots among eligible stations."""
 out=['</table><h2>Capture by neighborhood group</h2><p>Monitoring, not evaluation. Capture is confirmed later-empty alerts divided by known shortage snapshots among eligible candidates in that group. Groups are thirds of the cohort by 500-meter ACS neighborhood estimates; they describe neighborhoods, not riders. Poverty share is also a model input. Small counts move a lot from day to day; treat any gap as a signal for review, not proof of a cause. Rows marked * have fewer than 20 known shortages.</p>']
 for gname,gmap in groups.items():
  # Group labels in a fixed order, plus "missing" only if any station lacked a group.
  labels=[g for g in ['lower','middle','higher'] if g in set(gmap.values())]+(['missing'] if any(gtot[(gname,o,'eligible','missing')] for o in ('bike','dock')) else [])
  out.append('<h3>'+esc(gname.replace('_',' '))+'</h3><table><tr><th>Outcome</th><th>Rule</th><th>Group</th><th>Known shortages</th><th>Caught</th><th>Capture</th><th>Share of alerts</th><th>Share of known shortages</th><th>Unknown alerts</th><th>Pending alerts</th></tr>')
  for outcome in ['bike','dock']:
   pos_total=sum(gtot[(gname,outcome,'positives',g)] for g in labels)  # All known shortages for this outcome
   for rule in ['model','low count']:
    alert_total=sum(gtot[(gname,outcome,rule,'alerts',g)] for g in labels)  # All alerts under this rule
    for g in labels:
     pos=gtot[(gname,outcome,'positives',g)];caught=gtot[(gname,outcome,rule,'caught',g)];al=gtot[(gname,outcome,rule,'alerts',g)]
     # Capture is Recall: true positives (the caught field) / known shortages in the group; * flags fewer than 20 known shortages.
     out.append('<tr>'+''.join('<td>'+esc(v)+'</td>' for v in [outcome,rule,g+('*' if pos<20 else ''),pos,caught,pct(caught,pos),pct(al,alert_total),pct(pos,pos_total),gtot[(gname,outcome,rule,'unknown',g)],gtot[(gname,outcome,rule,'pending',g)]])+'</tr>')
  out.append('</table>')
 return ''.join(out)

## Load station group labels from a CSV file
def load_groups(path):
 """CSV with station_id plus one column per grouping (for example poverty_share, no_vehicle_share)."""
 with open(path,newline='',encoding='utf-8') as f:
  rows=list(csv.DictReader(f))
 names=[c for c in rows[0] if c!='station_id']  # Every column except station_id is a grouping
 return {n:{r['station_id']:r[n] for r in rows} for n in names}  # grouping -> {station_id: label}

## Command-line entry point
def main():
 # Arguments: --db checkpoint file, --output HTML file, optional --groups CSV.
 p=argparse.ArgumentParser();p.add_argument('--db',required=True);p.add_argument('--output',required=True);p.add_argument('--groups');a=p.parse_args()
 # Open the checkpoint read-only (mode=ro), so the view can never change it.
 with sqlite3.connect('file:'+str(Path(a.db).resolve())+'?mode=ro',uri=True) as db: page=report(db,load_groups(a.groups) if a.groups else None)
 Path(a.output).write_text(page,encoding='utf-8')  # Save the page
if __name__=='__main__':main()
