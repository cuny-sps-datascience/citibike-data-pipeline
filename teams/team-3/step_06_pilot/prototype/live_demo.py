"""One read-only, unscheduled public-GBFS demonstration. Prints JSON; sends no alerts."""
# Reading copy with comments added. The code is unchanged from the original module (this file is not embedded in the deployed notebook).
#
# Purpose: a one-off demonstration. Download the public Citi Bike feed (GBFS,
# the General Bikeshare Feed Specification) once, turn it into the row format
# the scorer expects, score it with the logistic baseline, and print the result.

## Packages
import json  # Parse the feed and print the result
from datetime import datetime, timezone  # Record when each step happened, in UTC
from pathlib import Path  # Locate files next to this script
from urllib.request import Request, urlopen  # Download the public feed

from scoring import load_artifact, load_cohort, score_snapshot  # Model, cohort and scoring path
from normalization import flag  # Convert 0/1 feed flags to True/False

## Setup
ROOT = Path(__file__).resolve().parent  # Folder holding the model and cohort files
URLS = {
    'info': 'https://gbfs.citibikenyc.com/gbfs/en/station_information.json',  # Station list with capacity
    'status': 'https://gbfs.citibikenyc.com/gbfs/en/station_status.json',  # Live bikes and docks per station
}


## Download one feed and return its station list
def get_feed(kind):
    """Return the 'stations' list from the information or status feed."""
    # Identify the project in the request header, as public APIs ask clients to do.
    request = Request(URLS[kind], headers={'User-Agent': 'DATA622-Team3-Development-Demo/1.0'})
    with urlopen(request, timeout=15) as response:  # Give up after 15 seconds
        data = json.load(response)
    stations = data['data']['stations']
    # Confirm the feed has the expected shape.
    if not isinstance(stations, list):
        raise ValueError('station feed is not a list')
    return stations


## Run the demonstration once
def main():
    """Download, score and print one poll of the cohort stations."""
    ## Load the model and cohort
    artifact = load_artifact(ROOT / 'model_parameters.json')  # Frozen logistic baseline
    cohort = load_cohort(ROOT / 'cohort_reconstructed.csv', artifact['cohort_sha256_sorted_newline'])  # 100 stations, fingerprint-checked

    ## Download station information first, then status
    info = get_feed('info')
    metadata_ready = datetime.now(timezone.utc)  # Metadata is known from this moment
    metadata = {r['station_id']: r for r in info}  # station_id -> station information
    # Confirm the information feed lists each station once.
    if len(metadata) != len(info):
        raise ValueError('duplicate metadata station ID')
    poll_start = datetime.now(timezone.utc)  # Poll start (used as the prediction time)
    status = get_feed('status')
    poll_ready = datetime.now(timezone.utc)  # Poll result available
    # Confirm the status feed lists each station once.
    if len({r['station_id'] for r in status}) != len(status):
        raise ValueError('duplicate status station ID')

    ## Build scorer rows for cohort stations
    rows, rejected, seen = [], {}, set()
    for s in status:
        sid = s['station_id']
        if sid not in cohort:
            continue  # Keep only the 100 cohort stations
        seen.add(sid)
        # A station without information has no capacity, so it cannot be scored.
        if sid not in metadata:
            rejected['missing_metadata'] = rejected.get('missing_metadata', 0) + 1
            continue
        try:
            reported = datetime.fromtimestamp(s['last_reported'], timezone.utc)  # Feed gives seconds since 1970
            row = {'station_id': sid, 'fetched_at': poll_start.isoformat(),
                   'available_at': poll_ready.isoformat(),
                   'station_reported_at': reported.isoformat(),
                   'metadata_available_at': metadata_ready.isoformat(),
                   'num_bikes_available': s['num_bikes_available'],
                   'num_docks_available': s['num_docks_available'],
                   'capacity': metadata[sid]['capacity'],  # Capacity comes from the information feed
                   'is_installed': flag(s.get('is_installed')),
                   'is_renting': flag(s.get('is_renting')),
                   'is_returning': flag(s.get('is_returning'))}
            rows.append(row)
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            rejected['bad_fields'] = rejected.get('bad_fields', 0) + 1  # Missing or malformed field

    ## Score and display the result
    scored_at = datetime.now(timezone.utc)
    scores, alerts, score_rejected = score_snapshot(rows, artifact, cohort, scored_at.isoformat())
    # Display timing, coverage, rejection counts and the top alerts so the run can be checked by eye.
    print(json.dumps({'purpose': 'development only; no alerts sent',
                      'metadata_available_at': metadata_ready.isoformat(),
                      'poll_started_at': poll_start.isoformat(),
                      'poll_available_at': poll_ready.isoformat(),
                      'scored_at': scored_at.isoformat(),
                      'cohort_seen': len(seen), 'cohort_missing': len(cohort-seen),  # Cohort stations present and absent in the feed
                      'adapter_rejected': rejected, 'score_rejected': score_rejected,
                      'score_counts': {o: sum(x['outcome'] == o for x in scores) for o in ('bike','dock')},
                      'alerts': alerts}, indent=2))


if __name__ == '__main__':
    main()
