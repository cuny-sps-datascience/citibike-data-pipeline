"""Development-only, in-memory 60-minute Citi Bike scorer; no network or writes."""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Purpose: the shared scoring path. For one poll of the station feed it checks
# each station row, builds the seven model features, asks a model for the
# probability of a shortage within 60 minutes, and returns the scored rows,
# the top-5 alerts per outcome, and counts of why rows were rejected.

## Packages
import csv  # Read the cohort station list
import hashlib  # Fingerprint the cohort
import json  # Read the logistic model artifact
import math  # Sine and cosine of the hour, logistic function, finiteness checks
from datetime import datetime, timedelta, timezone  # Time parsing and the 60-minute target
from zoneinfo import ZoneInfo  # New York local time for the hour-of-day features
from collections import Counter  # Count rejection reasons
from normalization import fields  # Read and validate the six station fields

## Constants
# The seven live station features, in the order the baseline models expect.
FEATURES = ['num_bikes_available', 'num_docks_available', 'capacity', 'bike_fraction',  # Counts and bike share of capacity
            'dock_fraction', 'hour_sin', 'hour_cos']  # Dock share and time of day as a circle
NY = ZoneInfo('America/New_York')  # Hour-of-day is measured in New York time


## Parse an ISO 8601 timestamp into UTC
def timestamp(value):
    """Return a UTC datetime; refuse non-strings and times without a time zone."""
    # Confirm the value is text; numbers or datetimes would hide time zone mistakes.
    if not isinstance(value, str):
        raise ValueError('timestamps must be ISO 8601 strings with timezone')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))  # Accept the "Z" suffix for UTC
    # Confirm the time zone is present, so times from different sources compare correctly.
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError('timestamp lacks timezone')
    return result.astimezone(timezone.utc)  # Return the same moment expressed in UTC


## Load the 100-station cohort and confirm it is the frozen one
def load_cohort(path, expected_digest):
    """Return the set of 100 cohort station IDs after a fingerprint check."""
    with open(path, newline='', encoding='utf-8') as f:
        ids = [row['station_id'] for row in csv.DictReader(f)]  # Extract the station ID column
    # Confirm exactly 100 distinct, non-blank station IDs.
    if len(ids) != 100 or len(set(ids)) != 100 or any(not x for x in ids):
        raise ValueError('cohort must contain 100 distinct station IDs')
    # Fingerprint the sorted IDs (one per line) and compare with the model artifact.
    digest = hashlib.sha256('\n'.join(sorted(ids)).encode()).hexdigest()
    if digest != expected_digest:
        raise ValueError('cohort hash does not match model artifact')
    return set(ids)


## Load the frozen logistic baseline and confirm its structure
def load_artifact(path):
    """Return the logistic model artifact after shape and value checks."""
    with open(path, encoding='utf-8') as f:
        a = json.load(f)
    # Confirm a 60-minute model with exactly a bike model and a dock model.
    if a['horizon_minutes'] != 60 or set(a['models']) != {'bike','dock'}:
        raise ValueError('unexpected model artifact')
    for m in a['models'].values():
        # Confirm the feature order and the 0/1 class coding match this code.
        if m['feature_order'] != FEATURES or m['classes'] != [0, 1]:
            raise ValueError('unexpected feature order or class encoding')
        # Confirm one mean, scale and coefficient per feature.
        if any(len(m[k]) != len(FEATURES) for k in ('mean','scale','coef')):
            raise ValueError('invalid model dimensions')
        # Confirm no parameter is NaN or infinite.
        if not all(math.isfinite(float(x)) for k in ('mean','scale','coef') for x in m[k]):
            raise ValueError('non-finite model parameter')
        # Confirm every scale is positive (it is a divisor) and the intercept is finite.
        if not all(float(x) > 0 for x in m['scale']) or not math.isfinite(float(m['intercept'])):
            raise ValueError('invalid model parameter')
    return a


## Calculate a logistic regression Risk Score by hand
def probability(model, features):
    """Return the logistic probability for one feature row, using the saved standardization."""
    # Standardize each feature, multiply by its coefficient, and add the intercept.
    z = float(model['intercept']) + sum(float(c) * (float(x)-float(mu))/float(sd)
        for x,mu,sd,c in zip(features, model['mean'], model['scale'], model['coef']))
    # Two algebraically equal forms; each avoids overflow for its sign of z.
    return 1/(1+math.exp(-z)) if z >= 0 else math.exp(z)/(1+math.exp(z))


## Score one poll of the station feed
def score_snapshot(records, model_artifact, cohort, scored_at, *, probability_fn=None, model_version=None, positive_only=False, station_probability_fn=None):
    """Score one poll. Required fields documented in README. Returns records and alerts.

    Caller must supply point-in-time metadata and collection completion time. A malformed
    snapshot fails closed. Currently empty stations are scored, but excluded from alerts.
    """
    # Arguments:
    #   probability_fn          -> optional model taking (outcome, features)
    #   station_probability_fn  -> optional model taking (outcome, station_id, features); used by the histogram gradient boosting model with ACS
    #   positive_only           -> skip stations already at zero (the candidate models were trained without them)
    now = timestamp(scored_at)  # The scoring clock
    seen, scored, candidates = set(), [], {'bike': [], 'dock': []}  # Seen IDs, scored rows, alert candidates
    rejected = Counter()  # Reason -> number of rows rejected
    poll_key = None  # (poll start, poll available) shared by every row of one poll

    for r in records:
        sid = r['station_id']
        # Refuse a poll that lists the same station twice.
        if sid in seen:
            raise ValueError('duplicate station ID in snapshot')
        seen.add(sid)
        fetched = timestamp(r['fetched_at'])  # Poll start time
        available = timestamp(r['available_at'])  # Time the poll result was stored
        # Confirm every row comes from the same poll.
        if poll_key is None:
            poll_key = (fetched, available)
        elif poll_key != (fetched, available):
            raise ValueError('all rows must belong to one poll')
        # Keep only the 100 cohort stations.
        if sid not in cohort:
            rejected['outside_cohort'] += 1
            continue
        # Parse the station's own report time and the metadata time.
        try:
            reported = timestamp(r['station_reported_at'])
            metadata_at = timestamp(r['metadata_available_at'])
        except (KeyError, ValueError, TypeError, OverflowError):
            rejected['invalid_station_or_metadata_timestamp'] += 1
            continue
        # Training timestamps use the poll-start time. A later availability time is
        # necessary for live use and is separately checked against the scoring clock.
        if not (metadata_at <= fetched <= available <= now):
            rejected['invalid_availability_order'] += 1
            continue
        # A station report later than the poll start means the clocks disagree.
        if (fetched-reported).total_seconds() < 0:
            rejected['report_after_poll_start_clock_skew'] += 1
            continue
        # Reject stale data: the station must have reported within 5 minutes of the poll and of now.
        if not (0 <= (fetched-reported).total_seconds() <= 300 and
                0 <= (now-reported).total_seconds() <= 300):
            rejected['stale_report'] += 1
            continue
        # Read the six station fields; malformed values reject the row.
        try:
            bikes, docks, capacity, installed, renting, returning = fields(r)
        except (KeyError, ValueError, TypeError, OverflowError):
            rejected['invalid_numeric_or_flag'] += 1
            continue
        # Confirm counts are possible (capacity above zero, no negative counts).
        if capacity <= 0 or bikes < 0 or docks < 0:
            rejected['invalid_counts_or_capacity'] += 1
            continue
        # Skip stations that are not installed.
        if not installed:
            rejected['not_installed'] += 1
            continue

        ## Build the seven features
        local = fetched.astimezone(NY)  # Poll time in New York
        # Map the time of day onto a circle, so 23:59 and 00:00 are close together.
        phase = (local.hour + local.minute/60 + local.second/3600)*2*math.pi/24
        x = [bikes,docks,capacity,bikes/capacity,docks/capacity,math.sin(phase),math.cos(phase)]

        ## Score each outcome: bike shortage uses bikes; dock shortage uses open docks
        for outcome, enabled, count in [('bike',renting,bikes),('dock',returning,docks)]:
            # Skip an outcome when the station has that service turned off.
            if not enabled:
                rejected[outcome + '_service_disabled'] += 1
                continue
            # Candidate models skip stations already at zero (they are already short).
            if positive_only and count == 0:
                rejected[outcome + '_already_empty'] += 1
                continue
            if station_probability_fn is not None:
                # Station-aware model (ACS candidate): needs the station's neighborhood values.
                try:
                    risk = float(station_probability_fn(outcome,sid,x))
                except StationFeatureMissing:
                    rejected[outcome + '_station_features_missing'] += 1
                    continue
            else:
                # Logistic baseline unless another model function was passed in.
                risk = probability(model_artifact['models'][outcome],x) if probability_fn is None else float(probability_fn(outcome,x))
            # Confirm the model returned a real probability between 0 and 1.
            if not math.isfinite(risk) or not 0 <= risk <= 1:
                raise ValueError('model returned an invalid probability')
            # Create the prediction row that is saved and later matched to an outcome.
            item = {'station_id':sid,'outcome':outcome,'fetched_at':fetched.isoformat(),
                    'available_at':available.isoformat(),'scored_at':now.isoformat(),
                    'station_reported_at':reported.isoformat(),
                    'target_at':(fetched+timedelta(minutes=60)).isoformat(),  # Prediction is about 60 minutes after the poll
                    'model_version':model_version or model_artifact['version'],
                    'probability':risk,
                    'current_count':count}
            scored.append(item)
            # Only stations that are not already empty can become new alerts.
            if count > 0:
                candidates[outcome].append(item)

    ## Choose alerts
    # Demonstration policy only: up to five per outcome, per poll; ties by station ID.
    alerts = {outcome: sorted(rows, key=lambda v:(-v['probability'],v['station_id']))[:5]
              for outcome,rows in candidates.items()}
    diagnostics = dict(rejected)  # Rejection counts, reported in the window health file
    # Fail the poll if nothing could be scored; the caller records it as an error.
    if not scored:
        raise ZeroScoreError(diagnostics)
    return scored, alerts, diagnostics


## Errors used by the scoring path
class StationFeatureMissing(KeyError):
    """Raised by a station-aware probability function when a station has no static feature row."""


class ZeroScoreError(ValueError):
    """Raised when a poll produces no scored rows; carries the rejection counts."""
    def __init__(self, reasons):
        self.reasons = reasons  # Keep the counts so the caller can report them
        super().__init__('zero scores: ' + str(reasons))
