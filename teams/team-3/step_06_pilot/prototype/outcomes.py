"""Development outcome matching. No I/O; the caller supplies ingestion completeness."""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Purpose: each prediction says "station S may run out of bikes (or docks) 60
# minutes after this poll." This module finds the observation closest to that
# target time and records whether the shortage actually happened.
#
# Possible results for each prediction:
#   pending -> the observations around the target time are not all in yet
#   unknown -> no usable observation within 2.5 minutes of the target
#   known   -> label 1 if the count was 0 at the target time, otherwise label 0

## Packages
from datetime import timedelta  # Time spans for the horizon and tolerance
from scoring import timestamp  # Parse ISO time strings into UTC datetimes
from normalization import fields  # Read and validate the six station fields

## Constants
HORIZON = timedelta(minutes=60)  # Predictions look 60 minutes ahead
TOLERANCE = timedelta(seconds=150)  # Accept an observation up to 2.5 minutes from the target time


## Match predictions to what later happened
def match_outcomes(predictions, observations, *, as_of, complete_through):
    """Match predictions to nearest eligible observation time.

    complete_through is an externally verified event-time ingestion watermark, not
    the current time or latest observed poll. Without one, outcomes remain pending.
    The nearest observation is chosen before testing its outcome eligibility.
    """
    now = timestamp(as_of)  # The evaluation time
    # The watermark is the latest time for which all observations are known to be in.
    watermark = timestamp(complete_through) if complete_through is not None else None
    # Confirm the watermark is not in the future relative to the evaluation time.
    if watermark is not None and watermark > now:
        raise ValueError('watermark cannot be later than evaluation time')

    ## Index observations by station, keeping only those available by "now"
    by_station = {}  # station_id -> list of (poll time, observation row)
    seen_observations = set()  # Detect duplicate (station, poll time) pairs
    for row in observations:
        sid = row['station_id']
        fetched = timestamp(row['fetched_at'])  # When the poll started
        available = timestamp(row['available_at'])  # When the poll result was stored and usable
        # Confirm the data did not become available before it was collected.
        if available < fetched:
            raise ValueError('observation available before collection began')
        # Skip observations that were not yet available at evaluation time (no peeking ahead).
        if available > now or fetched > now:
            continue
        key = (sid, fetched)
        # Refuse duplicates, which would make "closest observation" ambiguous.
        if key in seen_observations:
            raise ValueError('duplicate station and observation timestamp')
        seen_observations.add(key)
        by_station.setdefault(sid, []).append((fetched, row))

    ## Evaluate each prediction
    result, seen_predictions = [], set()
    for prediction in predictions:
        sid, outcome = prediction['station_id'], prediction['outcome']
        # Confirm the outcome type is one of the two the project predicts.
        if outcome not in ('bike', 'dock'):
            raise ValueError('unknown prediction outcome')
        start = timestamp(prediction['fetched_at'])  # Poll time of the prediction
        target = timestamp(prediction['target_at'])  # Time the prediction is about
        # Confirm the target is exactly 60 minutes after the poll.
        if target != start + HORIZON:
            raise ValueError('prediction target does not equal poll time plus 60 minutes')
        key = (sid, outcome, start, prediction['model_version'])
        # Refuse duplicate predictions, which would double-count results.
        if key in seen_predictions:
            raise ValueError('duplicate prediction key')
        seen_predictions.add(key)
        # Create the result row, starting as pending.
        item = {'station_id': sid, 'outcome': outcome, 'fetched_at': start.isoformat(),
                'target_at': target.isoformat(), 'model_version': prediction['model_version'],
                'evaluated_at': now.isoformat(), 'status': 'pending', 'label': None,
                'matched_at': None, 'reason': None}
        # Stay pending until observations are complete through target + 2.5 minutes.
        if watermark is None or watermark < target + TOLERANCE:
            item['reason'] = 'awaiting_complete_observation_window'
            result.append(item)
            continue
        # List observations within 2.5 minutes of the target, with their distance in seconds.
        choices = [(abs((when-target).total_seconds()), when, obs)
                   for when, obs in by_station.get(sid, [])
                   if when <= watermark and abs(when-target) <= TOLERANCE]
        # No observation close enough: the outcome cannot be known.
        if not choices:
            item.update(status='unknown', reason='no_observation_within_tolerance')
            result.append(item)
            continue
        # Pick the closest observation; on a tie, the earlier one.
        _, when, obs = min(choices, key=lambda x: (x[0], x[1]))
        item['matched_at'] = when.isoformat()
        # Check that the chosen observation is trustworthy. It is chosen first and
        # checked second, so a bad nearest observation gives "unknown" instead of
        # quietly falling back to a farther one.
        try:
            reported = timestamp(obs['station_reported_at'])  # When the station last reported
            metadata_time = timestamp(obs['metadata_available_at'])  # When station metadata became known
            bikes, docks, capacity, installed, renting, returning = fields(obs)
            age = (when - reported).total_seconds()  # Seconds since the station last reported
            base_ok = (capacity > 0 and bikes >= 0 and docks >= 0 and
                       0 <= age <= 300 and metadata_time <= when and installed)  # Valid counts, fresh (5 min), installed
            outcome_ok = renting if outcome == 'bike' else returning  # Bike outcome needs renting; dock outcome needs returning
        except (KeyError, ValueError, TypeError, OverflowError):
            base_ok = outcome_ok = False  # Any missing or malformed field makes the observation ineligible
        if not (base_ok and outcome_ok):
            item.update(status='unknown', reason='nearest_observation_ineligible')
        else:
            # Label 1 means the shortage happened: zero bikes (or zero docks) at the target time.
            count = bikes if outcome == 'bike' else docks
            item.update(status='known', label=int(count == 0), reason=None)
        result.append(item)
    return result  # One result row per prediction, in input order
