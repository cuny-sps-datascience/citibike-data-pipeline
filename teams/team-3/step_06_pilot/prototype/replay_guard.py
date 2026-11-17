"""Apply before reading a historical replay interval, including outcome lookahead."""
# Reading copy with comments added. The code is unchanged from the original module (this file is not embedded in the deployed notebook).
#
# Purpose: October 5 to 11, 2026 is the reserved test week. It was scored once
# and must never be used again for tuning or replays. This guard refuses any
# replay interval that touches that week.

## Packages
from datetime import datetime  # Build the test-week boundary timestamps
from zoneinfo import ZoneInfo  # New York time zone for the boundaries
from scoring import timestamp  # Parse interval start and end into aware datetimes

## Constants: the reserved test week in New York time
NY=ZoneInfo('America/New_York')  # Citi Bike operates on New York local time
TEST_START=datetime(2026,10,5,tzinfo=NY)  # First moment of the reserved week (Monday Oct 5, midnight)
TEST_END=datetime(2026,10,12,tzinfo=NY)  # First moment after the reserved week (end-exclusive)


## Refuse an interval that overlaps the reserved week
def guard_interval(start, end):
    a,b=timestamp(start),timestamp(end)  # Convert both ends to comparable timestamps
    # Confirm the interval has positive length; the end is exclusive.
    if a>=b: raise ValueError('interval must be nonempty and end-exclusive')
    # Two intervals overlap when each starts before the other ends.
    # The caller must include outcome lookahead in `end`, so labels cannot leak test-week data.
    if a<TEST_END and b>TEST_START: raise ValueError('reserved test period: replay refused')
