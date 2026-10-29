"""Small review examples for proposed departures-only policy changes.

This module does not change a published package or activate a model. Inferred
24-hour operation is an explicit retrospective assumption, never verified GBFS
coverage. Profiles infer exposure again from release-eligible trips only.
"""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .borough_mvp import label_assignable, release_at

NY = ZoneInfo("America/New_York")
UTC = timezone.utc


def assumed_label(observed, target, active_start, active_end, complete):
    if target.tzinfo is None:
        raise ValueError("Target requires timezone")
    if not complete:
        return None, "incomplete_trip_files"
    if not label_assignable(target):
        return None, "ambiguous_trip_hour"
    if active_start is None or not active_start <= target < active_end:
        return None, "outside_inferred_active_period"
    if observed is not None:
        if observed < 0:
            raise ValueError("Negative departures")
        return int(observed), "observed_trips"
    return 0, "assumed_24h_zero"


def active_days(hours):
    hours = list(hours)
    if not hours:
        return None, None
    first = min(hours).astimezone(NY).replace(hour=0, minute=0, second=0, microsecond=0)
    end = max(hours).astimezone(NY).replace(hour=0, minute=0, second=0, microsecond=0)+timedelta(days=1)
    return first.astimezone(UTC), end.astimezone(UTC)


def released_profile(rows, issue_cutoff, target, complete_releases, days=56):
    """Hour-of-week mean; no target-period labels or later release cohorts.

    Each input row has hour_utc, nominal_release and departures. The 56-day
    window ends after the latest complete nominal release available strictly
    before the issue cutoff. Source dates use the existing assumed 45-day rule.
    """
    available = {m for m in complete_releases if release_at(m) < issue_cutoff}
    if not available:
        return {"released_profile_mean": None, "profile_hours": 0}
    latest = max(available)
    year, month = map(int, latest.split("-"))
    end = datetime(year+1, 1, 1, tzinfo=NY) if month == 12 else datetime(year, month+1, 1, tzinfo=NY)
    start = end-timedelta(days=days)
    eligible = [r for r in rows if r["nominal_release"] in available
                and r["hour_utc"] < issue_cutoff
                and release_at(r["nominal_release"]) < issue_cutoff]
    first_active, end_active = active_days(r["hour_utc"] for r in eligible)
    counts = defaultdict(int)
    for r in eligible:
        counts[r["hour_utc"]] += r["departures"]
    numerator, denominator = 0, 0
    used = set()
    matching = []
    hour = start.astimezone(UTC)
    while hour < end.astimezone(UTC):
        local = hour.astimezone(NY)
        if (local.strftime("%Y-%m") in available and first_active is not None
                and first_active <= hour < end_active and label_assignable(hour)
                and (local.weekday(), local.hour) == (target.astimezone(NY).weekday(), target.astimezone(NY).hour)):
            numerator += counts[hour]
            denominator += 1
            matching.append(hour)
            used.add(local.strftime("%Y-%m"))
        hour += timedelta(hours=1)
    for r in eligible:
        if r["hour_utc"] in matching:
            used.add(r["nominal_release"])
    return {
        "released_profile_mean": numerator/denominator if denominator else None,
        "profile_departures": numerator, "profile_hours": denominator,
        "profile_window_start_utc": start.astimezone(UTC),
        "profile_window_end_exclusive_utc": end.astimezone(UTC),
        "profile_included_releases": "|".join(sorted(used)),
        "profile_latest_input_available_utc": max((release_at(m) for m in used), default=None),
        "profile_denominator_assumption": "24h within active days inferred from previously released trips only",
    }


def publication_versions_asof(rows, cutoff, ingestion_delay_minutes=0):
    """Event-level timing prototype, not a replacement for aggregate inputs.

    A 09:40 publication is eligible at 09:55 if the ingestion assumption permits
    it. A later update must never replace the event version available at cutoff.
    Geography/route matching and archive completeness still need a raw handoff.
    """
    selected = {}
    for row in rows:
        publication = row["publication_utc"]
        available = row.get("available_at_utc", publication+timedelta(minutes=ingestion_delay_minutes))
        if publication >= cutoff or available >= cutoff:
            continue
        previous = selected.get(row["event_id"])
        if previous is None or publication > previous["publication_utc"]:
            selected[row["event_id"]] = row
    return selected


def weather_rows(con, weather_class, dataset_build, origin, cutoff):
    """Instantaneous fields at target start; preceding-hour sums at target end."""
    result = con.execute("""SELECT h.horizon,
        ?::TIMESTAMPTZ+(h.horizon-1)*INTERVAL '1 hour' AS target_start_utc,
        ?::TIMESTAMPTZ+h.horizon*INTERVAL '1 hour' AS target_end_utc,
        i.forecast_origin_utc::TIMESTAMPTZ AS weather_source_origin_utc,
        i.valid_time_utc::TIMESTAMPTZ AS instant_valid_time_utc,
        a.valid_time_utc::TIMESTAMPTZ AS accumulation_valid_time_utc,
        a.valid_time_utc::TIMESTAMPTZ-INTERVAL '1 hour' AS accumulation_start_utc,
        i.forecast_temperature_f,i.forecast_wind_speed_mph,
        a.forecast_precipitation_in,a.forecast_rain_in,a.forecast_snowfall_in,
        i.forecast_precipitation_in AS legacy_start_stamped_precipitation_in,
        i.selected_model_run_utc::TIMESTAMPTZ AS instant_run_utc,
        a.selected_model_run_utc::TIMESTAMPTZ AS accumulation_run_utc,
        i.selected_run_available_utc::TIMESTAMPTZ AS instant_available_utc,
        a.selected_run_available_utc::TIMESTAMPTZ AS accumulation_available_utc
        FROM (VALUES (1),(2),(3)) h(horizon)
        LEFT JOIN weather_classes i ON i.weather_request_class=? AND i.dataset_build_id=?
            AND i.forecast_origin_utc::TIMESTAMPTZ=?::TIMESTAMPTZ-INTERVAL '1 hour'
            AND i.forecast_horizon=h.horizon
        LEFT JOIN weather_classes a ON a.weather_request_class=? AND a.dataset_build_id=?
            AND a.forecast_origin_utc::TIMESTAMPTZ=?::TIMESTAMPTZ-INTERVAL '1 hour'
            AND a.forecast_horizon=h.horizon+1
        ORDER BY h.horizon""", [origin, origin, weather_class, dataset_build, origin,
                                weather_class, dataset_build, origin])
    fields = [d[0] for d in result.description]
    rows = [dict(zip(fields, r)) for r in result.fetchall()]
    for row in rows:
        if (row["instant_valid_time_utc"] != row["target_start_utc"]
                or row["accumulation_valid_time_utc"] != row["target_end_utc"]
                or row["accumulation_start_utc"] != row["target_start_utc"]):
            raise ValueError("Weather variable interval mismatch/missing source row")
        if (row["instant_available_utc"] is None or row["accumulation_available_utc"] is None
                or max(row["instant_available_utc"],row["accumulation_available_utc"]) >= cutoff):
            raise ValueError("Weather unavailable at issue cutoff")
    return rows
