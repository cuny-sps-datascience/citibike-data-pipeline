"""Retrospective station/day departure mix in local half-hour intervals."""

from __future__ import annotations

from datetime import date
from uuid import uuid4

import duckdb

from .db import sql_literal


def day_trip_files(cfg, day: date) -> list[str]:
    """Read selected, published start-month partitions from the ledger."""
    if not cfg.manifest_db.exists():
        raise FileNotFoundError("manifest not found; normalize trips first")
    partition = f"trips/trip_year={day.year}/trip_month={day.month:02d}/%"
    with duckdb.connect(str(cfg.manifest_db), read_only=True) as ledger:
        rows = ledger.execute("""
            SELECT DISTINCT p.path
            FROM published_files p JOIN sources s USING (source_key)
            WHERE s.status = 'selected' AND p.dataset = 'trips' AND p.path LIKE ?
            ORDER BY p.path
        """, [partition]).fetchall()
    files = [str(cfg.processed_root / r[0]) for r in rows]
    if not files:
        raise FileNotFoundError(f"no published trip files for {day:%Y-%m}")
    return files


_MIX_SQL = """
WITH eligible AS (
    SELECT start_station_id AS station_id, start_station_name AS station_name,
           CASE WHEN schema_family = 'modern' THEN 'short_name' ELSE 'legacy_id' END AS id_scheme,
           2 * hour(started_at_local) + CASE WHEN minute(started_at_local) >= 30 THEN 1 ELSE 0 END AS slot,
           rideable_type
    FROM trips
    WHERE eligible_departure AND started_at_local >= $day::TIMESTAMP
      AND started_at_local < $day::TIMESTAMP + INTERVAL 1 DAY
      AND ($station IS NULL OR start_station_id = $station)
), names AS (
    SELECT station_id, id_scheme, station_name, count(*) AS n
    FROM eligible GROUP BY ALL
), stations AS (
    SELECT station_id, id_scheme, station_name FROM names
    QUALIFY row_number() OVER (
        PARTITION BY station_id, id_scheme ORDER BY n DESC, station_name NULLS LAST
    ) = 1
), counts AS (
    SELECT station_id, id_scheme, slot, count(*) AS departures,
           count(*) FILTER (WHERE rideable_type = 'classic_bike') AS classic_departures,
           count(*) FILTER (WHERE rideable_type = 'electric_bike') AS electric_departures,
           count(*) FILTER (WHERE rideable_type IS NULL OR trim(rideable_type) = '') AS unknown_departures,
           count(*) FILTER (WHERE trim(rideable_type) != '' AND
               rideable_type NOT IN ('classic_bike', 'electric_bike')) AS other_departures
    FROM eligible GROUP BY ALL
), grid AS (
    SELECT s.*, r.slot::INTEGER AS slot,
           coalesce(c.departures, 0) AS departures,
           coalesce(c.classic_departures, 0) AS classic_departures,
           coalesce(c.electric_departures, 0) AS electric_departures,
           coalesce(c.unknown_departures, 0) AS unknown_departures,
           coalesce(c.other_departures, 0) AS other_departures
    FROM stations s CROSS JOIN range(48) r(slot)
    LEFT JOIN counts c USING (station_id, id_scheme, slot)
), shares AS (
    SELECT *, classic_departures + electric_departures AS known_type_departures,
           100.0 * classic_departures / nullif(classic_departures + electric_departures, 0) AS classic_pct,
           100.0 * electric_departures / nullif(classic_departures + electric_departures, 0) AS electric_pct,
           100.0 * (classic_departures + electric_departures) / nullif(departures, 0) AS known_type_pct,
           (sum(departures) OVER w)::BIGINT AS day_departures,
           100.0 * sum(electric_departures) OVER w /
               nullif(sum(classic_departures + electric_departures) OVER w, 0) AS day_electric_pct
    FROM grid WINDOW w AS (PARTITION BY station_id, id_scheme)
), changes AS (
    SELECT *, lag(electric_pct) OVER (
        PARTITION BY station_id, id_scheme ORDER BY slot
    ) AS previous_electric_pct
    FROM shares
)
SELECT $day::DATE AS trip_date, station_id, id_scheme, station_name, slot,
       $day::TIMESTAMP + slot * INTERVAL 30 MINUTE AS interval_start_local,
       $day::TIMESTAMP + (slot + 1) * INTERVAL 30 MINUTE AS interval_end_local,
       departures, classic_departures, electric_departures, unknown_departures, other_departures,
       known_type_departures, round(known_type_pct, 2) AS known_type_pct,
       round(classic_pct, 2) AS classic_pct, round(electric_pct, 2) AS electric_pct,
       day_departures, round(day_electric_pct, 2) AS day_electric_pct,
       round(electric_pct - day_electric_pct, 2) AS electric_change_from_day_pp,
       round(electric_pct - previous_electric_pct, 2) AS electric_change_from_previous_pp,
       known_type_departures < $min_rides AS low_sample
FROM changes ORDER BY station_id, id_scheme, slot
"""


def build_halfhour_mix(con, day: date, station_id: str | None = None, min_rides: int = 20) -> int:
    """Reads the trips view; creates halfhour_mix with 48 slots per observed station."""
    if min_rides < 1:
        raise ValueError("min-rides must be at least 1")
    con.execute("CREATE OR REPLACE TEMP TABLE halfhour_mix AS " + _MIX_SQL,
                {"day": day, "station": station_id, "min_rides": min_rides})
    return con.execute("SELECT count(*) FROM halfhour_mix").fetchone()[0]


def run_bike_mix(cfg, day: date, station_id: str | None = None, min_rides: int = 20, out=print) -> int:
    if min_rides < 1:
        raise ValueError("min-rides must be at least 1")
    files = day_trip_files(cfg, day)
    cfg.tmp_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.execute("SET memory_limit = '3GB'")
        con.execute(f"SET threads = {min(cfg.threads, 4)}")
        con.execute(f"SET temp_directory = {sql_literal(str(cfg.tmp_dir))}")
        paths = ", ".join(sql_literal(f) for f in files)
        con.execute(f"""CREATE TEMP VIEW trips AS SELECT * FROM read_parquet([{paths}],
            hive_partitioning = true, hive_types = {{'trip_year': INTEGER, 'trip_month': INTEGER}})""")
        rows = build_halfhour_mix(con, day, station_id, min_rides)
        if not rows:
            raise ValueError(f"no eligible departures for {day} at the requested stations")
        bad = con.execute("""SELECT count(*) FROM halfhour_mix WHERE departures !=
            classic_departures + electric_departures + unknown_departures + other_departures""").fetchone()[0]
        if bad:
            raise ValueError("bike-type counts do not reconcile")
        target = cfg.reports_dir / "bike_mix_halfhour" / f"date={day}" / uuid4().hex[:12]
        target.mkdir(parents=True, exist_ok=False)
        for name, options in (("intervals.csv", "FORMAT CSV, HEADER TRUE"),
                              ("intervals.parquet", "FORMAT PARQUET, COMPRESSION ZSTD")):
            con.execute(f"COPY halfhour_mix TO {sql_literal(str(target / name))} ({options})")
        con.execute("""CREATE TEMP TABLE station_summary AS
            SELECT trip_date, station_id, id_scheme, station_name,
                   max(day_departures) AS departures, max(day_electric_pct) AS electric_pct,
                   count(*) FILTER (WHERE NOT low_sample) AS comparable_intervals,
                   CASE WHEN count(*) FILTER (WHERE NOT low_sample) >= 2 THEN
                       round(max(electric_pct) FILTER (WHERE NOT low_sample) -
                             min(electric_pct) FILTER (WHERE NOT low_sample), 2)
                   END AS electric_share_range_pp
            FROM halfhour_mix GROUP BY ALL
            ORDER BY electric_share_range_pp DESC NULLS LAST, station_id, id_scheme
        """)
        con.execute(f"COPY station_summary TO {sql_literal(str(target / 'station_summary.csv'))} (FORMAT CSV, HEADER TRUE)")
        trips = con.execute("SELECT sum(departures) FROM halfhour_mix").fetchone()[0]
        out(f"{day}: {rows // 48} stations, {rows} half-hour rows, {trips} departures")
        out(f"Reports: {target}")
    return 0
