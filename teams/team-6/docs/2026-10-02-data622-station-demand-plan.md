# DATA 622 Final Project: Station Demand Aggregates and As-Of Profiles Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the 323.8 M normalized trips on the RAID into station-level demand tables (hourly and monthly activity, station location evidence, release availability) and leakage-safe historical demand profiles, including demand by bike type, that the availability model can use as features (Stage 3 of `docs/spec/Citi_Bike_Local_to_Databricks_Codex_Workflow.pdf`).

**Architecture:** A new `citibike aggregate` command group. Every table is a full, deterministic rebuild from the trip files listed in the published-file ledger (never a glob), computed with DuckDB and written to a run-specific build directory under `FinalProject_processed/aggregates/<table>/builds/<run_id>/`. A `CURRENT` pointer file, replaced atomically, makes a build visible; superseded builds move to `retired/`. Demand profiles are computed "as of" a cutoff date and use only trips that had been published by that date.

**Tech Stack:** Python 3.12, DuckDB >= 1.3, PyArrow, pytest (unchanged).

## Global Constraints

- Inputs are the published trip files named in the manifest ledger (`published_files`, dataset `trips`) for sources whose status is `selected`. Never glob `trips/` for analysis.
- Read trip partitions with `hive_types = {'trip_year': INTEGER, 'trip_month': INTEGER}`.
- Outputs live under `/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/aggregates/`. Nothing is written to the code project or the source archive. Nothing is deleted; superseded builds move to `retired/<run_id>/aggregates/...`.
- Departures and arrivals are counted separately and never summed into one "activity" number in stored tables.
- Bike-type counts use four exhaustive buckets: `classic`, `electric`, `other`, `unknown`. Retain raw `rideable_type` in trips; never infer classic from a missing type or from total minus electric. Type-level counts reconcile to totals at every stored grain.
- Departures count trips with `eligible_departure`; arrivals count trips with `eligible_arrival` (definitions in `docs/data_dictionary.md`).
- Station IDs are strings. Legacy integer IDs (2013-06 to 2019-12, `schema_family` legacy) and modern short-name IDs (2020-01 onward, `schema_family = 'modern'`) are different identifier schemes and are never equated. Every table carries `id_scheme` (`legacy_id` or `short_name`).
- Profiles must not use any trip from a release whose availability date is after the profile's `as_of` date, nor any trip that starts on or after `as_of`.
- DuckDB connections for aggregation use `threads <= 4` and `memory_limit <= 3GB` with `temp_directory` on the RAID (whole-archive scans were killed by macOS at higher settings).
- Never build a large view with `con.sql(..., params=...)`: DuckDB executes a parameterized relation when it is created and holds the rows outside `memory_limit`. Create views with `CREATE TEMP VIEW ... read_parquet([literal paths])`, or pass parameters to `con.execute` of the final query (for example `COPY (...)`).
- No em dashes in any file or output. Commits use the repo-local Vaene identity, no co-author trailers, never push without asking.
- Run all commands from `/Users/randyhowk/Sites/CUNY/citibike-data-pipeline/teams/team-6` on branch `team-6`.

## Prerequisite

The cleanup wave (validation memory fix, report pipe escaping, deferred minors) must be merged first: Task 4 extends `report.parse_listing`, which that wave changes.

---

## Decisions

| # | Decision | Reason |
|---|---|---|
| S1 | Build tables over the whole archive, not only recent years. | The aggregates are cheap to rebuild locally (one scan of 9.8 GB of Parquet), and the history-depth choice moves to the profile window, where it can be tuned without rebuilding. |
| S2 | Profiles default to a trailing 56-day window over the `short_name` scheme only. | The live GBFS feed's short names match modern trip IDs. Legacy-ID history needs a crosswalk (Stage 4) before it can describe a current station. |
| S3 | Release availability date = the bucket listing's `LastModified` for monthly files from 2024-01 onward; otherwise an assumed lag of 35 days after the month ends (`release_lag_days` in config). Stored with `availability_basis`. | Year ZIPs for 2013 to 2023 were re-packaged in 2024, so their timestamps do not show when the data first became public. The assumed lag is explicit and configurable, as the workflow PDF requires ("simulate release availability or explicitly label retrospective features"). |
| S4 | `station_hour_activity` is sparse (only hours with at least one departure or arrival). Zero hours are represented through `station_active_periods` and handled when profiles are computed, by dividing by the number of active days, not the number of stored rows. | A dense station-by-hour grid over 13 years would be several hundred million mostly-zero rows; the active-period table states the assumption explicitly. |
| S5 | A station's active period is inferred as first to last activity date per `(station_id, id_scheme)`. | The trip data cannot show closures or gaps within that range. It is labeled `inferred_from_trips` and will be replaced by `station_information` history in Stage 4. |
| S6 | Hours are local wall-clock hours (`date_trunc('hour', started_at_local)`). | Matches how the data is published. On fall-back days the two 01:00 hours merge into one row; the trips keep their DST flags. |
| S7 | Capacity and utilization tables are deferred to Stage 4. | Effective-dated capacity exists only in the class `station_information` history; applying current dock counts to past years is not allowed. |

## Bike-type requirement added October 5, 2026

`rideable_type` is required in the Stage 3 demand features. A station can have bikes but still lack the type riders use. The code examples below include the revised counts and profiles; Stage 3 remains unimplemented until its tasks are completed.

- Map `classic_bike` to `classic`, `electric_bike` to `electric`, null or blank to `unknown`, and any other nonblank value to `other`. Legacy records lack this field and stay unknown. New raw types must be reported for review.
- Hourly and monthly tables carry departures and arrivals for every bucket. Demand profiles carry each bucket's departure, arrival and net-flow means, observed departure counts and completed-departure shares. All means use the same station active-weekday denominator, including zero-trip hours. Shares use all departures as denominator and are null when total departures are zero. Counts and unknown shares expose sample support; the type means and shares reconcile to totals.
- Completed-trip mix is a descriptive demand benchmark. It does not establish the available mix that maximizes ridership, because historic supply, stockouts and pricing affect completed trips. For a classic/electric-only benchmark, normalize their counts only when their combined count is positive and show the fraction of trips excluded as other/unknown.
- Apply every as-of and release-availability rule to the type features as well as totals. No inventory observations after forecast time may enter model inputs. Later trips or status intervals are evaluation outcomes only after they become available.

**Stage 4 inputs and rate definitions:** Audit the class feed for historical `vehicle_types_available` and an effective-dated mapping from vehicle type IDs to classic/electric. GBFS defines these counts in station status and type definitions in `vehicle_types.json` ([GBFS reference](https://gbfs.org/documentation/reference/)). Check the feed's actual version and retained schema before using them. An omitted type is zero only within a confirmed complete report with a known type mapping; a missing type breakdown or unmapped ID is unknown. Disabled bikes do not count as rentable supply.

Build covered intervals from fresh status reports at their actual timestamps, with a bounded carry-forward interval; stop at gaps or disabled service. Map station IDs through the effective-dated crosswalk. Retain UTC interval endpoints to measure elapsed exposure without DST ambiguity, and local hour/weekday for the trip join; flag ambiguous trip timestamps. Count only eligible departures in the same covered intervals. Define:

- `station_departure_rate = completed departures / covered operating station-hours`.
- `type_departure_rate = completed departures of that type / available bike-hours of that type`, where bike-hours = sum of rentable type count times interval hours. Zero exposure gives a null rate; departures with zero measured exposure are inconsistencies to audit.

Keep coverage, counts, hours with both types available, type stockout time and type-mapping uncertainty. Status count changes cannot stand in for trips because returns and rebalancing also change inventory.

**Mix objective and evaluation:** Compare historically supported candidate electric shares at fixed total bikes, within effective-dated capacity and fleet availability. Account for station, weekday/hour, weather, recent demand and pricing periods where known. Rank candidates by expected total completed departures per operating station-hour; report bike-hour rates by type as supporting diagnostics, plus uncertainty and sample support. Evaluate demand predictions chronologically against total-only and observed-trip-share benchmarks. Sparse station/hour cells use a documented pooled estimate and carry a fallback flag. Type-specific shortage predictions require observed future type counts and the existing freshness/operating-state label rules. Observational associations do not establish ridership uplift; that requires a controlled rebalancing evaluation. Without historical supply by type, publish descriptive trip mix and defer rate-based recommendations.

## Half-hour station/day variation (implemented October 5, 2026)

`citibike bike-mix --date YYYY-MM-DD [--station ID] [--min-rides 20]` is a retrospective departure-mix report implemented in `src/citibike_pipeline/bike_mix.py`. It reads only selected published trip files in the requested start-month partition, through the ledger, and writes a unique report directory under `reports/bike_mix_halfhour/date=YYYY-MM-DD/`. It is separate from the pending Stage 3 aggregate build and as-of feature commands.

- Grain: station, identifier scheme, local date, half-hour slot (0 through 47). The grid covers all 48 slots for stations with at least one eligible departure on that date. A station with no departures all day is absent; station-status history is needed to establish its operating state.
- Files: `intervals.csv`, `intervals.parquet`, `station_summary.csv`. Retain raw departure counts by classic/electric/unknown/other, total counts, known-type coverage, classic/electric percentages, daily totals and electric percentage, and percentage-point changes from the preceding slot and daily mix.
- For this report, classic/electric percentages use only classic + electric departures as the denominator; `known_type_pct`, unknown and other counts expose the excluded share. Zero known-type departures gives null shares and changes. A gap breaks the previous-slot comparison rather than skipping over it.
- `low_sample` flags fewer than `--min-rides` known-type departures (default 20). The summary reports the range of electric shares only across slots meeting that threshold, and only if at least two slots qualify. This is descriptive variation, not a significance test or an estimated causal response to stocking changes.
- Slots are half-open: 08:00 through just before 08:30, then 08:30 through just before 09:00. They follow local wall-clock timestamps; repeated fall-back slots merge, and nonexistent spring-forward slots have no departures. The 48-slot grid does not assert 24 hours of operating exposure.
- Future as-of features must retain half-hour demand patterns (station, ISO weekday, slot), with the existing publication cutoffs, zero-slot denominators, support counts and unknown-type rules. The existing hourly SQL examples are coarser supporting tables; they cannot reconstruct half-hour variation. Those model features require a direct trip-level half-hour aggregation and remain planned.

## Module map and shared interfaces

| Module | Public names |
|---|---|
| `config.py` (modified) | `Config.release_lag_days: int` (default 35); property `aggregates_dir` (`processed_root / "aggregates"`) |
| `trips.py` (new) | `HIVE_TYPES: str`; `published_trip_files(cfg, manifest) -> list[str]`; `source_releases(manifest) -> list[tuple[str, str]]` (source_key, nominal_release); `register_trips(con, cfg, manifest) -> None` (creates views `trips` and `trip_sources`); `aggregate_connection(cfg) -> duckdb.DuckDBPyConnection` |
| `builds.py` (new) | `TABLES: tuple[str, ...]`; `build_dir(cfg, table, run_id) -> Path`; `current_build(cfg, table) -> Path | None`; `promote(cfg, table, run_id) -> Path`; `table_files(cfg, table) -> list[str]` |
| `aggregate.py` (new) | `build_station_hour_activity(con, out_dir)`, `build_station_month_activity(con, out_dir, hour_files)`, `build_station_active_periods(con, out_dir, hour_files)`, `build_station_location_observations(con, out_dir)`, `build_release_availability(con, out_dir, cfg, manifest, listing_dates)`, `run_build(cfg, manifest, tables, out) -> int` |
| `availability.py` (new) | `assumed_availability(release, lag_days) -> date`; `release_availability_rows(releases, listing_dates, lag_days) -> list[dict]` |
| `report.py` (modified) | `parse_listing_dates(xml_text) -> dict[str, date]` |
| `profiles.py` (new) | `build_profiles(con, cfg, as_of, window_days, out_dir) -> dict`; `run_profiles(cfg, manifest, as_of, window_days, out) -> int` |
| `aggregate_validate.py` (new) | `validate_aggregates(cfg, manifest) -> dict`; `run_aggregate_validate(cfg, out) -> int` |
| `cli.py` (modified) | `aggregate build [--table T ...]`, `aggregate validate`, `aggregate profiles --as-of YYYY-MM-DD [--window-days N]` |

Output tables (Parquet), each under `aggregates/<table>/builds/<run_id>/`:

| Table | Grain | Columns |
|---|---|---|
| `station_hour_activity` | station, scheme, hour, release | `station_id, id_scheme, hour_start, release, departures, arrivals, departures_member, departures_casual, arrivals_member, arrivals_casual`, plus `departures_{classic,electric,other,unknown}` and `arrivals_{classic,electric,other,unknown}`, partitioned by `year` |
| `station_month_activity` | station, scheme, month | `station_id, id_scheme, month_start, departures, arrivals, active_days, first_activity, last_activity`, plus the eight bike-type counts from the hour table |
| `station_active_periods` | station, scheme | `station_id, id_scheme, first_activity_date, last_activity_date, active_days_observed, basis` |
| `station_location_observations` | station, scheme, name | `station_id, id_scheme, station_name, trips_observed, median_lat, median_lng, first_seen, last_seen` |
| `release_availability` | release | `release, month_start, coverage_end, availability_date, availability_basis` |
| `demand_profiles` | as_of, station, weekday, hour | `as_of, station_id, id_scheme, isodow, hour, departures_mean, arrivals_mean, net_flow_mean, active_weekdays, window_start, window_end, latest_release`, plus `departures_{type}_mean`, `arrivals_{type}_mean`, `net_flow_{type}_mean`, `departures_{type}_count`, `departures_{type}_share` for each of classic/electric/other/unknown (stored under `aggregates/demand_profiles/as_of=YYYY-MM-DD/builds/<run_id>/`) |

### Fixture facts used by the tests

From `tests/archive_fixture.py` after `run_normalize(cfg, m, FIXTURE_RELEASES)` on `selected_cfg`:

- Eligible departures: 2015-01: 2, 2016-12: 1, 2018-04: 1 (the other row has no start station), 2023-11: 4, 2025-01: 3. **Total 11.**
- Eligible arrivals: 2015-01: 2, 2016-12: 1, 2018-04: 2, 2023-11: 4, 2025-01: 2 (B2 has no end station). **Total 11.**
- Station `6322.01` (short_name), hour 2023-11-05 01:00: 2 departures (D4 twice) and 1 arrival (the D4 copy that ends at 6322.01).
- Station `6257.03`, hour 2023-11-05 01:00: 1 arrival.
- Trip C3 (release 2025-01) departs `6322.01` at 2024-12-31 23:50 and arrives at `6257.03` at 2025-01-01 00:10.
- Station `72` (legacy_id) departs 2018-04-01 03:00 hour: 1.

---
### Task 1: Config additions, ledger-based trip reader, and build promotion

**Files:**
- Modify: `src/citibike_pipeline/config.py`, `config/local.yaml`, `tests/test_config.py`
- Create: `src/citibike_pipeline/trips.py`, `src/citibike_pipeline/builds.py`
- Test: `tests/test_trips_builds.py`

**Interfaces:**
- Consumes: `Manifest.all()`, `Manifest.published_for_sources(keys)` (paths relative to `processed_root`), `db.sql_literal`.
- Produces: `Config.release_lag_days`, `Config.aggregates_dir`, everything listed for `trips.py` and `builds.py` in the module map.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_config.py`:

```python
def test_aggregate_settings_default(tmp_path):
    cfg = load_config(write_cfg(tmp_path, tmp_path / "src", tmp_path / "out"))
    assert cfg.release_lag_days == 35
    assert cfg.aggregates_dir == (tmp_path / "out").resolve() / "aggregates"


def test_release_lag_days_configurable(tmp_path):
    cfg = load_config(write_cfg(tmp_path, tmp_path / "src", tmp_path / "out", release_lag_days=10))
    assert cfg.release_lag_days == 10
```

Create `tests/test_trips_builds.py`:

```python
import pytest

from archive_fixture import FIXTURE_RELEASES, QUIET
from citibike_pipeline.builds import current_build, promote, build_dir, table_files
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.pipeline import run_normalize
from citibike_pipeline.trips import aggregate_connection, published_trip_files, register_trips


@pytest.fixture
def published_cfg(selected_cfg):
    with Manifest(selected_cfg) as m:
        assert run_normalize(selected_cfg, m, FIXTURE_RELEASES, out=QUIET) == 0
    return selected_cfg


def test_published_trip_files_come_from_the_ledger(published_cfg):
    stray = published_cfg.trips_dir / "trip_year=2025" / "trip_month=01" / "stray.parquet"
    stray.write_bytes(b"not parquet")
    with Manifest(published_cfg) as m:
        files = published_trip_files(published_cfg, m)
    assert files and all(f.endswith(".parquet") for f in files)
    assert str(stray) not in files


def test_register_trips_exposes_typed_partitions_and_release(published_cfg):
    con = aggregate_connection(published_cfg)
    with Manifest(published_cfg) as m:
        register_trips(con, published_cfg, m)
    assert con.execute("SELECT count(*) FROM trips").fetchone()[0] == 12
    types = dict(con.execute("SELECT column_name, column_type FROM (DESCRIBE trips)").fetchall())
    assert types["trip_year"] == "INTEGER" and types["trip_month"] == "INTEGER"
    rel = con.execute("SELECT release FROM trips JOIN trip_sources USING (source_key) "
                      "WHERE ride_id = 'C3'").fetchone()[0]
    assert rel == "2025-01"


def test_aggregate_connection_is_bounded(published_cfg):
    con = aggregate_connection(published_cfg)
    assert int(con.execute("SELECT current_setting('threads')").fetchone()[0]) <= 4


def test_promote_switches_current_and_retires_previous(published_cfg):
    assert current_build(published_cfg, "station_hour_activity") is None
    first = build_dir(published_cfg, "station_hour_activity", "run-a")
    first.mkdir(parents=True)
    (first / "x.parquet").write_bytes(b"a")
    promote(published_cfg, "station_hour_activity", "run-a")
    assert current_build(published_cfg, "station_hour_activity") == first
    second = build_dir(published_cfg, "station_hour_activity", "run-b")
    second.mkdir(parents=True)
    (second / "y.parquet").write_bytes(b"b")
    promote(published_cfg, "station_hour_activity", "run-b")
    assert current_build(published_cfg, "station_hour_activity") == second
    assert not first.exists()
    retired = published_cfg.retired_dir / "run-b" / "aggregates" / "station_hour_activity" / "run-a"
    assert (retired / "x.parquet").exists()
    assert table_files(published_cfg, "station_hour_activity") == [str(second / "y.parquet")]


def test_table_files_requires_a_build(published_cfg):
    with pytest.raises(FileNotFoundError):
        table_files(published_cfg, "station_month_activity")
```

The fixture publishes 12 trips (2015-01: 2, 2016-12: 1, 2018-04: 2, 2023-11: 4, 2025-01: 3); quarantined rows are not trips.

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_config.py tests/test_trips_builds.py -q`
Expected: FAIL (`AttributeError: 'Config' object has no attribute 'release_lag_days'`, `ModuleNotFoundError: citibike_pipeline.trips`).

- [ ] **Step 3: Extend `config.py`**

Add the field after `pilot_releases` in the `Config` dataclass:

```python
    release_lag_days: int = 35
```

Add the property next to the other derived paths:

```python
    @property
    def aggregates_dir(self) -> Path:
        return self.processed_root / "aggregates"
```

In `load_config`, pass:

```python
        release_lag_days=int(raw.get("release_lag_days", 35)),
```

Add to `config/local.yaml` (after `max_quarantine_fraction`):

```yaml
# Days after a month ends before its trip file is assumed public, when the bucket
# listing cannot date it (year ZIPs re-packaged in 2024). Used by demand profiles.
release_lag_days: 35
```

- [ ] **Step 4: Write `src/citibike_pipeline/trips.py`**

```python
"""Reads published trips through the ledger, never by globbing trips/."""

from __future__ import annotations

import duckdb

from .config import Config
from .db import sql_literal

HIVE_TYPES = "hive_partitioning = true, hive_types = {'trip_year': INTEGER, 'trip_month': INTEGER}"


def aggregate_connection(cfg: Config) -> duckdb.DuckDBPyConnection:
    """Bounded settings: whole-archive scans at 6 GB / 6 threads were killed by macOS."""
    cfg.tmp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit = '3GB'")
    con.execute("SET threads = 4")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = {sql_literal(str(cfg.tmp_dir))}")
    return con


def published_trip_files(cfg: Config, manifest) -> list[str]:
    keys = [r["source_key"] for r in manifest.all() if r["status"] == "selected"]
    return sorted(str(cfg.processed_root / e["path"])
                  for e in manifest.published_for_sources(keys) if e["dataset"] == "trips")


def source_releases(manifest) -> list[tuple[str, str]]:
    return [(r["source_key"], r["nominal_release"]) for r in manifest.all()
            if r["status"] == "selected" and r["nominal_release"]]


def register_trips(con: duckdb.DuckDBPyConnection, cfg: Config, manifest) -> None:
    files = published_trip_files(cfg, manifest)
    if not files:
        raise FileNotFoundError("no published trip files in the ledger; run citibike normalize first")
    # A parameterized con.sql(...) relation is executed when it is created, which would load every
    # trip into memory; a view over literal paths stays lazy (same finding as the validate fix).
    paths = ", ".join(sql_literal(f) for f in files)
    con.execute(f"CREATE OR REPLACE TEMP VIEW trips AS SELECT * FROM read_parquet([{paths}], {HIVE_TYPES})")
    con.execute("CREATE OR REPLACE TABLE trip_sources (source_key VARCHAR, release VARCHAR)")
    con.executemany("INSERT INTO trip_sources VALUES (?, ?)", source_releases(manifest))
```

- [ ] **Step 5: Write `src/citibike_pipeline/builds.py`**

```python
"""Run-specific build directories with an atomically replaced CURRENT pointer."""

from __future__ import annotations

import os
from pathlib import Path

from .config import Config

TABLES = (
    "station_hour_activity",
    "station_month_activity",
    "station_active_periods",
    "station_location_observations",
    "release_availability",
)


def _table_root(cfg: Config, table: str) -> Path:
    return cfg.aggregates_dir / table


def build_dir(cfg: Config, table: str, run_id: str) -> Path:
    return _table_root(cfg, table) / "builds" / run_id


def current_build(cfg: Config, table: str) -> Path | None:
    pointer = _table_root(cfg, table) / "CURRENT"
    if not pointer.exists():
        return None
    return build_dir(cfg, table, pointer.read_text().strip())


def promote(cfg: Config, table: str, run_id: str) -> Path:
    """Makes run_id the visible build, then moves every other build to retired/<run_id>/."""
    target = build_dir(cfg, table, run_id)
    if not target.is_dir():
        raise FileNotFoundError(target)
    root = _table_root(cfg, table)
    tmp = root / "CURRENT.tmp"
    tmp.write_text(run_id + "\n")
    os.replace(tmp, root / "CURRENT")
    for other in sorted((root / "builds").iterdir()):
        if other.name != run_id:
            dest = cfg.retired_dir / run_id / "aggregates" / table / other.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(other, dest)
    return target


def table_files(cfg: Config, table: str) -> list[str]:
    current = current_build(cfg, table)
    if current is None:
        raise FileNotFoundError(f"no current build for {table}; run citibike aggregate build")
    return sorted(str(p) for p in current.rglob("*.parquet"))
```

- [ ] **Step 6: Run the tests**

Run: `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/citibike_pipeline/config.py config/local.yaml src/citibike_pipeline/trips.py src/citibike_pipeline/builds.py tests/test_config.py tests/test_trips_builds.py
git commit -m "Add ledger-based trip reader and atomic aggregate builds"
```

### Task 2: Station activity tables

**Files:**
- Create: `src/citibike_pipeline/aggregate.py`
- Test: `tests/test_aggregate.py`

**Interfaces:**
- Consumes: `aggregate_connection`, `register_trips` (Task 1); trip columns from `docs/data_dictionary.md`.
- Produces:
  - `build_station_hour_activity(con, out_dir: Path) -> int` (rows written). Needs views `trips` and `trip_sources`. Writes Parquet partitioned by `year`.
  - `build_station_month_activity(con, out_dir: Path, hour_files: list[str]) -> int`.
  - `build_station_active_periods(con, out_dir: Path, hour_files: list[str]) -> int`.
  - `build_station_location_observations(con, out_dir: Path) -> int`. Needs the trip views.
  - Each writes into `out_dir`, which must not already contain Parquet files.

- [ ] **Step 1: Write the failing tests**

```python
from datetime import date, datetime

import pytest

from archive_fixture import FIXTURE_RELEASES, QUIET
from citibike_pipeline.aggregate import (
    build_station_active_periods, build_station_hour_activity, build_station_location_observations,
    build_station_month_activity,
)
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.pipeline import run_normalize
from citibike_pipeline.trips import aggregate_connection, register_trips


@pytest.fixture
def trip_con(selected_cfg):
    with Manifest(selected_cfg) as m:
        assert run_normalize(selected_cfg, m, FIXTURE_RELEASES, out=QUIET) == 0
        con = aggregate_connection(selected_cfg)
        register_trips(con, selected_cfg, m)
    return con


def _rows(con, files, order):
    rel = con.sql("SELECT * FROM read_parquet(?)", params=[files]).order(order)
    return [dict(zip(rel.columns, r)) for r in rel.fetchall()]


@pytest.fixture
def hours(trip_con, tmp_path):
    build_station_hour_activity(trip_con, tmp_path / "hours")
    return sorted(str(p) for p in (tmp_path / "hours").rglob("*.parquet"))


def test_hour_activity_counts_departures_and_arrivals_separately(trip_con, hours):
    total = trip_con.execute("SELECT sum(departures), sum(arrivals) FROM read_parquet(?)", [hours]).fetchone()
    assert total == (11, 11)
    rows = {(r["station_id"], r["hour_start"], r["release"]): r
            for r in _rows(trip_con, hours, "station_id, hour_start")}
    d4 = rows[("6322.01", datetime(2023, 11, 5, 1), "2023-11")]
    assert (d4["departures"], d4["arrivals"], d4["id_scheme"]) == (2, 1, "short_name")
    assert rows[("6257.03", datetime(2023, 11, 5, 1), "2023-11")]["arrivals"] == 1


def test_hour_activity_assigns_hours_by_timestamp_and_keeps_release(trip_con, hours):
    rows = {(r["station_id"], r["hour_start"]): r for r in _rows(trip_con, hours, "station_id, hour_start")}
    c3 = rows[("6322.01", datetime(2024, 12, 31, 23))]
    assert (c3["departures"], c3["release"]) == (1, "2025-01")
    assert rows[("6257.03", datetime(2025, 1, 1, 0))]["arrivals"] == 1
    assert rows[("72", datetime(2018, 4, 1, 3))]["id_scheme"] == "legacy_id"


def test_hour_activity_splits_membership_and_bike_type(trip_con, hours):
    row = next(r for r in _rows(trip_con, hours, "station_id, hour_start")
               if r["station_id"] == "6322.01" and r["hour_start"] == datetime(2025, 1, 10, 14))
    assert (row["departures_member"], row["departures_casual"], row["departures_electric"]) == (1, 0, 1)
    assert (row["departures_classic"], row["departures_other"], row["departures_unknown"]) == (0, 0, 0)
    for r in _rows(trip_con, hours, "station_id, hour_start"):
        for flow in ("departures", "arrivals"):
            assert sum(r[f"{flow}_{b}"] for b in ("classic", "electric", "other", "unknown")) == r[flow]
            if r["id_scheme"] == "legacy_id":
                assert r[f"{flow}_unknown"] == r[flow]
                assert r[f"{flow}_classic"] == 0


def test_hour_activity_preserves_missing_and_unfamiliar_modern_types(trip_con, tmp_path):
    trip_con.execute("CREATE OR REPLACE TEMP TABLE trips AS SELECT * FROM trips")
    trip_con.execute("UPDATE trips SET rideable_type = NULL WHERE ride_id = 'B2'")
    trip_con.execute("UPDATE trips SET rideable_type = ' ' WHERE ride_id = 'E4A8132DACF670C3'")
    trip_con.execute("UPDATE trips SET rideable_type = 'cargo_bike' WHERE ride_id = 'C3'")
    build_station_hour_activity(trip_con, tmp_path / "typed_hours")
    paths = sorted(str(p) for p in (tmp_path / "typed_hours").rglob("*.parquet"))
    totals = trip_con.execute("""
        SELECT sum(departures_unknown), sum(departures_other), sum(departures_classic),
               sum(departures_electric)
        FROM read_parquet(?) WHERE id_scheme = 'short_name'
    """, [paths]).fetchone()
    assert totals == (2, 1, 4, 0)


def test_month_activity_and_active_periods(trip_con, hours, tmp_path):
    build_station_month_activity(trip_con, tmp_path / "months", hours)
    months = {(r["station_id"], r["month_start"]): r
              for r in _rows(trip_con, sorted(str(p) for p in (tmp_path / "months").rglob("*.parquet")),
                             "station_id, month_start")}
    nov = months[("6322.01", date(2023, 11, 1))]
    assert (nov["departures"], nov["arrivals"], nov["active_days"]) == (4, 1, 2)
    assert (nov["departures_classic"], nov["arrivals_classic"], nov["departures_electric"]) == (4, 1, 0)
    build_station_active_periods(trip_con, tmp_path / "periods", hours)
    periods = {r["station_id"]: r for r in _rows(
        trip_con, sorted(str(p) for p in (tmp_path / "periods").rglob("*.parquet")), "station_id")}
    p = periods["6322.01"]
    assert (p["first_activity_date"], p["last_activity_date"], p["basis"]) == (
        date(2023, 11, 5), date(2025, 1, 10), "inferred_from_trips")


def test_location_observations(trip_con, tmp_path):
    build_station_location_observations(trip_con, tmp_path / "locations")
    files = sorted(str(p) for p in (tmp_path / "locations").rglob("*.parquet"))
    obs = {(r["station_id"], r["station_name"]): r for r in _rows(trip_con, files, "station_id, station_name")}
    e33 = obs[("6322.01", "E 33 St & 5 Ave")]
    assert e33["trips_observed"] == 7 and e33["median_lat"] == pytest.approx(40.74765947)
    assert e33["first_seen"] == date(2023, 11, 5) and e33["last_seen"] == date(2025, 1, 10)
    assert obs[("379", "W 31 St & 7 Ave")]["trips_observed"] == 2
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_aggregate.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'citibike_pipeline.aggregate'`.

- [ ] **Step 3: Write `src/citibike_pipeline/aggregate.py`**

```python
"""Station-level demand tables built from the published trips (Stage 3)."""

from __future__ import annotations

from pathlib import Path

from .db import sql_literal

_TRIPS_WITH_RELEASE = """
CREATE OR REPLACE TEMP VIEW trips_r AS
SELECT t.*, s.release,
       CASE WHEN t.schema_family = 'modern' THEN 'short_name' ELSE 'legacy_id' END AS id_scheme,
       CASE WHEN t.rideable_type = 'classic_bike' THEN 'classic'
            WHEN t.rideable_type = 'electric_bike' THEN 'electric'
            WHEN t.rideable_type IS NULL OR trim(t.rideable_type) = '' THEN 'unknown'
            ELSE 'other' END AS bike_type
FROM trips t JOIN trip_sources s USING (source_key)
"""


def _fresh(out_dir: Path) -> Path:
    if out_dir.exists() and any(out_dir.rglob("*.parquet")):
        raise FileExistsError(f"{out_dir} already holds Parquet files")
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    return out_dir


def _copy(con, select_sql: str, out_dir: Path, params=None, partition: str | None = None) -> int:
    _fresh(out_dir)
    options = "FORMAT parquet, COMPRESSION zstd"
    if partition:
        options += f", PARTITION_BY ({partition})"
    else:
        out_dir.mkdir(parents=True, exist_ok=True)
        out_dir = out_dir / "data_0.parquet"
    con.execute(f"COPY ({select_sql}) TO {sql_literal(str(out_dir))} ({options})", params or [])
    target = out_dir if partition else out_dir.parent
    files = [str(p) for p in Path(target).rglob("*.parquet")]
    return con.execute("SELECT count(*) FROM read_parquet(?)", [files]).fetchone()[0] if files else 0


def build_station_hour_activity(con, out_dir: Path) -> int:
    con.execute(_TRIPS_WITH_RELEASE)
    sql = """
    WITH dep AS (
      SELECT start_station_id AS station_id, id_scheme, date_trunc('hour', started_at_local) AS hour_start,
             release,
             count(*) AS departures,
             count(*) FILTER (WHERE membership_group = 'member') AS departures_member,
             count(*) FILTER (WHERE membership_group = 'casual') AS departures_casual,
             count(*) FILTER (WHERE bike_type = 'classic') AS departures_classic,
             count(*) FILTER (WHERE bike_type = 'electric') AS departures_electric,
             count(*) FILTER (WHERE bike_type = 'other') AS departures_other,
             count(*) FILTER (WHERE bike_type = 'unknown') AS departures_unknown
      FROM trips_r WHERE eligible_departure GROUP BY ALL
    ), arr AS (
      SELECT end_station_id AS station_id, id_scheme, date_trunc('hour', ended_at_local) AS hour_start,
             release,
             count(*) AS arrivals,
             count(*) FILTER (WHERE membership_group = 'member') AS arrivals_member,
             count(*) FILTER (WHERE membership_group = 'casual') AS arrivals_casual,
             count(*) FILTER (WHERE bike_type = 'classic') AS arrivals_classic,
             count(*) FILTER (WHERE bike_type = 'electric') AS arrivals_electric,
             count(*) FILTER (WHERE bike_type = 'other') AS arrivals_other,
             count(*) FILTER (WHERE bike_type = 'unknown') AS arrivals_unknown
      FROM trips_r WHERE eligible_arrival GROUP BY ALL
    )
    SELECT station_id, id_scheme, hour_start, release,
           coalesce(departures, 0)::BIGINT AS departures,
           coalesce(arrivals, 0)::BIGINT AS arrivals,
           coalesce(departures_member, 0)::BIGINT AS departures_member,
           coalesce(departures_casual, 0)::BIGINT AS departures_casual,
           coalesce(departures_classic, 0)::BIGINT AS departures_classic,
           coalesce(departures_electric, 0)::BIGINT AS departures_electric,
           coalesce(departures_other, 0)::BIGINT AS departures_other,
           coalesce(departures_unknown, 0)::BIGINT AS departures_unknown,
           coalesce(arrivals_member, 0)::BIGINT AS arrivals_member,
           coalesce(arrivals_casual, 0)::BIGINT AS arrivals_casual,
           coalesce(arrivals_classic, 0)::BIGINT AS arrivals_classic,
           coalesce(arrivals_electric, 0)::BIGINT AS arrivals_electric,
           coalesce(arrivals_other, 0)::BIGINT AS arrivals_other,
           coalesce(arrivals_unknown, 0)::BIGINT AS arrivals_unknown,
           year(hour_start) AS year
    FROM dep FULL OUTER JOIN arr USING (station_id, id_scheme, hour_start, release)
    """
    return _copy(con, sql, out_dir, partition="year")


def build_station_month_activity(con, out_dir: Path, hour_files: list[str]) -> int:
    sql = """
    SELECT station_id, id_scheme, date_trunc('month', hour_start)::DATE AS month_start,
           sum(departures)::BIGINT AS departures, sum(arrivals)::BIGINT AS arrivals,
           sum(departures_classic)::BIGINT AS departures_classic,
           sum(departures_electric)::BIGINT AS departures_electric,
           sum(departures_other)::BIGINT AS departures_other,
           sum(departures_unknown)::BIGINT AS departures_unknown,
           sum(arrivals_classic)::BIGINT AS arrivals_classic,
           sum(arrivals_electric)::BIGINT AS arrivals_electric,
           sum(arrivals_other)::BIGINT AS arrivals_other,
           sum(arrivals_unknown)::BIGINT AS arrivals_unknown,
           count(DISTINCT hour_start::DATE) AS active_days,
           min(hour_start) AS first_activity, max(hour_start) AS last_activity
    FROM read_parquet(?) GROUP BY ALL
    """
    return _copy(con, sql, out_dir, params=[hour_files])


def build_station_active_periods(con, out_dir: Path, hour_files: list[str]) -> int:
    sql = """
    SELECT station_id, id_scheme,
           min(hour_start)::DATE AS first_activity_date, max(hour_start)::DATE AS last_activity_date,
           count(DISTINCT hour_start::DATE) AS active_days_observed,
           'inferred_from_trips' AS basis
    FROM read_parquet(?) GROUP BY ALL
    """
    return _copy(con, sql, out_dir, params=[hour_files])


def build_station_location_observations(con, out_dir: Path) -> int:
    con.execute(_TRIPS_WITH_RELEASE)
    sql = """
    WITH ends AS (
      SELECT start_station_id AS station_id, id_scheme, start_station_name AS station_name,
             start_lat AS lat, start_lng AS lng, started_at_local AS ts,
             NOT list_contains(quality_flags, 'start_coords_invalid') AS coords_ok
      FROM trips_r WHERE start_station_id IS NOT NULL
      UNION ALL
      SELECT end_station_id, id_scheme, end_station_name, end_lat, end_lng, ended_at_local,
             NOT list_contains(quality_flags, 'end_coords_invalid')
      FROM trips_r WHERE end_station_id IS NOT NULL AND ended_at_local IS NOT NULL
    )
    SELECT station_id, id_scheme, station_name, count(*) AS trips_observed,
           median(lat) FILTER (WHERE coords_ok) AS median_lat,
           median(lng) FILTER (WHERE coords_ok) AS median_lng,
           min(ts)::DATE AS first_seen, max(ts)::DATE AS last_seen
    FROM ends GROUP BY ALL
    """
    return _copy(con, sql, out_dir)
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_aggregate.py -q` then `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/citibike_pipeline/aggregate.py tests/test_aggregate.py
git commit -m "Build station hour, month, active-period and location tables"
```

### Task 3: Release availability dates

**Files:**
- Create: `src/citibike_pipeline/availability.py`
- Modify: `src/citibike_pipeline/report.py` (add `parse_listing_dates`), `src/citibike_pipeline/aggregate.py` (add `build_release_availability`)
- Test: `tests/test_availability.py`

**Interfaces:**
- Consumes: the S3 listing at `cfg.reports_dir / "tripdata_listing.xml"` when present; `Config.release_lag_days`.
- Produces:
  - `parse_listing_dates(xml_text) -> dict[str, date]` (key name to `LastModified` date; entries without Key or LastModified are skipped; the S3 namespace is optional, matching the cleanup wave's `parse_listing`).
  - `assumed_availability(release: str, lag_days: int) -> date`: (last day of the release month) + `lag_days` days.
  - `release_availability_rows(releases, listing_dates, lag_days) -> list[dict]` with keys `release, month_start, coverage_end, availability_date, availability_basis`. `coverage_end` is the first day of the month after the release month (exclusive end). Basis is `listing_last_modified` when the release is 2024-01 or later and a monthly file `YYYYMM-citibike-tripdata.zip` (or `.csv.zip`) is in the listing; otherwise `assumed_lag_<N>d`.
  - `build_release_availability(con, out_dir, cfg, manifest, listing_dates) -> int` writes one row per release that has selected sources.

- [ ] **Step 1: Write the failing tests**

```python
from datetime import date

from citibike_pipeline.availability import assumed_availability, release_availability_rows
from citibike_pipeline.report import parse_listing_dates

LISTING = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Contents><Key>202502-citibike-tripdata.zip</Key><LastModified>2025-03-10T14:02:11.000Z</LastModified><Size>1</Size></Contents>
  <Contents><Key>2018-citibike-tripdata.zip</Key><LastModified>2024-02-21T00:00:00.000Z</LastModified><Size>1</Size></Contents>
  <Contents><Key>broken.zip</Key><Size>1</Size></Contents>
</ListBucketResult>"""


def test_parse_listing_dates():
    assert parse_listing_dates(LISTING) == {
        "202502-citibike-tripdata.zip": date(2025, 3, 10),
        "2018-citibike-tripdata.zip": date(2024, 2, 21),
    }


def test_parse_listing_dates_without_namespace():
    xml = "<ListBucketResult><Contents><Key>a.zip</Key><LastModified>2025-01-02T00:00:00Z</LastModified></Contents></ListBucketResult>"
    assert parse_listing_dates(xml) == {"a.zip": date(2025, 1, 2)}


def test_assumed_availability_is_month_end_plus_lag():
    assert assumed_availability("2025-01", 35) == date(2025, 3, 7)
    assert assumed_availability("2024-02", 35) == date(2024, 4, 4)  # leap year February
    assert assumed_availability("2023-12", 10) == date(2024, 1, 10)


def test_release_availability_rows_use_listing_only_for_monthly_files_from_2024():
    rows = {r["release"]: r for r in release_availability_rows(
        ["2018-04", "2025-02", "2025-03"], parse_listing_dates(LISTING), 35)}
    assert rows["2025-02"]["availability_date"] == date(2025, 3, 10)
    assert rows["2025-02"]["availability_basis"] == "listing_last_modified"
    assert rows["2018-04"]["availability_date"] == date(2018, 6, 4)
    assert rows["2018-04"]["availability_basis"] == "assumed_lag_35d"
    assert rows["2025-03"]["availability_basis"] == "assumed_lag_35d"  # not in the listing
    assert rows["2025-02"]["month_start"] == date(2025, 2, 1)
    assert rows["2025-02"]["coverage_end"] == date(2025, 3, 1)
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_availability.py -q`
Expected: FAIL with `ImportError` on `parse_listing_dates` / `ModuleNotFoundError: citibike_pipeline.availability`.

- [ ] **Step 3: Add `parse_listing_dates` to `report.py`** (next to `parse_listing`; add `from datetime import date` to the imports)

```python
def parse_listing_dates(xml_text: str) -> dict[str, date]:
    """Key -> LastModified date. The S3 namespace is optional; incomplete entries are skipped."""
    root = ET.fromstring(xml_text)
    ns = S3_NS if root.tag.startswith("{") else {}
    prefix = "s3:" if ns else ""
    out: dict[str, date] = {}
    for c in root.findall(f"{prefix}Contents", ns):
        key = c.findtext(f"{prefix}Key", namespaces=ns)
        modified = c.findtext(f"{prefix}LastModified", namespaces=ns)
        if key and modified:
            out[key] = date.fromisoformat(modified[:10])
    return out
```

- [ ] **Step 4: Write `src/citibike_pipeline/availability.py`**

```python
"""When each monthly release became public: listing evidence where it exists, otherwise an assumed lag."""

from __future__ import annotations

import calendar
from datetime import date, timedelta

_LISTING_FROM = "2024-01"  # earlier year ZIPs were re-packaged in 2024; their timestamps are not release dates


def _month_end(release: str) -> date:
    year, month = map(int, release.split("-"))
    return date(year, month, calendar.monthrange(year, month)[1])


def assumed_availability(release: str, lag_days: int) -> date:
    return _month_end(release) + timedelta(days=lag_days)


def release_availability_rows(releases, listing_dates: dict[str, date], lag_days: int) -> list[dict]:
    rows = []
    for release in sorted(set(releases)):
        year, month = map(int, release.split("-"))
        stem = f"{year:04d}{month:02d}-citibike-tripdata"
        listed = listing_dates.get(f"{stem}.zip") or listing_dates.get(f"{stem}.csv.zip")
        if release >= _LISTING_FROM and listed:
            available, basis = listed, "listing_last_modified"
        else:
            available, basis = assumed_availability(release, lag_days), f"assumed_lag_{lag_days}d"
        rows.append({
            "release": release,
            "month_start": date(year, month, 1),
            "coverage_end": _month_end(release) + timedelta(days=1),
            "availability_date": available,
            "availability_basis": basis,
        })
    return rows
```

- [ ] **Step 5: Add `build_release_availability` to `aggregate.py`**

Add `import pyarrow as pa` and `from .availability import release_availability_rows` to the imports, then:

```python
def build_release_availability(con, out_dir: Path, cfg, manifest, listing_dates) -> int:
    releases = [r["nominal_release"] for r in manifest.all()
                if r["status"] == "selected" and r["nominal_release"]]
    rows = release_availability_rows(releases, listing_dates, cfg.release_lag_days)
    table = pa.Table.from_pylist(rows, schema=pa.schema([
        ("release", pa.string()), ("month_start", pa.date32()), ("coverage_end", pa.date32()),
        ("availability_date", pa.date32()), ("availability_basis", pa.string()),
    ]))
    con.register("release_rows", table)
    try:
        return _copy(con, "SELECT * FROM release_rows ORDER BY release", out_dir)
    finally:
        con.unregister("release_rows")
```

Add this test to `tests/test_aggregate.py`:

```python
def test_release_availability_table(trip_con, selected_cfg, tmp_path):
    from citibike_pipeline.aggregate import build_release_availability

    with Manifest(selected_cfg) as m:
        n = build_release_availability(trip_con, tmp_path / "avail", selected_cfg, m, {})
    files = sorted(str(p) for p in (tmp_path / "avail").rglob("*.parquet"))
    rows = {r["release"]: r for r in _rows(trip_con, files, "release")}
    assert n == 5 and set(rows) == set(FIXTURE_RELEASES)
    assert rows["2025-01"]["availability_date"] == date(2025, 3, 7)
```

- [ ] **Step 6: Run the tests**

Run: `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/citibike_pipeline/availability.py src/citibike_pipeline/report.py src/citibike_pipeline/aggregate.py tests/test_availability.py tests/test_aggregate.py
git commit -m "Date each release from the bucket listing or an explicit assumed lag"
```

### Task 4: `citibike aggregate build`

**Files:**
- Modify: `src/citibike_pipeline/aggregate.py` (add `run_build`), `src/citibike_pipeline/cli.py`, `tests/test_cli.py`
- Test: `tests/test_aggregate_build.py`

**Interfaces:**
- Consumes: Tasks 1 to 3; `Manifest.start_run/finish_run`; `report.parse_listing_dates`.
- Produces: `run_build(cfg, manifest, tables: list[str] | None, out) -> int`. It builds every requested table into `builds/<run_id>/` first and promotes them only after all builds succeed, so a failure never leaves a mix of old and new tables. `station_month_activity` and `station_active_periods` read the hour build from the same run when it is being rebuilt, otherwise the current hour build.
- CLI: `citibike aggregate build [--table NAME ...]`, `citibike aggregate validate` (Task 6), `citibike aggregate profiles --as-of YYYY-MM-DD [--window-days 56]` (Task 5). The handlers import their modules lazily, as the other commands do.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_aggregate_build.py`:

```python
import pytest

from archive_fixture import FIXTURE_RELEASES, QUIET
from citibike_pipeline.aggregate import run_build
from citibike_pipeline.builds import TABLES, current_build, table_files
from citibike_pipeline.cli import main
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.pipeline import run_normalize


@pytest.fixture
def published_cfg(selected_cfg):
    with Manifest(selected_cfg) as m:
        assert run_normalize(selected_cfg, m, FIXTURE_RELEASES, out=QUIET) == 0
    return selected_cfg


def test_build_all_tables_and_rebuild(published_cfg):
    with Manifest(published_cfg) as m:
        assert run_build(published_cfg, m, None, QUIET) == 0
        first = {t: current_build(published_cfg, t) for t in TABLES}
        assert all(p is not None and table_files(published_cfg, t) for t, p in first.items())
        assert run_build(published_cfg, m, None, QUIET) == 0
    second = {t: current_build(published_cfg, t) for t in TABLES}
    assert all(first[t] != second[t] for t in TABLES)
    assert all(not first[t].exists() for t in TABLES)


def test_build_single_table_reuses_current_hours(published_cfg):
    with Manifest(published_cfg) as m:
        run_build(published_cfg, m, None, QUIET)
        hours = current_build(published_cfg, "station_hour_activity")
        assert run_build(published_cfg, m, ["station_month_activity"], QUIET) == 0
    assert current_build(published_cfg, "station_hour_activity") == hours


def test_unknown_table_is_rejected(published_cfg):
    with Manifest(published_cfg) as m:
        assert run_build(published_cfg, m, ["nope"], QUIET) == 2


def test_failed_build_promotes_nothing(published_cfg, monkeypatch):
    import citibike_pipeline.aggregate as agg

    with Manifest(published_cfg) as m:
        run_build(published_cfg, m, None, QUIET)
        before = {t: current_build(published_cfg, t) for t in TABLES}

        def boom(*a, **k):
            raise RuntimeError("disk full")

        monkeypatch.setattr(agg, "build_station_location_observations", boom)
        with pytest.raises(RuntimeError):
            run_build(published_cfg, m, None, QUIET)
        status = m.con.execute("SELECT status FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()[0]
    assert status == "failed"
    assert {t: current_build(published_cfg, t) for t in TABLES} == before


def test_cli_aggregate_build(published_cfg, capsys):
    config_path = published_cfg.processed_root.parent / "local.yaml"
    assert main(["--config", str(config_path), "aggregate", "build", "--table", "release_availability"]) == 0
    assert "release_availability" in capsys.readouterr().out
```

In `tests/test_cli.py`, replace `test_later_stage_commands_are_explicitly_unavailable` with:

```python
def test_later_stage_commands_are_explicitly_unavailable(cfg_file, capsys):
    assert main(["--config", str(cfg_file), "publish-plan"]) == 2
    assert "not implemented" in capsys.readouterr().err


def test_aggregate_requires_an_action(cfg_file):
    with pytest.raises(SystemExit) as exc:
        main(["--config", str(cfg_file), "aggregate"])
    assert exc.value.code == 2
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_aggregate_build.py tests/test_cli.py -q`
Expected: FAIL (`ImportError: cannot import name 'run_build'`; `aggregate` with no action returns 2 from `_later_stage` instead of raising `SystemExit`).

- [ ] **Step 3: Add `run_build` to `aggregate.py`**

Add to the imports:

```python
import time

from .builds import TABLES, build_dir, promote, table_files
from .report import parse_listing_dates
from .trips import aggregate_connection, register_trips
```

Then:

```python
def run_build(cfg, manifest, tables, out) -> int:
    requested = [t for t in TABLES if t in set(tables or TABLES)]
    unknown = sorted(set(tables or ()) - set(TABLES))
    if unknown:
        out(f"unknown table(s): {', '.join(unknown)}; choose from {', '.join(TABLES)}")
        return 2
    run_id = manifest.start_run("aggregate build", {"tables": requested})
    con = aggregate_connection(cfg)
    try:
        register_trips(con, cfg, manifest)
        listing_file = cfg.reports_dir / "tripdata_listing.xml"
        listing_dates = parse_listing_dates(listing_file.read_text()) if listing_file.exists() else {}

        def hour_files() -> list[str]:
            if "station_hour_activity" in requested:
                return sorted(str(p) for p in build_dir(cfg, "station_hour_activity", run_id).rglob("*.parquet"))
            return table_files(cfg, "station_hour_activity")

        builders = {
            "station_hour_activity": lambda d: build_station_hour_activity(con, d),
            "station_month_activity": lambda d: build_station_month_activity(con, d, hour_files()),
            "station_active_periods": lambda d: build_station_active_periods(con, d, hour_files()),
            "station_location_observations": lambda d: build_station_location_observations(con, d),
            "release_availability": lambda d: build_release_availability(con, d, cfg, manifest, listing_dates),
        }
        for table in requested:
            started = time.monotonic()
            rows = builders[table](build_dir(cfg, table, run_id))
            out(f"{table}: {rows} rows in {time.monotonic() - started:.1f}s")
        for table in requested:
            promote(cfg, table, run_id)
    except Exception:
        manifest.finish_run(run_id, "failed")
        raise
    finally:
        con.close()
    manifest.finish_run(run_id, "ok")
    out(f"aggregate build {run_id}: {len(requested)} table(s) promoted")
    return 0
```

Note: `builders` looks the builder functions up at call time through the module globals, so the test's `monkeypatch.setattr(agg, "build_station_location_observations", ...)` takes effect.

- [ ] **Step 4: Wire the CLI in `cli.py`**

Add handlers next to `_validate`:

```python
def _aggregate(cfg, args) -> int:
    from .manifest import Manifest

    if args.action == "build":
        from .aggregate import run_build

        with Manifest(cfg) as manifest:
            return run_build(cfg, manifest, args.table, print)
    if args.action == "validate":
        from .aggregate_validate import run_aggregate_validate

        return run_aggregate_validate(cfg, print)
    from .profiles import run_profiles

    with Manifest(cfg) as manifest:
        return run_profiles(cfg, manifest, args.as_of, args.window_days, print)
```

Replace the `aggregate` line in `build_parser` with:

```python
    agg = sub.add_parser("aggregate", help="build and check station demand tables (Stage 3)")
    agg.set_defaults(handler=_aggregate)
    asub = agg.add_subparsers(dest="action", required=True)
    build = asub.add_parser("build", help="rebuild station demand tables from the published trips")
    build.add_argument("--table", action="append", help="table name (repeatable); default all")
    asub.add_parser("validate", help="reconcile the current aggregate builds with the trips")
    prof = asub.add_parser("profiles", help="build as-of demand profiles")
    prof.add_argument("--as-of", required=True, type=date.fromisoformat, help="YYYY-MM-DD cutoff")
    prof.add_argument("--window-days", type=int, default=56)
```

Add `from datetime import date` to the imports of `cli.py`.

- [ ] **Step 5: Run the tests**

Run: `uv run pytest -q`
Expected: all PASS. (`aggregate validate` and `aggregate profiles` are wired but their modules arrive in Tasks 5 and 6; nothing calls them yet.)

- [ ] **Step 6: Commit**

```bash
git add src/citibike_pipeline/aggregate.py src/citibike_pipeline/cli.py tests/test_aggregate_build.py tests/test_cli.py
git commit -m "Add citibike aggregate build with all-or-nothing promotion"
```

### Task 5: As-of demand profiles

**Files:**
- Create: `src/citibike_pipeline/profiles.py`
- Test: `tests/test_profiles.py`

**Interfaces:**
- Consumes: current builds of `station_hour_activity` and `release_availability` (`builds.table_files`); `builds.build_dir`, `builds.promote`; `trips.aggregate_connection`.
- Produces: `ProfileError`; `build_profiles(con, cfg, as_of: date, window_days: int, out_dir: Path) -> dict` (returns `latest_release`, `window_start`, `window_end`, `rows`); `run_profiles(cfg, manifest, as_of, window_days, out) -> int`. Output table `demand_profiles` under `aggregates/demand_profiles/as_of=YYYY-MM-DD/builds/<run_id>/`, promoted with `promote(cfg, f"demand_profiles/as_of={as_of}", run_id)`.

**Definitions (the leakage rules):**
1. A release is *eligible* when `availability_date < as_of` (strictly before the cutoff day).
2. `latest_release` = the latest eligible release. `window_end` = min(`as_of`, the latest `coverage_end` among eligible releases), an exclusive end. `window_start` = `window_end - window_days`.
3. Only hour rows from eligible releases, with `id_scheme = 'short_name'` and `hour_start < window_end`, are read.
4. A station's active span inside the window is [max(first activity, `window_start`), min(last activity, `window_end` - 1 day)], using its eligible history. `active_weekdays` for (station, ISO weekday) = the number of calendar days of that weekday inside the span.
5. Every bike-type mean uses the same `active_weekdays` denominator as totals; departure shares are null with zero departures. Missing bike type is unknown, not classic.
6. Output is dense: every station with a non-empty span gets 7 x 24 rows (zero means where no trips occurred) **for the weekdays inside its span**. `departures_mean = window departures / active_weekdays`, likewise for arrivals; `net_flow_mean = departures_mean - arrivals_mean`.

- [ ] **Step 1: Write the failing tests**

```python
from datetime import date

import pytest

from archive_fixture import FIXTURE_RELEASES, QUIET
from citibike_pipeline.aggregate import run_build
from citibike_pipeline.builds import table_files
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.pipeline import run_normalize
from citibike_pipeline.profiles import ProfileError, build_profiles, run_profiles
from citibike_pipeline.trips import aggregate_connection

# Fixture availability (no listing, 35-day lag): 2015-01 -> 2015-03-07, 2016-12 -> 2017-02-04,
# 2018-04 -> 2018-06-04, 2023-11 -> 2024-01-04, 2025-01 -> 2025-03-07.


@pytest.fixture
def built_cfg(selected_cfg):
    with Manifest(selected_cfg) as m:
        assert run_normalize(selected_cfg, m, FIXTURE_RELEASES, out=QUIET) == 0
        assert run_build(selected_cfg, m, None, QUIET) == 0
    return selected_cfg


def _profile(cfg, tmp_path, as_of, window_days):
    con = aggregate_connection(cfg)
    meta = build_profiles(con, cfg, as_of, window_days, tmp_path / str(as_of))
    files = sorted(str(p) for p in (tmp_path / str(as_of)).rglob("*.parquet"))
    rel = con.sql("SELECT * FROM read_parquet(?)", params=[files])
    rows = {(r[1], r[3], r[4]): dict(zip(rel.columns, r)) for r in rel.fetchall()}
    return meta, rows


def test_profile_excludes_releases_not_yet_published(built_cfg, tmp_path):
    meta, rows = _profile(built_cfg, tmp_path, date(2025, 2, 15), 30)
    assert meta["latest_release"] == "2023-11"
    assert (meta["window_start"], meta["window_end"]) == (date(2023, 11, 1), date(2023, 12, 1))
    assert meta["rows"] == 2 * 2 * 24  # two stations, Sunday and Monday only, 24 hours
    sunday_1am = rows[("6322.01", 7, 1)]
    assert (sunday_1am["departures_mean"], sunday_1am["arrivals_mean"], sunday_1am["net_flow_mean"]) == (2.0, 1.0, 1.0)
    assert rows[("6322.01", 1, 8)]["departures_mean"] == 2.0
    assert rows[("6322.01", 7, 13)]["departures_mean"] == 0.0
    assert sunday_1am["departures_classic_mean"] == 2.0
    assert sunday_1am["departures_classic_count"] == 2
    assert sunday_1am["departures_classic_share"] == 1.0
    assert sunday_1am["departures_electric_mean"] == 0.0
    assert rows[("6322.01", 7, 13)]["departures_classic_share"] is None
    for r in rows.values():
        for flow in ("departures", "arrivals", "net_flow"):
            assert sum(r[f"{flow}_{b}_mean"] for b in ("classic", "electric", "other", "unknown")) == pytest.approx(r[f"{flow}_mean"])
    assert ("6322.01", 3, 1) not in rows  # no Wednesday inside the active span


def test_profile_uses_a_release_only_after_it_is_available(built_cfg, tmp_path):
    meta, _ = _profile(built_cfg, tmp_path, date(2025, 3, 7), 30)
    assert meta["latest_release"] == "2023-11"  # 2025-01 becomes available on 2025-03-07, not before
    meta, rows = _profile(built_cfg, tmp_path, date(2025, 3, 8), 60)
    assert meta["latest_release"] == "2025-01"
    assert (meta["window_start"], meta["window_end"]) == (date(2024, 12, 3), date(2025, 2, 1))
    c3 = rows[("6322.01", 2, 23)]  # C3 departs Tuesday 2024-12-31 23:50
    assert c3["active_weekdays"] == 6 and c3["departures_mean"] == pytest.approx(1 / 6)
    assert c3["departures_classic_mean"] == pytest.approx(1 / 6)
    electric = rows[("6322.01", 5, 14)]
    assert electric["departures_electric_mean"] == pytest.approx(1 / 6)
    assert electric["departures_electric_share"] == 1.0


def test_profile_never_reads_trips_on_or_after_as_of(built_cfg, tmp_path):
    meta, rows = _profile(built_cfg, tmp_path, date(2025, 1, 5), 400)
    assert meta["window_end"] <= date(2025, 1, 5)
    assert all(r["window_end"] <= date(2025, 1, 5) for r in rows.values())


def test_profile_without_any_available_release_fails(built_cfg, tmp_path):
    with pytest.raises(ProfileError):
        _profile(built_cfg, tmp_path, date(2015, 1, 15), 30)


def test_run_profiles_promotes_per_as_of(built_cfg):
    with Manifest(built_cfg) as m:
        assert run_profiles(built_cfg, m, date(2025, 2, 15), 30, QUIET) == 0
        assert run_profiles(built_cfg, m, date(2015, 1, 15), 30, QUIET) == 1
    assert table_files(built_cfg, "demand_profiles/as_of=2025-02-15")
```

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_profiles.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'citibike_pipeline.profiles'`.

- [ ] **Step 3: Write `src/citibike_pipeline/profiles.py`**

```python
"""Station x weekday x hour demand profiles computed only from trips published before a cutoff."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

from .builds import build_dir, promote, table_files
from .db import sql_literal
from .trips import aggregate_connection


class ProfileError(RuntimeError):
    pass


_PROFILE_SQL = """
WITH h AS (
  SELECT station_id, hour_start, departures, arrivals,
         departures_classic, departures_electric, departures_other, departures_unknown, arrivals_classic, arrivals_electric, arrivals_other, arrivals_unknown
  FROM read_parquet($hours)
  WHERE id_scheme = 'short_name' AND list_contains($releases, release) AND hour_start < $window_end_ts
),
life AS (
  SELECT station_id,
         greatest(min(hour_start)::DATE, $window_start) AS first_d,
         least(max(hour_start)::DATE, $last_day) AS last_d
  FROM h GROUP BY station_id
),
cal AS (
  SELECT d::DATE AS d FROM range($window_start_ts, $window_end_ts, INTERVAL 1 DAY) t(d)
),
active AS (
  SELECT station_id, isodow(d) AS isodow, count(*) AS active_weekdays
  FROM life JOIN cal ON d BETWEEN first_d AND last_d GROUP BY ALL
),
sums AS (
  SELECT station_id, isodow(hour_start) AS isodow, hour(hour_start) AS hour,
         sum(departures) AS dep, sum(arrivals) AS arr,
         sum(departures_classic) AS dep_classic,
         sum(departures_electric) AS dep_electric,
         sum(departures_other) AS dep_other,
         sum(departures_unknown) AS dep_unknown,
         sum(arrivals_classic) AS arr_classic,
         sum(arrivals_electric) AS arr_electric,
         sum(arrivals_other) AS arr_other,
         sum(arrivals_unknown) AS arr_unknown
  FROM h WHERE hour_start >= $window_start_ts GROUP BY ALL
),
grid AS (
  SELECT a.station_id, a.isodow, hr.hour, a.active_weekdays FROM active a, range(24) hr(hour)
)
SELECT $as_of AS as_of, g.station_id, 'short_name' AS id_scheme, g.isodow::INTEGER AS isodow,
       g.hour::INTEGER AS hour,
       coalesce(s.dep, 0) / g.active_weekdays AS departures_mean,
       coalesce(s.arr, 0) / g.active_weekdays AS arrivals_mean,
       (coalesce(s.dep, 0) - coalesce(s.arr, 0)) / g.active_weekdays AS net_flow_mean,
       coalesce(s.dep_classic, 0) / g.active_weekdays AS departures_classic_mean,
       coalesce(s.arr_classic, 0) / g.active_weekdays AS arrivals_classic_mean,
       (coalesce(s.dep_classic, 0) - coalesce(s.arr_classic, 0)) / g.active_weekdays AS net_flow_classic_mean,
       coalesce(s.dep_classic, 0)::BIGINT AS departures_classic_count,
       coalesce(s.dep_classic, 0) / nullif(s.dep, 0) AS departures_classic_share,
       coalesce(s.dep_electric, 0) / g.active_weekdays AS departures_electric_mean,
       coalesce(s.arr_electric, 0) / g.active_weekdays AS arrivals_electric_mean,
       (coalesce(s.dep_electric, 0) - coalesce(s.arr_electric, 0)) / g.active_weekdays AS net_flow_electric_mean,
       coalesce(s.dep_electric, 0)::BIGINT AS departures_electric_count,
       coalesce(s.dep_electric, 0) / nullif(s.dep, 0) AS departures_electric_share,
       coalesce(s.dep_other, 0) / g.active_weekdays AS departures_other_mean,
       coalesce(s.arr_other, 0) / g.active_weekdays AS arrivals_other_mean,
       (coalesce(s.dep_other, 0) - coalesce(s.arr_other, 0)) / g.active_weekdays AS net_flow_other_mean,
       coalesce(s.dep_other, 0)::BIGINT AS departures_other_count,
       coalesce(s.dep_other, 0) / nullif(s.dep, 0) AS departures_other_share,
       coalesce(s.dep_unknown, 0) / g.active_weekdays AS departures_unknown_mean,
       coalesce(s.arr_unknown, 0) / g.active_weekdays AS arrivals_unknown_mean,
       (coalesce(s.dep_unknown, 0) - coalesce(s.arr_unknown, 0)) / g.active_weekdays AS net_flow_unknown_mean,
       coalesce(s.dep_unknown, 0)::BIGINT AS departures_unknown_count,
       coalesce(s.dep_unknown, 0) / nullif(s.dep, 0) AS departures_unknown_share,
       g.active_weekdays::INTEGER AS active_weekdays,
       $window_start AS window_start, $window_end AS window_end, $latest_release AS latest_release
FROM grid g LEFT JOIN sums s USING (station_id, isodow, hour)
ORDER BY g.station_id, g.isodow, g.hour
"""


def build_profiles(con, cfg, as_of: date, window_days: int, out_dir: Path) -> dict:
    eligible = con.execute(
        "SELECT release, coverage_end FROM read_parquet(?) WHERE availability_date < ? ORDER BY release",
        [table_files(cfg, "release_availability"), as_of],
    ).fetchall()
    if not eligible:
        raise ProfileError(f"no release was available before {as_of}")
    latest_release = eligible[-1][0]
    window_end = min(as_of, max(coverage_end for _, coverage_end in eligible))
    window_start = window_end - timedelta(days=window_days)
    params = {
        "hours": table_files(cfg, "station_hour_activity"),
        "releases": [release for release, _ in eligible],
        "as_of": as_of,
        "window_start": window_start,
        "window_end": window_end,
        "last_day": window_end - timedelta(days=1),
        "window_start_ts": datetime.combine(window_start, datetime.min.time()),
        "window_end_ts": datetime.combine(window_end, datetime.min.time()),
        "latest_release": latest_release,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "data_0.parquet"
    con.execute(f"COPY ({_PROFILE_SQL}) TO {sql_literal(str(target))} (FORMAT parquet, COMPRESSION zstd)", params)
    rows = con.execute("SELECT count(*) FROM read_parquet(?)", [str(target)]).fetchone()[0]
    return {"latest_release": latest_release, "window_start": window_start, "window_end": window_end, "rows": rows}


def run_profiles(cfg, manifest, as_of: date, window_days: int, out) -> int:
    table = f"demand_profiles/as_of={as_of.isoformat()}"
    run_id = manifest.start_run("aggregate profiles", {"as_of": as_of.isoformat(), "window_days": window_days})
    con = aggregate_connection(cfg)
    try:
        meta = build_profiles(con, cfg, as_of, window_days, build_dir(cfg, table, run_id))
    except ProfileError as exc:
        manifest.finish_run(run_id, "failed")
        out(f"profiles {as_of}: {exc}")
        return 1
    finally:
        con.close()
    promote(cfg, table, run_id)
    manifest.finish_run(run_id, "ok")
    out(f"profiles {as_of}: {meta['rows']} rows, window {meta['window_start']}..{meta['window_end']} "
        f"(exclusive), latest release {meta['latest_release']}")
    return 0
```

If DuckDB rejects a `$name` parameter inside `COPY (...)`, materialize first: `con.execute("CREATE OR REPLACE TEMP TABLE profile_rows AS " + _PROFILE_SQL, params)` and then `COPY profile_rows TO ...` without parameters. Keep the tests unchanged either way.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_profiles.py -q` then `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/citibike_pipeline/profiles.py tests/test_profiles.py
git commit -m "Add leakage-safe as-of station demand profiles"
```

### Task 6: `citibike aggregate validate`

**Files:**
- Create: `src/citibike_pipeline/aggregate_validate.py`
- Test: `tests/test_aggregate_validate.py`

**Interfaces:**
- Consumes: `register_trips`, `aggregate_connection` (Task 1); `current_build`, `table_files` (Task 1); `runs` and `published_files` tables in the manifest.
- Produces: `validate_aggregates(cfg, manifest) -> dict` with keys `ok: bool`, `problems: list[str]`, `checks: list[tuple[str, int, int]]` (name, expected, actual); `run_aggregate_validate(cfg, out) -> int` (writes `reports/aggregate_validation.md`, returns 0 when `ok`, else 1).

**Checks:**
1. Every table in `TABLES` has a current build.
2. Staleness: each current build's run started after the latest `published_at` in the ledger. Otherwise the problem is "stale: <table> was built before the last normalize".
3. `sum(departures)` in `station_hour_activity` equals the count of trips with `eligible_departure`, overall and per `id_scheme`. The same holds for arrivals with `eligible_arrival`.
4. `station_month_activity` totals equal `station_hour_activity` totals (departures and arrivals).
5. `station_active_periods` has one row per distinct `(station_id, id_scheme)` in the hour table.
6. `release_availability` has one row per release with selected sources.
7. Per-row classic + electric + other + unknown equals total departures and arrivals in hourly and monthly tables; each bucket reconciles to eligible raw trips and between hourly/monthly tables. Task 5 profile tests require type means to sum to total means; shares sum to 1 when departures are positive and are null otherwise.
8. Exercise synthetic missing/blank and unfamiliar modern types so they remain unknown/other, and test that unpublished electric-trip history cannot change a profile.

- [ ] **Step 1: Write the failing tests**

```python
import pytest

from archive_fixture import FIXTURE_RELEASES, QUIET
from citibike_pipeline.aggregate import run_build
from citibike_pipeline.aggregate_validate import run_aggregate_validate, validate_aggregates
from citibike_pipeline.manifest import Manifest
from citibike_pipeline.pipeline import run_normalize


@pytest.fixture
def built_cfg(selected_cfg):
    with Manifest(selected_cfg) as m:
        assert run_normalize(selected_cfg, m, FIXTURE_RELEASES, out=QUIET) == 0
        assert run_build(selected_cfg, m, None, QUIET) == 0
    return selected_cfg


def test_fresh_build_validates(built_cfg):
    with Manifest(built_cfg) as m:
        res = validate_aggregates(built_cfg, m)
    assert res["ok"], res["problems"]
    checks = {name: (expected, actual) for name, expected, actual in res["checks"]}
    assert checks["departures"] == (11, 11) and checks["arrivals"] == (11, 11)
    assert checks["departures[short_name]"] == (7, 7)
    assert checks["release_availability rows"] == (5, 5)


def test_build_older_than_last_normalize_is_stale(built_cfg):
    with Manifest(built_cfg) as m:
        assert run_normalize(built_cfg, m, ["2025-01"], out=QUIET) == 0
        res = validate_aggregates(built_cfg, m)
    assert not res["ok"]
    assert any(p.startswith("stale:") for p in res["problems"])


def test_missing_build_is_reported(selected_cfg):
    with Manifest(selected_cfg) as m:
        assert run_normalize(selected_cfg, m, FIXTURE_RELEASES, out=QUIET) == 0
        res = validate_aggregates(selected_cfg, m)
    assert not res["ok"] and any("no current build" in p for p in res["problems"])


def test_run_aggregate_validate_writes_report(built_cfg):
    assert run_aggregate_validate(built_cfg, QUIET) == 0
    text = (built_cfg.reports_dir / "aggregate_validation.md").read_text()
    assert "ok: True" in text and "| departures | 11 | 11 |" in text
```

Fixture short_name departures: 2023-11: 4 plus 2025-01: 3, so 7.

- [ ] **Step 2: Run to see them fail**

Run: `uv run pytest tests/test_aggregate_validate.py -q`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write `src/citibike_pipeline/aggregate_validate.py`**

```python
"""Reconciles the current aggregate builds with the published trips."""

from __future__ import annotations

from .builds import TABLES, current_build, table_files
from .db import sql_literal
from .manifest import Manifest
from .trips import aggregate_connection, register_trips


def _build_started(manifest, run_id: str):
    row = manifest.con.execute("SELECT started_at FROM runs WHERE run_id = ?", [run_id]).fetchone()
    return row[0] if row else None


def validate_aggregates(cfg, manifest) -> dict:
    problems: list[str] = []
    checks: list[tuple[str, int, int]] = []
    builds = {t: current_build(cfg, t) for t in TABLES}
    for table, path in builds.items():
        if path is None:
            problems.append(f"no current build for {table}")
    if problems:
        return {"ok": False, "problems": problems, "checks": checks}

    last_publish = manifest.con.execute("SELECT max(published_at) FROM published_files").fetchone()[0]
    for table, path in builds.items():
        started = _build_started(manifest, path.name)
        if started is None or (last_publish is not None and started < last_publish):
            problems.append(f"stale: {table} was built before the last normalize")

    con = aggregate_connection(cfg)
    try:
        register_trips(con, cfg, manifest)
        hour_paths = ", ".join(sql_literal(f) for f in table_files(cfg, "station_hour_activity"))
        con.execute(f"CREATE OR REPLACE TEMP VIEW hours AS SELECT * FROM read_parquet([{hour_paths}])")

        def check(name: str, expected: int, actual: int) -> None:
            checks.append((name, int(expected), int(actual)))
            if expected != actual:
                problems.append(f"{name}: expected {expected}, got {actual}")

        trips_dep, trips_arr = con.execute(
            "SELECT count(*) FILTER (WHERE eligible_departure), count(*) FILTER (WHERE eligible_arrival) FROM trips"
        ).fetchone()
        hour_dep, hour_arr = con.execute("SELECT sum(departures), sum(arrivals) FROM hours").fetchone()
        check("departures", trips_dep, hour_dep or 0)
        check("arrivals", trips_arr, hour_arr or 0)
        per_scheme_trips = dict(((s, k), n) for s, k, n in con.execute("""
            SELECT CASE WHEN schema_family = 'modern' THEN 'short_name' ELSE 'legacy_id' END, 'dep',
                   count(*) FILTER (WHERE eligible_departure) FROM trips GROUP BY 1
            UNION ALL
            SELECT CASE WHEN schema_family = 'modern' THEN 'short_name' ELSE 'legacy_id' END, 'arr',
                   count(*) FILTER (WHERE eligible_arrival) FROM trips GROUP BY 1
        """).fetchall())
        per_scheme_hours = dict(((s, k), n) for s, k, n in con.execute("""
            SELECT id_scheme, 'dep', sum(departures) FROM hours GROUP BY 1
            UNION ALL SELECT id_scheme, 'arr', sum(arrivals) FROM hours GROUP BY 1
        """).fetchall())
        for (scheme, kind), expected in sorted(per_scheme_trips.items()):
            label = "departures" if kind == "dep" else "arrivals"
            check(f"{label}[{scheme}]", expected, per_scheme_hours.get((scheme, kind)) or 0)

        month_dep, month_arr = con.execute("SELECT sum(departures), sum(arrivals) FROM read_parquet(?)",
                                           [table_files(cfg, "station_month_activity")]).fetchone()
        check("month departures", hour_dep or 0, month_dep or 0)
        check("month arrivals", hour_arr or 0, month_arr or 0)
        for flow, eligible in (("departures", "eligible_departure"), ("arrivals", "eligible_arrival")):
            bucket_sum = " + ".join(f"{flow}_{b}" for b in ("classic", "electric", "other", "unknown"))
            bad_hours = con.execute(f"SELECT count(*) FROM hours WHERE {bucket_sum} != {flow}").fetchone()[0]
            check(f"hour {flow} type reconciliation failures", 0, bad_hours)
            bad_months = con.execute(
                f"SELECT count(*) FROM read_parquet(?) WHERE {bucket_sum} != {flow}",
                [table_files(cfg, "station_month_activity")],
            ).fetchone()[0]
            check(f"month {flow} type reconciliation failures", 0, bad_months)
            for bucket in ("classic", "electric", "other", "unknown"):
                predicate = {
                    "classic": "rideable_type = 'classic_bike'",
                    "electric": "rideable_type = 'electric_bike'",
                    "unknown": "rideable_type IS NULL OR trim(rideable_type) = ''",
                    "other": "trim(rideable_type) != '' AND rideable_type NOT IN ('classic_bike', 'electric_bike')",
                }[bucket]
                expected = con.execute(f"SELECT count(*) FROM trips WHERE {eligible} AND ({predicate})").fetchone()[0]
                actual = con.execute(f"SELECT coalesce(sum({flow}_{bucket}), 0) FROM hours").fetchone()[0]
                check(f"{flow}[{bucket}]", expected, actual)
                monthly = con.execute(
                    f"SELECT coalesce(sum({flow}_{bucket}), 0) FROM read_parquet(?)",
                    [table_files(cfg, "station_month_activity")],
                ).fetchone()[0]
                check(f"month {flow}[{bucket}]", actual, monthly)
        stations = con.execute("SELECT count(*) FROM (SELECT DISTINCT station_id, id_scheme FROM hours)").fetchone()[0]
        periods = con.execute("SELECT count(*) FROM read_parquet(?)",
                              [table_files(cfg, "station_active_periods")]).fetchone()[0]
        check("active period rows", stations, periods)
        releases = len({r["nominal_release"] for r in manifest.all()
                        if r["status"] == "selected" and r["nominal_release"]})
        avail = con.execute("SELECT count(*) FROM read_parquet(?)",
                            [table_files(cfg, "release_availability")]).fetchone()[0]
        check("release_availability rows", releases, avail)
    finally:
        con.close()
    return {"ok": not problems, "problems": problems, "checks": checks}


def run_aggregate_validate(cfg, out) -> int:
    with Manifest(cfg) as manifest:
        res = validate_aggregates(cfg, manifest)
    cfg.reports_dir.mkdir(parents=True, exist_ok=True)
    lines = ["# Aggregate validation", "", f"- ok: {res['ok']}", "", "| check | expected | actual |",
             "|---|---|---|"]
    lines += [f"| {name} | {expected} | {actual} |" for name, expected, actual in res["checks"]]
    lines += ["", "## Problems", ""] + ([f"- {p}" for p in res["problems"]] or ["- none"])
    path = cfg.reports_dir / "aggregate_validation.md"
    path.write_text("\n".join(lines) + "\n")
    out(f"aggregate validate: ok={res['ok']} problems={len(res['problems'])} -> {path}")
    return 0 if res["ok"] else 1
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/citibike_pipeline/aggregate_validate.py tests/test_aggregate_validate.py
git commit -m "Reconcile aggregate builds with published trips and flag stale builds"
```

### Task 7: Build on the real archive, document, and hand off to Stage 4

**Files:**
- Modify: `docs/data_dictionary.md`, `docs/runbook.md`, `docs/implementation_status.md`
- Create: `docs/validation/aggregate_validation.md` (copied from the generated report)

**Interfaces:**
- Consumes: everything above.
- Produces: current builds of all five tables and two profile snapshots on the RAID, with measured cost.

This task runs against the real data and makes no code changes. If a step fails, fix the cause with a test in the module concerned, then rerun.

- [ ] **Step 1: Build all tables, timed**

```bash
P=/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed
/usr/bin/time -l uv run citibike aggregate build 2>&1 | tee "$P/reports/aggregate_build.log"
grep -E "rows in|promoted|maximum resident|real" "$P/reports/aggregate_build.log"
du -sh "$P/aggregates"/*
```

Expected: one `<table>: N rows in Xs` line per table, then `aggregate build <run_id>: 5 table(s) promoted`, exit 0. Record N, timings, peak RSS and sizes.

- [ ] **Step 2: Validate**

```bash
uv run citibike aggregate validate
cat "$P/reports/aggregate_validation.md"
```

Expected: `ok=True`. Departures must equal the eligible-departure count of all 323,817,638 published trips, and arrivals the eligible-arrival count (both are well under the total because of missing stations and end times). If any check fails, stop and investigate before Step 3.

- [ ] **Step 3: Spot-check the tables (read-only)**

```bash
uv run python - <<'EOF'
import duckdb
from citibike_pipeline.config import load_config
from citibike_pipeline.builds import table_files
cfg = load_config()
c = duckdb.connect(); c.execute("SET threads=4; SET memory_limit='3GB'")
avail = table_files(cfg, "release_availability")
print(c.execute("SELECT availability_basis, count(*), min(release), max(release) FROM read_parquet(?) GROUP BY 1", [avail]).fetchall())
periods = table_files(cfg, "station_active_periods")
print(c.execute("SELECT id_scheme, count(*), min(first_activity_date), max(last_activity_date) FROM read_parquet(?) GROUP BY 1", [periods]).fetchall())
months = table_files(cfg, "station_month_activity")
print(c.execute("SELECT year(month_start) y, count(DISTINCT station_id), sum(departures) FROM read_parquet(?) GROUP BY 1 ORDER BY 1", [months]).fetchall())
EOF
```

Record: how many releases are dated from the listing and how many by the assumed lag; station counts per scheme; departures per year. These should match the per-year totals in `docs/validation/`.

- [ ] **Step 4: Build two profile snapshots**

```bash
uv run citibike aggregate profiles --as-of 2026-09-15 --window-days 56
uv run citibike aggregate profiles --as-of 2025-11-16 --window-days 56
```

Expected: for 2026-09-15, `latest release 2026-08` if the listing dates 2026-08 before September 15, otherwise 2026-07. Record which, since that shows the real publication lag the production model will face. 2025-11-16 is the backtest analogue of the production start date (November 16).

- [ ] **Step 5: Document**

Add a "Station demand tables (Stage 3)" section to `docs/data_dictionary.md`. Copy the grain and column tables from this plan's module map, then add:
- the departure and arrival eligibility rules
- `id_scheme` and why legacy and modern IDs are never equated
- `release_availability` bases and the 35-day assumption
- the profile leakage and bike-type rules from Task 5
- the active-period assumption (`inferred_from_trips`)
- the DST note (fall-back hours merge)

Add to `docs/runbook.md` under a new "Aggregates" heading:

    uv run citibike aggregate build [--table NAME ...]
    uv run citibike aggregate validate
    uv run citibike aggregate profiles --as-of YYYY-MM-DD [--window-days 56]

Also note that builds are all-or-nothing, that `retired/` holds superseded builds, and that `aggregate validate` reports a build as stale after any later `normalize`.

Append to `docs/implementation_status.md`: run IDs, timings, peak RSS, sizes, validation result, spot-check numbers, the two profile results, and tick "Stage 3: aggregates and as-of features".

```bash
cp "$P/reports/aggregate_validation.md" docs/validation/aggregate_validation.md
git add docs/data_dictionary.md docs/runbook.md docs/implementation_status.md docs/validation/aggregate_validation.md
git commit -m "Build and validate station demand tables on the full archive"
```

- [ ] **Step 6: Record the Stage 4 hand-off**

Add to `docs/implementation_status.md`, under a "Stage 4 inputs needed" heading:
- Join to the live feed: the class `station_information` table must map GBFS `station_id` (UUID) to `short_name` (the modern trip ID) and to `legacy_id`. Confirm those fields exist in the class table.
- Bike type: historical rentable classic/electric counts, effective-dated vehicle-type mapping, coverage audit and station crosswalk; required for exposure-normalized rates, type-specific shortage labels and mix recommendations.
- Capacity history: effective-dated `capacity` from daily `station_information`, needed before any utilization or capacity table (decision S7).
- Profiles to upload: the `demand_profiles` snapshot(s) chosen for training and production, plus `release_availability`, so cloud features use the same leakage rules.

Commit:

```bash
git add docs/implementation_status.md
git commit -m "Record Stage 4 inputs needed for station demand features"
```

---

## Self-review notes

- Spec coverage against workflow Stage 3:
  - `station_hour_activity`, `station_month_activity` and `station_location_observations`: Tasks 2 and 3.
  - As-of `historical_demand_profiles` with release-availability metadata and leakage tests: Tasks 3 and 5.
  - Zero-trip hours over active periods with stated uncertainty: decisions S4 and S5, Task 5's dense grid.
  - Aggregate totals reconcile: Task 6.
  - Unknown station identity and capacity are visible rather than invented: S2, S7, and the Task 7 Step 6 hand-off.
  - Throughput and remaining uncertainties reported: Task 7.
- Deferred by design: `station_capacity_utilization` and the cross-era station crosswalk. Both need class data and are listed as Stage 4 inputs.
