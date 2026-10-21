"""Resumable manifest of sources, runs, and the published-file ledger (DuckDB on the RAID)."""

from __future__ import annotations

import json
import secrets
from datetime import datetime
from pathlib import Path

from .config import Config
from .db import connect, sql_literal

SOURCE_COLUMNS = (
    "source_key", "kind", "path", "container", "member", "size", "file_size", "modified",
    "header_json", "schema_fingerprint", "schema_family", "nominal_release", "nominal_part",
    "sha256", "row_digest", "rows", "malformed_rows", "invalid_start_rows",
    "min_start_date", "max_start_date", "day_counts_json",
    "status", "status_reason", "duplicate_group",
    "discovered_run", "profiled_run", "parser_version", "profile_seconds",
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
  source_key VARCHAR PRIMARY KEY, kind VARCHAR NOT NULL, path VARCHAR NOT NULL,
  container VARCHAR, member VARCHAR, size BIGINT, file_size BIGINT, modified DOUBLE,
  header_json VARCHAR, schema_fingerprint VARCHAR, schema_family VARCHAR,
  nominal_release VARCHAR, nominal_part INTEGER,
  sha256 VARCHAR, row_digest VARCHAR, rows BIGINT, malformed_rows BIGINT, invalid_start_rows BIGINT,
  min_start_date VARCHAR, max_start_date VARCHAR, day_counts_json VARCHAR,
  status VARCHAR NOT NULL, status_reason VARCHAR, duplicate_group VARCHAR,
  discovered_run VARCHAR, profiled_run VARCHAR, parser_version VARCHAR, profile_seconds DOUBLE
);
CREATE TABLE IF NOT EXISTS runs (
  run_id VARCHAR PRIMARY KEY, command VARCHAR, args_json VARCHAR,
  started_at TIMESTAMP, finished_at TIMESTAMP, status VARCHAR
);
CREATE TABLE IF NOT EXISTS published_files (
  source_key VARCHAR, run_id VARCHAR, dataset VARCHAR, path VARCHAR, rows BIGINT, published_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS source_runs (
  run_id VARCHAR, source_key VARCHAR, release VARCHAR, source_rows BIGINT, published_rows BIGINT,
  quarantined_rows BIGINT, status VARCHAR, message VARCHAR, seconds DOUBLE, finished_at TIMESTAMP
);
-- schema_version 2: rows removed as cross-release duplicates; NULL in rows recorded before (read as 0)
ALTER TABLE source_runs ADD COLUMN IF NOT EXISTS removed_rows BIGINT;
"""


class Manifest:
    """One process at a time: DuckDB holds a write lock on the manifest file."""

    def __init__(self, cfg: Config):
        cfg.manifest_db.parent.mkdir(parents=True, exist_ok=True)
        self.con = connect(cfg, str(cfg.manifest_db))
        self.con.execute("SET memory_limit = '512MB'")  # small tables; leave memory to the normalize connection
        self.con.execute(SCHEMA)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.con.close()

    def _dicts(self, sql: str, params=()) -> list[dict]:
        cur = self.con.execute(sql, list(params))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]

    # sources
    def get(self, source_key: str) -> dict | None:
        rows = self._dicts("SELECT * FROM sources WHERE source_key = ?", [source_key])
        return rows[0] if rows else None

    def all(self) -> list[dict]:
        return self._dicts("SELECT * FROM sources ORDER BY source_key")

    def upsert(self, rec: dict) -> None:
        cols = ", ".join(SOURCE_COLUMNS)
        marks = ", ".join("?" * len(SOURCE_COLUMNS))
        self.con.execute(f"INSERT OR REPLACE INTO sources ({cols}) VALUES ({marks})",
                         [rec.get(c) for c in SOURCE_COLUMNS])

    def mark_missing(self, seen_keys: set[str]) -> int:
        gone = self.con.execute("""
            UPDATE sources SET status = 'missing', status_reason = 'not found during discovery'
            WHERE status != 'missing' AND NOT list_contains(?, source_key)
            RETURNING source_key
        """, [sorted(seen_keys)]).fetchall()
        return len(gone)

    def assign_duplicate_groups(self) -> None:
        """Groups present sources with identical bytes; a missing source is in no group."""
        self.con.execute("""
            UPDATE sources SET duplicate_group = CASE WHEN status != 'missing' AND sha256 IN (
                SELECT sha256 FROM sources WHERE sha256 IS NOT NULL AND status != 'missing'
                GROUP BY sha256 HAVING count(*) > 1
            ) THEN left(sha256, 12) END
        """)

    def export_sources_csv(self, path: Path) -> None:
        self.con.execute(f"COPY (SELECT * FROM sources ORDER BY source_key) TO {sql_literal(str(path))} (HEADER)")

    # runs
    def start_run(self, command: str, args: dict) -> str:
        run_id = f"{datetime.now():%Y%m%dT%H%M%S}-{secrets.token_hex(3)}"
        self.con.execute("INSERT INTO runs VALUES (?, ?, ?, ?, NULL, 'running')",
                         [run_id, command, json.dumps(args, default=str), datetime.now()])
        return run_id

    def finish_run(self, run_id: str, status: str) -> None:
        self.con.execute("UPDATE runs SET finished_at = ?, status = ? WHERE run_id = ?",
                         [datetime.now(), status, run_id])

    # published-file ledger: the commit point for publishing
    def replace_published(self, source_key: str, run_id: str, entries: list[tuple[str, str, int]]) -> None:
        now = datetime.now()
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.execute("DELETE FROM published_files WHERE source_key = ?", [source_key])
            for dataset, path, rows in entries:
                self.con.execute("INSERT INTO published_files VALUES (?, ?, ?, ?, ?, ?)",
                                 [source_key, run_id, dataset, path, rows, now])
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise

    def unpublish(self, source_keys: list[str]) -> None:
        self.con.execute("BEGIN TRANSACTION")
        try:
            for key in source_keys:
                self.con.execute("DELETE FROM published_files WHERE source_key = ?", [key])
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise

    def published_paths(self) -> set[str]:
        return {r[0] for r in self.con.execute("SELECT path FROM published_files").fetchall()}

    def published_for_sources(self, source_keys: list[str]) -> list[dict]:
        return self._dicts("SELECT * FROM published_files WHERE list_contains(?, source_key) ORDER BY path",
                           [list(source_keys)])

    def published_keys_for_releases(self, releases: list[str]) -> list[str]:
        rows = self.con.execute("""
            SELECT DISTINCT p.source_key FROM published_files p JOIN sources s USING (source_key)
            WHERE list_contains(?, s.nominal_release) ORDER BY 1
        """, [list(releases)]).fetchall()
        return [r[0] for r in rows]

    def published_trip_paths(self, release: str) -> list[str]:
        """Ledger paths of the trips files of one nominal release's selected sources.

        Published sources that are no longer selected are left out: they are unpublished at the end of
        the run that publishes their replacement, so no removal may rely on their copies.
        """
        rows = self.con.execute("""
            SELECT p.path FROM published_files p JOIN sources s USING (source_key)
            WHERE s.nominal_release = ? AND s.status = 'selected' AND p.dataset = 'trips' ORDER BY 1
        """, [release]).fetchall()
        return [r[0] for r in rows]

    def record_source_run(self, run_id, rec, result, status, message, seconds) -> None:
        self.con.execute("""
            INSERT INTO source_runs (run_id, source_key, release, source_rows, published_rows, quarantined_rows,
                                     removed_rows, status, message, seconds, finished_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            run_id, rec["source_key"], rec["nominal_release"], rec["rows"],
            result.staged_rows if result else None, result.quarantined_rows if result else None,
            result.removed_rows if result else None,
            status, message, seconds, datetime.now(),
        ])
