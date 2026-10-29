"""DuckDB connections with the configured memory, thread, and spill settings."""

from __future__ import annotations

import duckdb

from .config import Config


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(cfg: Config, database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    cfg.tmp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database)
    con.execute(f"SET memory_limit = {sql_literal(cfg.memory_limit)}")
    con.execute(f"SET threads = {int(cfg.threads)}")
    con.execute(f"SET temp_directory = {sql_literal(str(cfg.tmp_dir))}")
    con.execute("SET preserve_insertion_order = true")
    return con
