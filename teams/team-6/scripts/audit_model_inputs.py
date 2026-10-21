"""Read-only audit of selected published trips for modeling-export decisions."""

from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import time
from zoneinfo import ZoneInfo

import duckdb

from citibike_pipeline.config import load_config
from citibike_pipeline.db import sql_literal


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2025, 1, 1))
    parser.add_argument("--end-exclusive", type=date.fromisoformat, default=date(2026, 9, 1))
    parser.add_argument("--output", type=Path,
                        default=Path("docs/validation/model_input_audit_2026-10-11.json"))
    args = parser.parse_args()
    if args.start >= args.end_exclusive:
        parser.error("start must precede end-exclusive")
    cfg = load_config()
    with duckdb.connect(str(cfg.manifest_db), read_only=True) as ledger:
        totals = ledger.execute("SELECT dataset, count(*), sum(rows) FROM published_files GROUP BY 1 ORDER BY 1").fetchall()
        paths = [row[0] for row in ledger.execute("""
            SELECT DISTINCT p.path FROM published_files p JOIN sources s USING (source_key)
            WHERE s.status = 'selected' AND p.dataset = 'trips' ORDER BY 1
        """).fetchall()]
    # Start-date partitions bound the scan; the SQL date condition sets the exact cohort.
    files = [cfg.processed_root / p for p in paths
             if any(f"/trip_year={year}/" in p
                    for year in range(args.start.year, args.end_exclusive.year + 1))]
    if not files:
        raise RuntimeError("No selected published files in the requested years")
    missing = [str(p) for p in files if not p.is_file()]
    if missing:
        raise FileNotFoundError(f"Published inputs are missing: {missing[:5]}")
    literals = ",".join(sql_literal(str(p)) for p in files)
    began = time.monotonic()
    with duckdb.connect() as con:
        con.execute("SET memory_limit='3GB'")
        con.execute("SET threads=4")
        con.execute(f"CREATE VIEW trips AS SELECT * FROM read_parquet([{literals}], hive_partitioning=false)")
        cur = con.execute("""
            SELECT date_trunc('month', started_at_local)::DATE AS month_start,
                count(*) AS trip_rows,
                count(*) FILTER (WHERE eligible_departure) AS eligible_departures,
                count(*) FILTER (WHERE eligible_departure AND duration_seconds IS NULL) AS missing_duration,
                count(*) FILTER (WHERE eligible_departure AND duration_seconds <= 0) AS nonpositive_duration,
                count(*) FILTER (WHERE eligible_departure AND duration_seconds > 0 AND duration_seconds < 30) AS positive_under_30,
                count(*) FILTER (WHERE eligible_departure AND duration_seconds > 0 AND duration_seconds < 60) AS positive_under_60,
                count(*) FILTER (WHERE eligible_departure AND duration_seconds > 0 AND duration_seconds < 120) AS positive_under_120,
                count(*) FILTER (WHERE eligible_departure AND list_contains(quality_flags, 'crosses_dst')) AS crosses_dst,
                count(*) FILTER (WHERE eligible_departure AND duration_seconds > 0 AND duration_seconds < 60 AND
                    NOT list_has_any(quality_flags, ['crosses_dst', 'start_dst_ambiguous', 'end_dst_ambiguous',
                                                    'start_dst_nonexistent', 'end_dst_nonexistent'])) AS positive_under_60_without_dst_flags,
                count(*) FILTER (WHERE source_duration_seconds IS NOT NULL) AS source_duration_present
            FROM trips WHERE started_at_local >= ? AND started_at_local < ?
            GROUP BY 1 ORDER BY 1
        """, [args.start, args.end_exclusive])
        columns = [d[0] for d in cur.description]
        months = [dict(zip(columns, row)) for row in cur.fetchall()]
    for record in months:
        record["month_start"] = record["month_start"].isoformat()
    report = {
        "audit_date": datetime.now(ZoneInfo(cfg.timezone)).date().isoformat(),
        "period_start": args.start.isoformat(),
        "period_end_exclusive": args.end_exclusive.isoformat(),
        "processed_root": str(cfg.processed_root),
        "ledger_totals": [{"dataset": d, "files": n, "rows": r} for d, n, r in totals],
        "input_files": len(files),
        "seconds": round(time.monotonic() - began, 2),
        "duration_basis": "source wall-clock timestamp difference; no threshold applied",
        "months": months,
        "totals": {key: sum(r[key] for r in months) for key in columns if key != "month_start"},
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "months"}, indent=2))
    print(f"Audit written to {args.output}")


if __name__ == "__main__":
    main()
