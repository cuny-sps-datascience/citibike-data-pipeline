"""`citibike` command line entry point."""

from __future__ import annotations

import argparse
import os
import platform
import re
import shutil
import sys
from datetime import date
from pathlib import Path

import yaml

from .config import DEFAULT_CONFIG, ConfigError, load_config

_RELEASE_ARG = re.compile(r"^(\d{4})(?:-(\d{2}))?$")


def expand_releases(values: list[str]) -> list[str]:
    """Accepts YYYY (all twelve months) or YYYY-MM; returns sorted unique YYYY-MM."""
    out: list[str] = []
    for value in values:
        m = _RELEASE_ARG.match(value)
        if not m or (m[2] and not 1 <= int(m[2]) <= 12):
            raise argparse.ArgumentTypeError(f"release must be YYYY or YYYY-MM, got {value!r}")
        out.extend([value] if m[2] else [f"{m[1]}-{mm:02d}" for mm in range(1, 13)])
    return sorted(set(out))


def _doctor(cfg, args) -> int:
    import duckdb
    import pyarrow

    print(f"python {platform.python_version()} ({platform.machine()})")
    print(f"duckdb {duckdb.__version__}, pyarrow {pyarrow.__version__}")
    ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    print(f"cpus {os.cpu_count()}, ram {ram / 2**30:.1f} GiB")
    ok = True
    for label, path in (("source_root", cfg.source_root), ("processed_root", cfg.processed_root),
            *[("additional_source_root",p) for p in cfg.additional_source_roots]):
        exists = path.exists()
        print(f"{label}: {path} ({'ok' if exists else 'not present'})")
        probe = path if exists else path.parent
        if probe.exists():
            print(f"  free {shutil.disk_usage(probe).free / 2**30:.0f} GiB on {probe}")
        if label in ("source_root","additional_source_root") and not exists:
            ok = False
    print(f"duckdb memory_limit {cfg.memory_limit}, threads {cfg.threads}, temp_directory {cfg.tmp_dir}")
    return 0 if ok else 1


def _later_stage(cfg, args) -> int:
    print(f"`{args.command}` is not implemented in this phase (see docs/implementation_status.md)", file=sys.stderr)
    return 2


def _inventory(cfg, args) -> int:
    from .inventory import run_inventory

    return run_inventory(cfg, args, print)


def _normalize(cfg, args) -> int:
    from .manifest import Manifest
    from .pipeline import run_normalize

    with Manifest(cfg) as manifest:
        return run_normalize(cfg, manifest, args.releases, dry_run=args.dry_run, out=print)


def _validate(cfg, args) -> int:
    from .validate import run_validate

    return run_validate(cfg, args.releases, print)


def _bike_mix(cfg, args) -> int:
    import duckdb
    from .bike_mix import run_bike_mix

    try:
        return run_bike_mix(cfg, args.date, args.station, args.min_rides)
    except (OSError, ValueError, duckdb.Error) as exc:
        print(f"bike-mix error: {exc}", file=sys.stderr)
        return 2


def _model_export(cfg, args) -> int:
    import duckdb
    from .model_export import run_model_export, validate_package

    try:
        if args.validate:
            report = validate_package(args.validate)
            print(f"Package checksums valid: {args.validate}; {report['label_rows']:,} label rows")
        else:
            run_model_export(cfg, args.start, args.end_exclusive, args.release_lag_days, args.dry_run)
        return 0
    except (OSError, ValueError, duckdb.Error) as exc:
        print(f"model-export error: {exc}", file=sys.stderr)
        return 2


def _forecast_sources(cfg, args) -> int:
    from .forecast_sources import collect_sources
    collect_sources(cfg, args.start, args.end_exclusive, args.weather_run)
    return 0


def _hourly_export(cfg, args) -> int:
    from .hourly_export import run_hourly_export, validate_hourly_package
    if args.validate:
        report = validate_hourly_package(args.validate)
        print(f'Hourly package verified: {len(report["months"])} months; operation gate pending')
    else:
        run_hourly_export(cfg, args.start, args.end_exclusive, args.source_package)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="citibike", description="Citi Bike archive pipeline (DATA 622)")
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="path to local.yaml")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="report environment, resources, and resolved paths").set_defaults(handler=_doctor)

    inv = sub.add_parser("inventory", help="discover, profile, report, and select archive sources")
    inv.set_defaults(handler=_inventory)
    isub = inv.add_subparsers(dest="action", required=True)
    disc = isub.add_parser("discover", help="list CSV and ZIP-member candidates and their headers")
    disc.add_argument('--additional-only', action='store_true', help='scan configured raw downloads; preserve primary archive inventory')
    prof = isub.add_parser("profile", help="stream candidates for hashes, row counts, and date coverage")
    prof.add_argument("--release", action="append", help="YYYY or YYYY-MM (repeatable)")
    prof.add_argument("--all", action="store_true", help="profile every candidate")
    prof.add_argument("--force", action="store_true", help="re-profile even if already profiled")
    prof.add_argument("--workers", type=int, default=1)
    isub.add_parser("report", help="write the inventory report and CSV manifest export")
    sel = isub.add_parser("select", help="choose one representative source set per release")
    sel.add_argument("--release", action="append", required=True)
    sel.add_argument("--set", action="append", default=[], metavar="RELEASE=SET_ID",
                     help="manual choice for a release whose copies conflict")
    sel.add_argument("--dry-run", action="store_true")

    norm = sub.add_parser("normalize", help="normalize selected releases to Parquet and publish")
    norm.add_argument("--release", action="append", required=True)
    norm.add_argument("--dry-run", action="store_true")
    norm.set_defaults(handler=_normalize)

    val = sub.add_parser("validate", help="reconcile and profile published releases")
    val.add_argument("--release", action="append", required=True)
    val.set_defaults(handler=_validate)

    mix = sub.add_parser("bike-mix", help="export station bike mix in half-hour intervals for a date")
    mix.add_argument("--date", type=date.fromisoformat, required=True, help="local date YYYY-MM-DD")
    mix.add_argument("--station", help="exact start station ID; omit for all observed stations")
    mix.add_argument("--min-rides", type=int, default=20, help="known-type rides per interval for comparison")
    mix.set_defaults(handler=_bike_mix)

    export = sub.add_parser("model-export", help="export provisional observed station-day modeling tables")
    export.add_argument("--start", type=date.fromisoformat, default=date(2025, 1, 1))
    export.add_argument("--end-exclusive", type=date.fromisoformat, default=date(2026, 9, 1))
    export.add_argument("--release-lag-days", type=int, default=45,
                        help="explicit unverified month-end publication-lag assumption")
    export.add_argument("--dry-run", action="store_true")
    export.add_argument("--validate", type=Path, help="verify checksums and counts of an existing build")
    export.set_defaults(handler=_model_export)

    sources = sub.add_parser('forecast-sources', help='collect versioned public three-hour forecast inputs')
    sources.add_argument('--start', type=date.fromisoformat, default=date(2025,3,1))
    sources.add_argument('--end-exclusive', type=date.fromisoformat, default=date(2026,9,1))
    sources.add_argument('--weather-run', action='append', default=[], help='UTC initialization YYYY-MM-DDTHH:00')
    sources.set_defaults(handler=_forecast_sources)

    hourly = sub.add_parser('hourly-export', help='export observed hourly labels; unsupported zeros remain gated')
    hourly.add_argument('--start', type=date.fromisoformat, default=date(2024,8,1))
    hourly.add_argument('--end-exclusive', type=date.fromisoformat, default=date(2026,9,1))
    hourly.add_argument('--source-package', type=Path)
    hourly.add_argument('--validate', type=Path)
    hourly.set_defaults(handler=_hourly_export)

    sub.add_parser("aggregate", help="Stage 3 station aggregates (later plan)").set_defaults(handler=_later_stage)
    sub.add_parser("publish-plan", help="Stage 4 Databricks publish plan (later plan)").set_defaults(handler=_later_stage)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.releases = expand_releases(args.release) if getattr(args, "release", None) else None
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    try:
        cfg = load_config(args.config)
    except (ConfigError, OSError, KeyError, yaml.YAMLError, TypeError, AttributeError) as exc:
        # TypeError/AttributeError: the YAML parsed, but not to a mapping of the expected shape
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    return args.handler(cfg, args)


if __name__ == "__main__":
    sys.exit(main())
