"""Resumable public station and single-run weather acquisition; no class data needed.

Run from the project root. Bulk data stays on the processed RAID, outside Git.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import threading
import time
from urllib.parse import urlencode

from citibike_pipeline.forecast_sources import digest, fetch

ROOT = Path('/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour/source_candidates')


def cached(url, path, expected_bytes=None):
    sidecar = path.with_suffix(path.suffix + '.json')
    if path.exists() and sidecar.exists():
        meta = json.loads(sidecar.read_text())
        if meta['url'] == url and digest(path) == meta['sha256']:
            if expected_bytes is None or path.stat().st_size == expected_bytes:
                return meta
    meta = fetch(url, path)
    if expected_bytes is not None and meta['bytes'] != expected_bytes:
        raise ValueError(f'Listing size mismatch: {path.name}')
    sidecar.write_text(json.dumps(meta, indent=2) + '\n')
    return meta


def citybikes(root, first='202411', last='202608'):
    target = root / 'citybikes'
    artifacts = []
    for year in range(int(first[:4]), int(last[:4]) + 1):
        url = f'https://data.citybik.es/dumps/by-network/{year}/'
        listing = target / f'listing_{year}.json'
        # Listings are dated evidence of what was available when acquired.
        fetch(url, listing)
        for item in json.loads(listing.read_text()):
            name = item['name']
            if not name.endswith('-citi-bike-nyc-stats.parquet') or not first <= name[:6] <= last:
                continue
            meta = cached(url + name, target / name, item['size'])
            meta['listing_mtime'] = item['mtime']
            artifacts.append(meta)
            print('CityBikes', name, meta['bytes'], flush=True)
    manifest = {'schema_version': 'citybikes-public-history-v1', 'artifacts': artifacts,
                'license': 'ODbL-1.0; CityBikes attribution; underlying operator terms also apply',
                'source': 'https://data.citybik.es/', 'change_only': True,
                'continuous_coverage_verified': False}
    (target / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest


def weather(root, first='2025-02-28', last='2026-09-01'):
    start,stop=datetime.fromisoformat(first),datetime.fromisoformat(last)
    if start>=stop or any(d.time()!=datetime.min.time() or d.tzinfo is not None for d in (start,stop)):
        raise ValueError('Weather dates must be increasing naive midnight boundaries')
    target = root / 'weather'
    target.mkdir(parents=True, exist_ok=True)
    # Preserve immutable acquisition evidence when the requested window grows.
    current = target / 'manifest.json'
    history = target / 'manifests'
    history.mkdir(exist_ok=True)
    if current.exists():
        previous = history / (digest(current) + '.json')
        if not previous.exists():
            previous.write_bytes(current.read_bytes())
    # Four workers, globally <= four starts/second, below the public 600/min limit.
    lock = threading.Lock()
    next_call = [0.0]

    def get(run):
        query = {'latitude': 40.75, 'longitude': -73.98, 'models': 'ecmwf_ifs',
                 'run': run.isoformat(timespec='minutes'), 'forecast_days': 2,
                 'timezone': 'UTC', 'wind_speed_unit': 'ms',
                 'hourly': 'temperature_2m,precipitation,rain,snowfall,wind_speed_10m'}
        path = target / (run.strftime('%Y%m%dT%H') + '.json')
        with lock:
            pause = max(0, next_call[0] - time.monotonic())
            if pause:
                time.sleep(pause)
            next_call[0] = time.monotonic() + .25
        meta = cached('https://single-runs-api.open-meteo.com/v1/forecast?' + urlencode(query), path)
        data = json.loads(path.read_text())
        if 'hourly' not in data or len(data['hourly']['time']) < 24:
            raise ValueError(f'Incomplete run: {run}')
        return meta | {'initialization_utc': run.isoformat() + '+00:00'}

    runs = []
    run = start
    while run < stop:
        runs.append(run)
        run += timedelta(hours=6)
    artifacts, failures = [], []
    with ThreadPoolExecutor(max_workers=4) as pool:
        pending = {pool.submit(get, run): run for run in runs}
        for future in as_completed(pending):
            try:
                artifacts.append(future.result())
            except Exception as exc:
                failures.append({'run': str(pending[future]), 'error': str(exc)})
            if (len(artifacts) + len(failures)) % 100 == 0:
                print('Weather', len(artifacts), '/', len(runs), 'failures', len(failures), flush=True)
    manifest = {'schema_version': 'single-run-history-v1',
                'artifacts': sorted(artifacts, key=lambda a: a['initialization_utc']),
                'failures': failures, 'expected_runs': len(runs),
                'requested_point': [40.75, -73.98], 'scope': 'common Manhattan reference point proxy',
                'assumed_delay_hours': 7, 'historical_availability_verified': False,
                'early_archive_hindcast_caveat': True,
                'attribution': 'Weather data by Open-Meteo.com; ECMWF IFS; CC BY 4.0'}
    content = json.dumps(manifest, indent=2) + '\n'
    temporary = target / 'manifest.pending.json'
    temporary.write_text(content)
    archived = history / (digest(temporary) + '.json')
    if not archived.exists():
        archived.write_text(content)
    temporary.replace(current)
    print('Weather finished', len(artifacts), 'failures', len(failures), flush=True)
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', choices=['citybikes', 'weather'])
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--first', help='Weather start date, inclusive (YYYY-MM-DD)')
    parser.add_argument('--last-exclusive', help='Weather end date, exclusive (YYYY-MM-DD)')
    args = parser.parse_args()
    if args.source == 'weather':
        weather(args.root, args.first or '2025-02-28', args.last_exclusive or '2026-09-01')
    else:
        if args.first or args.last_exclusive:
            parser.error('Date overrides apply only to weather')
        citybikes(args.root)
