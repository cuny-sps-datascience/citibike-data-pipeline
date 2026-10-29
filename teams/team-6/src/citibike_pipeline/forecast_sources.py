"""Versioned public inputs for the three-hour project; original archives stay read-only."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(2**20), b''):
            h.update(block)
    return h.hexdigest()


def stamp() -> str:
    return datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]


def fetch(url: str, path: Path) -> dict:
    """Bounded retries, atomic file publication, and conservative retrieval provenance."""
    for attempt in range(3):
        try:
            request = Request(url, headers={'User-Agent': 'CUNY-DATA622-research/0.1'})
            with urlopen(request, timeout=45) as response:
                body = response.read()
                content_type = response.headers.get('Content-Type', '')
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(path.name + '.partial')
            temporary.write_bytes(body)
            temporary.replace(path)
            return {'url': url, 'path': str(path), 'bytes': len(body),
                    'sha256': digest(path), 'content_type': content_type,
                    'retrieved_at_utc': datetime.now(timezone.utc).isoformat()}
        except HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 2:
                raise
        except (URLError, TimeoutError):
            if attempt == 2:
                raise
        time.sleep(2 ** attempt)
    raise RuntimeError('unreachable')


def socrata_url(dataset: str, **query) -> str:
    return 'https://data.ny.gov/resource/' + dataset + '.json?' + urlencode(query)


def normalize_run(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    if parsed.minute or parsed.second or parsed.microsecond or parsed.hour not in (0,6,12,18):
        raise ValueError('Weather run must resolve to a UTC 00/06/12/18 initialization')
    return parsed


def alert_coverage(alerts: list[dict], start: date, end: date) -> dict:
    days = {row['date'][:10] for row in alerts}
    missing = []
    day = start
    while day < end:
        if str(day) not in days:
            missing.append(str(day))
        day += timedelta(days=1)
    return {'revisions':len(alerts), 'dates_without_records':missing,
        'latest_source_timestamp':max((row['date'] for row in alerts), default=None),
        'timestamp_timezone_verified':False, 'continuous_feed_coverage_verified':False,
        'interpretation':'No records does not establish normal service.'}


def collect_sources(cfg, start: date, end: date, weather_runs: list[str], out=print) -> Path:
    if start >= end:
        raise ValueError('start must precede end-exclusive')
    root = cfg.processed_root / 'modeling_exports' / 'three_hour' / 'sources' / stamp()
    root.mkdir(parents=True)
    manifest = {'schema_version': 'three-hour-sources-v2', 'start': str(start),
                'end_exclusive': str(end), 'artifacts': [], 'failures': []}

    def get(name, url, suffix='json'):
        meta = fetch(url, root / 'raw' / f'{name}.{suffix}')
        meta['name'] = name
        meta['path'] = str(Path(meta['path']).relative_to(root))
        manifest['artifacts'].append(meta)
        return (root / meta['path']).read_bytes(), meta

    def table(name, rows):
        if not rows:
            return
        path = root / 'tables' / (name + '.parquet')
        path.parent.mkdir(exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd')
        manifest['artifacts'].append({'name': name, 'path': str(path.relative_to(root)),
                                      'rows': len(rows), 'bytes': path.stat().st_size,
                                      'sha256': digest(path)})

    try:
        for name, url in (
            ('alert_metadata', 'https://data.ny.gov/api/views/7kct-peq7.json'),
            ('subway_stations', socrata_url('39hk-dx4f', **{'$limit': 1000})),
            ('boroughs', 'https://data.cityofnewyork.us/resource/gthc-hcne.geojson?$limit=10'),
            ('gbfs_discovery', 'https://gbfs.citibikenyc.com/gbfs/gbfs.json'),
        ):
            raw, _ = get(name, url)
            data = json.loads(raw)
            if name == 'subway_stations':
                table(name, [{k: v for k, v in row.items() if not k.startswith(':') and not isinstance(v, dict)}
                             for row in data])
            if name == 'gbfs_discovery':
                for feed in data['data']['en']['feeds']:
                    if feed['name'] in ('station_information', 'station_status'):
                        snapshot, meta = get('gbfs_' + feed['name'], feed['url'])
                        payload = json.loads(snapshot)
                        table('gbfs_' + feed['name'], [
                            {k: v for k, v in row.items() if not isinstance(v, (dict, list))}
                            | {'retrieved_at_utc': meta['retrieved_at_utc'],
                               'feed_version': payload.get('version'),
                               'feed_last_updated': payload.get('last_updated')}
                            for row in payload['data']['stations']])

        # Include the lookback before the first origin; timestamps remain in source representation.
        first = datetime.combine(start, datetime.min.time()) - timedelta(hours=3)
        where = ("agency = 'NYCT Subway' AND date >= '" + first.isoformat()
                 + "' AND date < '" + end.isoformat() + "T00:00:00'")
        raw, _ = get('alert_expected_count', socrata_url('7kct-peq7', **{
            '$where': where, '$select': 'count(*) as rows'}))
        expected = int(json.loads(raw)[0]['rows'])
        alerts, offset = [], 0
        while offset < expected:
            raw, _ = get(f'alerts_{offset:08}', socrata_url('7kct-peq7', **{
                '$where': where, '$order': 'date,alert_id', '$limit': 10000, '$offset': offset}))
            page = json.loads(raw)
            if not page:
                raise ValueError('Alert archive changed during paging or returned an empty page')
            alerts.extend(page)
            offset += len(page)
        if len(alerts) != expected or len({r['alert_id'] for r in alerts}) != expected:
            raise ValueError('Alert pagination count/key reconciliation failed')
        table('subway_alert_archive', alerts)
        manifest['alert_coverage'] = alert_coverage(alerts,start,end)

        from google.transit import gtfs_realtime_pb2
        raw, meta = get('live_alerts', 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fall-alerts', 'pb')
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw)
        live = []
        for entity in feed.entity:
            if not entity.HasField('alert'):
                continue
            for selector in entity.alert.informed_entity:
                live.append({'entity_id': entity.id, 'agency_id': selector.agency_id,
                    'route_id': selector.route_id or None, 'stop_id': selector.stop_id or None,
                    'feed_timestamp': feed.header.timestamp, 'first_seen_utc': meta['retrieved_at_utc'],
                    'active_period_json': json.dumps([{'start': p.start if p.HasField('start') else None,
                        'end': p.end if p.HasField('end') else None} for p in entity.alert.active_period]),
                    'publication_time_verified': False})
        table('subway_alert_live_snapshot', live)

        weather = []
        for dt in sorted({normalize_run(run) for run in weather_runs}):
            query = {'latitude': 40.75, 'longitude': -73.98, 'models': 'ecmwf_ifs',
                'run': dt.replace(tzinfo=None).isoformat(timespec='minutes'), 'forecast_days': 2, 'timezone': 'UTC',
                'wind_speed_unit': 'ms', 'hourly': 'temperature_2m,precipitation,rain,snowfall,wind_speed_10m'}
            raw, meta = get('weather_' + dt.strftime('%Y%m%dT%H'),
                'https://single-runs-api.open-meteo.com/v1/forecast?' + urlencode(query))
            data = json.loads(raw)
            for index, valid in enumerate(data['hourly']['time']):
                weather.append({'requested_latitude': 40.75, 'requested_longitude': -73.98,
                    'cell_latitude': data['latitude'], 'cell_longitude': data['longitude'],
                    'model': 'ecmwf_ifs', 'initialization_utc': dt.replace(tzinfo=timezone.utc),
                    'valid_time_utc': datetime.fromisoformat(valid).replace(tzinfo=timezone.utc),
                    'accumulation_interval_start_utc':datetime.fromisoformat(valid).replace(tzinfo=timezone.utc)-timedelta(hours=1),
                    'accumulation_interval_end_utc':datetime.fromisoformat(valid).replace(tzinfo=timezone.utc),
                    'assumed_available_at_utc': (dt + timedelta(hours=7)).replace(tzinfo=timezone.utc),
                    'availability_verified': False, 'early_archive_hindcast_caveat': True,
                    'retrieved_at_utc': meta['retrieved_at_utc'],
                    'units_json': json.dumps(data['hourly_units'], sort_keys=True),
                    **{k: v[index] for k, v in data['hourly'].items() if k != 'time'}})
        table('nyc_weather_forecast_run', weather)
        manifest['status'] = 'collected_with_documented_coverage_gaps'
    except Exception as exc:
        manifest['status'] = 'failed'
        manifest['failures'].append({'type': type(exc).__name__, 'message': str(exc)})
        raise
    finally:
        (root / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        out(f'Source package: {root}; status={manifest.get("status")}')
    return root
