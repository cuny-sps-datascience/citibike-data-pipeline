#!/usr/bin/env python3
"""Generate a tiny review sample and data-driven map storyboard, never a model."""

import argparse
import csv
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import re

import duckdb

from citibike_pipeline.config import load_config
from citibike_pipeline.departure_preview import NY, active_days, assumed_label, released_profile, weather_rows
from citibike_pipeline.forecast_sources import digest
from citibike_pipeline.historical_station_panel import checked_path, load_transfer, parquet_view, reference_input


def serial(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


def records(cursor):
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, r)) for r in cursor.fetchall()]


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows({k: serial(v) for k,v in row.items()} for row in rows)


def markdown_table(rows, fields):
    text = ["| " + " | ".join(fields) + " |", "|" + "---|"*len(fields)]
    for row in rows:
        values = [str(round(row[k],4) if isinstance(row.get(k),float) else serial(row.get(k)))
                  if row.get(k) is not None else "unknown" for k in fields]
        text.append("| " + " | ".join(v.replace("|", ", ") for v in values) + " |")
    return "\n".join(text)


def render_map(output, stations, frame_rows, boundary, origin, selected):
    """Standard drawing primitives render actual coordinates; no map tile service."""
    from PIL import Image, ImageDraw, ImageFont
    def font(size):
        for path in ["/System/Library/Fonts/Supplemental/Arial.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]:
            if Path(path).exists():
                return ImageFont.truetype(path, size)
        return ImageFont.load_default()
    polygons = boundary["coordinates"] if boundary["type"] == "MultiPolygon" else [boundary["coordinates"]]
    coords = [point for polygon in polygons for ring in polygon for point in ring]
    minx, maxx = min(p[0] for p in coords), max(p[0] for p in coords)
    miny, maxy = min(p[1] for p in coords), max(p[1] for p in coords)
    sx, sy = 630/(maxx-minx), 640/(maxy-miny)
    # WGS84 longitude is shortened by cos(latitude), preserving local aspect.
    scale = min(sx/math.cos(math.radians(40.78)), sy)
    def project(lon, lat):
        return 65+(lon-minx)*scale*math.cos(math.radians(40.78)), 185+(maxy-lat)*scale
    station_map = {s["station_id"]: s for s in stations}
    frames = []
    for horizon in [1,2,3]:
        rows = [r for r in frame_rows if r["horizon"] == horizon]
        image = Image.new("RGB", (760,1000), "#f7fafc")
        draw = ImageDraw.Draw(image)
        draw.text((38,26), "MANHATTAN / DEPARTURES", fill="#122b40", font=font(29))
        start = (origin+timedelta(hours=horizon-1)).astimezone(NY)
        end = start+timedelta(hours=1)
        draw.text((38,72), f"H{horizon}  {start:%b %d, %Y · %H:%M}–{end:%H:%M} New York", fill="#17374f", font=font(22))
        draw.text((38,111), "Retrospective label preview · not predictions or incident impacts", fill="#526474", font=font(16))
        draw.rectangle((35,163,725,844), fill="#e6f0f4")
        for polygon in polygons:
            for i, ring in enumerate(polygon):
                points = [project(lon,lat) for lon,lat,*_ in ring]
                draw.polygon(points, fill="#f2f0e3" if i == 0 else "#e6f0f4", outline="#c7cabb")
        for row in sorted(rows, key=lambda r:r["departures"] or 0):
            station = station_map[row["station_id"]]
            x,y = project(station["station_longitude"],station["station_latitude"])
            value = row["departures"]
            radius = 2 if value is None else 1.8+min(math.sqrt(value),6)
            if row["recent_event_count"] is not None and row["recent_event_count"]>0:
                draw.ellipse((x-radius-2,y-radius-2,x+radius+2,y+radius+2),outline="#d88818",width=2)
            fill = "#ffffff" if value is None else "#b6c7d1" if value == 0 else "#236287"
            draw.ellipse((x-radius,y-radius,x+radius,y+radius),fill=fill,outline="#657f90",width=1)
            if row["station_id"] == selected:
                draw.ellipse((x-12,y-12,x+12,y+12),outline="#a93545",width=3)
        known = sum(r["departures"] is not None for r in rows)
        zeros = sum(r["label_basis"]=="assumed_24h_zero" for r in rows)
        draw.text((38,859), f"{known}/{len(rows)} known counts · {zeros} zeros under inferred 24h operation", fill="#17374f",font=font(17))
        draw.text((38,889), "Dot size: recorded departures   Gray: zero   Hollow: unknown", fill="#526474",font=font(16))
        draw.text((38,917), "Amber ring: recent linked publication signal, frozen at prior origin",fill="#8c5a18",font=font(16))
        draw.text((38,945), "Red ring: W 31 St & 7 Ave sample · static October geography",fill="#526474",font=font(16))
        draw.text((38,972), "No inferred causal affected area",fill="#526474",font=font(15))
        frames.append(image)
    frames[0].save(output/"map_preview.gif",save_all=True,append_images=frames[1:],duration=1500,loop=0,disposal=2)
    storyboard = Image.new("RGB", (2280,1000), "white")
    for i,image in enumerate(frames):
        storyboard.paste(image,(i*760,0))
    storyboard.save(output/"map_storyboard.png",optimize=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--station", default="6331.01")
    parser.add_argument("--origin", default="2026-06-15T10:00:00-04:00")
    parser.add_argument("--output", type=Path, default=Path("docs/previews/departure_features"))
    args = parser.parse_args()
    origin = datetime.fromisoformat(args.origin)
    if origin.tzinfo is None:
        parser.error("--origin requires an offset")
    origin = origin.astimezone(timezone.utc)
    first = datetime(2025,3,1,tzinfo=NY).astimezone(timezone.utc)
    end = datetime(2026,8,11,tzinfo=NY).astimezone(timezone.utc)
    if not first <= origin < origin+timedelta(hours=3) <= end:
        parser.error("Preview must stay in the agreed historical study window")
    cutoff = origin-timedelta(minutes=5)
    output = args.output; output.mkdir(parents=True,exist_ok=True)
    cfg = load_config(); base = cfg.processed_root/"modeling_exports/three_hour"
    observed_root = base/"builds/20261016T172712Z-eb309064"
    panel_root = base/"historical_station_panels/20261027T015443Z-80ae1edf"
    transfer_root = base/"weather_handoffs/20261023T174914038037Z/extracted"
    reference_root = base/"sources/20261015T234334Z-d29974e2"
    manifest = json.loads((observed_root/"manifest.json").read_text())
    panel_manifest = json.loads((panel_root/"manifest.json").read_text())
    con = duckdb.connect(); con.execute("SET TimeZone='UTC'; SET memory_limit='3GB'; SET threads=2")
    try:
        transfer_audit = load_transfer(con,transfer_root)
        for table,view in [("hourly_observed","observations"),("hourly_release","released_counts")]:
            entries = [a for a in manifest["artifacts"] if a["table"]==table]
            paths = []
            for a in entries:
                p=checked_path(observed_root,a["path"])
                if digest(p)!=a["sha256"]:raise ValueError("Observed aggregate checksum mismatch")
                paths.append(p)
            parquet_view(con,view,paths)
        a=next(a for a in panel_manifest['artifacts'] if a['table']=='station_crosswalk')
        crosswalk=checked_path(panel_root,a['path'])
        if digest(crosswalk)!=a['sha256']:raise ValueError('Crosswalk checksum mismatch')
        parquet_view(con,'cohort',[crosswalk])
        stations=records(con.execute('SELECT * FROM cohort ORDER BY station_id'))
        station=next((s for s in stations if s['station_id']==args.station),None)
        if station is None:raise ValueError('Station is not in the pinned cohort')
        inventory_path=cfg.reports_dir/'inventory_report.md'
        inventory=inventory_path.read_text()
        complete=set(re.findall(r'^\| (\d{4}-\d{2}) \|.*\| confirmed \|',inventory,re.M))
        if not complete:raise ValueError('No confirmed trip-file months in pinned inventory audit')
        month=origin.astimezone(NY).strftime('%Y-%m')
        counts=records(con.execute("SELECT station_id,hour_utc,departures FROM observations WHERE id_scheme='short_name' AND hour_utc>=? AND hour_utc<?",[origin,origin+timedelta(hours=3)]))
        lookup={(r['station_id'],r['hour_utc']):r['departures'] for r in counts}
        spans=records(con.execute("""SELECT station_id,min(hour_utc) first_hour,max(hour_utc) last_hour
            FROM observations WHERE id_scheme='short_name' AND hour_utc>=? AND hour_utc<? GROUP BY station_id""",[first,end]))
        spans={r['station_id']:active_days([r['first_hour'],r['last_hour']]) for r in spans}
        history=records(con.execute("SELECT hour_utc,nominal_release,departures FROM released_counts WHERE station_id=? AND id_scheme='short_name'",[args.station]))
        weather=weather_rows(con,station['weather_request_class'],station['dataset_build_id'],origin,cutoff)
        subway=records(con.execute("SELECT * FROM subway_alert_features WHERE station_id=? AND forecast_origin_utc=?",[station['station_uuid'],origin-timedelta(hours=1)]))
        subway=subway[0] if subway else None
        source_origin=origin-timedelta(hours=1) if subway else None
        labels=[];features=[];alignment=[]
        start_active,end_active=spans.get(args.station,(None,None))
        for w in weather:
            target=w['target_start_utc'];target_end=w['target_end_utc']
            value,basis=assumed_label(lookup.get((args.station,target)),target,start_active,end_active,target.astimezone(NY).strftime('%Y-%m') in complete)
            labels.append({'station_id':args.station,'origin_utc':origin,'horizon':w['horizon'],
                'target_start_utc':target,'target_end_utc':target_end,'target_start_new_york':target.astimezone(NY),
                'departures':value,'label_basis':basis,'operation_assumed_24h':True,'operation_verified':False,
                'inventory_required':False,'trip_files_complete':target.astimezone(NY).strftime('%Y-%m') in complete})
            profile=released_profile(history,cutoff,target,complete)
            features.append({'station_id':args.station,'station_uuid':station['station_uuid'],'origin_utc':origin,
                'issue_cutoff_utc':cutoff,'horizon':w['horizon'],'target_start_utc':target,'target_end_utc':target_end,
                'forecast_temperature_f':w['forecast_temperature_f'],'forecast_wind_speed_mph':w['forecast_wind_speed_mph'],
                'forecast_precipitation_in':w['forecast_precipitation_in'],'forecast_rain_in':w['forecast_rain_in'],
                'forecast_snowfall_in':w['forecast_snowfall_in'],'instant_valid_time_utc':w['instant_valid_time_utc'],
                'accumulation_start_utc':w['accumulation_start_utc'],'accumulation_valid_time_utc':w['accumulation_valid_time_utc'],
                'weather_source_origin_utc':w['weather_source_origin_utc'],'instant_run_utc':w['instant_run_utc'],
                'accumulation_run_utc':w['accumulation_run_utc'],'instant_available_utc':w['instant_available_utc'],
                'accumulation_available_utc':w['accumulation_available_utc'],
                'subway_source_origin_utc':source_origin,'recent_event_count':subway['recent_event_count'] if subway else None,
                'latest_publication_time_utc':subway['latest_publication_time_utc'].replace(tzinfo=timezone.utc) if subway and subway['latest_publication_time_utc'] else None,
                'subway_strategy':'legacy_prior_origin_for_comparison; current-origin event rebuild planned',
                **profile})
            alignment.append({'horizon':w['horizon'],'departure_start_new_york':target.astimezone(NY),
                'departure_end_new_york':target_end.astimezone(NY),'temperature_stamp_new_york':w['instant_valid_time_utc'].astimezone(NY),
                'precipitation_stamp_new_york':w['accumulation_valid_time_utc'].astimezone(NY),
                'previous_package_precipitation_in':w['legacy_start_stamped_precipitation_in'],
                'interval_correct_precipitation_in':w['forecast_precipitation_in'],
                'subway_snapshot_new_york':source_origin.astimezone(NY) if source_origin else None,
                'issue_cutoff_new_york':cutoff.astimezone(NY)})
        station_preview={**station,'inferred_active_start_utc':start_active,'inferred_active_end_exclusive_utc':end_active,
                         'retrospective_geography':True,'inferred_operation_not_verified':True}
        write_csv(output/'features.csv',features);write_csv(output/'labels.csv',labels)
        write_csv(output/'station_crosswalk.csv',[station_preview]);write_csv(output/'timing_alignment.csv',alignment)
        # Context map: three hours only, not a bulk dataset or fitted forecast.
        signals=records(con.execute('SELECT station_id,recent_event_count FROM subway_alert_features WHERE forecast_origin_utc=?',[origin-timedelta(hours=1)]))
        signals={r['station_id']:r['recent_event_count'] for r in signals}
        frames=[]
        for s in stations:
            active_start,active_end=spans.get(s['station_id'],(None,None))
            for h in [1,2,3]:
                target=origin+timedelta(hours=h-1)
                value,basis=assumed_label(lookup.get((s['station_id'],target)),target,active_start,active_end,target.astimezone(NY).strftime('%Y-%m') in complete)
                frames.append({'station_id':s['station_id'],'horizon':h,'departures':value,'label_basis':basis,'recent_event_count':signals.get(s['station_uuid'])})
        examples=[]
        for basis in ['assumed_24h_zero','outside_inferred_active_period']:
            sample=next((r for r in frames if r['label_basis']==basis),None)
            if sample:
                target=origin+timedelta(hours=sample['horizon']-1)
                examples.append({'station_id':sample['station_id'],'origin_utc':origin,'horizon':sample['horizon'],
                    'target_start_new_york':target.astimezone(NY),'departures':sample['departures'],
                    'label_basis':sample['label_basis'],'inventory_required':False})
        if examples:write_csv(output/'zero_policy_examples.csv',examples)
        boundary_path,boundary_receipt=reference_input(reference_root,'boroughs')
        boundary=next(f['geometry'] for f in json.loads(boundary_path.read_text())['features'] if f['properties']['boroname']=='Manhattan')
        render_map(output,stations,frames,boundary,origin,args.station)
        summary='# Three-horizon departure feature preview\n\n'
        summary+='Preview of proposed revisions, not a replacement for the published panel or a trained model. '+station['station_name']+' (`'+args.station+'`), origin '+origin.astimezone(NY).isoformat()+'. Blank CSV cells mean unknown.\n\n'
        summary+='## Labels\n\n'+markdown_table(labels,['horizon','target_start_new_york','departures','label_basis','inventory_required'])+'\n\n'
        summary+='## Weather and subway timing\n\n'+markdown_table(alignment,['horizon','departure_start_new_york','departure_end_new_york','temperature_stamp_new_york','precipitation_stamp_new_york','subway_snapshot_new_york','issue_cutoff_new_york'])+'\n\n'
        summary+='## Feature values\n\n'+markdown_table(features,['horizon','forecast_temperature_f','forecast_precipitation_in','forecast_wind_speed_mph','recent_event_count'])+'\n\n'
        summary+='## Released demand profiles\n\n'+markdown_table(features,['horizon','released_profile_mean','profile_departures','profile_hours','profile_included_releases','profile_latest_input_available_utc'])+'\n\n'
        summary+='Full column names and three actual rows are in [features.csv](features.csv), [labels.csv](labels.csv), [timing_alignment.csv](timing_alignment.csv), and [station_crosswalk.csv](station_crosswalk.csv).\n\n'
        if examples:summary+='## Zero-policy edge examples\n\n'+markdown_table(examples,list(examples[0]))+'\n\n[CSV](zero_policy_examples.csv). These two examples use actual missing source counts; only the first is inside its inferred active days.\n\n'
        summary+='## Map preview\n\n![Three-hour map storyboard](map_storyboard.png)\n\n[Animated three-frame demo](map_preview.gif). This animates recorded departure labels, not model forecasts or measured incident impacts. Amber rings are prior-origin publication signals and stay fixed across horizons.\n'
        (output/'README.md').write_text(summary)
        schema={name:list(rows[0]) for name,rows in [('features',features),('labels',labels),('station_crosswalk',[station_preview]),('timing_alignment',alignment)]}
        files=['features.csv','labels.csv','station_crosswalk.csv','timing_alignment.csv','map_storyboard.png','map_preview.gif','README.md']
        if examples:files.append('zero_policy_examples.csv')
        receipt={'schema_version':'departure-feature-preview-v1','review_date_local':'2026-10-27',
            'station_id':args.station,'origin_utc':origin.isoformat(),'issue_cutoff_utc':cutoff.isoformat(),
            'feature_rows':3,'label_rows':3,'crosswalk_rows':1,'map_stations':len(stations),'map_frames':3,
            'preview_only':True,'published_packages_modified':False,'models_trained':False,'schemas':schema,
            'inputs':{'observed_manifest_sha256':digest(observed_root/'manifest.json'),
                'panel_manifest_sha256':digest(panel_root/'manifest.json'),'crosswalk_sha256':digest(crosswalk),
                'transfer_manifest_sha256':digest(transfer_root/'transfer_manifest.json'),'transfer_audit':transfer_audit,
                'inventory_report_sha256':digest(inventory_path),'label_month_complete':month in complete,
                'boundary_sha256':boundary_receipt['sha256']},
            'policies':{'labels':'24h from first to last observed study day; complete files required; explicitly assumed',
                'profiles':'56-day hour-of-week mean; release-filtered numerator and inferred exposure; assumed month-end +45 days',
                'weather':'instant at target start; preceding-hour sums at target end',
                'subway':'show legacy prior-origin aggregate honestly; rebuild current-origin events before changing production inputs'},
            'artifacts':[{'path':name,'bytes':(output/name).stat().st_size,'sha256':digest(output/name)} for name in files]}
        (output/'manifest.json').write_text(json.dumps(receipt,indent=2)+'\n')
        print(f'Preview written to {output}: three horizons for {args.station}, {len(stations)} map points.')
    finally:
        con.close()


if __name__=='__main__':main()
