"""Build the public-only station audit and reduced-scope borough benchmark."""
import argparse
from pathlib import Path

from citibike_pipeline.borough_mvp import build_borough_features, build_borough_labels
from citibike_pipeline.public_exposure import build_public_exposure


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['exposure','labels','features'])
    p.add_argument('--root',type=Path,default=Path('/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour'))
    p.add_argument('--observed',type=Path)
    p.add_argument('--sources',type=Path)
    args=p.parse_args();b=args.root
    if args.action=='exposure':
        observed=args.observed or Path((b/'CURRENT_OBSERVED').read_text().strip())
        if not observed.is_absolute():observed=b/'builds'/observed
        print(build_public_exposure(b/'source_candidates/citybikes',observed,b/'public_exposure'))
    elif args.action=='labels':
        observed=args.observed or Path((b/'CURRENT_OBSERVED').read_text().strip())
        if not observed.is_absolute():observed=b/'builds'/observed
        if not args.sources:p.error('labels requires --sources with a validated borough boundary')
        print(build_borough_labels(observed,args.sources,b/'borough_labels'))
    else:
        labels=Path((b/'borough_labels/CURRENT_BOROUGH_LABELS').read_text().strip())
        print(build_borough_features(labels,b/'source_candidates/weather',b/'borough_features'))
