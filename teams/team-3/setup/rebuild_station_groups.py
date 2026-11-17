"""Rebuild the station-to-group file that make_job_notebook.py packages into the pilot notebook.

Usage: python3 rebuild_station_groups.py ACS_STATION_FEATURES.parquet OUTPUT.csv

The file splits the 100 cohort stations into lower, middle and higher thirds of poverty share and of the share
of households without a vehicle (500-meter ACS estimates), with the same rule as step 03 cell 8. A station with a
missing value is labeled "missing". The output is only needed to rebuild the pilot notebook; it is not stored in
this repository. Run it with the ACS table from the `Citi Bike - ACS and July Trip Development - e1eb6e26` folder.
"""
import sys

import pandas as pd

## Read the ACS station table
acs = pd.read_parquet(sys.argv[1])
acs['station_id'] = acs.station_id.astype(str)
# Confirm it covers the 100-station cohort
assert len(acs) == 100 and not acs.station_id.duplicated().any(), 'expected 100 distinct stations'


def thirds(col):
    """Lower, middle and higher thirds of one ACS measure (ties broken by row order, as in step 03)."""
    g = acs[['station_id', col]].copy()
    g[col] = pd.to_numeric(g[col], errors='coerce')
    g['group'] = 'missing'
    known = g[col].notna()
    g.loc[known, 'group'] = pd.qcut(g.loc[known, col].rank(method='first'), 3, labels=['lower', 'middle', 'higher']).astype(str)
    return g.set_index('station_id').group


## Build the two groupings and write them in station ID order
out = pd.DataFrame({'poverty_share': thirds('poverty_pct_500m'), 'no_vehicle_share': thirds('households_no_vehicle_pct_500m')})
out.index.name = 'station_id'
out.sort_index().to_csv(sys.argv[2], lineterminator='\n')
# Display the group sizes
print('WROTE', sys.argv[2], out.poverty_share.value_counts().to_dict(), out.no_vehicle_share.value_counts().to_dict())
