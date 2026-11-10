"""Rank the 689 panel stations by reliability for the MVP cohort.
Reads labels/*.parquet (horizon 1 = one row per station-hour)."""
import pandas as pd, numpy as np, glob

df = pd.concat([pd.read_parquet(f, columns=['station_id','horizon','target_hour_utc','departures'])
                for f in sorted(glob.glob('labels/*.parquet'))])
df = df[df.horizon == 1].drop(columns='horizon').sort_values(['station_id','target_hour_utc'])
loc = df.target_hour_utc.dt.tz_convert('America/New_York')
df['month'] = loc.dt.to_period('M')
df['hour'] = loc.dt.hour

pos = df[df.departures > 0]
g = df.groupby('station_id')
out = pd.DataFrame({
    'hours': g.size(),
    'first_trip': pos.groupby('station_id').target_hour_utc.min().dt.date,
    'last_trip':  pos.groupby('station_id').target_hour_utc.max().dt.date,
    'total_dep': g.departures.sum(),
    'mean_hourly_known': g.departures.mean(),
    'positive_share': g.departures.apply(lambda s: (s > 0).mean()),
    'null_share': g.departures.apply(lambda s: s.isna().mean()),
})
# longest run of consecutive non-positive hours (nulls or zeros) per station
def max_gap(s):
    gaps = (s.target_hour_utc.diff().dt.total_seconds() / 3600 - 1)
    return gaps.max() if len(gaps) else np.nan
gp = pos.groupby('station_id').apply(max_gap)
out['max_gap_days'] = (gp / 24).round(1)
out['gaps_over_7d'] = pos.groupby('station_id').apply(
    lambda s: int(((s.target_hour_utc.diff().dt.total_seconds()/3600 - 1) > 168).sum()))
# seasonal balance: months with >=1 trip (want all 18), and winter volume share
out['active_months'] = pos.groupby('station_id').month.nunique()
winter = pos[pos.month.astype(str).isin(['2025-12','2026-01','2026-02'])].groupby('station_id').departures.sum()
out['winter_share'] = (winter / out.total_dep).fillna(0).round(3)
# overnight zeros are informative; daytime share of hours with a trip
day = df[df.hour.between(7, 21)]
out['daytime_positive_share'] = day.groupby('station_id').departures.apply(lambda s: (s > 0).mean()).round(3)

out = out.reset_index()
out['pass_basic'] = (pd.to_datetime(out.first_trip) <= '2025-03-15') & (pd.to_datetime(out.last_trip) >= '2026-08-01') & (out.gaps_over_7d == 0)
out['pass_strict'] = out.pass_basic & (out.active_months == 18) & (out.mean_hourly_known >= 5) & (out.daytime_positive_share >= 0.90) & (out.max_gap_days <= 2)
out = out.sort_values('total_dep', ascending=False)
out.round(3).to_csv('station_reliability.csv', index=False)

print(f"stations: {len(out)} | pass_basic: {out.pass_basic.sum()} | pass_strict: {out.pass_strict.sum()}")
cols = ['station_id','total_dep','mean_hourly_known','positive_share','daytime_positive_share','null_share','max_gap_days','winter_share']
print("\nTOP 25 strict-pass stations by volume:")
print(out[out.pass_strict][cols].head(25).round(3).to_string(index=False))
print("\nstrict-pass distribution of mean hourly departures:")
print(out[out.pass_strict].mean_hourly_known.describe().round(2).to_string())
