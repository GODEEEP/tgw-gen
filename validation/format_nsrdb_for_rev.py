# -*- coding: utf-8 -*-
"""
Created on Wed Sep 13 2022

@author: Cameron Bracken (cameron.bracken@pnnl.gov)
"""
# %%
import os
import sys
from datetime import timedelta
from itertools import repeat
from multiprocessing import Pool, cpu_count
from time import time

import h5py
import netCDF4 as nc
import numpy as np
import pandas as pd
import wrf
from scipy.interpolate import griddata, interpn

import pytz
from timezonefinder import TimezoneFinder
from datetime import datetime
from tqdm import tqdm

# %%
# which years to process (one year at a time)
years = list(range(1998, 2000+1))

csv_dir = 'valid_data/nsrdb_eia/'
# wrf_dir = '/rcfs/projects/godeeep/shared_data/tgw_wrf/tgw_wrf_historic/three_hourly'
output_h5_template = '../data/sam_resource/nsrdb_1h_{year}.h5'
config_fn = '../sam/configs/eia_solar_configs.csv'

# metadata with lat/lon sites, generated from meta.py
config = pd.read_csv(config_fn)
config_unique = config.loc[~config[['lat', 'lon']].duplicated()]

all_csv_files = os.listdir(csv_dir)

run_time = time()


def get_tz_offset2(latitude, longitude):
  """
  Get the UTC offset for a list of lat/lon points.

  """
  tf = TimezoneFinder()  # reuse

  # query_points = [(13.358, 52.5061), (-120,42)]
  offset = []
  for lon, lat in tqdm(zip(longitude, latitude), total=len(longitude)):
    tz = tf.timezone_at(lng=lon, lat=lat)
    timezone = pytz.timezone(tz)
    dt = datetime.utcnow()
    offset.append(timezone.utcoffset(dt).total_seconds()/60/60)

  return offset


for year in years:

  print(year)

  # output file for this year
  output_h5 = output_h5_template.format(year=year)

  # only use files for the current year
  csv_files = [x for x in all_csv_files if f'_{str(year)}_' in x]
  # PIC might not return files alphabetically
  csv_files.sort()

  # initilize hdf5 output file, will overwrite the old one
  f = h5py.File(output_h5, 'w')

  # metadata array
  meta = pd.DataFrame({'latitude': config_unique.lat,
                       'longitude': config_unique.lon,
                       'timezone': get_tz_offset2(config_unique.lat, config_unique.lon),
                       'elevation': [0] * len(config_unique.lat)})
  ll = meta[['latitude', 'longitude']].to_numpy()

  f['meta'] = meta.to_records()

  wsname = 'wind_speed'
  tcname = 'air_temperature'
  ghiname = 'ghi'
  dniname = 'dni'

  nsrdb = []
  for rowi, row in tqdm(config_unique.iterrows(), total=config_unique.shape[0]):

    # print(csv_files[fi])

    csv_filei = os.path.join(csv_dir, f'nsrdb_{year}_{row.plant_code:04}.csv')
    csv = (pd.read_csv(csv_filei, index_col='datetime', parse_dates=True)
           .rename({'Wind Speed': wsname,
                    'Temperature': tcname,
                    'GHI': ghiname,
                    'DNI': dniname}, axis='columns')
           [['wind_speed', 'air_temperature', 'ghi', 'dni']])
    # the nrel data is at the center of the hour, so interpolate to whole hours
    csv.index = csv.index + pd.Timedelta(minutes=-30)
    # tmp = csv.resample('30T').interpolate().resample('H').interpolate().bfill()
    nsrdb.append(csv)

  # 8760 except for leap years
  n_time_steps = nsrdb[0].shape[0]
  f['time_index'] = nsrdb[0].index.format()

  f[wsname] = pd.concat([x[wsname] for x in nsrdb], axis=1, ignore_index=True)
  f[tcname] = pd.concat([x[tcname] for x in nsrdb], axis=1, ignore_index=True)
  f[ghiname] = pd.concat([x[ghiname] for x in nsrdb], axis=1, ignore_index=True)
  f[dniname] = pd.concat([x[dniname] for x in nsrdb], axis=1, ignore_index=True)

  f.close()
  print('\nWrote data to ' + output_h5)

# %%
