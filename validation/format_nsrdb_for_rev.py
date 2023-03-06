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

# %%
# which years to process (one year at a time)
years = list(range(2007, 2020+1))

csv_dir = 'valid_data/nsrdb'
# wrf_dir = '/rcfs/projects/godeeep/shared_data/tgw_wrf/tgw_wrf_historic/three_hourly'
output_h5_template = '../data/sam_resource/nsrdb_1h_{year}.h5'
meta_fn = '../data/meta_solar.csv'

# metadata with lat/lon sites, generated from meta.py
meta = pd.read_csv(meta_fn)

all_csv_files = os.listdir(csv_dir)

run_time = time()

for year in years:

  print(year)

  # output file for this year
  output_h5 = output_h5_template.format(year=year)

  # only use files for the current year
  csv_files = [x for x in all_csv_files if str(year) in x]
  # PIC might not return files alphabetically
  csv_files.sort()

  # initilize hdf5 output file, will overwrite the old one
  f = h5py.File(output_h5, 'w')
  f['meta'] = meta.to_records()

  wsname = 'wind_speed'
  tcname = 'air_temperature'
  ghiname = 'ghi'
  dniname = 'dni'

  nsrdb = []
  for fi in range(len(csv_files)):

    print(csv_files[fi])

    csv_filei = os.path.join(csv_dir, csv_files[fi])
    csv = (pd.read_csv(csv_filei, index_col='datetime', parse_dates=True)
           .rename({'Wind Speed': wsname,
                    'Temperature': tcname,
                    'GHI': ghiname,
                    'DNI': dniname}, axis='columns')
           [['wind_speed', 'air_temperature', 'ghi', 'dni']])
    # the nrel data is at the center of the hour, so interpolate to whole hours
    tmp = csv.resample('30T').interpolate().resample('H').interpolate().bfill()
    nsrdb.append(tmp[tmp.index.notnull()])

  # 8760 except for leap years
  n_time_steps = nsrdb[0].shape[0]
  f['time_index'] = nsrdb[0].index.format()

  f[wsname] = pd.concat([x[wsname] for x in nsrdb], axis=1, ignore_index=True)
  f[tcname] = pd.concat([x[tcname] for x in nsrdb], axis=1, ignore_index=True)
  f[ghiname] = pd.concat([x[ghiname] for x in nsrdb], axis=1, ignore_index=True)
  f[dniname] = pd.concat([x[dniname] for x in nsrdb], axis=1, ignore_index=True)

  f.close()
  print('\nWrote data to ' + output_h5)
