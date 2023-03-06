# -*- coding: utf-8 -*-
"""
Created on Wed Sep 7 2022

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
years = list(range(2007, 2014+1))

csv_dir = 'valid_data/wtk'
# wrf_dir = '/rcfs/projects/godeeep/shared_data/tgw_wrf/tgw_wrf_historic/three_hourly'
output_h5_template = '../data/sam_resource/wtk_1h_{year}.h5'
meta_fn = '../data/meta_wind.csv'

# metadata with lat/lon sites, generated from meta.py
meta = pd.read_csv(meta_fn)

all_csv_files = os.listdir(csv_dir)

run_time = time()

for year in years:
  # output file for this year
  output_h5 = output_h5_template.format(year=year)

  # only use files for the current year
  csv_files = [x for x in all_csv_files if str(year) in x]
  # PIC might not return files alphabetically
  csv_files.sort()

  # initilize hdf5 output file, will overwrite the old one
  f = h5py.File(output_h5, 'w')
  f['meta'] = meta.to_records()

  wsname = 'windspeed_80m'
  wdname = 'winddirection_80m'
  prname = 'pressure_80m'
  tcname = 'temperature_80m'

  wtk = []
  for fi in range(len(csv_files)):

    print(csv_files[fi])

    csv_filei = os.path.join(csv_dir, csv_files[fi])
    csv = (pd.read_csv(csv_filei, index_col='datetime', parse_dates=True)
           .rename({'wind speed at 80m (m/s)': wsname,
                    'wind direction at 80m (deg)': wdname,
                    'air pressure at 100m (Pa)': prname,
                    'air temperature at 80m (C)': tcname}, axis='columns')
           [[wsname, wdname, prname, tcname]])
    #csv['u_80m'] = - np.abs(csv['windspeed_80m'])*np.sin(csv['winddir_80m'])
    #csv['v_80m'] = - np.abs(csv['windspeed_80m'])*np.cos(csv['winddir_80m'])
    # the nrel data is at the center of the hour, so interpolate to whole hours
    wtk.append(csv.resample('30T').interpolate().resample('H').interpolate().bfill())

  # 8760 except for leap years
  n_time_steps = wtk[0].shape[0]
  f['time_index'] = wtk[0].index.format()

  f[wsname] = pd.concat([x[wsname] for x in wtk], axis=1, ignore_index=True)
  f[wdname] = pd.concat([x[wdname] for x in wtk], axis=1, ignore_index=True)
  f[prname] = pd.concat([x[prname] for x in wtk], axis=1, ignore_index=True)
  f[tcname] = pd.concat([x[tcname] for x in wtk], axis=1, ignore_index=True)

  f.close()
  print('\nWrote data to ' + output_h5)
