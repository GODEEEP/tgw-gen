#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compute solar power generation for WRF and NSRDB data using reV.

Created on 2022-09-21 10:02:56

@author = Cameron Bracken (cameron.bracken@pnnl.gov)
"""

# %%
import calendar
import warnings

import h5py
import numpy as np
import pandas as pd
from reV.config.project_points import ProjectPoints
from reV.generation.generation import Gen
import logging
from tqdm import tqdm

# ignore rev and rex warnings
warnings.filterwarnings("ignore")
logging.getLogger('rex').setLevel(logging.CRITICAL)
logging.getLogger('reV').setLevel(logging.CRITICAL)

years = list(range(2018, 2020+1))

config_fn = '../sam/configs/eia_solar_configs.csv'
config = pd.read_csv(config_fn)

config_unique = config.loc[~config[['lat', 'lon']].duplicated()]
config_dupe = config.loc[config[['lat', 'lon']].duplicated()]
lat_lons = config_unique[['lat', 'lon']].to_numpy()

solar_config_dicts = []
for rowi, row in config_unique.iterrows():
  solar_config_dicts.append(row.to_dict())


def group_duplicate_index(df):
  # find duplicate rows
  a = df.values
  sidx = np.lexsort(a.T)
  b = a[sidx]

  m = np.concatenate(([False], (b[1:] == b[:-1]).all(1), [False]))
  idx = np.flatnonzero(m[1:] != m[:-1])
  I = df.index[sidx].tolist()
  return [I[i:j] for i, j in zip(idx[::2], idx[1::2]+1)]


dupe_rows = group_duplicate_index(config[['lat', 'lon']])

# %%
for year in years:

  print(year)

  res_file_nsrdb = f'../data/sam_resource/nsrdb_1h_{year}.h5'

  h5_nsrdb = h5py.File(res_file_nsrdb, 'r+')
  time_index_nsrdb = pd.to_datetime([x.decode() for x in h5_nsrdb['time_index'][:]])

  # rev will drop the last day of the year for leap years
  if calendar.isleap(year):
    time_index_nsrdb = time_index_nsrdb[:-24]

  h5_nsrdb.close()

  gen_nsrdb = []
  for i in tqdm(range(lat_lons.shape[0])):
    config_dict = {0: solar_config_dicts[i]}
    pp_nsrdb = ProjectPoints.lat_lon_coords(lat_lons[i, :], res_file_nsrdb, config_dict)
    gen_nsrdb.append(Gen.reV_run('pvwattsv7', pp_nsrdb, config_dict, res_file_nsrdb,
                                 max_workers=1, out_fpath=None,
                                 output_request=('cf_profile')))

  # reformat data to have one row
  gen_nsrdb2 = []
  nondupe_rowi = 0
  for rowi, row in config.iterrows():
    found_dupe_row = False
    for r in dupe_rows:
      if rowi in r and rowi != r[0]:
        found_dupe_row = True
        break
    if found_dupe_row:
      gen_nsrdb2.append(gen_nsrdb[np.where(config_unique.index == r[0])[0][0]])
    else:
      gen_nsrdb2.append(gen_nsrdb[nondupe_rowi])
      nondupe_rowi += 1

  cf_nsrdb_array = np.concatenate([x.out['cf_profile'] for x in gen_nsrdb2], axis=1)
  cf_nsrdb = pd.DataFrame(cf_nsrdb_array, index=time_index_nsrdb, columns=config.plant_code_unique)
  cf_nsrdb.reset_index().to_csv(f'./valid_data/nsrdb_eia_power_{year}.csv', index=False)
