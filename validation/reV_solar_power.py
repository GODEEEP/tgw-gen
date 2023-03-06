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

# ignore rev warnings related to chunk size
warnings.filterwarnings("ignore")

years = list(range(2007, 2020+1))

# %%
for year in years:

  print(year)

  res_file_wrf = '../data/sam_resource/wrf_solar_1h_{}.h5'.format(year)
  res_file_nsrdb = '../data/sam_resource/nsrdb_1h_{}.h5'.format(year)
  sam_file = '../sam/naris_pv_1axis_inv13.json'

  meta_fn = '../data/meta_solar.csv'

  lat_lons = pd.read_csv(meta_fn)[['latitude', 'longitude']].to_numpy()
  # lat_lons = np.array([[43.936687, -117.381214]])

  h5_wrf = h5py.File(res_file_wrf)
  h5_nsrdb = h5py.File(res_file_nsrdb, 'r+')

  time_index_wrf = pd.to_datetime([x.decode() for x in h5_wrf['time_index'][:]])
  time_index_nsrdb = pd.to_datetime([x.decode() for x in h5_nsrdb['time_index'][:]])

  # rev will drop the last day of the year for leap years
  if calendar.isleap(year):
    time_index_wrf = time_index_wrf[:-24]
    time_index_nsrdb = time_index_nsrdb[:-24]

  h5_wrf.close()
  h5_nsrdb.close()

  gen_wrf, gen_nsrdb = [], []

  for i in range(lat_lons.shape[0]):

    print(year, i)

    pp_wrf = ProjectPoints.lat_lon_coords(lat_lons[i, :], res_file_wrf, sam_file)
    pp_nsrdb = ProjectPoints.lat_lon_coords(lat_lons[i, :], res_file_nsrdb, sam_file)

    gen_wrf.append(Gen.reV_run('pvwattsv5', pp_wrf, sam_file, res_file_wrf,
                               max_workers=1, out_fpath=None,
                               output_request=('cf_mean', 'cf_profile', 'lcoe_fcr')))
    gen_nsrdb.append(Gen.reV_run('pvwattsv5', pp_nsrdb, sam_file, res_file_nsrdb,
                                 max_workers=1, out_fpath=None,
                                 output_request=('cf_mean', 'cf_profile', 'lcoe_fcr')))

  cf_wrf_array = np.concatenate([x.out['cf_profile'] for x in gen_wrf], axis=1)
  cf_nsrdb_array = np.concatenate([x.out['cf_profile'] for x in gen_nsrdb], axis=1)

  cf_wrf = pd.DataFrame(cf_wrf_array, index=time_index_wrf)
  cf_nsrdb = pd.DataFrame(cf_nsrdb_array, index=time_index_nsrdb)

  cf_wrf.reset_index().to_csv('./valid_data/wrf_solar_power_{}.csv'.format(year), index=False)
  cf_nsrdb.reset_index().to_csv('./valid_data/nsrdb_power_{}.csv'.format(year), index=False)
