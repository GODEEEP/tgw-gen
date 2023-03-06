#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compute wind power generation for WRF and NSRDB data using reV.

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

years = list(range(2008, 2014+1))

for year in years:

  print(year)

  res_file_wrf = '../data/sam_resource/wrf_wind_1h_{}.h5'.format(year)
  res_file_wtk = '../data/sam_resource/wtk_1h_{}.h5'.format(year)
  sam_file = '../sam/wind_gen_standard_losses_0.json'

  meta_fn = '../data/meta_wind.csv'

  # %%
  # windpower
  lat_lons = pd.read_csv(meta_fn)[['latitude', 'longitude']].to_numpy()

  # %%
  h5_wrf = h5py.File(res_file_wrf)
  h5_wtk = h5py.File(res_file_wtk)

  time_index_wrf = pd.to_datetime([x.decode() for x in h5_wrf['time_index'][:]])
  time_index_wtk = pd.to_datetime([x.decode() for x in h5_wtk['time_index'][:]])

  # rev will drop the last day of the year for leap years
  if calendar.isleap(year):
    time_index_wrf = time_index_wrf[:-24]
    time_index_wtk = time_index_wtk[:-24]

  h5_wrf.close()
  h5_wtk.close()

  gen_wrf, gen_wtk = [], []

  for i in range(lat_lons.shape[0]):

    print(year, i)

    pp_wrf = ProjectPoints.lat_lon_coords(lat_lons[i, :], res_file_wrf, sam_file)
    pp_wtk = ProjectPoints.lat_lon_coords(lat_lons[i, :], res_file_wtk, sam_file)

    gen_wrf.append(Gen.reV_run('windpower', pp_wrf, sam_file, res_file_wrf,
                               max_workers=1, out_fpath=None,
                               output_request=('cf_mean', 'cf_profile')))
    gen_wtk.append(Gen.reV_run('windpower', pp_wtk, sam_file, res_file_wtk,
                               max_workers=1, out_fpath=None,
                               output_request=('cf_mean', 'cf_profile')))

  cf_wrf_array = np.concatenate([x.out['cf_profile'] for x in gen_wrf], axis=1)
  cf_wtk_array = np.concatenate([x.out['cf_profile'] for x in gen_wtk], axis=1)

  cf_wrf = pd.DataFrame(cf_wrf_array, index=time_index_wrf)
  cf_wtk = pd.DataFrame(cf_wtk_array, index=time_index_wtk)

  cf_wrf.reset_index().to_csv('./valid_data/wrf_wind_power_{}.csv'.format(year), index=False)
  cf_wtk.reset_index().to_csv('./valid_data/wtk_power_{}.csv'.format(year), index=False)
