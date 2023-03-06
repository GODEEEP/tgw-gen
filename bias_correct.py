#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Bias correct GHI using NSRDB and re-compute DNI.

This script will use quantile mapping to bias correct GHI data in existing sam resource (hdf5) files. 
DNI will be recomputed from the bias corrected GHI using the DISC model.

Created on 2022-09-30

@author = Cameron Bracken (cameron.bracken@pnnl.gov)
"""

import h5py
import numpy as np
import pandas as pd
from statsmodels.distributions.empirical_distribution import ECDF as ecdf
from utils.disc import disc
from utils.sza import solar_zenith_and_azimuth_angle as sza_saa

# which years to process wrf data (one year at a time)
wrf_years = list(range(1980, 2019+1))  # [1998]

# years to build the quantile mapping
nsrdb_years = list(range(1998, 2020+1))

output_h5_template = 'data/sam_resource_gridview/wrf_solar_1h_{year}.h5'
meta_fn = 'data/meta_gv_solar.csv'

# metadata with lat/lon sites, generated from meta.py
meta = pd.read_csv(meta_fn)
n_points = meta.shape[0]

# pull all the nsrdb obs into a big dict
ghi_obs = dict()
for year in nsrdb_years:

  print(year)

  # read validation data csvs, files were written by R so have index starting at 1
  for i in range(1, n_points+1):
    fn = f'./validation/valid_data/nsrdb_gridview/nsrdb_{year}_{i:04d}.csv'
    print(i, fn)
    # read data and interpolate to whole hours
    csv = (pd.read_csv(fn, index_col='datetime', parse_dates=True)
             .resample('30T')
             .interpolate()
             .resample('H')
             .interpolate()
             .bfill())
    csv = csv[csv.index.notnull()]

    if year == nsrdb_years[0]:
      ghi_obs[i] = csv['GHI']
    else:
      ghi_obs[i] = pd.concat([ghi_obs[i], csv['GHI']])

print('Bias correcting')
for year in wrf_years:

  print(year)

  # output file for this year
  output_h5 = output_h5_template.format(year=year)

  # open existing hdf5 file
  f = h5py.File(output_h5, 'r+')

  # bias correction for GHI and DNI
  ghi = f['ghi'][:, :]
  dni = f['dni'][:, :]
  pr = f['surface_pressure'][:, :]

  # get the hdf5 time index as a pandas date time index
  time_index = [x.decode('utf8') for x in f['time_index'][:]]
  date_time = pd.to_datetime(time_index)

  # read validation data csvs, files were written by R so have index starting at 1
  for i in range(1, n_points+1):

    ghi_q = ecdf(ghi[:, i-1])(ghi[:, i-1])

    ghi[:, i-1] = np.percentile(ghi_obs[i], 100*ghi_q)

    ghi[ghi_obs == 0, i-1] = 0

  # estimate dhi from ghi
  for pointi in range(n_points):
    sza_fast, saa = sza_saa(longitude=meta['longitude'][pointi],
                            latitude=meta['latitude'][pointi],
                            time_utc=date_time)

    dni[:, pointi] = disc(ghi[:, pointi], sza_fast, date_time.day_of_year.to_numpy(), pressure=pr[:, pointi])

  f['ghi'][:, :] = ghi
  f['dni'][:, :] = dni

  f.close()
