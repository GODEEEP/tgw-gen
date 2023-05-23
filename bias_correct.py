#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Bias correct solar using NSRDB 

Created on 2022-05-04

@author = Cameron Bracken (cameron.bracken@pnnl.gov)
"""

import numpy as np
import pandas as pd
from statsmodels.distributions.empirical_distribution import ECDF as ecdf
from utils.disc import disc
from utils.sza import solar_zenith_and_azimuth_angle as sza_saa
from tqdm import tqdm

# which years to process data (one year at a time)
wrf_years = list(range(1980, 2019+1))  # [1998]

# years to build the quantile mapping
nsrdb_years = list(range(1998, 2020+1))

in_csv_template = 'data/tgw-gen/solar/historical/solar_gen_cf_{year}.csv'
out_csv_template = 'data/tgw-gen/solar/historical/solar_gen_cf_{year}_bc.csv'
config_fn = 'data/tgw-gen/solar/eia_solar_configs.csv'

# metadata with lat/lon sites, generated from meta.py
configs = pd.read_csv(config_fn)
n_points = configs.shape[0]

print('Reading data')
# pull in all the power data
for year in nsrdb_years:

  print(year)

  csv = pd.read_csv(f'./validation/valid_data/nsrdb_eia_power_{year}.csv', index_col='index', parse_dates=True)

  if year == nsrdb_years[0]:
    nsrdb_gen = csv
  else:
    nsrdb_gen = pd.concat([nsrdb_gen, csv])

print('Bias correcting')
for year in wrf_years:

  print(year)

  # output file for this year
  input_csv = in_csv_template.format(year=year)
  output_csv = out_csv_template.format(year=year)

  # open existing csv file
  solar_gen = pd.read_csv(input_csv, index_col='datetime', parse_dates=True)

  # read validation data csvs, files were written by R so have index starting at 1
  for i, plant_code in tqdm(configs.plant_code.items(), total=n_points):

    plant_code = str(plant_code)
    q = ecdf(solar_gen[plant_code])(solar_gen[plant_code])
    solar_gen[plant_code] = np.round(np.percentile(nsrdb_gen[plant_code], 100*q), 3)

  solar_gen.to_csv(output_csv)
