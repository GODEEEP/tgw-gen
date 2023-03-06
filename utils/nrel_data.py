#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 11:10:27 2022

@author: brac840
"""


import os

import dotenv
import pandas as pd

load_dotenv()


def nsrdb_point(lat, lon, year):
  """
  Extract data for a single point, for one year, from the NSRDB.

  Example:
  https://developer.nrel.gov/docs/solar/nsrdb/python-examples/

  API reference:
  https://developer.nrel.gov/docs/solar/nsrdb/psm3-download/
  """

  # lat, lon, year = 43, -120, 1998
  # You must request an NSRDB api key from the link above
  api_key = os.environ('nrel_api_key')
  email = 'cameron.bracken@pnnl.gov'
  # Set the attributes to extract (dhi, ghi, etc.), separated by commas.
  # https://developer.nrel.gov/docs/solar/nsrdb/psm3-download/
  attributes = ('ghi,dhi,dni,wind_speed,wind_direction,air_temperature,'
                'solar_zenith_angle,surface_pressure')
  # leap='true' will return leap day data if present, false will not.
  # Set time interval in minutes, Valid intervals are 30 & 60.
  interval = '60'

  # Declare url string
  url = (('https://developer.nrel.gov/api/nsrdb/v2/solar/'
          'psm3-download.csv?wkt=POINT({lon}%20{lat})&names={year}&'
          'leap_day={leap}&interval={interval}&utc={utc}&email={email}&'
          'api_key={api}&attributes={attr}')
         .format(year=year, lat=lat, lon=lon, leap='true', interval=interval,
                 utc='true', email=email, api=api_key, attr=attributes))
  # Return just the first 2 lines to get metadata:
  # info = pd.read_csv(url, nrows=1)
  # See metadata for specified properties, e.g., timezone and elevation
  # timezone, elevation = info['Local Time Zone'], info['Elevation']

  # Return all but first 2 lines of csv to get data:
  df = pd.read_csv(url, skiprows=2)

  # Set the time index in the pandas dataframe:
  df = df.set_index(pd.date_range('1/1/{yr}'.format(yr=year),
                                  freq=interval+'Min',
                                  periods=525600/int(interval)))
  return df


def wtk_point(lat, lon, year):
  """
  Extract data for a single point, for one year, from the WTK.

  Created on Fri Aug 12 12:48:24 2022

  API reference:
  https://developer.nrel.gov/docs/wind/wind-toolkit/wtk-download/

  """

  # lat, lon, year = 43, -120, 2014
  # latest available year is 2014
  if year > 2014:
    return None

  api_key = os.environ('nrel_api_key')
  email = 'cameron.bracken@pnnl.gov'
  # Set the attributes to extract (dhi, ghi, etc.), separated by commas.
  attributes = ('windspeed_80m,winddirection_80m,temperature_80m,'
                'windspeed_140m,winddirection_140m,temperature_140m,'
                'pressure_0m,pressure_100m,pressure_200m')
  # leap='true' will return leap day data if present, false will not.
  # Set time interval in minutes, Valid intervals are 30 & 60.
  interval = '60'

  # Declare url string
  url = (('https://developer.nrel.gov/api/wind-toolkit/v2/wind/'
          'wtk-download.csv?wkt=POINT({lon}%20{lat})&names={year}&'
          'leap_day={leap}&interval={interval}&utc={utc}&email={email}&'
          'api_key={api}&attributes={attr}')
         .format(year=year, lat=lat, lon=lon, leap='true', interval=interval,
                 utc='true', email=email, api=api_key, attr=attributes))
  # Return just the first 2 lines to get metadata:
  # info = pd.read_csv(url, nrows=1)
  # See metadata for specified properties, e.g., timezone and elevation
  # timezone, elevation = info['Local Time Zone'], info['Elevation']

  # Return all but first 2 lines of csv to get data:
  df = pd.read_csv(url, skiprows=1)

  # Set the time index in the pandas dataframe:
  df = df.set_index(pd.date_range('1/1/{yr}'.format(yr=year),
                                  freq=interval+'Min',
                                  periods=525600/int(interval)))
  return df
