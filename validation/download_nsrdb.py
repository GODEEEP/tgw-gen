"""
This script will download NSRDB data from the NREL API. 
This requires you to have an NREL API key:

https://developer.nrel.gov/signup/

Once you have that, create a file named `.env` in the `WRF-to-reV` directory. 
The file should have the following lines:

    nrel_api_key = 'key'
    nrel_api_email = 'email'

Adjust the valid years based on how much data you want to download. See the 
API reference for available years:

    https://developer.nrel.gov/docs/solar/nsrdb/psm3-download/
    
@author = Cameron Bracken (cameron.bracken@pnnl.gov)
"""

from dotenv import load_dotenv
import os
import pandas as pd
import urllib3
from urllib3.util.ssl_ import create_urllib3_context
from urllib3.util import Timeout, Retry
from io import StringIO
import ssl

valid_years = list(range(1998, 2020+1))
valid_data_dir = 'valid_data/nsrdb_eia'
cache_dir = 'valid_data/cache_eia'

load_dotenv('../.env')

pv_plants = pd.read_csv('../sam/configs/eia_solar_configs.csv')

# fix to prevent vpn errors
ctx = create_urllib3_context()
ctx.load_default_certs()
ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT
# ssl._create_default_https_context = ssl._create_unverified_context


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
  api_key = os.getenv('nrel_api_key')
  email = os.getenv('nrel_api_email')

  # Set the attributes to extract (dhi, ghi, etc.), separated by commas.
  # https://developer.nrel.gov/docs/solar/nsrdb/psm3-download/
  attributes = 'ghi,dhi,dni,wind_speed,wind_direction,air_temperature,solar_zenith_angle,surface_pressure'

  # leap='true' will return leap day data if present, false will not.
  # Set time interval in minutes, Valid intervals are 30 & 60.
  interval = '60'

  # Declare url string
  url = (f'https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-download.csv?wkt=POINT({lon}%20{lat})&names={year}&' +
         f'leap_day=true&interval={interval}&utc=true&email={email}&api_key={api_key}&attributes={attributes}')
  # Return just the first 2 lines to get metadata:
  # info = pd.read_csv(url, nrows=1)
  # See metadata for specified properties, e.g., timezone and elevation
  # timezone, elevation = info['Local Time Zone'], info['Elevation']

  # Return all but first 2 lines of csv to get data:
  with urllib3.PoolManager(ssl_context=ctx) as http:
    r = http.request('GET', url, retries=urllib3.util.Retry(5), timeout=urllib3.util.Timeout(10))
  csv = pd.read_csv(StringIO("".join(map(chr, r.data))), skiprows=2)
  csv['datetime'] = pd.to_datetime(csv[['Year', 'Month', 'Day', 'Hour', 'Minute']], utc=True)
  return csv


for valid_year in valid_years:

  for i, row in pv_plants.iterrows():

    pointi = i + 1

    lat = row.lat
    lon = row.lon
    plant_code = row.plant_code
    print(valid_year, pointi, plant_code, lat, lon)
    csv_fn = os.path.join(valid_data_dir, f'nsrdb_{valid_year}_{plant_code:04}.csv')

    if os.path.exists(csv_fn):
      pass
    else:
      point_data = nsrdb_point(lat, lon, valid_year)
      point_data['point'] = pointi
      point_data['lat'] = lat
      point_data['lon'] = lon

      # # data is at the 30 of every hour, assume this is representative of the hour
      # # so basically drop the minute component
      # nsrdb_list[[pointi]] = nsrdb_list[[pointi]] |> select(-Minute)

      point_data.to_csv(csv_fn, index=False)
      # import sys
      # sys.exit()
