# -*- coding: utf-8 -*-
"""
Created on Aug 22 08:46:43 2022

@author: Cameron Bracken (cameron.bracken@pnnl.gov)
"""

import urllib
import rasterio
import requests
from tqdm import tqdm

# coordinates with known elevation
# latitude = [48.633, 48.733, 45.1947, 45.1962]
# longitude = [-93.9667, -94.6167, -93.3257, -93.2755]


def get_usgs_elevation(latitude, longitude):
  """Query service using lat, lon.
  add the elevation values as a new column."""

  # USGS Elevation Point Query Service
  url = r'https://nationalmap.gov/epqs/pqs.php?'

  elevations = []
  for lat, lon in zip(latitude, longitude):

    # define rest query params
    params = {
        'output': 'json',
        'x': lon,
        'y': lat,
        'units': 'Meters'
    }

    # format query string and return query value
    result = requests.get((url + urllib.parse.urlencode(params)))
    elevations.append((result.json()['USGS_Elevation_Point_Query_Service']
                      ['Elevation_Query']['Elevation']))

  return elevations


def get_raster_elevation(p, dem_fn):
  """
  Get elevation for a lat/lon point from a geotif input file.

  p is a (lon, lat) tuple

  """

  with rasterio.open(dem_fn) as src:
    vals = src.sample([p])
    for val in vals:
      elevation = val[0]
      return elevation


def get_multi_raster_elevation(latitude, longitude, dem_fns):
  """
  Get elevation for a lat/lon point from a list geotif input files.

  """

  elev = []
  for p in tqdm(zip(longitude, latitude), total=len(longitude)):
    for dem_fn in dem_fns:
      v = get_raster_elevation(p, dem_fn)
      # print(v)
      if v > -100:
        elev.append(v)
        break
    # if the dem value is still very negative we didnt find
    # the point in any file
    if v <= -100:
      elev.append(None)
  return elev
