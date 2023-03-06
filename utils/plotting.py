#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 18 10:58:34 2022

@author: brac840
"""

import cartopy.crs as ccrs
import cartopy.feature as cf
import matplotlib.pyplot as plt


def plot_sites_map(plants, title, latN=50, latS=30, lonW=-123.0, lonE=-102.5):

  cLat = (latN + latS)/2
  cLon = (lonW + lonE)/2

  proj = ccrs.LambertConformal(central_longitude=cLon, central_latitude=cLat)
  # Coarsest and quickest to display;
  # other options are '10m' (slowest) and '50m'.
  res = '50m'
  fig = plt.figure(figsize=(18, 12))
  ax = plt.subplot(1, 1, 1, projection=proj)
  ax.set_extent([lonW, lonE, latS, latN])
  ax.add_feature(cf.LAND.with_scale(res))
  ax.add_feature(cf.OCEAN.with_scale(res))
  ax.add_feature(cf.COASTLINE.with_scale(res))
  ax.add_feature(cf.LAKES.with_scale(res), alpha=0.5)
  ax.add_feature(cf.STATES.with_scale(res))
  # gridlines = ax.gridlines(draw_labels=False)

  plt.scatter(x=plants.longitude,
              y=plants.latitude,
              s=9, c='black',
              # edgecolor='black',
              transform=ccrs.PlateCarree(),  # Important
              label='Plant Locations')
  plt.title(title)
