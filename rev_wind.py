import numpy as np
import xarray as xr
import pandas as pd

from scipy.spatial import cKDTree

from time import time
from datetime import timedelta
from tqdm import tqdm
import calendar

import h5py
import json
import glob
import warnings
import logging
import tempfile
import sys

from joblib import Parallel, delayed

from reV.config.project_points import ProjectPoints
from reV.generation.generation import Gen

from utils.misc import dedup_names

# make reV and rex shut up
warnings.filterwarnings("ignore")
logging.getLogger('rex').setLevel(logging.CRITICAL)
logging.getLogger('reV').setLevel(logging.CRITICAL)

offset = xr.load_dataset('data/offset.nc')


def run_rev_wind_single_point(
    i,
    j,
    temperature,
    pressure,
    windspeed,
    winddirection,
    date_stamps,
    config,
    offset
):
  lat = temperature['XLAT'].to_numpy().astype('float')[()]
  lon = temperature['XLONG'].to_numpy().astype('float')[()]
  # offset = float(offset.offset[i, j].values)

  # metadata array
  meta = pd.DataFrame({'latitude': [lat],
                       'longitude': [lon],
                       'timezone': [offset],
                       'elevation': [0]})
  ll = meta[['latitude', 'longitude']].to_numpy()

  # temporary directory to hold the hdf5 input file for this point
  with tempfile.TemporaryDirectory() as tmpdirname:

    resource_fn = f'{tmpdirname}/solar_{i}_{j}.h5'
    f = h5py.File(resource_fn, 'w')

    f['meta'] = meta.to_records()

    # the wind data is hour ending and has an extra point at the beginning
    # so just need to cut it off
    # rev will drop the last day in a leap year
    f['time_index'] = date_stamps[1:]

    # rev needs variables at multiple heights and will interpolate between
    heights = temperature['interp_level']
    for i in range(len(heights)):

      postfix = f'_{int(heights[i] * 1000)}m'

      # the wind data is hour ending and has an extra point at the beginning
      # so need to cut it off
      f['temperature' + postfix] = temperature[1:, i]
      f['pressure' + postfix] = pressure[1:, i]
      f['windspeed' + postfix] = windspeed[1:, i]
      f['winddirection' + postfix] = winddirection[1:, i]

    f.close()

    config_dict = {0: config}

    # run reV
    pp_wrf = ProjectPoints.lat_lon_coords(ll, resource_fn, config_dict)
    gen = Gen('windpower', pp_wrf, config_dict, resource_fn,
                     output_request=('cf_profile'))
    gen.run(max_workers=1)
  return gen.out['cf_profile']


def run_rev_wind_grid_year(
    year,
    input_dir,
    output_dir,
    hub_height,
    tasks=64,
    load_full_dataset=True,
):

  start = time()

  nc_file = glob.glob(f"{input_dir}/*wind_{year}*")[0]

  if load_full_dataset:
    # load entire data set ~1min
    wind = xr.load_dataset(nc_file)
  else:
    wind = xr.open_dataset(nc_file)

  # get date stamps as string
  wind_date_times = pd.to_datetime(wind['Time'], utc=True)
  wind_date_stamps = list(wind_date_times.strftime('%Y-%m-%d %H:%M:%S'))

  # copy one of the variables from the netcdf file to get all dimensions
  wind_cf = wind['temperature'][:8760, 0, :, :].rename('capacity_factor')
  # for debugging
  # wind_cf = wind['temperature'][:8760, 0, :10, :10].rename('capacity_factor')
  wind_cf.attrs['projection'] = wind.attrs['projection']
  # shape[0] = time, shape[1] = south_north, shape[2] = east_west
  ni = wind_cf.shape[1]
  nj = wind_cf.shape[2]

  # ni = 10
  # nj = 10

  # load default config
  with open('sam/wind_default_config.json') as f:
    wind_config = json.load(f)

  # change the hub height
  wind_config['wind_turbine_hub_ht'] = hub_height
  # estimated relationship from EIA data using robust regression
  wind_config['wind_turbine_rotor_diameter'] = hub_height*1.15

  # reV memory issue, the runs get slower and slower over time.
  # to avoid this, kill the processes after a while and start over
  # testing shows about 50 i loop iterations is when slowdown starts
  # that means every chunk is 50*424 points
  iloop_restart_size = 50
  n_chunks = int(np.ceil(ni/iloop_restart_size))
  wind_cf_list = []

  start_parallel = time()

  for chunki in range(n_chunks):
    start_irange = chunki*iloop_restart_size
    end_irange = np.min((ni, (chunki+1)*iloop_restart_size))
    irange = list(range(start_irange, end_irange))

    wind_cf_list_chunki = Parallel(
        n_jobs=tasks,
        backend='multiprocessing',
        # backend='threading'
    )(delayed(run_rev_wind_single_point)(
        i,
        j,
        wind['temperature'][:, :, i, j],
        wind['pressure'][:, :, i, j],
        wind['windspeed'][:, :, i, j],
        wind['winddirection'][:, :, i, j],
        wind_date_stamps,
        wind_config,
        float(offset.offset[i, j].values)
    ) for i in irange for j in tqdm(range(nj)))

    wind_cf_list += wind_cf_list_chunki

  print("\tParallel took:", str(timedelta(seconds=np.round(time() - start_parallel))))

  # big matrix for all the new generation data
  cf = np.zeros((8760, ni, nj))

  # loop through the list of points and save them all into an array format
  counter = 0
  for i in range(ni):
    for j in range(nj):
      cf[:, i, j] = wind_cf_list[counter][:, 0]
      counter += 1

  wind_cf.values = cf
  wind_cf.to_netcdf(f"{output_dir}/wind_gen_cf_{year}_{int(hub_height)}m.nc")

  print("\tYear took:", str(timedelta(seconds=np.round(time() - start))))


def run_rev_wind_points_year(
        year,
        input_dir,
        output_dir,
        config_fn,
        tasks=64,
        load_full_dataset=True
):
  start = time()

  nc_file = glob.glob(f"{input_dir}/*wind_{year}*")[0]

  if load_full_dataset:
    # load entire data set ~1min
    wind = xr.load_dataset(nc_file)
  else:
    wind = xr.open_dataset(nc_file)

  if year == '2024':
    wind = dupe_last3_timesteps(wind)

  # get date stamps as string
  wind_date_times = pd.to_datetime(wind['Time'], utc=True)
  wind_date_stamps = list(wind_date_times.strftime('%Y-%m-%d %H:%M:%S'))

  # load default config
  wind_configs = pd.read_csv(config_fn)
  wind_config_dicts = wind_config_dicts_from_rows(wind_configs)
  n_plants = wind_configs.shape[0]

  grid_points = [wind.XLONG.stack(z=('south_north', 'west_east')),
                 wind.XLAT.stack(z=('south_north', 'west_east'))]
  tree = cKDTree(np.array(grid_points).transpose())
  dists, rows = tree.query(wind_configs[['lon', 'lat']])
  # indexes = pd.DataFrame({'i': grid_points[0]['south_north'][rows],
  #                         'j': grid_points[0]['west_east'][rows]})
  indexi = grid_points[0]['south_north'][rows].to_numpy()
  indexj = grid_points[0]['west_east'][rows].to_numpy()

  start_parallel = time()

  wind_cf_list = Parallel(
      n_jobs=tasks,
      backend='multiprocessing',
      # backend='threading'
  )(delayed(run_rev_wind_single_point)(
      i,
      j,
      wind['temperature'][:, :, i, j],
      wind['pressure'][:, :, i, j],
      wind['windspeed'][:, :, i, j],
      wind['winddirection'][:, :, i, j],
      wind_date_stamps,
      wind_config_dicts[p],
      float(offset.offset[i, j].values)
  ) for i, j, p in tqdm(zip(indexi, indexj, range(n_plants)), total=n_plants))

  print("\tParallel took:", str(timedelta(seconds=np.round(time() - start_parallel))))

  # the wind data is hour ending and has an extra point at the beginning
  # so just need to cut it off
  # also, rev will drop the last day in a leap year
  if calendar.isleap(int(year)):
    date_times_output = wind_date_times[1:][:-24]
  else:
    date_times_output = wind_date_times[1:]

  # format the output list back to a data frame, rows are timesteps
  # columns are plants/grid cells
  gen = pd.DataFrame(np.concatenate(wind_cf_list, axis=1),
                     index=date_times_output,
                     columns=dedup_names(wind_configs.plant_code))
  # write out the data to a csv
  (gen.reset_index()
      .rename({'index': 'datetime'}, axis=1)
      .to_csv(f'{output_dir}/wind_gen_cf_{year}.csv', index=False))

  print("\tYear took:", str(timedelta(seconds=np.round(time() - start))))


def wind_config_dicts_from_rows(config):

  # need to parse the list values from string to list
  keys_to_parse = ['wind_farm_xCoordinates',
                   'wind_farm_yCoordinates',
                   'wind_turbine_powercurve_powerout',
                   'wind_turbine_powercurve_windspeeds']
  config_dicts = {}
  for rowi, row in config.iterrows():
    for key in keys_to_parse:
      row[key] = json.loads(row[key])
    config_dicts[rowi] = row.to_dict()
  return config_dicts


def dupe_last3_timesteps(data):
  # Select timesteps to duplicate
  data_to_duplicate = data.sel(Time=data.Time[-3:])

  # add 3 hours
  data_to_duplicate['Time'] = pd.to_datetime(data_to_duplicate.Time) + pd.Timedelta("3h") 

  # convert back to int64
  data_to_duplicate['Time'] = data_to_duplicate['Time'].astype('int64')

  # Merge the duplicated timestep back into the original dataset
  duplicated_dataset = xr.concat([data, data_to_duplicate], dim="Time")

  return duplicated_dataset


if __name__ == '__main__':
  mode = sys.argv[1]
  year = sys.argv[2]
  input_dir = sys.argv[3]
  output_dir = sys.argv[4]
  config_fn = sys.argv[5]
  # only applies to grid mode
  try:
    hub_height = sys.argv[5]
  except:
    hub_height = '125'

  print(f'Running reV wind {mode} mode for {year}...')
  if mode == 'grid':
    run_rev_wind_grid_year(year, input_dir, output_dir, hub_height)
  elif mode == 'points':
    # config_fn = 'sam/configs/eia_wind_configs.csv'
    run_rev_wind_points_year(year, input_dir, output_dir, config_fn)
  else:
    raise Exception(
        """Valid mode must be 'grid' or 'points'
        
        Usage:
        
        python rev_wind.py mode year in_dir out_dir config_fn [hub_height]""")
