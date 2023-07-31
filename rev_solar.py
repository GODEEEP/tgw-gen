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


def run_rev_solar_single_point(
    i,
    j,
    air_temperature,
    wind_speed,
    surface_pressure,
    ghi,
    dni,
    date_stamps,
    config,
    offset
):
  lat = air_temperature['XLAT'].to_numpy().astype('float')[()]
  lon = air_temperature['XLONG'].to_numpy().astype('float')[()]

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
    f['time_index'] = date_stamps
    f['air_temperature'] = air_temperature
    f['wind_speed'] = wind_speed
    f['surface_pressure'] = surface_pressure
    # some values are NaN sometimes, not exactly sure why
    # interpolating would be better
    f['ghi'] = ghi.fillna(0)
    f['dni'] = dni.fillna(0)
    f.close()

    config_dict = {0: config}

    # run reV
    pp_wrf = ProjectPoints.lat_lon_coords(ll, resource_fn, config_dict)
    cf = Gen.reV_run('pvwattsv5', pp_wrf, config_dict, resource_fn,
                     max_workers=1, out_fpath=None,
                     output_request=('cf_profile'))
  return cf.out['cf_profile']


def run_rev_solar_grid_year(
    year,
    input_dir,
    output_dir,
    tasks=64,
    load_full_dataset=True,
):

  start = time()

  nc_file = glob.glob(f"{input_dir}/*solar_{year}*")[0]

  if load_full_dataset:
    # load entire data set ~1min
    solar = xr.load_dataset(nc_file)
  else:
    solar = xr.open_dataset(nc_file)

  # get date stamps as string
  solar_date_times = pd.to_datetime(solar['Time'], utc=True)
  solar_date_stamps = list(solar_date_times.strftime('%Y-%m-%d %H:%M:%S'))

  # copy one of the variables from the netcdf file to get all dimensions
  # solar_cf = solar['air_temperature'][:8760, :10, :10].rename('capacity_factor')
  solar_cf = solar['air_temperature'][:8760, :, :].rename('capacity_factor')
  solar_cf.attrs['projection'] = solar.attrs['projection']
  # shape[0] = time, shape[1] = south_north, shape[2] = east_west
  ni = solar_cf.shape[1]
  nj = solar_cf.shape[2]

  # ni = 10
  # nj = 10

  # load default config
  with open('sam/solar_default_config.json') as f:
    solar_config = json.load(f)

  # reV memory issue, the runs get slower and slower over time.
  # to avoid this, kill the processes after a while and start over
  # testing shows about 50 i loop iterations is when slowdown starts
  # that means every chunk is 50*424 points
  iloop_restart_size = 50
  n_chunks = int(np.ceil(ni/iloop_restart_size))
  solar_cf_list = []

  start_parallel = time()

  for chunki in range(n_chunks):
    start_irange = chunki*iloop_restart_size
    end_irange = np.min((ni, (chunki+1)*iloop_restart_size))
    irange = list(range(start_irange, end_irange))

    solar_cf_list_chunki = Parallel(
        n_jobs=tasks,
        backend='multiprocessing',
        # backend='threading'
    )(delayed(run_rev_solar_single_point)(
        i,
        j,
        solar['air_temperature'][:, i, j],
        solar['wind_speed'][:, i, j],
        solar['surface_pressure'][:, i, j],
        solar['ghi'][:, i, j],
        solar['dni'][:, i, j],
        solar_date_stamps,
        solar_config,
        float(offset.offset[i, j].values)
    ) for i in irange for j in tqdm(range(nj)))

    solar_cf_list += solar_cf_list_chunki

  print("\tParallel took:", str(timedelta(seconds=np.round(time() - start_parallel))))

  # big matrix for all the new generation data
  cf = np.zeros((8760, ni, nj))

  # loop through the list of points and save them all into an array format
  counter = 0
  for i in range(ni):
    for j in range(nj):
      cf[:, i, j] = solar_cf_list[counter][:, 0]
      counter += 1

  solar_cf.values = cf
  solar_cf.to_netcdf(f"{output_dir}/solar_gen_cf_{year}.nc")

  print("\tYear took:", str(timedelta(seconds=np.round(time() - start))))


def run_rev_solar_points_year(
        year,
        input_dir,
        output_dir,
        config_fn,
        tasks=64,
        load_full_dataset=True
):
  start = time()

  nc_file = glob.glob(f"{input_dir}/*solar_{year}*")[0]

  if load_full_dataset:
    # load entire data set ~1min
    solar = xr.load_dataset(nc_file)
  else:
    solar = xr.open_dataset(nc_file)

  # get date stamps as string
  solar_date_times = pd.to_datetime(solar['Time'], utc=True)
  solar_date_stamps = list(solar_date_times.strftime('%Y-%m-%d %H:%M:%S'))

  # load default config
  solar_configs = pd.read_csv(config_fn)
  solar_config_dicts = solar_config_dicts_from_rows(solar_configs)
  n_plants = solar_configs.shape[0]

  grid_points = [solar.XLONG.stack(z=('south_north', 'west_east')),
                 solar.XLAT.stack(z=('south_north', 'west_east'))]
  tree = cKDTree(np.array(grid_points).transpose())
  dists, rows = tree.query(solar_configs[['lon', 'lat']])
  # indexes = pd.DataFrame({'i': grid_points[0]['south_north'][rows],
  #                         'j': grid_points[0]['west_east'][rows]})
  indexi = grid_points[0]['south_north'][rows].to_numpy()
  indexj = grid_points[0]['west_east'][rows].to_numpy()

  start_parallel = time()

  # debugging
  # solar_cf_list = []
  # for i, j, p in zip(indexi[3000:4026], indexj[3000:4026], list(range(n_plants))[3000:4026]):
  #   print(i, j, p)
  #   x = run_rev_solar_single_point(
  #       i,
  #       j,
  #       solar['air_temperature'][:, i, j],
  #       solar['wind_speed'][:, i, j],
  #       solar['surface_pressure'][:, i, j],
  #       solar['ghi'][:, i, j],
  #       solar['dni'][:, i, j],
  #       solar_date_stamps,
  #       solar_config_dicts[p],
  #       float(offset.offset[i, j].values)
  #   )
  #   solar_cf_list.append(x)

  solar_cf_list = Parallel(
      n_jobs=tasks,
      backend='multiprocessing',
      # backend='threading'
  )(delayed(run_rev_solar_single_point)(
      i,
      j,
      solar['air_temperature'][:, i, j],
      solar['wind_speed'][:, i, j],
      solar['surface_pressure'][:, i, j],
      solar['ghi'][:, i, j],
      solar['dni'][:, i, j],
      solar_date_stamps,
      solar_config_dicts[p],
      float(offset.offset[i, j].values)
  ) for i, j, p in tqdm(zip(indexi, indexj, range(n_plants)), total=n_plants))

  print("\tParallel took:", str(timedelta(seconds=np.round(time() - start_parallel))))

  # the solar data is hour ending
  # also, rev will drop the last day in a leap year
  if calendar.isleap(int(year)):
    date_times_output = solar_date_times[:-24]
  else:
    date_times_output = solar_date_times

  # format the output list back to a data frame, rows are timesteps
  # columns are plants/grid cells
  gen = pd.DataFrame(np.concatenate(solar_cf_list, axis=1),
                     index=date_times_output,
                     columns=dedup_names(solar_configs.plant_code))
  # write out the data to a csv
  (gen.reset_index()
      .rename({'index': 'datetime'}, axis=1)
      .to_csv(f'{output_dir}/solar_gen_cf_{year}.csv', index=False))

  print("\tYear took:", str(timedelta(seconds=np.round(time() - start))))


def solar_config_dicts_from_rows(config):

  config_dicts = {}
  for rowi, row in config.iterrows():
    # if the array type is tracking (2 or 4), delete the tilt parameter to avoid
    # a warning message. reV will set the tilt to the latitude which is what
    # it should be anyway.
    # array_type = row['array_type']
    # if array_type == 2 or array_type == 4:
    #   row.pop('tilt')

    config_dicts[rowi] = row.to_dict()
  return config_dicts


if __name__ == '__main__':
  mode = sys.argv[1]
  year = sys.argv[2]
  input_dir = sys.argv[3]
  output_dir = sys.argv[4]
  config_fn = sys.argv[5]

  print(f'Running reV solar {mode} mode for {year}...')
  if mode == 'grid':
    run_rev_solar_grid_year(year, input_dir, output_dir)
  elif mode == 'points':
    # config_fn = 'sam/configs/eia_solar_configs.csv'
    run_rev_solar_points_year(year, input_dir, output_dir, config_fn)
  else:
    raise Exception(
        """Valid mode must be 'grid' or 'points'
        
        Usage:
        
        python rev_solar.py mode year in_dir out_dir config_fn""")
