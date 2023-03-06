from glob import glob
import sys
import time

# joblib allows for parallel threads
from joblib import Parallel, delayed
from netCDF4 import Dataset
import numpy as np
import pandas as pd
from tqdm import tqdm
import xarray as xr
import wrf
# from farms.disc import disc
from utils.disc import disc
from utils.sza import solar_zenith_and_azimuth_angle as sza_saa

import warnings

# ignore warnings related to invlaid math in disc model
warnings.filterwarnings("ignore")

# wrf_dir = '/global/cfs/cdirs/m2702/gsharing/tgw-wrf-conus/historical_1980_2019/hourly'
# output_dir = '/global/cfs/cdirs/m2702/gsharing/solar-wind/met_data_fullgrid/historical'


def estimate_dni(ghi, psfc):
  """
  Compute direct normal irradiance (DNI) from global horizontal irradiance (GHI)
  (aka downward shortwave radiation) using the NREL DISC model. 
  """
  ghi_stacked = ghi.stack(s=('Time', ...))
  psfc_stacked = psfc.stack(s=('Time', ...))
  datetime = pd.DatetimeIndex(ghi_stacked['Time'])
  sza, saa = sza_saa(longitude=ghi_stacked['XLONG'], latitude=ghi_stacked['XLAT'], time_utc=datetime)

  dni = disc(ghi_stacked,
             sza,
             datetime.day_of_year.to_numpy(),
             pressure=psfc_stacked)
  return dni.unstack()


def process_file(f):
  """
  Extract data for a solar power model from a single WRF output file. 
  """
  ds = Dataset(f)
  data = {
      'T2': None,
      'WSPD': None,
      'PSFC': None,
      'SWDOWN': None,
  }
  cache = wrf.extract_vars(ds, wrf.ALL_TIMES, ("PSFC", "SWDOWN", "WSPD", "T2"))
  for v in data.keys():
    # cache makes repeated access of variables faster, not totally sure if it helps here
    data[v] = wrf.getvar(wrfin=ds, varname=v, squeeze=False, timeidx=wrf.ALL_TIMES, cache=cache)
  merged = xr.merge([v for v in data.values()])
  merged['PSFC'] = merged['PSFC'] / 100.0  # convert to mb/hPa
  merged['T2'] = merged['T2'] - 273.15  # convert to C
  # start = time.time()
  merged['dni'] = estimate_dni(merged['SWDOWN'], merged['PSFC'])
  # end = time.time()
  # print(end - start)
  merged = merged.rename({
      'T2': 'air_temperature', 'WSPD': 'wind_speed',
      'PSFC': 'surface_pressure', 'SWDOWN': 'ghi'
  })
  merged = merged.round(2).astype(np.float32)
  return merged


def process_year(
    year,
    wrf_dir='/global/cfs/cdirs/m2702/gsharing/tgw-wrf-conus/historic_1980_2019/three_hourly',
    # 52 files per year so 13 divides them evenly into 4 chunks
    tasks=13,
    output_dir='./',
):
  start = time.time()
  print(f'OMP enabled: {wrf.omp_enabled()}, procs: {wrf.omp_get_num_procs()}')
  # The number of threads available to process
  wrf.omp_set_num_threads(16)
  wrf_files = sorted(glob(f'{wrf_dir}/*{year}*.nc'))
  data = Parallel(
      n_jobs=tasks,
      # prefer='threads',
  )(delayed(process_file)(f) for f in tqdm(wrf_files))
  # merge all the data into one xarray
  merged = xr.concat(data, dim='Time').drop_duplicates('Time')
  merged['Time'] = merged['Time'].astype(np.int64)
  # hold onto the projection information
  merged.attrs['projection'] = str(merged.attrs['projection'])
  del merged.attrs['description']
  del merged.attrs['units']
  for var in merged.data_vars:
    if 'projection' in merged[var].attrs:
      del merged[var].attrs['projection']
  # compression takes longer for little gain
  # merged.to_netcdf(f'{output_dir}/wrf_wind_{year}.nc',
  # encoding={var: dict(zlib=True, complevel=5) for var in merged.data_vars})
  merged.to_netcdf(f'{output_dir}/wrf_solar_{year}.nc')
  end = time.time()
  print(f'Total time: {end - start}s')


if __name__ == "__main__":
  year = sys.argv[1]
  wrf_dir = sys.argv[2]
  output_dir = sys.argv[3]

  print(f'Processing year {year}...')
  process_year(year, wrf_dir=wrf_dir, output_dir=output_dir)
