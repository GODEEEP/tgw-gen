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

#wrf_dir = '/global/cfs/cdirs/m2702/gsharing/tgw-wrf-conus/historical_1980_2019/three_hourly'
#output_dir = '/global/cfs/cdirs/m2702/gsharing/solar-wind/met_data_fullgrid/historical'


def magnitude(a, b):
  def func(x, y): return np.sqrt(x**2 + y**2)
  return xr.apply_ufunc(func, a, b)


def direction(a, b):
  def func(x, y): return np.mod((180 + 180/np.pi * np.arctan2(y, x)), 360)
  return xr.apply_ufunc(func, a, b)


def process_file(f, heights):
  ds = Dataset(f)
  data = {
      'ua': None,
      'va': None,
      'tc': None,
      'pressure': None,
  }
  cache = wrf.extract_vars(
      ds,
      wrf.ALL_TIMES,
      # why all these variables?
      ("P", "PSFC", "PB", "PH", "PHB", "T", "QVAPOR", "HGT", "U", "V", "W")
  )
  for v in data.keys():
    # what does cache do?
    var = wrf.getvar(wrfin=ds, varname=v, squeeze=False, timeidx=wrf.ALL_TIMES, cache=cache)
    data[v] = wrf.vinterp(
        wrfin=ds,
        field=var,
        vert_coord="ght_agl",
        interp_levels=heights,
        extrapolate=True,
        timeidx=wrf.ALL_TIMES,
        squeeze=False,
        cache=cache
    )
  hourly = pd.date_range(data['ua'].Time[0].values, data['ua'].Time[-1].values, freq='H')
  merged = xr.merge([v for v in data.values()])
  # so much easier than how I did it
  merged = merged.interp(Time=hourly, assume_sorted=True)
  merged['windspeed'] = magnitude(merged['ua'], merged['va'])
  merged['winddirection'] = direction(merged['ua'], merged['va'])
  merged['pressure'] = merged['pressure'] * 100.0
  merged = merged.rename({
      'temp': 'temperature'
  })
  merged = merged.drop_vars(['ua', 'va'])
  merged = merged.round(2).astype(np.float32)
  return merged


def process_year(
    year,
    wrf_dir='/global/cfs/cdirs/m2702/gsharing/tgw-wrf-conus/historical_1980_2019/three_hourly/',
    heights=[0.020, 0.080, 0.110, 0.140, 0.200],
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
  )(delayed(process_file)(f, heights) for f in tqdm(wrf_files))
  # ?
  merged = xr.concat(data, dim='Time').drop_duplicates('Time')
  merged['Time'] = merged['Time'].astype(np.int64)
  merged.attrs['projection'] = str(merged.attrs['projection'])
  del merged.attrs['description']
  del merged.attrs['units']
  for var in merged.data_vars:
    if 'projection' in merged[var].attrs:
      del merged[var].attrs['projection']
  # compression takes longer for little gain
  # merged.to_netcdf(f'{output_dir}/wrf_wind_{year}.nc',
  # encoding={var: dict(zlib=True, complevel=5) for var in merged.data_vars})
  merged.to_netcdf(f'{output_dir}/wrf_wind_{year}.nc')
  end = time.time()
  print(f'Total time: {end - start}s')


if __name__ == "__main__":
  year = sys.argv[1]
  wrf_dir = sys.argv[2]
  output_dir = sys.argv[3]

  print(f'Processing year {year}...')
  process_year(year, wrf_dir=wrf_dir, output_dir=output_dir)
