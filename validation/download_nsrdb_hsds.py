import h5pyd
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from scipy.spatial import cKDTree
from itertools import repeat
from tqdm import tqdm
import os

out_dir = 'valid_data/nsrdb_hsds_eia_wecc'
vars = ['ghi']  # , 'dni', 'dhi']
years = list(range(1998, 2020+1))

meta = pd.read_csv('../data/meta_eia_wecc_solar.csv')

for year in years:

  print(year)

  f = h5pyd.File(f"/nrel/nsrdb/v3/nsrdb_{year}.h5", 'r')
  # list(f) # show the available variables

  dset_coords = f['coordinates'][...]
  tree = cKDTree(dset_coords)

  time_index = pd.to_datetime(f['time_index'][...].astype(str))

  def nearest_site(tree, lat_coord, lon_coord):
    lat_lon = np.array([lat_coord, lon_coord])
    dist, pos = tree.query(lat_lon)
    return pos

  args_iter = zip(repeat(tree), list(meta.latitude), list(meta.longitude))
  idxs = list(map(nearest_site, repeat(tree), list(meta.latitude), list(meta.longitude)))

  for var in vars:

    print(var)

    out_fn = os.path.join(out_dir, f'{var}_{year}.csv')
    # skip over if the data exists
    if os.path.exists(out_fn):
      continue

    dset = f[var]
    tseries = []

    for idx in tqdm(idxs):
      tseries.append(dset[:, idx] / dset.attrs['psm_scale_factor'])

    var_data = pd.DataFrame(np.column_stack(tseries), index=time_index, columns=meta.generator_key)
    var_data.to_csv(out_fn)
