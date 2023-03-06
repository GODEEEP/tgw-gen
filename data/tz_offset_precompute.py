import xarray as xr
from utils import tz


grid = xr.open_dataset('data/grid.nc')
offset = tz.get_tz_offset_grid(grid.XLAT, grid.XLONG)
grid_with_offset = grid.assign(offset=offset)
grid_with_offset.to_netcdf('data/offset.nc')
