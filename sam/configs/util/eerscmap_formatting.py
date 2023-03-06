"""Author: Scott Underwood

This script takes the eia wind farm turbine coordinates file (eia_eerscmap.csv) and
reformats the data from single entries for coordinate set for a generator to a list
of coordinates for each generator.
"""
import os
import pandas as pd
from pyproj import Transformer

map = pd.read_csv('data/eia_eerscmap.csv')
map = map[['eia_id', 'p_name', 'xlong', 'ylat']]

#join plants table and filter to only WECC plants (to speed up processing)
plants = pd.read_csv('data/eia_plants.csv')
plants = plants[['plant_code', 'nerc_region']]
map = map.join(plants.set_index('plant_code'), on='eia_id')
map = map[map.nerc_region == 'WECC']

#set up transformer to go from lat-lon to projected coordinates
transformer = Transformer.from_crs('epsg:4326', 'epsg:2163', always_xy = True)

ids = []
names = []
x_coords = []
y_coords = []
ids = []
num_coords_list = []

#loop through plants and combine x, y coordinates into lists of coordinates rather than single rows
for index, row in map.iterrows():
    id = row['eia_id']
    name = row['p_name']
    x_coord = []
    y_coord = []
    #if this plant and model has been added already, skip over it
    if id in ids:
        pass
    #otherwise, add the new plant/model combo and the coordinates
    else:
        ids.append(id)
        num_coords = 0
        for index2, row2 in map.iterrows():
            if id == row2['eia_id']:
                x_coord.append(row2['xlong'])
                y_coord.append(row2['ylat'])
                num_coords += 1
        names.append(name)
        x, y = transformer.transform(x_coord, y_coord)
        x_coords.append(x)
        y_coords.append(y)
        num_coords_list.append(num_coords)

coords = pd.DataFrame(list(zip(ids, x_coords, y_coords, num_coords_list)),
                                columns = ['id', 'x_coords', 'y_coords', 'num_coords'])
coords.to_csv('data/eia_eerscmap_fixed.csv')
