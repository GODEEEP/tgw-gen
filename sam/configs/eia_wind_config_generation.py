"""Author: Scott Underwood

This script reads in a power plant inventory, matches up the plants from the inventory
with their corresponding entry in the EIA database, and outputs a config file for each
generator that can be used in reV.
"""

from fuzzywuzzy import fuzz
from geopy.distance import geodesic
import json
import numpy as np
import os
import pandas as pd
from util.power_curve_generation import power_curve_generation
from util.coordinate_generation import generate_coordinates
from util.misc import dedup_names
pd.options.mode.chained_assignment = None  # default='warn'

WECC_ONLY = False  # switch for whether you want to filter to wecc only or not

# read in eia 'steel in ground' database to match up Generators
eia_gen = pd.read_excel('./eia8602020/3_2_Wind_Y2020.xlsx', index_col=False, skiprows=1)
eia_plant = pd.read_excel('./eia8602020/2___Plant_Y2020.xlsx', index_col=False, skiprows=1)
eia_plant = eia_plant[['Plant Code', 'NERC Region', 'Longitude', 'Latitude', 'Balancing Authority Code']]
eia_gen = eia_gen.join(eia_plant.set_index('Plant Code'), on='Plant Code')
# filter to wecc plants if desired
if (WECC_ONLY):
  eia_gen = eia_gen[eia_gen['NERC Region'] == 'WECC']  # only want WECC plants

eia_gen = eia_gen[~eia_gen.State.isin(['AK', 'HI'])]  # exclude hawaii and alaska

# get turbine specs for each generator
eia_gen = eia_gen[['Plant Code', 'Plant Name', 'Balancing Authority Code', 'NERC Region', 'State',
                   'Latitude', 'Longitude', 'Design Wind Speed (mph)', 'Turbine Hub Height (Feet)',
                   'Nameplate Capacity (MW)', 'Number of Turbines', 'Generator ID',
                   'Predominant Turbine Manufacturer', 'Predominant Turbine Model Number']]  # add turbine model
# convert eia_gen plant_id column to float
eia_gen = eia_gen.astype({'Plant Code': 'float64'})

# aggregate plants with same plant code and model/hub height

# Continue file generation using matched eia_gen, ignore unmatched for now
# add corresponding model/manufacturer names from eia models table using created model name key
eia_model_matching = pd.read_csv('./data/eia_wind_model_matching_full.csv', encoding='latin1')
eia_model_matching = eia_model_matching[['Predominant Turbine Manufacturer', 'Predominant Turbine Model Number',
                                        'id', 'model', 'manufacturer']]
# matching doesn't work for int
eia_gen['Predominant Turbine Model Number'] = eia_gen['Predominant Turbine Model Number'].replace(108, '108A')
eia_gen = eia_gen.merge(eia_model_matching, how='left',
                        on=['Predominant Turbine Manufacturer', 'Predominant Turbine Model Number'])


# add specs from models table to gens_with_models dataframe
eia_models = pd.read_csv('./data/eia_wind_models.csv')
eia_models = eia_models[['id', 'rotor_diameter', 'power_rated_power', 'power_cut_in_wind_speed',
                        'power_rated_wind_speed', 'power_cut_out_wind_speed']]
eia_gen = eia_gen.join(eia_models.set_index('id'), on='id')

# take out units from new columns and convert to float
eia_gen['power_rated_power'] = eia_gen['power_rated_power'].str.replace(' kW', '')
eia_gen['power_rated_power'] = eia_gen['power_rated_power'].str.replace(',', '').astype('float64')
eia_gen['power_cut_in_wind_speed'] = eia_gen['power_cut_in_wind_speed'].str.replace(' m/s', '')
eia_gen['power_cut_in_wind_speed'] = eia_gen['power_cut_in_wind_speed'].str.replace(',', '').astype('float64')
eia_gen['power_rated_wind_speed'] = eia_gen['power_rated_wind_speed'].str.replace(' m/s', '')
eia_gen['power_rated_wind_speed'] = eia_gen['power_rated_wind_speed'].str.replace(',', '').astype('float64')
eia_gen['power_cut_out_wind_speed'] = eia_gen['power_cut_out_wind_speed'].str.replace(' m/s', '')
eia_gen['power_cut_out_wind_speed'] = eia_gen['power_cut_out_wind_speed'].str.replace(',', '').astype('float64')
eia_gen['rotor_diameter'] = eia_gen['rotor_diameter'].str.replace(' m', '').astype(float)

# import power curve data formatted using power_curve_formatting.py
power_curves = pd.read_csv('./data/eia_power_curve_fixed.csv')

# add power curve data
power_curves.drop(columns=['manufacturer', 'source', 'Unnamed: 0'], inplace=True)
eia_gen = eia_gen.join(power_curves.set_index('model'), on='model')

# separate into eia_gen with power curve and eia_gen without
gens_with_powercurve = eia_gen[eia_gen['powers'].notnull()]
gens_without_powercurve = eia_gen[eia_gen['powers'].isnull()]

# call function to generate power curves, drop duplicate model entries
generated_power_curves = power_curve_generation(gens_without_powercurve)
generated_power_curves.drop_duplicates(subset=['model'], inplace=True)

# drop empty power and speed columns from gens_without_powercurve table and join the generated powercurves
gens_without_powercurve.drop(columns=['powers', 'wind_speeds'], inplace=True)
gens_without_powercurve = gens_without_powercurve.join(generated_power_curves.set_index('model'), on='model')

# add the generated powercurves to the existing power curves to recombine the tables
eia_gen = pd.concat([gens_with_powercurve, gens_without_powercurve])

# import coordinate data formatted from coordinate_formatting.py
coords = pd.read_csv('./data/eia_eerscmap_fixed.csv')
coords.drop(columns=['Unnamed: 0'], inplace=True)
coords = coords.rename(columns={'id': 'Plant Code'})

# join coords table to eia_gen table on Plant Code
eia_gen = eia_gen.astype({'Plant Code': 'int64'})  # change to int
eia_gen = eia_gen.join(coords.set_index('Plant Code'), on='Plant Code')

# Some of the plants don't have the exact right number of coordinates, leading to a
# discrepancy between 'effective capacity' defined as the number of turbines (or
# coordinate sets) * the rated power of each turbine and the 'actual capacity' (
# the capacity in the hydrowires database)
# to account for this, we will filter out the coordinate sets with too large of a
# discrepancy between the effective and actual capacity and generate the coordinate
# sets ourselves to match capacity for those
eia_gen['cap_pct_difference'] = (eia_gen['power_rated_power']/1000 * eia_gen['num_coords'] -
                                 eia_gen['Nameplate Capacity (MW)']) / eia_gen['Nameplate Capacity (MW)']
complete_gens = eia_gen[(eia_gen.cap_pct_difference > -.05) & (eia_gen.cap_pct_difference <= 0)]

# get gens that don't have the correct number of coordinates and generate those coordinates
incomplete_gens = eia_gen[(eia_gen.cap_pct_difference <= -.05) |
                          (eia_gen.cap_pct_difference > 0) | (eia_gen.cap_pct_difference.isnull())]
incomplete_gens['num_turbines_needed'] = incomplete_gens['Nameplate Capacity (MW)'] * \
    1000 / incomplete_gens['power_rated_power']
incomplete_gens.drop(columns=['x_coords', 'y_coords', 'cap_pct_difference'], inplace=True)

# PYSAM can only handle coordinate sets of size 300 or less - we have a few sites that are larger than that.
# So, we will split those plants evenly in half
new_rows = []
for index, row in incomplete_gens.iterrows():
  if row['num_turbines_needed'] > 300:
    total_cap = row['Nameplate Capacity (MW)']
    num_turbs = int(row['num_turbines_needed'])  # round down
    plant_id = row['Plant Code']
    print(f'splitting plant {plant_id}')
    new_row = row.copy()  # initialize new row
    incomplete_gens.loc[index, 'Nameplate Capacity (MW)'] = total_cap / 2
    incomplete_gens.loc[index, 'num_turbines_needed'] = num_turbs / 2
    # incomplete_gens.loc[index, 'Plant Code'] = plant_id * 1000
    new_row['Nameplate Capacity (MW)'] = total_cap / 2
    new_row['num_turbines_needed'] = num_turbs - num_turbs / 2  # rest of turbines
    # new_row['Plant Code'] = plant_id * 1000 + 1
    new_rows.append(new_row.values)

incomplete_gens = pd.concat([incomplete_gens, pd.DataFrame(new_rows, columns=incomplete_gens.columns)]).reset_index()

# use generate coords function to get coordinates for incomplete gens
x_coords = []
y_coords = []

for index, row in incomplete_gens.iterrows():
  n_turbines = int(row['num_turbines_needed'])  # round down
  n_turbines = max(n_turbines, 1)  # must have at least one turbine
  rotor_diameter = row['rotor_diameter']
  x, y = generate_coordinates(n_turbines, rotor_diameter)
  x_coords.append(x)
  y_coords.append(y)

coords = pd.DataFrame(list(zip(x_coords, y_coords)), columns=['x_coords', 'y_coords'])
incomplete_gens = pd.concat([incomplete_gens, coords], axis=1)

# because of some weird json.loads caveat, write table to csv and then load it in
# if we don't do this, we get an error that json.loads can't read a list, but doing this fixes the issue
incomplete_gens.to_csv('util/temp2.csv', index=False)
incomplete_gens = pd.read_csv('util/temp2.csv')

eia_gen = pd.concat([complete_gens, incomplete_gens], ignore_index=True)

# renname columns to sam_config input names
ft_to_m = 0.3048
# convert to meters, used thh_feet column from eia eia_gen
# first fill empty columns with default value of 100 m
eia_gen['thh_m'] = eia_gen['Turbine Hub Height (Feet)'].astype(float) * ft_to_m
# table but eia_models table also has hub height column which could be used instead
mw_to_kw = 1000
eia_gen['Nameplate Capacity (kW)'] = eia_gen['Nameplate Capacity (MW)'].astype(float) * mw_to_kw  # convert mw to kw
# convert power curves to string
eia_gen['powers'] = eia_gen['powers'].astype(str)
eia_gen['wind_speeds'] = eia_gen['wind_speeds'].astype(str)
# convert generator key to int
eia_gen['Plant Code'] = eia_gen['Plant Code'].astype(int)

eia_gen.rename(columns={'thh_m': 'wind_turbine_hub_ht', 'powers': 'wind_turbine_powercurve_powerout',
                        'wind_speeds': 'wind_turbine_powercurve_windspeeds',
                        'rotor_diameter': 'wind_turbine_rotor_diameter', 'Nameplate Capacity (kW)': 'system_capacity',
                        'x_coords': 'wind_farm_xCoordinates', 'Latitude': 'lat', 'Longitude': 'lon', 'y_coords':
                        'wind_farm_yCoordinates', 'Balancing Authority Code': 'ba', 'NERC Region': 'nerc_region',
                        'Plant Code': 'plant_code', 'State': 'state', 'Generator ID': 'generator_id'}, inplace=True)
# add a column of unique plant codes that correspond to the gen data
eia_gen['plant_code_unique'] = dedup_names(eia_gen['plant_code'])

# choose only desired outputs and create output csv file
output = eia_gen[['plant_code', 'plant_code_unique', 'generator_id', 'lat', 'lon', 'ba', 'nerc_region', 'state', 'system_capacity',
                  'wind_farm_xCoordinates', 'wind_farm_yCoordinates',
                  'wind_turbine_hub_ht', 'wind_turbine_powercurve_powerout',
                  'wind_turbine_powercurve_windspeeds', 'wind_turbine_rotor_diameter']]
output.sort_values(by='plant_code', inplace=True)
# add generic constants
output['wind_resource_shear'] = 0.14
output['wind_resource_turbulence_coeff'] = 0.1
output['wind_resource_model_choice'] = 0
output['wind_farm_wake_model'] = 0
output['turb_generic_loss'] = 15
output['adjust:constant'] = 0
output.to_csv('eia{wecc}_wind_configs.csv'.format(wecc='_wecc' if WECC_ONLY else ''), index=False)
