"""
Author: Scott Underwood

This script reads in the EIA wind power plant inventory, 
and outputs a config file for each generator that can be 
used in reV.
"""

import pandas as pd
import requests
from util.coordinate_generation import generate_coordinates
from util.k_value_determination import determine_k_value
from util.misc import dedup_names
from util.power_curve_generation import power_curve_generation
import yaml
pd.options.mode.chained_assignment = None  # default='warn'

WECC_ONLY = False  # switch for whether you want to filter to wecc only or not


# read in eia 'steel in ground' database to match up Generators
gens = pd.read_excel('./eia8602020/3_2_Wind_Y2020.xlsx', index_col=False, skiprows=1)
plants = pd.read_excel('./eia8602020/2___Plant_Y2020.xlsx', index_col=False, skiprows=1)
plants = plants[['Plant Code', 'NERC Region', 'Longitude', 'Latitude', 'Balancing Authority Code']]
gens = gens.join(plants.set_index('Plant Code'), on='Plant Code')
# filter to wecc plants if desired
if (WECC_ONLY):
  gens = gens[gens['NERC Region'] == 'WECC']  # only want WECC plants

gens = gens[~gens.State.isin(['AK', 'HI'])]  # exclude hawaii and alaska

# get turbine specs for each generator
gens = gens[['Plant Code', 'Plant Name', 'Balancing Authority Code', 'NERC Region', 'State',
                   'Latitude', 'Longitude', 'Design Wind Speed (mph)', 'Turbine Hub Height (Feet)',
                   'Nameplate Capacity (MW)', 'Number of Turbines', 'Generator ID',
                   'Predominant Turbine Manufacturer', 'Predominant Turbine Model Number']]
# convert gens plant_id column to float
gens = gens.astype({'Plant Code': 'float64'})

# Continue file generation using matched gens, ignore unmatched for now
# add corresponding model/manufacturer names from eia models table using created model name key
model_matching = pd.read_csv('./data/turbine_model_matching.csv')
model_matching = model_matching[['Predominant Turbine Manufacturer', 'Predominant Turbine Model Number',
                                        'model', 'manufacturer']]
# matching doesn't work for int
gens['Predominant Turbine Model Number'] = gens['Predominant Turbine Model Number'].replace(108, '108A')
gens = gens.merge(model_matching, how='left',
                        on=['Predominant Turbine Manufacturer', 'Predominant Turbine Model Number'])

# get turbine rotor diameter and power curve
turbine_models = pd.read_csv('./data/turbine_model_database.csv')

# fix one incorrect entry identified (Unison U57 rated speed)
i = turbine_models.index[turbine_models['model'] == 'U57'].tolist()[0]
turbine_models.loc[i,'rated_speed'] = 11 # from manufacturer website

# add one missing power curve that we found from 
# https://github.com/PyPSA/atlite/tree/master/atlite/resources/windturbine
yml_txt = requests.get('https://raw.githubusercontent.com/PyPSA/atlite/master/atlite/resources/windturbine/Vestas_V90_3MW.yaml').text
yml = yaml.safe_load(yml_txt)
i = turbine_models.index[turbine_models['model'] == 'V90-3.0'].tolist()[0]
turbine_models.loc[i,'wind_spd_ms'] = str(yml['V'])
yml_power_kw = [p*1000 for p in yml['POW']] # convert to kw
turbine_models.loc[i,'power_kw'] = str(yml_power_kw)

# if there are duplicate entries for a model, only keep one with powercurve
turbine_models = turbine_models.sort_values(by="power_kw", na_position='last').drop_duplicates(subset = ['manufacturer','model'], keep = 'first').sort_values(by='model').sort_values(by='manufacturer')
gens = gens.merge(turbine_models, how='left', on=['manufacturer', 'model'])

# separate into gens with power curve and gens without
gens_with_powercurve = gens[gens['power_kw'].notnull()]
gens_without_powercurve = gens[gens['power_kw'].isnull()]

# call function to generate power curves, drop duplicate model entries
k = determine_k_value(turbine_models)
generated_power_curves = power_curve_generation(gens_without_powercurve, k)
generated_power_curves.drop_duplicates(subset=['model'], inplace=True)

# drop empty power and speed columns from gens_without_powercurve table and join the generated powercurves
gens_without_powercurve.drop(columns=['power_kw', 'wind_spd_ms'], inplace=True)
gens_without_powercurve = gens_without_powercurve.join(generated_power_curves.set_index('model'), on='model')

# add the generated powercurves to the existing power curves to recombine the tables
gens = pd.concat([gens_with_powercurve, gens_without_powercurve])

# import coordinate data formatted from coordinate_formatting.py
coords = pd.read_csv('./data/turbine_coordinate_database.csv')
coords = coords.rename(columns={'id': 'Plant Code'})

# join coords table to gens table on Plant Code
gens = gens.astype({'Plant Code': 'int64'})  # change to int
gens = gens.join(coords.set_index('Plant Code'), on='Plant Code')

# Some of the plants don't have the exact right number of coordinates, leading to a
# discrepancy between 'effective capacity' defined as the number of turbines (or
# coordinate sets) * the rated power of each turbine and the 'actual capacity' (
# the capacity in the hydrowires database)
# to account for this, we will filter out the coordinate sets with too large of a
# discrepancy between the effective and actual capacity and generate the coordinate
# sets ourselves to match capacity for those
gens['cap_pct_difference'] = (gens['rated_power']/1000 * gens['num_coords'] -
                                 gens['Nameplate Capacity (MW)']) / gens['Nameplate Capacity (MW)']
complete_gens = gens[(gens.cap_pct_difference > -.05) & (gens.cap_pct_difference <= 0)]

# get gens that don't have the correct number of coordinates and generate those coordinates
incomplete_gens = gens[(gens.cap_pct_difference <= -.05) |
                          (gens.cap_pct_difference > 0) | (gens.cap_pct_difference.isnull())]
incomplete_gens['num_turbines_needed'] = incomplete_gens['Nameplate Capacity (MW)'] * \
    1000 / incomplete_gens['rated_power']
incomplete_gens.drop(columns=['x_coords', 'y_coords', 'cap_pct_difference'], inplace=True)

# PYSAM can only handle coordinate sets of size 300 or less - we have a few sites that are larger than that.
# So, we will split those plants evenly in half
new_rows = []
for index, row in incomplete_gens.iterrows():
  if row['num_turbines_needed'] > 300:
    total_cap = row['Nameplate Capacity (MW)']
    n_turbs = int(row['num_turbines_needed'])  # round down
    plant_id = row['Plant Code']
    print(f'splitting plant {plant_id}')
    new_row = row.copy()  # initialize new row
    incomplete_gens.loc[index, 'Nameplate Capacity (MW)'] = total_cap / 2
    incomplete_gens.loc[index, 'num_turbines_needed'] = n_turbs / 2
    new_row['Nameplate Capacity (MW)'] = total_cap / 2
    new_row['num_turbines_needed'] = n_turbs - n_turbs / 2  # rest of turbines
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

gens = pd.concat([complete_gens, incomplete_gens], ignore_index=True)
gens.drop(columns=['index', 'num_turbines_needed', 'cap_pct_difference', 'num_coords', 'source'], inplace=True)

# renname columns to sam_config input names
ft_to_m = 0.3048
# convert to meters, used thh_feet column from eia gens
# first fill empty columns with default value of 100 m
gens['Turbine Hub Height (m)'] = gens['Turbine Hub Height (Feet)'].astype(float) * ft_to_m
# table but models table also has hub height column which could be used instead
mw_to_kw = 1000
gens['Nameplate Capacity (kW)'] = gens['Nameplate Capacity (MW)'].astype(float) * mw_to_kw  # convert mw to kw
# convert power curves to string
gens['power_kw'] = gens['power_kw'].astype(str)
gens['wind_spd_ms'] = gens['wind_spd_ms'].astype(str)
# convert generator key to int
gens['Plant Code'] = gens['Plant Code'].astype(int)

gens.rename(columns={'Turbine Hub Height (m)': 'wind_turbine_hub_ht', 'power_kw': 'wind_turbine_powercurve_powerout',
                        'wind_spd_ms': 'wind_turbine_powercurve_windspeeds',
                        'rotor_diameter': 'wind_turbine_rotor_diameter', 'Nameplate Capacity (kW)': 'system_capacity',
                        'x_coords': 'wind_farm_xCoordinates', 'Latitude': 'lat', 'Longitude': 'lon', 'y_coords':
                        'wind_farm_yCoordinates', 'Balancing Authority Code': 'ba', 'NERC Region': 'nerc_region',
                        'Plant Code': 'plant_code', 'State': 'state', 'Generator ID': 'generator_id'}, inplace=True)
# add a column of unique plant codes that correspond to the gen data
gens['plant_code_unique'] = dedup_names(gens['plant_code'])

# choose only desired outputs and create output csv file
output = gens[['plant_code', 'plant_code_unique', 'generator_id', 'lat', 'lon', 'ba', 'nerc_region', 'state', 'system_capacity',
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
