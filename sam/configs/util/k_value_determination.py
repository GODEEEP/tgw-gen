"""Author: Scott Underwood

This script reads in the exisiting powercurve data from the EIA wind generator database
and back calculates for the constant k of a logistic growth equation, which
can be used to model wind turbine powercurves.
"""
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'


#read in unformatted power curves (easier for this analysis)
power_curves = pd.read_csv('data/eia_power_curve.csv')

#read in models table and drop unnecessary columns
models = pd.read_csv('data/eia_models.csv')
models = models[['model', 'power_rated_power', 'power_cut_in_wind_speed', 'power_rated_wind_speed']]

#join on model name
power_curves = power_curves.join(models.set_index('model'), on = 'model')

#take out units from new columns and convert to float
power_curves['power_rated_power'] = power_curves['power_rated_power'].str.replace(' kW', '')
power_curves['power_rated_power'] = power_curves['power_rated_power'].str.replace(',', '').astype('float64')
power_curves['power_cut_in_wind_speed'] = power_curves['power_cut_in_wind_speed'].str.replace(' m/s', '')
power_curves['power_cut_in_wind_speed'] = power_curves['power_cut_in_wind_speed'].str.replace(',', '').astype('float64')
power_curves['power_rated_wind_speed'] = power_curves['power_rated_wind_speed'].str.replace(' m/s', '')
power_curves['power_rated_wind_speed'] = power_curves['power_rated_wind_speed'].str.replace(',', '').astype('float64')

ks = []
for index, row in power_curves.iterrows():
    speed = row['wind_spd_ms']
    power = row['power_kw']
    rated_power = row['power_rated_power']
    cut_in = row['power_cut_in_wind_speed']
    rated_speed = row['power_rated_wind_speed']
    x0 = (cut_in + rated_speed)/2  
    if speed != x0 and power!= 0 and power < rated_power and speed > cut_in and speed < rated_speed:
        k = -np.log(rated_power/power - 1)/(speed - x0)
        ks.append(k)

k_avg = np.average(ks)
print(k_avg)