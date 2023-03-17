"""
Author: Scott Underwood

This script reads in the exisiting powercurve data from the EIA wind generator database
and back calculates for the constant k of a logistic growth equation, which
can be used to model wind turbine powercurves.
"""
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'


def determine_k_value(model_database):
    model_database = model_database[['model', 'rated_power', 'cut_in_speed', 'rated_speed', 'power_kw', 'wind_spd_ms']]

    ks = []
    for _, row in model_database.iterrows():
        if row['power_kw'] == row['power_kw']:
            powers = row['power_kw'][1:-1].split(',')
            speeds = row['wind_spd_ms'][1:-1].split(',')
            rated_power = float(row['rated_power'])
            cut_in = float(row['cut_in_speed'])
            rated_speed = float(row['rated_speed'])

            for i in range(len(powers)):
                speed = speeds[i].strip(' ')
                power = powers[i].strip(' ')
                if speed[0] == "'":
                    speed = speed[1:-1]
                    power = power[1:-1]
                speed = float(speed)
                power = float(power)
                x0 = (cut_in + rated_speed)/2  
                # only care about points within logistic region
                if speed != x0 and power!= 0 and power < rated_power and speed > cut_in and speed < rated_speed:
                    k = -np.log(rated_power/power - 1)/(speed - x0)
                    ks.append(k)

    k_avg = np.average(ks)
    
    return k_avg
