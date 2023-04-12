"""
Author: Scott Underwood

This script contains a function that generates a power curve for a given turbine
using its cut in, cut out, and rated wind speeds along with rated power.
"""
import math
import numpy as np
import pandas as pd


def power_curve_generation(input_data, k):
    """
    Takes in turbine models that don't have an existing power curve, generates a power curve
    using the cut in, cut out and rated wind speeds along with the rated power, and outputs a 
    dataframe having the generated power curves as lists in a wind_speeds column and a powers column.
    """

    models = []
    wind_speeds = []
    powers = []

    for _, row in input_data.iterrows():
        model = row['model']
        wind_speed = []
        power = []
        cut_in = float(row['cut_in_speed'])
        rated_speed = float(row['rated_speed'])
        cut_out = float(row['cut_out_speed'])
        rated_power = float(row['rated_power'])
        model_data = pd.read_csv('./data/turbine_model_database.csv')

        # generate defaults for cut_in, cut_out, rated speeds based on averages if they are missing
        if cut_in != cut_in:
            cut_in = model_data['cut_in_speed'].dropna().mean()
        if cut_out != cut_out:
            cut_out = model_data['cut_out_speed'].dropna().mean()
        if rated_speed != rated_speed:
            rated_speed = model_data['rated_speed'].dropna().mean()

        # iterate through wind speeds 0 to 40 mph to generate corresponding power
        for speed in np.arange(0,40,0.5):
            if speed < cut_in: # power is zero when less than cut in wind speed
                p = 0
            elif cut_in <= speed < rated_speed: # power generally follows a logistic growth equation
                midpoint = float((cut_in + rated_speed)/2) # midpoint of curve is halfway between cut in and rated speeds
                p = rated_power / (1 + math.exp(-1 * k * (speed - midpoint))) # logistic growth equation
            elif rated_speed <= speed <= cut_out: # power is rated power if wind speed is more than rated speed and less than cut out
                p = rated_power
            elif speed > cut_out: # power is zero when greater than cut out wind speed
                p = 0
            wind_speed.append(speed)
            power.append(p)
        models.append(model)
        wind_speeds.append(wind_speed)
        powers.append(power)

    generated_curves = pd.DataFrame(list(zip(models, wind_speeds, powers)), columns = ['model', 'wind_spd_ms', 'power_kw'])
    
    return generated_curves
