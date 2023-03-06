"""Author: Scott Underwood

This script takes the eia turbine power curves file (eia_eerscmap.csv) and
reformats the data from single entries for a given wind speed/power for each
generator to a list of wind speeds/powers for each generator.
"""
import pandas as pd

#add power curve data from power_curve table
eia_power_curve = pd.read_csv('data/eia_power_curve.csv')
#turn powercurve data from single row entries to the entire curve in a list
wind_speeds = []
powers = []
manufacturers = []
models = []
manufacturer_models = []
sources = []
#iterate rows to develop power curve for each model
for index, row in eia_power_curve.iterrows():
    manufacturer = row['manufacturer']
    model = row['model']
    wind_speed = []
    power = []
    source = row['source']
    manufacturer_model = (manufacturer, model)
    #check if this model has been done already, if so pass
    if manufacturer_model in manufacturer_models:
        pass
    #if not, iterate through to get list of speeds and powers, then add this model to list of models
    else:
        manufacturer_models.append(manufacturer_model)
        for index2, row2 in eia_power_curve.iterrows():
            if row2['manufacturer' ] == manufacturer and row2['model'] == model:
                wind_speed.append(row2['wind_spd_ms'])
                power.append(row2['power_kw'])
        models.append(model)
        manufacturers.append(manufacturer)
        wind_speeds.append(wind_speed)
        powers.append(power)
        sources.append(source)

#create dataframe of powers
power_curves = pd.DataFrame(list(zip(manufacturers, models, wind_speeds, powers, sources)), columns = ['manufacturer', 'model', 'wind_speeds', 'powers', 'source'])
power_curves.to_csv('data/eia_power_curve_fixed.csv')
