'''
Author: Scott Underwood

This script contains a function generate() which takes a desired number of turbines and a rotor
diameter as inputs and generates x and y coordinates for a rectangular wind farm layout of the 
desired size. The function returns the x and y coordinates as lists.
'''
import math


def generate_coordinates(n_turbines, rotor_diameter):
    spacing = 8 * rotor_diameter # taken from SAM default value
    offset = 4 * rotor_diameter # taken from SAM default value
    
    # layout will be rectangular, with num_rows = 1/2 num_cols
    num_rows = math.sqrt(1/2 * n_turbines)
    num_cols = num_rows * 2
    num_rows = int(num_rows) # round down for rows
    num_cols = round(num_cols) # round to nearest int for cols

    # initialize coordinate lists
    x_coords = []
    y_coords = []
    n = 0 # currently have added zero turbine coordinates

    y = 0 #y coordinate for first row is zero
    # loop through and add rows
    for r in range(num_rows):
        #determine if row is offset or not
        if r % 2 == 0:
            x = 0
        else:
            x = offset
        # loop through and add coords for each turbine
        for c in range(num_cols):
            x_coords.append(x)
            y_coords.append(y)
            n += 1
            x += spacing #increment x
        y += spacing #increment y
    
    # add rest of turbines to last row
    if num_rows % 2 == 0:
        x = 0
    else:
        x = offset
    while (n < n_turbines):
        x_coords.append(x)
        y_coords.append(y)
        n += 1
        x += spacing
    
    return x_coords, y_coords
