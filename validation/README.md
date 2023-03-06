# Validation of WRF solar and wind data against WTK and NSRDB

The purpose of this validation is to compare IM3 WRF simulations to publicky available datasets for wind (WTK) and solar (NSRDB). The code will ultimately produce one html report for each year of the validation period (2007 to 2014 for wind, 2007 to 2020 for solar).

## Download the data

To get both NSRDB and WTK data you'll need to [signup](%5Bhttps://developer.nrel.gov/signup/)](<https://developer.nrel.gov/signup/>)) for an API key. Once you have that, create a file named `.env` in the `WRF-to-reV` directory. The file should have the following lines:

    nrel_api_key = 'key'
    nrel_api_email = 'email'

Now run the `download_nsrdb.R` and (optionally) `download_nsrdb.R`. The working directory should be set to `WRF-to-reV/validation`. This scripts call the API once per point per year. Note that the NREL api has a limit of 5000 annual point location files per day, which could be an issue if you need many points and many years. These files take a few hours to download.

## Format the data

The scripts `format_nsrdb_for_rev.py` and `format_wtk_for_rev.py` will read the data downloaded from the NREL API and create hdf5 files in `../data/sam_resource`. These hdf5 files are formatted for use with reV. 

## Create power profiles 

The scripts `reV_solar_power.py` and `reV_wind_power.py` will create power profiles using generic plant configurations. One csv file will be created for each year and will be output into the `valid_data` directory. 

## Create validation reports

The reports are created based on the Rmarkdown files `validate_solar.Rmd` and `validate_wind.Rmd`. These files are designed to create one report per year. They can be run manually in RStudio, one year at a time, or you can use the `run_validation.R` script to run all years at once. 
