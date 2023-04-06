# reV Config File Generation
Scripts for generating .csv configuration files of EIA wind and solar generators for use in reV 

## Generating the config files
Run the scripts `eia_solar_config_generation.py` and `eia_wind_config_generation.py`, which will 
create output files `eia_solar_configs.csv` and `eia_wind_configs.csv`, respectively. Each 
script has a variable WECC_ONLY which, when set to True, will filter to only generators in 
WECC, and names the output files accordingly (`eia_wecc_solar_configs.csv` and `eia_wecc_wind_configs.csv`).

## Methodology for processing the data
* Import EIA generator inventory for initialization year
   * Download EIA Form-860 data and unzip folder
   * Filter to WECC only (if desired) and contiguous US 

### Solar specific steps
* Determine array type, module type, tilt angle, and azimuth angle
   * Array Type
      * Plants listed with a "Y" in the "Fixed Tilt?" column are classified as fixed open rack arrays
      * Plants listed with a "Y" in the "Single-Axis Tracking?" are classified as 1-axis tracking arrays
      * Plants listed with a "Y" in the "Dual-Axis Tracking?" are classified as 2-axis tracking arrays
      * Any plants without a listed array type are classified as fixed open rack arrays
   * Azimuth Angle
      * For any plant with a positive "Azimuth Angle", the value was used as is 
      * For any plant with a negative "Azimuth Angle", the value was assumed to be an entry error, in which case the absolute value was used
      * For any plant without a listed "Azimuth Angle", the values was assumed to be 180
   * Module Type
      * Plants listed with a "Y" in the "Thin-Film (CdTe)?", "Thin-Film (A-Si)?", "Thin-Film (CIGS)?", or "Thin-Film (Other)?" column are classified as thin film modules
      * Any other plants are classified as standard modules
   * Tilt Angle
      * For any plant with a positive "Tilt Angle", the value was used as is
      * For any plant with a negative "Tilt Angle", the value was assumed to be an entry error, in which case the absolute value was used
      * For any plant without a listed "Tilt Angle", the values was assumed to be the latitude of the plant
* A csv file with all required input parameters is generated that contains one generator per row
   * The constant value "losses" is set to be 14.
* The csv file is named `eia_{wecc}_solar_configs.csv` and is saved to the root directory

### Wind specific steps
* Specifications of turbine models and wind turbine coordinate sets for each generator are pulled from the web using `turbine_database_generation.py` and output into `turbine_model_database.csv` and `turbine_coordinate_database.csv`, respectively
* The naming conventions for turbine models between `turbine_model_database.csv` and the EIA wind generator database are 
inconsistent
   * To fix this, a turbine model naming key `turbine_model_matching.csv` was developed manually, which matches up the 
   turbine model names between the two databases
* Join the turbine model database to the EIA wind generator database using the model naming key to obtain turbine specifications
* Some turbines do not have powercurves available - powercurves are created for generators without them using the utility script `power_curve_generation.py`
* Join the turbine coordinate database using the plant code
   * Some generators do not have the correct number of coordinates in the coordinate database
   * To fix this, a new coordinate set is developed for generators that do not meet the relationship, 
   where the left side of the equation must be between 95 and 100 percent of the nameplate capacity

   $$P_{rated,turbine} * n_{coords} \approx Cap_{nameplate}$$
   
   * New coordinate sets are developed for the above plants using the utility script `coordinate_generation.py`
* reV can only handle generators with 300 coordinates or less - generators with more than 300 coordinates are split evenly into two entries
* A csv file with all required input parameters is generated that contains one generator per row
   * The constant values "wind_resource_shear", "wind_resource_turbulence_coeff", and "turb_generic_loss" are set to be 0.14, 0.1, and 15, respectively.
* The csv file is named `eia_{wecc}_wind_configs.csv` and is saved to the root directory
