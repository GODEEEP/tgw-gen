# TGW-Gen

Scripts for converting TGW (WRF) output to reV input files (wind and solar) and producing generation timeseries at points or grid cell locations.

```mermaid
flowchart TB

    subgraph godeeep[TGW-Gen]
        direction TB
    subgraph p0[ ]
        direction TB

        WRF[(TGW Data)]:::dataset
        timeseries[(Gen Timeseries)]:::dataset
        SAMH5[(SAM Resource Files)]:::dataset
        eia860[(EIA 860)]:::dataset
        ppconfigs{{Preprocess\nPlant Configurations}}:::interface
        plantconfig[(Plant configurations)]:::dataset
        biascorrect{{Bias Correction}}:::interface
        wrf2rev{{Preprocess\nTGW Data}}:::interface
        reV{{reV}}:::interface

        click reV "https://github.com/NREL/reV" _blank
        click biascorrect "https://github.com/GODEEEP/godeeep/blob/main/WRF-to-reV/bias_correct.py" _blank
        click wrf2rev "https://github.com/GODEEEP/godeeep/blob/main/WRF-to-reV/wrf2rev_solar_1h.py" _blank
        click tzconvert "https://github.com/GODEEEP/godeeep/blob/main/WRF-to-reV/data/convert_h5_to_mt.py" _blank

        subgraph workflow[ ]
        subgraph p1[ ]
            direction TB
            
            WRF-->wrf2rev
            wrf2rev-->SAMH5
            eia860-->ppconfigs
            ppconfigs-->plantconfig
            plantconfig-->reV
            SAMH5-->reV
            biascorrect-->timeseries
            reV-->biascorrect

        end
        end

        subgraph legend[ ]
        subgraph p2[ ]
            dataset[(dataset)]:::dataset
            interface{{interface}}:::interface
        end
        end

    end
    end

    class godeeep,workflow,legend title;
    class legend legend;
    class p0,p1,p2 padding;
    class godeeep,workflow bg;

    linkStyle default stroke:black,stroke-width:4;
    classDef marker stroke:black,fill:black;
    classDef default stroke:black,stroke-width:0,color:white,padding:10px 20px,font-size:48px,font-weight:bold;
    classDef bg fill:white,color:black,stroke:none;
    classDef legend fill:white,color:black,stroke:black;
    classDef title font-weight:bold,font-size:48px,line-height:48px;
    classDef padding stroke:none,fill:none;
    classDef dataset fill:#689c73;
    classDef interface fill:#e88824;
```

## Running the code 

The follwing steps will allow you to develop annual (8760) hourly solar and 
wind generation profiles from WRF output, and optionally format that output 
for GridView. 

### Setting up the wrf python environment



### Setting up the rev python environment

Before anything else, follow the instructions 
[here](https://github.com/NREL/reV#installing-rev) for
setting up a conda environment with the appropriate packages for running reV. 
Additionally you will need to install the 
[wrf-python](https://wrf-python.readthedocs.io/en/latest/installation.html) and
[farms](https://github.com/NREL/farms) packages. 
Other package requirements are included in the repo-wide `requirements.txt`.

### Metadata
The metadata (`sam/configs/eia_{wind,solar}_configs.csv`) contains information about each 
point location, name, lat, lon, elevation, etc. Metadata files are committed 
in this repo, but in case they need to be re-generated, please see the 
`eia_solar_config_generation.R` and `eia_wind_config_generation.py` file as well as the 
README in the `sam/configs` directory. 

### Process WRF data 
The `wrf2rev_*.py` code should be run on PIC where the WRF data is stored. 
It can't be run on constance because of issues with python and the older centos
version but it can be run on deception/slurm or constance7a/slurm7. The repo 
includes sample SLURM scripts (`run_*.sl`) for deception. See below for how to 
set up a conda environment before any runs are made. See comments in the 
scripts for more details on how to run. 

The code can be run locally as well but one year of 3 hour data is hundreds of 
GB, so for testing it is possible to run just using 1 or two week-long netcdf 
files. In this case the SAM resource file will not work because SAM requires an
entire year of data to run.   

The data will be output into `data/sam_resource/wrf_{wind,solar}_1h_{year}.h5`.

### Download NSRDB and (optionally) WTK data
NSRDB data is required for bias correcting the solar radiation data (GHI) and 
is also necessary for validation. To run wind validation you'll also need some 
WTK data. To get both NSRDB and WTK data you'll need to 
[signup](https://developer.nrel.gov/signup/) for an API key. Once you have 
that, create a file named `.env` in the `tgw-gen` directory. The file 
should have the following lines:

    nrel_api_key = 'key'
    nrel_api_email = 'email'
    
Now run the `download_nsrdb.R` and (optionally) `download_nsrdb.R`. The 
working directory should be set to `WRF-to-reV/validation`. This scripts call 
the API once per point per year. Note that the NREL api has a limit of 5000 
annual point location files per day, which could be an issue if you need many 
points and many years. These files take a few hours to download. 

### Bias correct the solar data 
In PNNL's WRF data, GHI has shown some consistent bias with respect to the 
NSRDB. To fix this, some bias correction is necessary. If the NSRDB data has 
been downloaded simply run the `bias_correct.py` script. This will modify the 
data in the SAM resource (hdf5) files directly. 

### Create generation profiles 
Now you are finally ready to run reV and create the generation profiles. The 
scripts `reV_solar.py` and `reV_wind.py` will create one csv file per year in 
`data/generation`. By default the scripts will output generation as a fraction
of total plant capacity. There is an option in the scripts to change the 
output to power (watts).

## Validation
There are several more scripts and reports related to validating the met and gen data, please see the `README.md` in in the `validation` directory for more details. 
