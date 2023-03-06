cat("
This script will download NSRDB data from the NREL API. 
This requires you to have an NREL API key:

https://developer.nrel.gov/signup/

Once you have that, create a file named `.env` in the `WRF-to-reV` directory. 
The file should have the following lines:

    nrel_api_key = 'key'
    nrel_api_email = 'email'

Adjust the valid years based on how much data you want to download. See the 
API reference for available years:

    https://developer.nrel.gov/docs/solar/nsrdb/psm3-download/
    
@author = Cameron Bracken (cameron.bracken@pnnl.gov)
")

xfun::pkg_load2('readr','hdf5r','dplyr','lubridate', 'tidyr', 'hydroGOF', 'zoo')

import::from(readr, read_csv, write_csv)
import::from(hdf5r, H5File)
import::from(dplyr, rename, mutate, bind_rows, select, summarise, group_by, inner_join,
             filter, left_join, full_join, ungroup)
import::from(lubridate, hours, minutes, ymd_hms, month, day, hour, with_tz)
import::from(tidyr, pivot_longer, fill)
import::from(tibble, tibble, rownames_to_column)
import::from(hydroGOF, NSE, me, mae, pbias, nrmse, rSD, KGE)
import::from(rgdal, readOGR)
import::from(zoo, na.approx)
import::from(R.utils, withTimeout)


# valid_years = 2007:2020
valid_years = 1998:2020
valid_data_dir = 'valid_data/nsrdb_eia'
cache_dir = 'valid_data/cache_eia'

readRenviron('../.env')

# pv_plants = read_csv('../data/meta_eia_wecc_solar.csv', show=FALSE, progress=FALSE) |> 
#   #tibble::rownames_to_column(var = 'site_id') |>
#   mutate(site_id=generator_key)

pv_plants = read_csv('../sam/configs/eia_solar_configs.csv', show=FALSE, progress=FALSE) 


nsrdb_point <- function(lat, lon, year){
    "
    Extract data for a single point, for one year, from the NSRDB.

    Example:
    https://developer.nrel.gov/docs/solar/nsrdb/python-examples/

    API reference:
    https://developer.nrel.gov/docs/solar/nsrdb/psm3-download/
    "
  
  # lat, lon, year = 43, -120, 1998
  # You must request an NSRDB api key from the link above
  api_key = Sys.getenv('nrel_api_key')
  email = Sys.getenv('nrel_api_email')
  
  # Set the attributes to extract (dhi, ghi, etc.), separated by commas.
  # https://developer.nrel.gov/docs/solar/nsrdb/psm3-download/
  attributes = paste0('ghi,dhi,dni,wind_speed,wind_direction,air_temperature,',
                      'solar_zenith_angle,surface_pressure')
  # leap='true' will return leap day data if present, false will not.
  # Set time interval in minutes, Valid intervals are 30 & 60.
  interval = '60'
  
  # Declare url string
  url = sprintf(paste0('https://developer.nrel.gov/api/nsrdb/v2/solar/',
                       'psm3-download.csv?wkt=POINT(%s%%20%s)&names=%s&',
                       'leap_day=%s&interval=%s&utc=%s&email=%s&',
                       'api_key=%s&attributes=%s'), lon, lat, year, 'true', interval,
                'true', email, api_key, attributes)
  # Return just the first 2 lines to get metadata:
  # info = pd.read_csv(url, nrows=1)
  # See metadata for specified properties, e.g., timezone and elevation
  # timezone, elevation = info['Local Time Zone'], info['Elevation']
  
  # Return all but first 2 lines of csv to get data:
  read_csv(url, skip = 2, show=FALSE) |> 
    mutate(datetime = ISOdatetime(Year, Month, Day, Hour, Minute, 0, tz='UTC'))
}

for(valid_year in valid_years){
  
  nsrdb_cache_fn = sprintf('%s/nsrdb_list_%s.rds',cache_dir,valid_year)
  
  nsrdb_list = list()
  
  if(file.exists(nsrdb_cache_fn)){
    nsrdb_list = readRDS(nsrdb_cache_fn)
  }else{
    
    for (pointi in 1:nrow(pv_plants)){
      lat = pv_plants$lat[pointi]
      lon = pv_plants$lon[pointi]
      plant_code = pv_plants$plant_code[pointi]
      message(valid_year, ' ', pointi, ' ', plant_code, ' ', lat,' ', lon)
      csv_fn = file.path(valid_data_dir, sprintf('nsrdb_%s_%04d.csv',valid_year, plant_code))
      if(file.exists(csv_fn)){
        nsrdb_list[[pointi]] = read_csv(csv_fn, show=FALSE, progress=FALSE)
      }else{
        res = try({
          withTimeout({
            nsrdb_list[[pointi]] = nsrdb_point(lat, lon, valid_year) |> 
              mutate(point=pointi,lat=lat,lon=lon)
            write_csv(nsrdb_list[[pointi]], csv_fn)
          }, timeout=10, onTimeout='error')
        })
        if(class(res)[1]=='try-error'){
          # do it again
          nsrdb_list[[pointi]] = nsrdb_point(lat, lon, valid_year) |> 
            mutate(point=pointi,lat=lat,lon=lon)
          write_csv(nsrdb_list[[pointi]], csv_fn)
        }
      }
      
      # data is at the 30 of every hour, assume this is representative of the hour
      # so basically drop the minute component
      nsrdb_list[[pointi]] = nsrdb_list[[pointi]] |> select(-Minute)
      
      # # interpolate the data at every 30 minutes to the whole hour
      # mind = min(nsrdb_list[[pointi]]$datetime, na.rm=TRUE)
      # maxd = max(nsrdb_list[[pointi]]$datetime, na.rm=TRUE)
      # d1 = data.frame(datetime=seq(mind-minutes(30),maxd,by='30 min'))
      # d2 = data.frame(datetime=seq(mind-minutes(30),maxd-minutes(30),by='1 hour'))
      # 
      # nsrdb_list[[pointi]] = d1 |>
      #   full_join(nsrdb_list[[pointi]],by='datetime') |>
      #   mutate(ghi = na.approx(GHI,na.rm=FALSE),
      #          dni = na.approx(DNI,na.rm=FALSE), 
      #          windspeed = na.approx(`Wind Speed`,na.rm=FALSE), 
      #          airtemp=na.approx(Temperature,na.rm=FALSE)) |>
      #   fill(point, lat, lon, ghi, dni, windspeed, airtemp, .direction='up') |>
      #   inner_join(d2,by='datetime')
    }
    saveRDS(nsrdb_list, nsrdb_cache_fn)
  }
}