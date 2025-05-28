cat("
This script will download WTK data from the NREL API. This requires you to have
an NREL API key:

https://developer.nrel.gov/signup/

Once you have that, create a file named `.env` in the `WRF-to-reV` directory.
The file should have the following lines:

    nrel_api_key = 'key'
    nrel_api_email = 'email'

Adjust the valid years based on how much data you want to download. See the
API reference for available years:

    https://developer.nrel.gov/docs/wind/wind-toolkit/wtk-download/

@author = Cameron Bracken (cameron.bracken@pnnl.gov)
")

xfun::pkg_load2("readr", "hdf5r", "dplyr", "lubridate", "tidyr", "hydroGOF", "zoo")

import::from(readr, read_csv, write_csv)
import::from(hdf5r, H5File)
import::from(
  dplyr, rename, mutate, bind_rows, select, summarise, group_by, inner_join,
  filter, left_join, full_join, ungroup
)
import::from(lubridate, hours, minutes, ymd_hms, month, day, hour, with_tz)
import::from(tidyr, pivot_longer, fill)
import::from(tibble, tibble, rownames_to_column)
import::from(hydroGOF, NSE, me, mae, pbias, nrmse, rSD, KGE)
import::from(rgdal, readOGR)
import::from(zoo, na.approx)

valid_years <- 2007:2014
valid_data_dir <- "valid_data/wtk_eia"

readRenviron("../.env")

wind_plants <- read_csv("../sam/configs/eia_wind_configs.csv", show = FALSE, progress = FALSE)

wtk_point <- function(lat, lon, year) {
  "
    Extract data for a single point, for one year, from the WTK.

    Created on Fri Aug 12 12:48:24 2022

    API reference:
    https://developer.nrel.gov/docs/wind/wind-toolkit/wtk-download/

    "

  # lat, lon, year = 43, -120, 2014
  # latest available year is 2014
  if (year > 2014) {
    return(NULL)
  }

  api_key <- Sys.getenv("nrel_api_key")
  email <- Sys.getenv("nrel_api_email")

  # Set the attributes to extract (dhi, ghi, etc.), separated by commas.
  attributes <- paste0(
    "windspeed_80m,winddirection_80m,temperature_80m,",
    "windspeed_140m,winddirection_140m,temperature_140m,",
    "pressure_0m,pressure_100m,pressure_200m" # ,
    # "power"
  )
  # leap='true' will return leap day data if present, false will not.
  # Set time interval in minutes, Valid intervals are 30 & 60.
  interval <- "30"

  # Declare url string
  url <- sprintf(
    paste0(
      "https://developer.nrel.gov/api/wind-toolkit/v2/wind/",
      "wtk-download.csv?wkt=POINT(%s%%20%s)&names=%s&",
      "leap_day=%s&interval=%s&utc=%s&email=%s&api_key=%s&attributes=%s"
    ),
    lon, lat, year, "true", interval, "true", email, api_key, attributes
  )
  # Return just the first 2 lines to get metadata:
  # info = pd.read_csv(url, nrows=1)
  # See metadata for specified properties, e.g., timezone and elevation
  # timezone, elevation = info['Local Time Zone'], info['Elevation']

  # Return all but first 2 lines of csv to get data:
  read_csv(url, skip = 1, show = FALSE) |>
    mutate(datetime = ISOdatetime(Year, Month, Day, Hour, Minute, 0, tz = "UTC"))
}


for (valid_year in valid_years) {
  # wtk_cache_fn <- sprintf("valid_data/cache/wtk_list_%s.rds", valid_year)
  message(valid_year)

  wtk_list <- list()

  # if (file.exists(wtk_cache_fn)) {
  #  wtk_list <- readRDS(wtk_cache_fn)
  # } else {
  for (pointi in 1:nrow(wind_plants)) {
    lat <- wind_plants$lat[pointi]
    lon <- wind_plants$lon[pointi]
    message(pointi, " ", lat, " ", lon)
    csv_fn <- file.path(valid_data_dir, sprintf("wtk_%s_%04d.csv", valid_year, pointi))
    if (!file.exists(csv_fn)) {
      # wtk_list[[pointi]] <- read_csv(csv_fn, show = FALSE, progress = FALSE)

      # } else {
      wtk_list[[pointi]] <- wtk_point(lat, lon, valid_year) |>
        mutate(point = pointi, lat = lat, lon = lon)
      write_csv(wtk_list[[pointi]], csv_fn)
    }

    # interpolate the data at every 30 minutes to the whole hour
    # mind <- min(wtk_list[[pointi]]$datetime)
    # maxd <- max(wtk_list[[pointi]]$datetime)
    # d1 <- data.frame(datetime = seq(mind - minutes(30), maxd, by = "30 min"))
    # d2 <- data.frame(datetime = seq(mind - minutes(30), maxd - minutes(30), by = "1 hour"))

    # wtk_list[[pointi]] <- d1 |>
    #   full_join(wtk_list[[pointi]], by = "datetime") |>
    #   mutate(
    #     windspeed_80m = na.approx(`wind speed at 80m (m/s)`, na.rm = FALSE),
    #     winddir_80m = na.approx(`wind direction at 80m (deg)`, na.rm = FALSE),
    #     temp_80m = na.approx(`air temperature at 80m (C)`, na.rm = FALSE),
    #     pressure_80m = na.approx(`air pressure at 100m (Pa)`, na.rm = FALSE)
    #   ) |>
    #   fill(point, lat, lon, windspeed_80m, winddir_80m, temp_80m, pressure_80m, .direction = "up") |>
    #   inner_join(d2, by = "datetime")
  }
  # saveRDS(wtk_list, wtk_cache_fn)
  # }
}
