
## ----setup, include=FALSE---------------
knitr::opts_chunk$set(echo = FALSE)

xfun::pkg_attach2("ggplot2", "scico", "sf", "spData")
xfun::pkg_load2("readr", "hdf5r", "dplyr", "lubridate", "tidyr", "hydroGOF", "rgdal", "zoo")

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
import::from(ggthemes, colorblind_pal)

valid_data_dir <- "valid_data/nsrdb"
plot_dir <- "tgw_solar_validation"
dir.create(valid_data_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(plot_dir, showWarnings = FALSE)
subdirs <- c(
  "hourly_ave", "hourly_ave_ghi_by_state", "hourly_ave_dni_by_state",
  "hourly_ave_error", "hourly_ave_ghi_error_by_state", "hourly_ave_dni_error_by_state",
  "monthly_ave", "monthly_ave_error", "hourly_error_by_state"
)
for (subdir in subdirs) {
  dir.create(file.path(plot_dir, subdir), showWarnings = FALSE)
}

lonlat_to_state <- function(pointsDF,
                            states = spData::us_states,
                            name_col = "NAME") {
  ## pointsDF: A data.frame whose first column contains longitudes and
  ##           whose second column contains latitudes.
  ##
  ## states:   An sf MULTIPOLYGON object with 50 states plus DC.
  ##
  ## name_col: Name of a column in `states` that supplies the states'
  ##           names.

  ## Convert points data.frame to an sf POINTS object
  pts <- st_as_sf(pointsDF, coords = 1:2, crs = 4326)

  ## Transform spatial data to some planar coordinate system
  ## (e.g. Web Mercator) as required for geometric operations
  states <- st_transform(states, crs = 3857)
  pts <- st_transform(pts, crs = 3857)

  ## Find names of state (if any) intersected by each point
  state_names <- states[[name_col]]
  ii <- as.integer(st_intersects(pts, states))
  state_names[ii]
}

pv_plants <- read_csv("../data/meta_solar.csv", show = FALSE, progress = FALSE) |>
  tibble::rownames_to_column(var = "site_id") |>
  mutate(
    site_id = as.integer(site_id),
    state = lonlat_to_state(data.frame(longitude, latitude))
  )

## ----shapefile, include=FALSE-----------
ba_shp <- readOGR("Elec_Control_Areas_BA/")
ba_data <- ba_shp@data
ba_data$id <- rownames(ba_data)
bas <- fortify(ba_shp) |>
  tibble() |>
  inner_join(ba_data, by = "id")
# takes forever to plot
# ggplot(bas)+geom_polygon(aes(long,lat,fill=COMP_ABRV,group=group))

ba_sf <- read_sf("Elec_Control_Areas_BA/")
ba_sf_pl <- st_transform(ba_sf, 2163)
dsf <- st_transform(st_as_sf(pv_plants, coords = c("longitude", "latitude"), crs = 4326), 2163)
int <- st_intersects(dsf, ba_sf_pl)
ba_ind <- sapply(int, function(x) {
  ifelse(length(x) > 0, x, NA)
})
pv_plants$ba <- ba_sf$COMP_ABRV[ba_ind]

valid_years <- 2007:2020
for (valid_year in valid_years) {
  message(valid_year)

  h5_file <- sprintf("../data/sam_resource_raw/wrf_solar_1h_%s.h5", valid_year)


  ## ----process-data, include=FALSE--------
  nsrdb_list <- wrf_list <- list()
  h5 <- H5File$new(h5_file)

  meta <- h5[["meta"]]$read()
  ghi_wrf <- h5[["ghi"]]$read()
  dni_wrf <- h5[["dni"]]$read()
  airtemp_wrf <- h5[["air_temperature"]]$read()
  windspeed_wrf <- h5[["wind_speed"]]$read()
  datetime_wrf <- ymd_hms(h5[["time_index"]]$read())

  h5$close_all()

  wrf_cache_fn <- sprintf("valid_data/cache/wrf_solar_list_%s.rds", valid_year)
  nsrdb_cache_fn <- sprintf("valid_data/cache/nsrdb_list_%s.rds", valid_year)

  if (file.exists(nsrdb_cache_fn)) {
    nsrdb_list <- readRDS(nsrdb_cache_fn)
  } else {
    stop("No cached NSRDB data.")
  }

  for (pointi in 1:nrow(pv_plants)) {
    lat <- pv_plants$latitude[pointi]
    lon <- pv_plants$longitude[pointi]
    wrf_list[[pointi]] <- tibble(
      datetime = datetime_wrf,
      point = pointi,
      lat = lat, lon = lon,
      ghi = ghi_wrf[pointi, ],
      dni = dni_wrf[pointi, ],
      airtemp = airtemp_wrf[pointi, ],
      windspeed = windspeed_wrf[pointi, ]
    )
  }

  nsrdb <- bind_rows(nsrdb_list) |>
    select(datetime, point, lat, lon, ghi, dni, windspeed, airtemp) |>
    pivot_longer(-c(datetime, lat, lon, point), names_to = "variable", values_to = "nsrdb")

  wrf <- bind_rows(wrf_list) |>
    select(datetime, point, lat, lon, ghi, dni, windspeed, airtemp) |>
    pivot_longer(-c(datetime, lat, lon, point), names_to = "variable", values_to = "wrf")

  valid <- inner_join(nsrdb, wrf, by = c("datetime", "point", "lat", "lon", "variable")) |>
    mutate(
      err = nsrdb - wrf,
      datetime_pacific = with_tz(datetime, "US/Pacific"),
      month = month(datetime_pacific),
      day = day(datetime_pacific),
      hour = hour(datetime_pacific)
    )

  valid_hour_ave <- valid |>
    group_by(hour, point, variable, lat, lon) |>
    summarise(nsrdb = mean(nsrdb), wrf = mean(wrf), error = mean(err), .groups = "drop") |>
    mutate(
      state = lonlat_to_state(data.frame(lon, lat)),
      variable = factor(variable, levels = c("ghi", "dni", "airtemp", "windspeed"))
    )

  valid_monthly_ave <- valid |>
    group_by(month, point, variable, lat, lon) |>
    summarise(nsrdb = mean(nsrdb), wrf = mean(wrf), error = mean(err), .groups = "drop") |>
    mutate(
      state = lonlat_to_state(data.frame(lon, lat)),
      variable = factor(variable, levels = c("ghi", "dni", "airtemp", "windspeed"))
    )


  stats <- valid |>
    group_by(lon, lat, point, variable) |>
    summarise(
      r = cor(wrf, nsrdb),
      NSE = NSE(wrf, nsrdb),
      KGE = KGE(wrf, nsrdb),
      NRMSE = nrmse(wrf, nsrdb),
      PBIAS = pbias(wrf, nsrdb),
      rSD = rSD(wrf, nsrdb),
      .groups = "drop"
    ) |>
    inner_join(pv_plants |>
      rename(lat = latitude, lon = longitude),
    by = c("lon", "lat")
    )

  suppressWarnings({
    stats_hourly <- valid |>
      group_by(lon, lat, point, variable, hour) |>
      summarise(
        r = cor(wrf, nsrdb),
        NSE = NSE(wrf, nsrdb),
        KGE = KGE(wrf, nsrdb),
        NRMSE = nrmse(wrf, nsrdb),
        PBIAS = pbias(wrf, nsrdb),
        rSD = rSD(wrf, nsrdb),
        ME = me(wrf, nsrdb),
        MAE = mae(wrf, nsrdb),
        .groups = "drop"
      ) |>
      inner_join(pv_plants |>
        rename(lat = latitude, lon = longitude),
      by = c("lon", "lat")
      )
  })



  ## ---- fig.width=10, fig.height=10-------
  p_hour_ave <- valid_hour_ave |>
    ggplot() +
    geom_boxplot(aes(hour, nsrdb, group = hour, fill = "NSRDB", color = "NSRDB"), alpha = .75, outlier.shape = NA) +
    geom_boxplot(aes(hour, wrf, group = hour, fill = "TGW", color = "TGW"), alpha = 0.75, outlier.shape = NA) +
    facet_wrap(~variable,
      scales = "free_y", labeller = as_labeller(
        c(
          dni = "DNI [W/m2]",
          ghi = "GHI [W/m2]",
          airtemp = "Air Temperature [C]",
          windspeed = "Wind Speed [m/s]"
        )
      ),
      strip.position = "left"
    ) +
    scale_fill_manual("Dataset", values = c(NSRDB = "orange", TGW = "steelblue")) +
    scale_color_manual("Dataset", values = c(NSRDB = "orange", TGW = "steelblue")) +
    theme_bw() +
    labs(y = NULL, x = "Hour", title = sprintf("Hourly average for all sites %s", valid_year)) +
    theme(
      strip.background = element_blank(),
      strip.placement = "outside"
    )
  # axis.text.x=element_text(angle=90))
  p_hour_ave
  ggsave(file.path(plot_dir, sprintf("hourly_ave/hourly_ave_%s.png", valid_year)),
    p_hour_ave,
    width = 12, height = 8
  )

  p_hour_ave_ghi_by_state <- valid_hour_ave |>
    filter(variable == "ghi") |>
    ggplot() +
    geom_boxplot(aes(hour, nsrdb, group = hour, fill = "NSRDB", color = "NSRDB"), alpha = .75, outlier.shape = NA) +
    geom_boxplot(aes(hour, wrf, group = hour, fill = "TGW", color = "TGW"), alpha = 0.75, outlier.shape = NA) +
    facet_wrap(~state) +
    scale_fill_manual("Dataset", values = c(NSRDB = "orange", TGW = "steelblue")) +
    scale_color_manual("Dataset", values = c(NSRDB = "orange", TGW = "steelblue")) +
    theme_bw() +
    labs(y = "GHI [W/m2]", x = "Hour", title = sprintf("GHI hourly average for all sites by state %s", valid_year)) +
    theme(
      strip.background = element_blank(),
      strip.placement = "outside"
    )
  p_hour_ave_ghi_by_state
  ggsave(file.path(plot_dir, sprintf("hourly_ave_ghi_by_state/hourly_ave_ghi_by_state_%s.png", valid_year)),
    p_hour_ave_ghi_by_state,
    width = 12, height = 8
  )


  p_hour_ave_dni_by_state <- valid_hour_ave |>
    filter(variable == "dni") |>
    ggplot() +
    geom_boxplot(aes(hour, nsrdb, group = hour, fill = "NSRDB", color = "NSRDB"), alpha = .75, outlier.shape = NA) +
    geom_boxplot(aes(hour, wrf, group = hour, fill = "TGW", color = "TGW"), alpha = 0.75, outlier.shape = NA) +
    facet_wrap(~state) +
    scale_fill_manual("Dataset", values = c(NSRDB = "orange", TGW = "steelblue")) +
    scale_color_manual("Dataset", values = c(NSRDB = "orange", TGW = "steelblue")) +
    theme_bw() +
    labs(y = "DNI [W/m2]", x = "Hour", title = sprintf("GHI hourly average for all sites by state %s", valid_year)) +
    theme(
      strip.background = element_blank(),
      strip.placement = "outside"
    )
  p_hour_ave_dni_by_state
  ggsave(file.path(plot_dir, sprintf("hourly_ave_dni_by_state/hourly_ave_dni_by_state_%s.png", valid_year)),
    p_hour_ave_ghi_by_state,
    width = 12, height = 8
  )


  ## ---- fig.width=10, fig.height=10-------
  p_hourly_ave_error <- valid_hour_ave |>
    ggplot() +
    geom_boxplot(aes(hour, error, group = hour, fill = variable), outlier.shape = NA) +
    facet_wrap(~variable,
      scales = "free_y", labeller = as_labeller(
        c(
          airtemp = "Air Temperature [C]",
          dni = "DNI [W/m2]",
          ghi = "GHI [W/m2]",
          windspeed = "Wind Speed [m/s]"
        )
      ),
      strip.position = "left"
    ) +
    theme_bw() +
    scale_fill_manual("Variable", values = colorblind_pal()(5)[-1]) +
    labs(y = NULL, x = "Hour", title = sprintf("Average hourly error (nsrdb-tgw) across all sites %s", valid_year)) +
    theme(strip.background = element_blank(), strip.placement = "outside")
  p_hourly_ave_error
  ggsave(file.path(plot_dir, sprintf("hourly_ave_error/hourly_ave_error_%s.png", valid_year)),
    p_hourly_ave_error,
    width = 12, height = 8
  )

  p_hourly_ave_ghi_error_by_state <- valid_hour_ave |>
    filter(variable == "ghi") |>
    ggplot() +
    geom_boxplot(aes(hour, error, group = hour), outlier.shape = NA) +
    facet_wrap(~state) +
    theme_bw() +
    scale_fill_manual("Variable", values = colorblind_pal()(5)[-1]) +
    labs(
      y = "GHI [W/m2]", x = "Hour",
      title = sprintf("Average hourly GHI error (nsrdb-tgw) across all sites %s", valid_year)
    ) +
    theme(strip.background = element_blank(), strip.placement = "outside")
  p_hourly_ave_ghi_error_by_state
  ggsave(file.path(plot_dir, sprintf("hourly_ave_ghi_error_by_state/hourly_ave_ghi_error_by_state_%s.png", valid_year)),
    p_hourly_ave_ghi_error_by_state,
    width = 12, height = 8
  )


  p_hourly_ave_dni_error_by_state <- valid_hour_ave |>
    filter(variable == "dni") |>
    ggplot() +
    geom_boxplot(aes(hour, error, group = hour), outlier.shape = NA) +
    facet_wrap(~state) +
    theme_bw() +
    scale_fill_manual("Variable", values = colorblind_pal()(5)[-1]) +
    labs(
      y = "GHI [W/m2]", x = "Hour",
      title = sprintf("Average hourly DNI error (nsrdb-tgw) across all sites %s", valid_year)
    ) +
    theme(strip.background = element_blank(), strip.placement = "outside")
  p_hourly_ave_dni_error_by_state
  ggsave(file.path(plot_dir, sprintf("hourly_ave_dni_error_by_state/hourly_ave_dni_error_by_state_%s.png", valid_year)),
    p_hourly_ave_dni_error_by_state,
    width = 12, height = 8
  )


  ## ----monthly-ave, fig.width=10, fig.height=10---------
  p_monthly_ave <- valid_monthly_ave |>
    ggplot() +
    geom_boxplot(aes(factor(month), nsrdb, group = month, fill = "NSRDB", color = "NSRDB"),
      alpha = 0.75, outlier.shape = NA
    ) +
    geom_boxplot(aes(factor(month), wrf, group = month, fill = "TGW", color = "TGW"),
      alpha = 0.75, outlier.shape = NA
    ) +
    facet_wrap(~variable,
      scales = "free_y", labeller = as_labeller(
        c(
          airtemp = "Air Temperature [C]",
          dni = "DNI [W/m2]",
          ghi = "GHI [W/m2]",
          windspeed = "Wind Speed [m/s]"
        )
      ),
      strip.position = "left"
    ) +
    scale_fill_manual("", values = c(NSRDB = "orange", TGW = "steelblue")) +
    scale_color_manual("", values = c(NSRDB = "orange", TGW = "steelblue")) +
    theme_bw() +
    labs(x = "Month", y = NULL, title = sprintf("Monthly average across all sites %s", valid_year)) +
    theme(strip.background = element_blank(), strip.placement = "outside")
  p_monthly_ave
  ggsave(file.path(plot_dir, sprintf("monthly_ave/monthly_ave_%s.png", valid_year)),
    p_monthly_ave,
    width = 12, height = 8
  )


  ## ---- fig.width=10, fig.height=10-------
  p_monthly_ave_error <- valid_monthly_ave |>
    ggplot() +
    geom_boxplot(aes(factor(month), error, group = month, fill = variable), outlier.shape = NA) +
    facet_wrap(~variable,
      scales = "free_y", labeller = as_labeller(
        c(
          airtemp = "Air Temperature [C]",
          dni = "DNI [W/m2]",
          ghi = "GHI [W/m2]",
          windspeed = "Wind Speed [m/s]"
        )
      ),
      strip.position = "left"
    ) +
    theme_bw() +
    scale_fill_manual("Variable", values = colorblind_pal()(5)[-1]) +
    labs(y = NULL, title = sprintf("Average monthly error (nsrdb-tgw) across all sites %s", valid_year)) +
    theme(strip.background = element_blank(), strip.placement = "outside")
  p_monthly_ave_error
  ggsave(file.path(plot_dir, sprintf("monthly_ave_error/monthly_ave_error_%s.png", valid_year)),
    p_monthly_ave,
    width = 12, height = 8
  )


  ## ---- fig.width=10, fig.height=10-------
  p_hourly_error_by_state <- ggplot(valid |> filter(variable == "ghi") |>
    inner_join(pv_plants |>
      rename(lat = latitude, lon = longitude),
    by = c("lon", "lat")
    )) +
    geom_boxplot(aes(hour, err, group = hour), outlier.shape = NA) +
    facet_wrap(~state) +
    labs(
      title = sprintf("All hourly errors (nsrdb-tgw) in GHI by State %s", valid_year),
      y = "GHI Error (W/m2)"
    ) +
    theme_bw() +
    coord_cartesian(ylim = c(-500, 500))
  p_hourly_error_by_state
  ggsave(file.path(plot_dir, sprintf("hourly_error_by_state/hourly_error_by_state_%s.png", valid_year)),
    p_hourly_error_by_state,
    width = 12, height = 8
  )
}
