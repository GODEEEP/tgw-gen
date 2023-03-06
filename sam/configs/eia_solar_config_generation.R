"
This script will eia8602020

"

import::from(readxl, read_excel)
import::from(readr, write_csv, read_csv)
import::from(dplyr, inner_join, mutate, group_by, summarise, filter, rename, case_when,
             select, '%>%', bind_rows, everything)
import::from(stringr, str_match)

# read plant metadata and solar specific info
solar_info = read_excel('./eia8602020/3_3_Solar_Y2020.xlsx', skip=1)
solar_plants = read_excel('./eia8602020/2___Plant_Y2020.xlsx', skip=1)

wecc_only = FALSE

dedup_names <- function(names){
  counts = list()
  names = as.character(names)
  for(i in 1:length(names)){
    col = names[i]
    #print(col)
    cur_count = counts[[col]]
    if(is.null(cur_count)) cur_count = 0
    if(cur_count > 0){
      names[i] = sprintf('%s_%d', col, cur_count)
    }
    counts[[col]] = cur_count + 1
  }
  return(names)
}

solar_configs_uncombined = 
  solar_info |> 
  # join on common column names
  inner_join(solar_plants) %>%
  # only western states
  {if(wecc_only)filter(., `NERC Region` == 'WECC') else .} |>
  filter(!(State %in% c('AK','HI'))) |>
  #filter(State %in% c('CA','OR','WA','ID','MT','WY','UT','CO','AZ','NM')) |>
  # infer config options
  mutate(
    # 0=standard, 1=premium, 2=thin film
    module_type = case_when(
      `Thin-Film (CdTe)?` == 'Y' ~ 2,
      `Thin-Film (A-Si)?` == 'Y' ~ 2,
      `Thin-Film (CIGS)?` == 'Y' ~ 2,
      `Thin-Film (Other)?` == 'Y' ~ 2,
      TRUE ~ 0),
    # 0=Fixed, 1=Fixed Roof, 2=1Axis, 3=Backtracked, 4=2Axis 
    array_type = case_when(
      `Fixed Tilt?` == 'Y' ~ 0,
      `Single-Axis Tracking?` == 'Y' ~ 2,
      `Dual-Axis Tracking?` =='Y' ~ 4,
      TRUE ~ 0),
    # default azimuth of 180 
    azimuth = ifelse(is.na(`Azimuth Angle`), 180, `Azimuth Angle`),
    # cant have negative azimuth
    azimuth = ifelse(azimuth < 0, abs(azimuth), azimuth),
    # default tilt to latitude 
    tilt = ifelse(is.na(`Tilt Angle`), Latitude, `Tilt Angle`),
    # cant have negative azimuth
    tilt = ifelse(tilt < 0, abs(tilt), tilt)) |>
  group_by(`Plant Code`) |>
  rename(plant_code = `Plant Code`,
         generator_id = `Generator ID`,
         lat = Latitude, 
         lon = Longitude, 
         system_capacity = `Nameplate Capacity (MW)`,
         ba = `Balancing Authority Code`,
         nerc_region = `NERC Region`,
         plant_name = `Plant Name`,
         state = `State`,
         county = `County`) 

cant_combine = solar_configs_uncombined |>
  summarise(n_entries = length(plant_code),
            plant_code = plant_code[1],
            azimuth = azimuth |> unique() |> length(),
            tilt = tilt |> unique() |> length(),
            module_type = module_type |> unique() |> length(),
            array_type = array_type |> unique() |> length()) |>
  # if any plany code does not have all of these parameters the same we have to split them out
  # for simplicity we'll ignore partial combinations like a plant that has 3 entries and 2 could 
  # be combined, we'll just leave those all split out
  filter(n_entries>1 & (azimuth>1 | tilt>1 | module_type>1 | array_type>1)) 

# these are the plants that have at least one of the solar parameters 
# different across EIA entries so they can't be combined
solar_configs_cant_combine = 
  solar_configs_uncombined |>
  filter(plant_code %in% cant_combine$plant_code) |>
  select(plant_code, plant_name, generator_id, state, county, ba, nerc_region, azimuth, 
         tilt, system_capacity, module_type, array_type, lat, lon) |>
  # reV uses kW 
  mutate(system_capacity = system_capacity * 1000)

# these are the plants that are confirmed to have all the solar parameters the 
# same accross EIA entries so they can be combined
solar_configs_combined = solar_configs_uncombined |>
  filter(!(plant_code %in% cant_combine$plant_code)) |>
  summarise(plant_code = plant_code[1],
            plant_name = plant_name[1],
            generator_id = paste0(generator_id,collapse=','),
            state = state[1],
            county = county[1],
            ba = ba[1],
            nerc_region = nerc_region[1],
            azimuth = azimuth[1],
            tilt = tilt[1],
            # reV uses kW 
            system_capacity = sum(system_capacity) * 1000,
            module_type = module_type[1],
            array_type = array_type[1],
            lat = mean(lat),
            lon = mean(lon)) 

# combine both parts
solar_configs = solar_configs_combined |>
  bind_rows(solar_configs_cant_combine) |>
  mutate(losses = 14,
         plant_code_unique = dedup_names(plant_code)) |>
  select(plant_code, plant_code_unique, plant_name, generator_id, everything())


write_csv(solar_configs,sprintf('eia%s_solar_configs.csv', ifelse(wecc_only, '_wecc', '')))
