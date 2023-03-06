"
Generate validation reports for every listed year. 

@author = Cameron Bracken (cameron.bracken@pnnl.gov)
"

library(rmarkdown)

solar_years = 2007:2020
wind_years = 2007:2014

for(year in solar_years){
  message('solar ', year)
  render('validate_solar.Rmd',
         output_file=sprintf('validate_solar_%s.html',year),
         params=list(valid_year=year),
         quiet=TRUE)
}

for(year in wind_years){
  message('wind ', year)
  render('validate_wind.Rmd',
         output_file=sprintf('validate_wind_%s.html',year),
         params=list(valid_year=year),
         quiet=TRUE)
}