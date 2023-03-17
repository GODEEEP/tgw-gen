'''
Author: Travis Thurber

This script gathers model specifications and coordinate data from different web sources.
The output data is written to csv files in the 'data' folder.
'''
from bs4 import BeautifulSoup
import io
import json
from multiprocessing.pool import ThreadPool
import pandas as pd
import requests
import re
from zipfile import ZipFile

wind_turbine_url_root = 'https://en.wind-turbine-models.com/turbines'
sam_url = 'https://raw.githubusercontent.com/NREL/SAM/patch/deploy/libraries/Wind%20Turbines.csv'
uswtdb_url = 'https://eerscmap.usgs.gov/uswtdb/assets/data/uswtdbCSV.zip'

turbine_output_path = './data/turbine_model_database.csv'
location_output_path = './data/turbine_coordinate_database.csv'

eia_wind_models_file_path = './data/eia_wind_model_matching_full.csv'

parallel_tasks = 4


def generate_turbine_database():
  # generate the turbine database files from wind-turbine-models.com, SAM, and USWTDB 
  
  print(ascii_turbine)
  print('')
  print('Generating turbine model and location database. This can take 60+ minutes...')
  print('')
  
  # From wind-turbine-models.com
  
  # first get a list of all available turbines with power data
  models_soup = BeautifulSoup(requests.get(wind_turbine_url_root, params={'view': 'table'}).content, 'lxml')
  turbine_urls = [x.find('a').attrs['href'] for x in models_soup.find('table').find_all('tr')[1:]]
  
  def get_turbine(turbine_url):
    # get a single turbine's data from wind-turbine-models.com

    wind_turbine_page = requests.get(turbine_url)
    turbine_soup = BeautifulSoup(wind_turbine_page.content, 'lxml')
    if turbine_soup.find(id='powercurve') is None:
      powercurve = None
    else:
      powercurve = turbine_soup.find(id='powercurve').script

    if powercurve is not None:
      labels = re.search('labels: \[(.+?)\]', powercurve.text)
      wind_speeds = list(json.loads(f'[{labels.group(1)}]'))

      data = re.search('"data":\[(.+?)\]', powercurve.text)
      data = data.group(1).replace('"', '')
      powers = list(json.loads(f'[{data}]'))
    else:
      wind_speeds = None
      powers = None

    rotor_diameter = turbine_soup.find(string='Diameter:').parent.next_sibling.next_sibling.string.split(' ')[0].replace(',', '')
    if rotor_diameter == '-':
      rotor_diameter = None
    else:
      rotor_diameter = float(rotor_diameter)

    cut_in_speed = turbine_soup.find(string='Cut-in wind speed:').parent.next_sibling.next_sibling.string.split(' ')[0].replace(',', '')
    if cut_in_speed == '-':
      cut_in_speed = None
    else:
      cut_in_speed = float(cut_in_speed)

    rated_speed = turbine_soup.find(string='Rated wind speed:').parent.next_sibling.next_sibling.string.split(' ')[0].replace(',', '')
    if rated_speed == '-':
      rated_speed = None
    else:
      rated_speed = float(rated_speed)

    cut_out_speed = turbine_soup.find(string='Cut-out wind speed:').parent.next_sibling.next_sibling.string.split(' ')[0].replace(',', '')
    if cut_out_speed == '-':
      cut_out_speed = None
    else:
      cut_out_speed = float(cut_out_speed)

    rated_power = turbine_soup.find(string='Rated power:').parent.next_sibling.next_sibling.string.split(' ')[0].replace(',', '')
    if rated_power == '-':
      rated_power = None
    else:
      rated_power = float(rated_power)

    return {
      'manufacturer': turbine_soup.find(attrs={'class': 'breadcrumb-bs'}).find_all('li')[2].span.string,
      'model': turbine_soup.find(attrs={'class': 'breadcrumb-bs'}).find_all('li')[3].span.span.string,
      'rotor_diameter': rotor_diameter,
      'cut_in_speed': cut_in_speed,
      'rated_speed': rated_speed,
      'cut_out_speed': cut_out_speed,
      'rated_power': rated_power,
      'wind_spd_ms': wind_speeds,
      'power_kw': powers,
      'source': 'wind turbines db'
    }
  
  # then get the power curve for each turbine and build a dataframe from them
  # run a few in parallel
  with ThreadPool(parallel_tasks) as pool:
    turbines = pool.map(
      get_turbine,
      turbine_urls
    )
    
  turbines = pd.DataFrame(turbines)
  
  
  # From SAM
  
  sam_turbines_page = requests.get(sam_url)
  
  # fix a rogue comma
  sam_csv = str(sam_turbines_page.content, 'utf-8').replace('1,4kW', '1.4kW')
  
  sam_turbines = pd.read_csv(io.StringIO(sam_csv), header=[0], skiprows=[1,2])
  sam_turbines['Wind Speed Array'] = sam_turbines['Wind Speed Array'].str.split('|')
  sam_turbines['Power Curve Array'] = sam_turbines['Power Curve Array'].str.split('|')
  sam_turbines['source'] = 'sam'
  
  sam_turbines = sam_turbines.rename(columns={
    'kW Rating': 'rated_power',
    'Rotor Diameter': 'rotor_diameter',
    'Wind Speed Array': 'wind_spd_ms',
    'Power Curve Array': 'power_kw',
    'Name': 'name',
  })[['name', 'rotor_diameter', 'rated_power', 'wind_spd_ms', 'power_kw', 'source']]
  
  # try to split name into manufacturer/model using the eia database
  eia_models = pd.read_csv(eia_wind_models_file_path)
  
  
  def get_manufacturer_model(name_row):
    # try to parse a SAM turbine name into a model and a manufacturer using the EIA list
    for i, row in eia_models.iterrows():
      if (row['manufacturer'].lower() in name_row['name'].lower()) or (row['Predominant Turbine Manufacturer'].lower() in name_row['name'].lower()):
        if (row['model'].lower() in name_row['name'].lower()) or (row['Predominant Turbine Model Number'].lower() in name_row['name'].lower()):
          return row['manufacturer'], row['model']
        if any(m in name_row['name'] for m in row['model'].split('/')):
          return row['manufacturer'], row['model']
        if any(m in name_row['name'] for m in row['model'].split('-')):
          return row['manufacturer'], row['model']
      if (row['model'].lower() in name_row['name'].lower()) or (row['Predominant Turbine Model Number'].lower() in name_row['name'].lower()):
        return row['manufacturer'], row['model']
    return None, None
  
  sam_turbines[['manufacturer', 'model']] = sam_turbines.apply(get_manufacturer_model, axis=1, result_type='expand')
  
  # combine the databases
  # and drop SAM turbines not found in EIA
  turbine_db = pd.concat([turbines, sam_turbines[sam_turbines.manufacturer.notna()][[
    'manufacturer', 'model', 'rotor_diameter', 'rated_power', 'wind_spd_ms', 'power_kw', 'source'
  ]]])
  
  
  # Get locations from USWTDB
  
  uswtdb_page = requests.get(uswtdb_url)
  uswtdb_file = ZipFile(io.BytesIO(uswtdb_page.content))
  uswtdb_csv = uswtdb_file.open(uswtdb_file.filelist[1].filename)
  uswtdb = pd.read_csv(uswtdb_csv)
  uswtdb_aggregated = uswtdb[['eia_id', 'xlong', 'ylat']].groupby('eia_id')[['xlong', 'ylat']].agg({
    'xlong': list,
    'ylat': list,
  }).reset_index().rename(columns={
    'eia_id': 'id',
    'xlong': 'x_coords',
    'ylat': 'y_coords',
  })
  uswtdb_aggregated['num_coords'] = uswtdb[['eia_id', 'xlong', 'ylat']].groupby('eia_id')['xlong'].count().reset_index(drop=True).values
  uswtdb_aggregated = uswtdb_aggregated.astype({
    'id': int,
  })
  
  
  # write everything to file
  turbine_db.to_csv(turbine_output_path, index=False)
  uswtdb_aggregated.to_csv(location_output_path, index=False)
  
  print('Done!')
  print('')
  

  
ascii_turbine = """
,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,''''''''''''''...........................
,,,,;;;;;;;;;;;;;;;;;;;;;;;;,,,,,,,,,,,,,,,,,,'''''''''''''.....................
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,,,,,,,,,,,,,,''''''''''''..............
;;::::::::::::::::::::::::;::;::::;;;;;;;;;;;;;,,;;,,,,,,,,,'''''''''...........
::::::::::::::::::::::::::;;::::::::::::;;;;;;;;;;;;;;,,,,,,,,,,,'''''''........
:::ccccccccccccccccccccccc,':c::cc::::::::::::;;;;;;;;;;;;;,,,,,,,,,,''''''.....
cccccccccccccccccccccccclc:''ccccccccccc:::::::::::;;;;;;;;;;;,,,,,,,,,,''''''''
cccccclllllllllllllllllllll:.;lclccccccccccc:::::::::::;;;;;;;;;;;;;,,,,,,,,''''
llllllllllllllllllllllllllll'.cllclllccccccccccc::::::::::::;;;;;;;;;;;;,,,,,,,,
lllllllllllllllllllllllollloc.'lolllllllllccccccccccc:::::::::::::;;;;;;;;,,,,,,
llllooooooooooooooooooooooooo;.;ooolllllllllllllccccccccccc::::::::::;;;;;;;;;;;
oooooooooooooooooooooooooooodo'.cooooooollllllllllllcccccccccccc:::::::::;;;;;;;
ooooooooooooooooddddddddoododdc.'oooooooooooollllllllllllccccccccccc:::::::::;;;
oooddddddddddddddddddddddoddodd;.;odooooooooooooooolllllllllllcccccccccc::::::::
dddddddddddddddddddddddddddddddo..:xdoddddddooooooooooooollllllllllccccccccccc::
dddddddddddddddddddddddddddddddd:..lxddddddddddddoooooooooooolllllllllllcccccccc
dddddddddddddddxxxxxxxxxdddddddxd' 'oxdxddddddddddddddddoooooooooooolllllllllccc
xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxl. ,dxdxxdddddddddddddddddooooooooooooollllllll
xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx,  :xxxxxxxxxxxxxddddddddddddddddoooooooooooll
xxxxxxxxkkkkkkkkkkkkkkkkxxxxxxxxxxo. .lkxxxxxxxxxxxxxxxxxdddddddddddddddoooooooo
kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkx:  ;kkkkkxxxxxxxxxxxxxxxxxxxdddddddddddddoooo
kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkOx' .:cccoxkxkxxxxxxxxxxxxxxxxxxxxxddddddddddd
kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkl.     'dkkkkkkkkkkkkxxxxxxxxxxxxxxxxxxddddd
OOOOOOOOOOOOOOOOOOOOOOOOkOOOOOOkOOkOx'     .dOkOkkkkkkkkkkkkkkkxxxxxxxxxxxxxxxxx
OOOOOOOOOOOOOOOOOOOOOOOOOOOOkOOOOOOx:.    ...,;:lodkkOkkkkkkkkkkkkkkkkkxxxxxxxxx
OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOo,..c'  .oxl;,.....';:ldxkOOkkkkkkkkkkkkkkkkkxx
OOOOOOO00000000000000000OO0000kl'..ckO;  .x0OOOkxdlcc:,',,,;:cloxkkkOOkkkkkkkkkk
00000000000000000000000000000d'..:kO0O,  .x0OOOOO0OOOOOOOkxolcc:cccclodxkkOOOkkk
000000000000000000000000000k:..ck0K00O,  .x0O000000OOOOOOOOOOOO0OOkdoolllodxkOOk
000000000000000000000000K0o'.ck000000k,  .xK00000000000000OOOOOOOOOOOOOOOkkkOOOO
0000000KKKKKKKKKKKKKKKK0x;.:k0KKKK00KO,  .xK00000000000000000000000OOOOOOOOOOOOO
KKKKKKKKKKKKKKKKKKKKKKOc':kKKKKKKKKKKO,  .xK00000000000000000000000000OOOOOOOOOO
KKKKKKKKKKKKKKKKKKKK0d;:xKKKKKKKKKKKKO'  .xKK0KKKKKK0000000000000000000000000000
KKKKKKKKKKKKKKKKXXKxccxKXKKKXKKKKKKKXk'  .xKKKKKKKKKKKKKKKK000000000000000000000
XXXXXXXXXXXXXXXKKOl:xKXXKKKKXXKKKKKKXk'  .dKKKKKKKKKKKKKKKKKKKKKK000000000000000
XXXXXXXXXXXXXKX0dldKXXXXXKKKKKXXKXXKXk'  .dXKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK00000
XXXXXXXXXXXXXKxldKXXKXXXXKKKXXXXXXXXXk.  .dXKXXXXXXXKKKKKKKKKKKKKKKKKKKKKKKKKK00
XXXXXXXXXXXXOdx0XXXXXXKKKXXXXXXXXXXXXk.  .dXXXXXXXXXXXXXKKKKKKKKKKKKKKKKKKKKKKKK
XXXXXXXXKXXOk0XXXNNXKKKXXXNXXXXXXXXXNk.  .dXXXXXXXXXXXXXXXXXXXXXXXXXXKKKKKKKKKKK
XXXXXXXXXXKKKKKK0xdOXXNXXXXXXXXXXXXXNk.  .dNXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXKKKKKK
NNNNNNNNNNNNNXX0k,;0XXNNNNNNNNNXNNXXNx.   dNXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
NNNNNNNNNNNNNNNWXcoKKNNNNNNNNNNNNNXXNx.   dNXXNNNNXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
NNNNNNNNNNNNNNNWX:;0NNNNNNNNNNNNNNNNNx.   dNNNNNNNNNNNNNNNNNXXXXXXXXXXXXXXXXXXXX
NNNNNNNNNNNXNNXX0;,0WNNNNNNNNNNNNNNNWx.   dWNNNNNNNNNNNNNNNNNNNNNNNXXXXXXXXXXXXX
WWNNNNNWXXNXXKK0k,;0WWWNNNNNNNNWWNNNWx.   oWNNWNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN
WWWWWWWNKXWWWWWW0;lNWWWWWWWWWWWWWWNNWx.   oWNNWWNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN
WWWWNNX0kKWWWWWW0,lWWWWWWWWWWWWWWWWWWx.   oWWWWWWWWWWWWWWWWWWWWWWWWWWWWWNNNNNNNN
WWWWWWWXO0NWWWWW0,lWWWWWWWWWWWWWWWWWXl    ckOXWWWWWWWWWWWWWWWWWWWWWNWWWWWWWWWWWW
WWMMWNNN0KWWWWWW0,oWWWWMMMMMMMMWMMWW0:    ;oxXWWWWMWWWWNNWWWWWWWWWNXNWWWWWWWWWWW
MMMWWNNN0XMMMMMMO'lWMMMMMMMMMMMMMMWWMx.   oWWWWWWMMMMMMWWWWMMMMMMMWNNMWWMWWWWWWW
MMMWWNWN0XMMMMMM0;oWMMMMMMMMMMMMMWMMMd.   dMWWMMMMMMMMMWWWMMMMMMMMWNWMMMMMWWWMMM
KXXKKKKK0KXXKXXXk:oXXXXXXXXXXXXXXXXXNd.   lXXXXXXXXXXXXXXXXXXXXXXXXKXXXXXXXXXXXX
xxxxkkkkkkkxkkkkd:cxkkkOOOkkkkkkkkOkk:    ;kkxkkxkkkkkkkkkkxxxxxxxxxxxxxxxxxxkxx
doooooooooooddddl;:oodddddddoddoooolc'    .clllllllooolllllollllooollcclllllllll
lllc::ccccccccc:;',;;;;;;;;:cc::cccc;.    .;:::cc::::;,,,,,,,;;;;;::::;;;;;;;;;;
llllllooloodoolc:,;:::c:::::::::;;,,'.    .;c::cccc::;;;;:;;;;;;;;;;;;,,,;;;;;;;
xxxxxddddddxxxddl::coooddddollllcc::;.    .:c::::::cc::::::c:;,;;,;;:c:;;;;,,'',
lcclloolllcclccc;,,cooolcc:ccllc:::;'.    .',;;;;;;;;,,',;;:::::::::ccc::clcc::c
;,,,;;;;;;;;;;,'..',;,,''...'......        ...............'',,,,,,,,,,,,,,,',,,,
............''','..,,,',,''',,''....       ..',,,;;;;;,,,;;;;;,'''',,,,,,'',,,,'
"""

if __name__ == "__main__":
  generate_turbine_database()
  