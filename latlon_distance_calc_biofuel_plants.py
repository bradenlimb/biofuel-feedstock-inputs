#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  9 08:55:34 2022

@author: bradenlimb
"""

#%% Import Modules
from IPython import get_ipython
get_ipython().run_line_magic('reset','-sf')

import pandas as pd # library for data analysis
import requests # library to handle requests
from bs4 import BeautifulSoup # library to parse HTML documents
from tqdm import tqdm

#%% Get Data
#Scraping Wiki tables
# https://medium.com/analytics-vidhya/web-scraping-a-wikipedia-table-into-a-dataframe-c52617e1f451

method_use = "geocenter"
# method_use = "countyseat"
if method_use == "geocenter":
    
    xslx_path = "/Users/bradenlimb/CloudStation/GitHub/CSU-ABM/input_data/county_distances/input_data/us-location-geocenter.xlsx"
    df_data=pd.read_excel(xslx_path,
                        dtype={"fips": str},
                        index_col=0)
    
    
elif method_use == "countyseat":
    xslx_path = "/Users/bradenlimb/CloudStation/GitHub/CSU-ABM/input_data/county_distances/input_data/us-county-seats.xlsx"
    df = pd.read_excel (xslx_path,
                        dtype={"FIPS": str})
    df = df.rename(columns={"FIPS": "fips",
                                "STATE": "state",
                                "COUNTY": "county",
                                "NAME": "name",
                                "Shape_Y": "lat",
                                "Shape_X": "lon"})
    remove_columns = True
    if remove_columns:
        keep_columns = ['fips',
                        'state',
                        'county',
                        'name',
                        'lat',
                        'lon'
                        ]
        df_data = df.filter(keep_columns)
    df_data.drop_duplicates(subset = ['fips'], keep = 'first', inplace = True) 
    
df_data.loc[df_data.fips.str.len()<5,"fips"]="0"+df_data.fips
df_data.sort_values(by="fips",inplace=True)

#%% Load Plant Data

biofuel_type = 'Ethanol'
# biofuel_type = 'Biodiesel'

xslx_path_biofuel = f'/Users/bradenlimb/Library/CloudStorage/OneDrive-Colostate/_BETO-ABM_MOEA/GIS Layer Data/biofuel_plant_locations_EIA/{biofuel_type}_Plants_w_County_Info.xlsx'
df_biofuel = pd.read_excel(xslx_path_biofuel,
                        dtype={"FIPS": str})
df_biofuel = df_biofuel.rename(columns={"FIPS": "fips",
                            "Latitude": "lat",
                            "Longitude": "lon"})
df_biofuel.loc[df_biofuel.fips.str.len()<5,"fips"]="0"+df_biofuel.fips

#%% Import FIPS
fips_sheet = 'all'
fips = pd.read_excel (r'/Users/bradenlimb/CloudStation/GitHub/CSU-ABM/input_data/usda_data/test_fips.xlsx',
                              sheet_name=fips_sheet,
                              dtype={"fips": str})
fips.loc[fips.fips.str.len()<5,"fips"]="0"+fips.fips

fips_list = fips['fips'].tolist()

# fips_hawaii = ['15001','15003','15005','15007','15009']
# fips_old = ['51515'] #City changed to county 51019 https://www.ddorn.net/data/FIPS_County_Code_Changes.pdf
# fips_remove = fips_hawaii + fips_old
# fips_list = [fip_temp for fip_temp in fips_list if fip_temp not in fips_remove]

fips = pd.DataFrame(fips_list,columns=['fips'])

#%% Lat Lon Distance Calcs
# https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude

from geopy import distance
import datetime
begin_time = datetime.datetime.now()


# coords_1 = (+39.761849,	-104.880625) # Denver
# coords_2 = (+40.667882,	-111.924244) # Salt Lake City
# print(distance.distance(coords_1, coords_2).miles)

# denver = (df_data.loc[df_data.county == "Denver"].lat.item(),
#           df_data.loc[df_data.county == "Denver"].lon.item())
# slc = (df_data.loc[df_data.county == "Salt Lake"].lat.item(),
#        df_data.loc[df_data.county == "Salt Lake"].lon.item())
# print(distance.distance(denver, slc).miles)

column_names = df_biofuel.OBJECTID.tolist()
index_names = fips_list
df_dist = pd.DataFrame(index = index_names, columns = column_names)
df_dist = df_dist.fillna('empty')

manual_only = True
if manual_only:
    base_str = '/Users/bradenlimb/Library/CloudStorage/OneDrive-Colostate/_BETO-ABM_MOEA/GIS Layer Data/biofuel_plant_locations_EIA'
    pickle_name = f'{biofuel_type}_fips_distance_{method_use}_miles'
    df_dist=pd.read_pickle(f'{base_str}/{pickle_name}.pkl')
    
    for rowname in tqdm(index_names):
        coords_1 = (df_data.loc[df_data.fips == rowname].lat.item(),
                    df_data.loc[df_data.fips == rowname].lon.item())
        
        for colname in df_biofuel.loc[df_biofuel['Manual Feedstock'] == 'Yes','OBJECTID'].tolist():
            coords_3 = (df_biofuel.loc[df_biofuel.OBJECTID == colname]['Manual Latitude'].item(),
                        df_biofuel.loc[df_biofuel.OBJECTID == colname]['Manual Longitude'].item())

            df_dist.loc[rowname,f'{colname}-alt'] = distance.distance(coords_1, coords_3).miles
        
else:
    for rowname in tqdm(index_names):
        coords_1 = (df_data.loc[df_data.fips == rowname].lat.item(),
                    df_data.loc[df_data.fips == rowname].lon.item())
        
        for colname in column_names:
            coords_2 = (df_biofuel.loc[df_biofuel.OBJECTID == colname].lat.item(),
                        df_biofuel.loc[df_biofuel.OBJECTID == colname].lon.item())
    
            df_dist.loc[rowname,colname] = distance.distance(coords_1, coords_2).miles
            if df_biofuel.loc[df_biofuel.OBJECTID == colname,'Manual Feedstock'].item() == 'Yes':
                coords_3 = (df_biofuel.loc[df_biofuel.OBJECTID == colname]['Manual Latitude'].item(),
                            df_biofuel.loc[df_biofuel.OBJECTID == colname]['Manual Longitude'].item())
    
                df_dist.loc[rowname,f'{colname}-alt'] = distance.distance(coords_1, coords_3).miles
        # print(i,j)

df_dist_inv = df_dist.replace(0,1)
df_dist_inv = df_dist_inv**2
df_dist_inv = df_dist_inv.rdiv(1)

execute_time = datetime.datetime.now() - begin_time

# save_time_in = datetime.datetime.now()
# # df_dist.loc["49005","49035"].item()
# # df.loc[3,"lat"]
# df_dist["49005"].sum()
# save_time = datetime.datetime.now() - save_time_in
# print('Time to save: ', save_time)

# pos = 3
# colname = df_dist.columns[pos]
# rowname = df_dist.index[pos]
# print (colname)
# print(distance.distance(denver, slc).miles)

#%% Save files

save_time_in = datetime.datetime.now()
save_outputs = True
if save_outputs:
    # today = pd.to_datetime('today')
    # today_str = today.strftime('%Y-%m-%d~%H_%M_%S')
    base_str = '/Users/bradenlimb/Library/CloudStorage/OneDrive-Colostate/_BETO-ABM_MOEA/GIS Layer Data/biofuel_plant_locations_EIA'
    file_str = 'fips_distance'
    writer = pd.ExcelWriter(f'{base_str}/{biofuel_type}_{file_str}_{method_use}_backup.xlsx', engine='xlsxwriter')
    df_data.to_excel(writer, sheet_name='county_locations')
    df_dist.to_excel(writer, sheet_name='distance_miles')
    df_dist_inv.to_excel(writer, sheet_name='inverse_square')
    writer.save() # Close the Pandas Excel writer and output the Excel file.
    
    pickle_name = f'{biofuel_type}_fips_distance_{method_use}_miles'
    df_dist.to_pickle(f'{base_str}/{pickle_name}.pkl')
    
    # pickle_name = f'{method_use}_inverse_square'
    # df_dist_inv.to_pickle(f'{base_str}{pickle_name}.pkl')
    
save_time = datetime.datetime.now() - save_time_in

## %% End of Code
print('Code execution time: ', execute_time)
# print('Time per run: ', execute_time/(i*j))
print('Time to save: ', save_time)