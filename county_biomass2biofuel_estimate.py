#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 09:41:59 2023

@author: bradenlimb
"""

#%% Import Modules
import pandas as pd
from openpyxl import workbook
from openpyxl import load_workbook
import pickle
from tqdm import tqdm
import datetime
begin_time = datetime.datetime.now()
from usda_data_simplify import usda_data

#%% Import FIPS
fips_sheet = 'all'
fips = pd.read_excel (r'usda_data/test_fips.xlsx',
                              sheet_name=fips_sheet,
                              dtype={"fips": str})
fips.loc[fips.fips.str.len()<5,"fips"]="0"+fips.fips
fips_list = fips['fips'].tolist()
fips = pd.DataFrame(fips_list,columns=['fips'])

#%% Import Corn and Soy Data
crops_list_old = ['CORN','SOYBEANS']
categories = ['PRODUCTION','AREA HARVESTED']
# fips = df_geo.fips.to_frame().reset_index(drop=True)
usda_program = 'CENSUS'

year_input = 2017
# year_train = 2017
years_input = [year_input]
usda_data_dict = usda_data(fips,years_input,crops_list_old,categories,usda_program,para_year = True)
corn_soy_land = usda_data_dict[f'{year_input} AREA HARVESTED'].copy(deep=True)
corn_soy_yield = usda_data_dict[f'{year_input} PRODUCTION PER AREA'].copy(deep=True)
corn_soy_production = usda_data_dict[f'{year_input} PRODUCTION'].copy(deep=True)

#%% Choose Biofuel Type
# biofuel_type = 'Ethanol'
biofuel_type = 'Biodiesel'

#%% Check Biorefinery Capacity
workbook = load_workbook('Volume and Biomass Requirement Summary 2017.xlsx')
worksheet = workbook["Only Vals"]

if biofuel_type == 'Ethanol':
    biofuel_production_2017 = worksheet["J3"].value # gallons
    biomass_required_2017 = worksheet["J5"].value # kg
    biomass_to_biofuel_conversion = worksheet["J7"].value #Gal Biofuel/kg biomass
elif biofuel_type == 'Biodiesel':
    biofuel_production_2017 = worksheet["J4"].value # gallons
    biomass_required_2017 = worksheet["J6"].value # kg
    biomass_to_biofuel_conversion = worksheet["J8"].value #Gal Biofuel/kg biomass

#%% Load Biorefinery Information and Distances
df_bioplants = pd.read_excel(f"{biofuel_type}_Plants_w_County_Info.xlsx")
df_bioplants["Cap_gal"] = df_bioplants["Cap_Mmgal"]*1e6

if biofuel_type == 'Biodiesel':
    df_bioplants=df_bioplants.loc[(df_bioplants['Soy']=='Yes') | (df_bioplants['Soy']=='Partial')]

distance_use = 'geocenter'
with open(f'{biofuel_type}_fips_distance_{distance_use}_miles.pkl', 'rb') as handle:
    df_distance = pickle.load(handle)
    
#%% Compare Biorefinery Capacity to 2017 Production
production_capacity = df_bioplants["Cap_gal"].sum()
capacity_diff = production_capacity-biofuel_production_2017
capacity_diff_pct = capacity_diff/production_capacity
capacity_comp_pct = biofuel_production_2017/production_capacity

soyoil_percent = 0.52 #Page 27 https://afdc.energy.gov/files/u/publication/2017_bioenergy_status_rpt.pdf
# Biodiesel by state 2019 -> https://www.eia.gov/todayinenergy/detail.php?id=41314 
biodiesel_total_production_2017 = 1.596 * 1e9 #https://www.agmrc.org/renewable-energy/renewable-energy-climate-change-report/renewable-energy-climate-change-report/april-2018-report/update-on-us-biodiesel-production-and-feedstocks-usage-in-2017#:~:text=The%20U.S.%20production%20of%20pure%20biodiesel%20in%202017%20was%201.596,highest%20recorded%20in%20U.S.%20history.

soy_capacity_comp_pct = biofuel_production_2017/biodiesel_total_production_2017
# biodiesel_total_production_2017/production_capacity

#%% Setup Biomass DataFrames
df_biomass_kg = pd.DataFrame()
if biofuel_type == 'Ethanol':
    df_biomass_kg['total_kg'] = corn_soy_production['CORN']
elif biofuel_type == 'Biodiesel':
    df_biomass_kg['total_kg'] = corn_soy_production['SOYBEANS']

max_biomass_for_biofuel = 0.90
df_biomass_kg['avalible_kg'] = df_biomass_kg['total_kg']*max_biomass_for_biofuel
df_biomass_kg['remaining_kg'] = df_biomass_kg['avalible_kg'].copy(deep=True)
df_biomass_kg['plants'] = ''
#%% Setup DFs
column_names = df_bioplants.OBJECTID.tolist()
index_names = fips_list
df_biomass_used = pd.DataFrame(index = index_names, columns = column_names)
df_biomass_used = df_biomass_used.fillna(0)

#%% Find Biorefinery Priority based on Size
bioplant_priority = df_bioplants.sort_values('Cap_gal',ascending=False)['OBJECTID'].tolist()

#%% Solve where biomass comes from
if biofuel_type == 'Ethanol':
    df_bioplants['biomass_needed_kg'] = df_bioplants['Cap_gal']*capacity_comp_pct/biomass_to_biofuel_conversion #kg biomass
elif biofuel_type == 'Biodiesel':
    if df_bioplants.loc[df_bioplants['Soy']=='Yes',"Cap_gal"].sum() < biofuel_production_2017:
        full_soy_capacity_factor = 0.95
        df_bioplants.loc[df_bioplants['Soy']=='Yes','biomass_needed_kg'] = df_bioplants.loc[df_bioplants['Soy']=='Yes','Cap_gal']*full_soy_capacity_factor/biomass_to_biofuel_conversion #kg biomass
        
        partial_soy_pct = (biofuel_production_2017-df_bioplants.loc[df_bioplants['Soy']=='Yes',"Cap_gal"].sum()*full_soy_capacity_factor)/df_bioplants.loc[df_bioplants['Soy']=='Partial',"Cap_gal"].sum()
        df_bioplants.loc[df_bioplants['Soy']=='Partial','biomass_needed_kg'] = df_bioplants.loc[df_bioplants['Soy']=='Partial','Cap_gal']*partial_soy_pct/biomass_to_biofuel_conversion #kg biomass

    else:
        yes_soy_pct = biofuel_production_2017/df_bioplants.loc[df_bioplants['Soy']=='Yes',"Cap_gal"].sum()
        df_bioplants=df_bioplants.loc[df_bioplants['Soy']=='Yes']
        df_bioplants['biomass_needed_kg'] = df_bioplants['Cap_gal']*yes_soy_pct/biomass_to_biofuel_conversion #kg biomass

manual_plants = df_bioplants.loc[df_bioplants['Manual Feedstock']=='Yes','OBJECTID'].tolist()
for bioplant in tqdm(bioplant_priority):
    if bioplant not in df_bioplants['OBJECTID'].tolist(): continue
    
    # fips_ordered_temp = df_distance.sort_values(bioplant).index.tolist()
    if bioplant in manual_plants: 
        fips_ordered_temp = df_distance.sort_values(f'{bioplant}-alt').index.tolist()
    else:
        fips_ordered_temp = df_distance.sort_values(bioplant).index.tolist()  
    biomass_needed_temp = df_bioplants.loc[df_bioplants['OBJECTID']==bioplant,'biomass_needed_kg'].item() #kg biomass
    
    for fip_temp in fips_ordered_temp:
        if df_biomass_kg.loc[fip_temp,'remaining_kg'] == 0:
            continue
        elif biomass_needed_temp > df_biomass_kg.loc[fip_temp,'remaining_kg']:
            df_biomass_used.loc[fip_temp,bioplant] = df_biomass_kg.loc[fip_temp,'remaining_kg']
            biomass_needed_temp -= df_biomass_kg.loc[fip_temp,'remaining_kg']
            df_biomass_kg.loc[fip_temp,'remaining_kg'] -= df_biomass_kg.loc[fip_temp,'remaining_kg']
            if df_biomass_kg.loc[fip_temp,'plants'] == '':
                df_biomass_kg.loc[fip_temp,'plants'] = str(bioplant)
            else:
                df_biomass_kg.loc[fip_temp,'plants'] = f'{df_biomass_kg.loc[fip_temp,"plants"]}, {bioplant}'
        else:
            df_biomass_used.loc[fip_temp,bioplant] = biomass_needed_temp
            df_biomass_kg.loc[fip_temp,'remaining_kg'] -= biomass_needed_temp
            biomass_needed_temp -= biomass_needed_temp
            if df_biomass_kg.loc[fip_temp,'plants'] == '':
                df_biomass_kg.loc[fip_temp,'plants'] = str(bioplant)
            else:
                df_biomass_kg.loc[fip_temp,'plants'] = f'{df_biomass_kg.loc[fip_temp,"plants"]}, {bioplant}'
            break
        
#%% Summary Calculations
df_biomass_kg['used_kg'] = df_biomass_kg['avalible_kg']-df_biomass_kg['remaining_kg']
df_biomass_kg['percent_used'] = df_biomass_kg['used_kg']/df_biomass_kg['total_kg']
df_biomass_dist_product = df_biomass_used * df_distance
df_biomass_kg['mean_distance_crow_flys_mi'] = df_biomass_dist_product.sum(axis=1)/df_biomass_kg['used_kg']
df_biomass_kg.fillna(0,inplace=True)
df_bioplants.set_index('OBJECTID',inplace=True, drop=False)
df_bioplants['used_kg'] = df_biomass_used.sum()
df_bioplants['mean_distance_crow_flys_mi'] = df_biomass_dist_product.sum(axis=0)/df_bioplants['used_kg']

if biofuel_type == 'Ethanol':
    truck_distance = 200
    df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] < truck_distance,'transport_method'] = 'truck'
    df_biomass_kg.loc[df_biomass_kg['mean_distance_crow_flys_mi'] < truck_distance,'transport_method'] = 'truck'
    df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] > truck_distance,'transport_method'] = 'train'
    df_biomass_kg.loc[df_biomass_kg['mean_distance_crow_flys_mi'] > truck_distance,'transport_method'] = 'train'
    df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] < truck_distance,'mean_distance_freight_mi'] = df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] < truck_distance,'mean_distance_crow_flys_mi']
    df_biomass_kg.loc[df_biomass_kg['mean_distance_crow_flys_mi'] < truck_distance,'mean_distance_freight_mi'] = df_biomass_kg.loc[df_biomass_kg['mean_distance_crow_flys_mi'] < truck_distance,'mean_distance_crow_flys_mi']
    # df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] > truck_distance,'mean_distance_freight_mi'] = df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] > truck_distance,'mean_distance_crow_flys_mi']*1.5
    df_biomass_kg.loc[df_biomass_kg['mean_distance_crow_flys_mi'] > truck_distance,'mean_distance_freight_mi'] = df_biomass_kg.loc[df_biomass_kg['mean_distance_crow_flys_mi'] > truck_distance,'mean_distance_crow_flys_mi']
    # df_biomass_kg.loc[df_biomass_kg['mean_distance_crow_flys_mi'] > truck_distance,'mean_distance_freight_mi'] = 200
    
    df_bioplants.loc[df_bioplants['OBJECTID'] == 1 ,'mean_distance_freight_mi'] = 1900
    df_bioplants.loc[df_bioplants['OBJECTID'] == 2 ,'mean_distance_freight_mi'] = 1800
    df_bioplants.loc[df_bioplants['OBJECTID'] == 3 ,'mean_distance_freight_mi'] = 1700
    df_bioplants.loc[df_bioplants['OBJECTID'] == 4 ,'mean_distance_freight_mi'] = 1860
    df_bioplants.loc[df_bioplants['OBJECTID'] == 160 ,'mean_distance_freight_mi'] = 1200
    
    df_biomass_kg.loc['17001' ,'mean_distance_freight_mi'] = 1900*(5.78048e+08/7.34828e+08)+1860*(1.56781e+08/7.34828e+08)
    df_biomass_kg.loc['17009' ,'mean_distance_freight_mi'] = 1860*(1.41819e+08/1.41819e+08)
    df_biomass_kg.loc['17067' ,'mean_distance_freight_mi'] = 1800*(4.5418e+08/6.75824e+08)+1700*(2.47735e+07/6.75824e+08)+1860*(1.9687e+08/6.75824e+08)
    df_biomass_kg.loc['38093' ,'mean_distance_freight_mi'] = 1200

elif biofuel_type == 'Biodiesel':
    truck_distance = 200
    df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] < truck_distance,'transport_method'] = 'truck'
    df_biomass_kg.loc[df_biomass_kg['mean_distance_crow_flys_mi'] < truck_distance,'transport_method'] = 'truck'
    df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] > truck_distance,'transport_method'] = 'barge'
    df_biomass_kg.loc[df_biomass_kg['mean_distance_crow_flys_mi'] > truck_distance,'transport_method'] = 'barge'
    df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] < truck_distance,'mean_distance_freight_mi'] = df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] < truck_distance,'mean_distance_crow_flys_mi']
    df_biomass_kg.loc[df_biomass_kg['mean_distance_crow_flys_mi'] < truck_distance,'mean_distance_freight_mi'] = df_biomass_kg.loc[df_biomass_kg['mean_distance_crow_flys_mi'] < truck_distance,'mean_distance_crow_flys_mi']
    # df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] > truck_distance,'mean_distance_freight_mi'] = df_bioplants.loc[df_bioplants['mean_distance_crow_flys_mi'] > truck_distance,'mean_distance_crow_flys_mi']*1.5
    
    df_bioplants.loc[df_bioplants['OBJECTID'] == 208 ,'mean_distance_freight_mi'] = 800
    df_bioplants.loc[df_bioplants['OBJECTID'] == 210 ,'mean_distance_freight_mi'] = 830
    
    df_biomass_kg.loc['28011' ,'mean_distance_freight_mi'] = 800
    df_biomass_kg.loc['05041' ,'mean_distance_freight_mi'] = 800*.803+830*.197
    df_biomass_kg.loc['28133' ,'mean_distance_freight_mi'] = 830
    
total_biomass_used_pct = df_biomass_kg['used_kg'].sum()/df_biomass_kg['total_kg'].sum()

#%% Export Crop Results to Excel
save_results = True
if save_results:
    if biofuel_type == 'Ethanol':
        excel_filename = 'results_by_county_ethanol_corn'
    elif biofuel_type == 'Biodiesel':
        excel_filename = 'results_by_county_biodiesel_soybean'
        
    df_biomass_kg.to_excel(f'{excel_filename}.xlsx',sheet_name='results')

#%% End of Code        
execute_time = datetime.datetime.now() - begin_time
print('')
print('Code execution time: ', execute_time)