#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 16:46:15 2022

@author: bradenlimb
"""

#%% Import Modules
from IPython import get_ipython
get_ipython().magic('reset -sf')
import pandas as pd
from sklearn.preprocessing import normalize
import numpy as np

from joblib import Parallel, delayed
import multiprocessing

#%% Simplify Data
def usda_data(fips,years,commodities,categories,source_desc,para_year = True):
    file_location = 'usda_data'
    output_dict = {}
    # source_desc = 'CENSUS'
    agg_level_desc = 'COUNTY'
    
    commodities = commodities.copy()
    commodities_in = commodities.copy()
    if 'CORN/SOY' in commodities:
        corn_soy = True
        commodities.insert(0,'SOYBEANS')
        commodities.insert(0,'CORN')
        # commodities_in = commodities.copy()
        commodities.remove('CORN/SOY')
    else:
        corn_soy = False
    
    if para_year:
        def para_years(year): 
            output_dict_temp = {}
            for category in categories:
                df_temp=pd.DataFrame(columns=commodities)
                df_temp['fips'] = fips
                df_temp.set_index('fips',inplace=True)            
                for commodity in commodities:
                    filename = file_str = f'{year}_{source_desc}_{agg_level_desc}_{commodity}_{category}'
                    df_data_temp = pd.read_excel (rf'{file_location}/{filename}.xlsx',
                                                  dtype={"fips": str})
                    df_data_temp.loc[df_data_temp.fips.str.len()<5,"fips"]="0"+df_data_temp.fips
                    
                    for fip in fips['fips'].tolist():
                        df_request = df_data_temp.loc[df_data_temp['fips']==fip]
                        if df_request.empty:
                            continue
                        
                        # Convert all yeilds to kg
                        if commodity == 'CORN' and category == 'PRODUCTION': 
                            crop_multiple = 25.4012 #Corn BU to kg
                        elif commodity == 'SOYBEANS' and category == 'PRODUCTION':
                            crop_multiple = 27.216 #Soybean BU to kg
                        elif commodity == 'WHEAT' and category == 'PRODUCTION':
                            crop_multiple = 27.216 #Wheat BU to kg
                        elif commodity == 'HAY & HAYLAGE' and category == 'PRODUCTION':
                            crop_multiple = 907.185 #Hay ton to kg
                        elif commodity == 'OATS' and category == 'PRODUCTION':
                            crop_multiple = 14.515 #Oats BU to kg
                        elif commodity == 'RYE' and category == 'PRODUCTION':
                            crop_multiple = 1000/39.3680 #Rye BU to kg
                        elif commodity == 'BARLEY' and category == 'PRODUCTION':
                            crop_multiple = 21.772 #BARLEY BU to kg
                        elif commodity == 'SORGHUM' and category == 'PRODUCTION':
                            crop_multiple = 25.4012 #SORGHUM BU to kg
                        elif commodity == 'RICE' and category == 'PRODUCTION':
                            crop_multiple = 45.36 #RICE CWT to kg    
                        elif commodity == 'COTTON' and category == 'PRODUCTION':
                            crop_multiple = 225 #Cotton Bale to kg
                        elif commodity == 'SWITCHGRASS' and category == 'PRODUCTION':
                            crop_multiple = 907.185 #Switchgrass ton to kg
                        elif commodity == 'MISCANTHUS' and category == 'PRODUCTION':
                            crop_multiple = 907.185 #Miscanthus ton to kg
                        else:
                            crop_multiple = 1
                        df_temp.loc[fip,commodity] = df_request['Value'].item()*crop_multiple
                        
                df_temp.fillna(0,inplace=True)
                if corn_soy:
                    df_temp['CORN/SOY'] = df_temp[['CORN', 'SOYBEANS']].mean(axis=1)
                    df_temp.drop(['CORN', 'SOYBEANS'], axis=1, inplace=True)
                    df_temp = df_temp[commodities_in]
                    
                output_dict_temp[f'{year} {category}'] = df_temp
                
                temp_norm = abs(normalize(df_temp, axis=1, norm='l1'))
                df_temp_norm=pd.DataFrame(temp_norm,columns=commodities_in)
                df_temp_norm['fips'] = fips
                df_temp_norm.set_index('fips',inplace=True) 
                output_dict_temp[f'{year} {category} NORMAL'] = df_temp_norm
            
            df_temp_prod_per_area = output_dict_temp[f'{year} PRODUCTION']/output_dict_temp[f'{year} AREA HARVESTED']
            df_temp_prod_per_area.fillna(0,inplace=True)
            df_temp_prod_per_area.replace([np.inf, -np.inf], 0, inplace=True)
            output_dict_temp[f'{year} PRODUCTION PER AREA'] = df_temp_prod_per_area
            
            temp_prod_per_area_norm = abs(normalize(df_temp_prod_per_area, axis=1, norm='max'))
            df_temp_prod_per_area_norm=pd.DataFrame(temp_prod_per_area_norm,columns=commodities_in)
            df_temp_prod_per_area_norm['fips'] = fips
            df_temp_prod_per_area_norm.set_index('fips',inplace=True) 
            output_dict_temp[f'{year} PRODUCTION PER AREA NORMAL'] = df_temp_prod_per_area_norm
            return output_dict_temp
        
        num_cores = multiprocessing.cpu_count()
        output_dict_parts = Parallel(n_jobs=num_cores-1)(delayed(para_years)(year) for year in years)  
        for i in range(len(output_dict_parts)):
            # print(i)
            if i == 0:
                output_dict = output_dict_parts[i]
            else:
                output_dict.update(output_dict_parts[i])
    else:
        for year in years:
            for category in categories:
                df_temp=pd.DataFrame(columns=commodities)
                df_temp['fips'] = fips
                df_temp.set_index('fips',inplace=True)            
                for commodity in commodities:
                    filename = file_str = f'{year}_{source_desc}_{agg_level_desc}_{commodity}_{category}'
                    df_data_temp = pd.read_excel (rf'{file_location}/{filename}.xlsx',
                                                  dtype={"fips": str})
                    df_data_temp.loc[df_data_temp.fips.str.len()<5,"fips"]="0"+df_data_temp.fips
                    
                    for fip in fips['fips'].tolist():
                        df_request = df_data_temp.loc[df_data_temp['fips']==fip]
                        if df_request.empty:
                            continue
                        
                        # Convert all yeilds to kg
                        if commodity == 'CORN' and category == 'PRODUCTION': 
                            crop_multiple = 25.4012 #Corn BU to kg
                        elif commodity == 'SOYBEANS' and category == 'PRODUCTION':
                            crop_multiple = 27.216 #Soybean BU to kg
                        elif commodity == 'WHEAT' and category == 'PRODUCTION':
                            crop_multiple = 27.216 #Wheat BU to kg
                        elif commodity == 'HAY & HAYLAGE' and category == 'PRODUCTION':
                            crop_multiple = 907.185 #Hay ton to kg
                        elif commodity == 'OATS' and category == 'PRODUCTION':
                            crop_multiple = 14.515 #Oats BU to kg
                        elif commodity == 'RYE' and category == 'PRODUCTION':
                            crop_multiple = 1000/39.3680 #Rye BU to kg
                        elif commodity == 'BARLEY' and category == 'PRODUCTION':
                            crop_multiple = 21.772 #BARLEY BU to kg
                        elif commodity == 'SORGHUM' and category == 'PRODUCTION':
                            crop_multiple = 25.4012 #SORGHUM BU to kg
                        elif commodity == 'RICE' and category == 'PRODUCTION':
                            crop_multiple = 45.36 #RICE CWT to kg    
                        elif commodity == 'COTTON' and category == 'PRODUCTION':
                            crop_multiple = 225 #Cotton Bale to kg
                        elif commodity == 'SWITCHGRASS' and category == 'PRODUCTION':
                            crop_multiple = 907.185 #Switchgrass ton to kg
                        elif commodity == 'MISCANTHUS' and category == 'PRODUCTION':
                            crop_multiple = 907.185 #Miscanthus ton to kg
                        else:
                            crop_multiple = 1
                        df_temp.loc[fip,commodity] = df_request['Value'].item()*crop_multiple
                        
                df_temp.fillna(0,inplace=True)
                if corn_soy:
                    df_temp['CORN/SOY'] = df_temp[['CORN', 'SOYBEANS']].mean(axis=1)
                    df_temp.drop(['CORN', 'SOYBEANS'], axis=1, inplace=True)
                    df_temp = df_temp[commodities_in]
                    
                output_dict[f'{year} {category}'] = df_temp
                
                temp_norm = abs(normalize(df_temp, axis=1, norm='l1'))
                df_temp_norm=pd.DataFrame(temp_norm,columns=commodities_in)
                df_temp_norm['fips'] = fips
                df_temp_norm.set_index('fips',inplace=True) 
                output_dict[f'{year} {category} NORMAL'] = df_temp_norm
            
            df_temp_prod_per_area = output_dict[f'{year} PRODUCTION']/output_dict[f'{year} AREA HARVESTED']
            df_temp_prod_per_area.fillna(0,inplace=True)
            df_temp_prod_per_area.replace([np.inf, -np.inf], 0, inplace=True)
            output_dict[f'{year} PRODUCTION PER AREA'] = df_temp_prod_per_area
            
            temp_prod_per_area_norm = abs(normalize(df_temp_prod_per_area, axis=1, norm='max'))
            df_temp_prod_per_area_norm=pd.DataFrame(temp_prod_per_area_norm,columns=commodities_in)
            df_temp_prod_per_area_norm['fips'] = fips
            df_temp_prod_per_area_norm.set_index('fips',inplace=True) 
            output_dict[f'{year} PRODUCTION PER AREA NORMAL'] = df_temp_prod_per_area_norm
        
    # if corn_soy:
    #     for item in list(output_dict.keys()):
    #         df_change = output_dict[item]
    #         df_change['CORN/SOY'] = df[['CORN', 'SOYBEANS']].mean(axis=1)
    #         print(item)
            
    return output_dict
            
#%% Test Run
test_run = True
if test_run:
    fips_sheet = 'corn_Is'
    fips = pd.read_excel (r'usda_data/test_fips.xlsx',
                                  sheet_name=fips_sheet,
                                  dtype={"fips": str})
    
    years = [2012,2017]
    commodities = ['CORN','SOYBEANS','WHEAT','HAY & HAYLAGE','OATS','RYE']
    # commodities = ['CORN/SOY','WHEAT','HAY & HAYLAGE','OATS','RYE']
    categories = ['PRODUCTION','AREA HARVESTED']
    
    usda_program = 'CENSUS'
    source_desc = usda_program
    usda_data_dict = usda_data(fips,years, commodities,categories,usda_program)
    
    x = usda_data_dict['2017 AREA HARVESTED']
    X = usda_data_dict['2017 PRODUCTION']
    
    y = usda_data_dict['2017 AREA HARVESTED NORMAL']
    yy = usda_data_dict['2012 AREA HARVESTED NORMAL']
    Y = usda_data_dict['2017 PRODUCTION NORMAL']
    
    Z = usda_data_dict['2017 PRODUCTION PER AREA']
    W = usda_data_dict['2017 PRODUCTION PER AREA NORMAL']
    w = usda_data_dict['2012 PRODUCTION PER AREA NORMAL']
    
    # temp_prod_per_area_norm = abs(normalize(Z, axis=1, norm='max'))