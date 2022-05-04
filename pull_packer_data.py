'''
writes data for a single packer to s3
Colman Seery

'''

#os.chdir("/home/ec2-user/Contacts-sensitive/")
from turnover import *
from mp_alt_custom import *
import dask
from dask import delayed
from dask.delayed import delayed

import dask.dataframe as dd
import dask.bag as db

import os
import pandas as pd
import geopandas as gpd
import numpy as np
import boto3
from datetime import date, timedelta
from pathlib import Path
import random
import matplotlib.pyplot as plt
from dask.distributed import Client
from io import BytesIO

import subprocess
from os.path import exists




def pull_data(mp_index, end_date, mppath = "s3://yse-bioecon/meat_packers/meat_packers.csv"):
    '''
    writes new packer file to s3, given inputs:
        mp_index = index of desired packer in mpdata
        mppath = path to packer csv file. use "s3://yse-bioecon/meat_packers/meat_packers.csv" for s3 version
        end_date = last date of data. example: date(2021, 3, 1)
    '''
    #skips missing date
    need_dates = []
    if end_date > date(2020,8,24):
        need_dates1 = daterange(date(2020,1,1), date(2020,8,24))
        need_dates2 = daterange(date(2020,8,26), end_date)
        need_dates = [*need_dates1, *need_dates2]
    else:
        need_dates = daterange(date(2020,1,1), end_date)
    
    mpdata = setup_mpdata(mppath)
    tmp_gpkg = get_gpkg(mpdata, mp_index)
    #Gets the polygon associated with a single meatpacker
    packer_tmp = get_packer_poly(mpdata[mp_index:mp_index+1], tmp_gpkg)[0:1]      #this line is problematic, get_packer_poly (from mp_alt) occasionally returns empty df, causing index error.
    packer_tmp = packer_tmp[packer_tmp.fips_code > 0][0:1]
    #c_device_list = build_device_list_series(packer_tmp, need_dates)
    build_device_list_series(packer_tmp, need_dates)
    if c_device_list.shape[0] == 0:
        print("no contacts")
        return
    c_contacts = build_contacts(c_device_list, need_dates, mpdata, mp_index, tmp_gpkg)
    if c_contacts["device_id_1"].iloc[0] == "foo":
        print("no contacts")
        return
    write_to_s3(c_contacts, mpdata, mp_index)
    print("success")
    print(mp_index)
    return


def setup_mpdata(mppath): 
    '''
    gets mpdata from mppath 
    
    '''
    
    #STEP 0, SETUP MPDATA
    mpdata = pd.read_csv(mppath)

    #these were google earth verifed
    mpdata = mpdata[mpdata['verified'] == 1]
    mpdata = mpdata[mpdata['close_to'] == 0]
    mpdata = mpdata[mpdata['polyid'].notna()]
    mpdata = mpdata.drop(['verified', 'close_to', 'census_tract.y'], axis = 1)
    mpdata = mpdata.rename(columns = {'census_tract.x' : 'census_tract'})

    mpdata['geoid'] = mpdata['fips_code'].apply(lambda x: ("0" + str(x)) if len(str(x)) == 4 else str(x))

    mpdata['path'] = mpdata['state'].apply(lambda x: x.lower()) + "/" + mpdata['geoid'] + "/"
    mpdata.reset_index(drop = True, inplace =True)
    #make mpdata spatial
    mpdata = gpd.GeoDataFrame(mpdata, geometry = gpd.points_from_xy(mpdata.lon, mpdata.lat)).set_crs(epsg=4326)
    return mpdata
    
    
    

def get_gpkg(mpdata, mp_index):
    '''
    gets the gpkg polygon
    mpdata = meat packer datafram from csv
    mp_index = index in mpdata of desired packer
    
    '''
    #STEP 0.5, GET PACKER POLYGON
    tmp = mpdata[mp_index:mp_index+1]
    #get the gpkg file -- this needs to be seperate because it is slow, and we can group this stream. 
    tmp_geoid = tmp['geoid'].iloc[0]
    tmp_gpkg =  getgpkg_gz('yse-bioecon', 'landgrid_geoid/' + tmp_geoid + ".gpkg.gz")

    return tmp_gpkg
    

#dask version, has trouble with declumped data.
#fails if more than 50ish days
def build_device_list(packer_tmp, need_dates):
    '''
    creates the list of all devices in the polygon
    
    '''
    result = pd.DataFrame()
    #STEP 1, GET DEVICE LIST
    i = 0 #used to limit number of days at a time, quick fix
    while i < len(need_dates):
        
        dfs =[]
        for f in need_dates[i:]:
            df = delayed(find_mp_device_old)(packer_tmp, f)
            dfs.append(df)
        #%%time    
        device_list = dd.from_delayed(dfs) 

        #c_device_list = device_list.compute()

        c_device_list = device_list.persist()

        #clean up meatpacker device list
        c_device_list = c_device_list[c_device_list['device_id'] != 'foo'].reset_index()
        #c_device_list.drop(['index'], axis = 1, inplace = True)
        c_device_list = c_device_list.drop(['index'], axis = 1).compute()
        result = result.append(c_device_list)
        i += 30
    return result

#series version, works with declumped data
def build_device_list_series(packer_tmp, need_dates):
    '''
    creates the list of all devices in the polygon
    
    '''
    result = []
    #STEP 1, GET DEVICE LIST
    dfs =[]
    for f in need_dates[0:]:
        df = find_mp_device(packer_tmp, f)
        
        dfs.append(df)
    #%%time    
    c_device_list = pd.concat(dfs)


    #clean up meatpacker device list
    c_device_list = c_device_list[c_device_list['device_id'] != 'foo'].reset_index()
    c_device_list = c_device_list.drop(['index'], axis = 1)
    return c_device_list


def build_contacts(c_device_list, need_dates, mpdata, mp_index, tmp_gpkg):
    #STEP 2. sweep county for meat packer device id contacts
    
    tmp = mpdata[mp_index:mp_index+1]
  
    dsample_pre = querysample(c_device_list['device_id'])

    dsample_list = dsample_pre.split(',')

    #max devices at a time. if this is too high, largest packers cause crashes
    #tradeoff is speed
    n = 2500

    ds_list = [dsample_list[i:i + n] for i in range(0, len(dsample_list), n)]
    dfs = []
    device_set = set(c_device_list.device_id.unique())
    for ds in ds_list:
  
        dsample = ",".join(ds)

        sql_sweep2 = ("SELECT * " +
                    "FROM s3Object " +
                    "WHERE device_id_1 in (" + dsample + ") OR device_id_2 in (" + dsample + ")" 
                    )


        mykeys = ['state=' + tmp['state'].iloc[0] + '/local_date=' + x for x in need_dates]

        for f, d in zip(mykeys, need_dates): 
            #prints 1st of every  month
            if d[-2:] == "01":
                print(f)
            df = delayed(pull_raw_um_dc)('yse-bioecon-um2-dc', f, sql_sweep2)  #add -dc
            dfs.append(df)

    all_mp_contacts = dd.from_delayed(dfs)
    all_mp_contacts.drop_duplicates() #needed for when  each device is in separate batch of packer employees
    
    all_mp_contacts['fips_code'] = all_mp_contacts['cbg'].map(lambda x: ("0" + str(x)[0:4]) if len(str(x)) == 11 else str(x)[0:5])
    
    c_all_mp_contacts = all_mp_contacts.compute()
    c_all_mp_contacts = gpd.GeoDataFrame(c_all_mp_contacts, geometry = gpd.points_from_xy(c_all_mp_contacts.lon, c_all_mp_contacts.lat)).set_crs(epsg=4326)
    
    packer_contacts = gpd.sjoin(c_all_mp_contacts, tmp_gpkg, how = 'left', op = 'intersects' )
    packer_contacts['use'] = packer_contacts['lbcs_activity_desc'] + '--' + packer_contacts['lbcs_function_desc'] 
    
    return packer_contacts

#only works on clumped data
def build_contacts_clumped(c_device_list, need_dates, mpdata, mp_index, tmp_gpkg):
    #STEP 2. sweep county for meat packer device id contacts
    
    tmp = mpdata[mp_index:mp_index+1]
    
    dsample_pre = querysample(c_device_list['device_id'])

    dsample_list = dsample_pre.split(',')

    #n = 2500
    n = 1000

    ds_list = [dsample_list[i:i + n] for i in range(0, len(dsample_list), n)]
    #print("setup done")
    dfs =[]
    for ds in ds_list:
        dsample = ",".join(ds)

        sql_sweep2 = ("SELECT * " +
                    "FROM s3Object " +
                    "WHERE device_id_1 in (" + dsample     + ") OR device_id_2 in (" + dsample + ")" 
                    )


        mykeys = ['state=' + tmp['state'].iloc[0] + '/local_date=' + x for x in need_dates]

        
        for f in mykeys:
            print("f")
            #df = delayed(pull_raw_um_dc)('yse-bioecon-um2', f, sql_sweep2)  #add -dc
            df = pull_raw_um_dc('yse-bioecon-um2', f, sql_sweep2)  #add -dc
            dfs.append(df)

    #print("main done")
    #print(dfs)
    #print(dd.from_delayed(dfs[0]))

    #all_mp_contacts = dd.from_delayed(dfs)

    #get fips
    all_mp_contacts['fips_code'] = all_mp_contacts['cbg'].map(lambda x: ("0" + str(x)[0:4]) if len(str(x)) == 11 else str(x)[0:5])
    #determine which is the meat packer 
    all_mp_contacts['mp_device_1'] = all_mp_contacts['device_id_1'].map(lambda x: 1 if x in dsample else 0)
    all_mp_contacts['mp_device_2'] = all_mp_contacts['device_id_2'].map(lambda x: 1 if x in dsample else 0)

    c_all_mp_contacts = all_mp_contacts.compute()
    c_all_mp_contacts = gpd.GeoDataFrame(c_all_mp_contacts, geometry = gpd.points_from_xy(c_all_mp_contacts.lon, c_all_mp_contacts.lat)).set_crs(epsg=4326)
    print(c_all_mp_contacts.columns)
    
    packer_contacts = gpd.sjoin(c_all_mp_contacts, tmp_gpkg, how = 'left', op = 'intersects' )

    packer_contacts['use'] = packer_contacts['lbcs_activity_desc'] + '--' + packer_contacts['lbcs_function_desc'] 
    return packer_contacts



def write_to_s3(packer_contacts, mpdata, mp_index):
    '''
    writes the end result to s3 according to existing naming convention:
        meat_packers/fips_code/index_company_name. 
    '''
    tmp = mpdata[mp_index:mp_index+1]
    my_buffer = BytesIO()
    packer_contacts.to_parquet(my_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object('yse-bioecon', 'meat_packers/contacts-dc/' + tmp['geoid'].iloc[0] + "/" + str(mp_index) + "_" + tmp['company'].iloc[0].replace(" ", "_") + '.parquet' ).put(Body=my_buffer.getvalue())
    return
