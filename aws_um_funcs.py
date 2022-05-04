import os
import boto3
import pandas as pd
import geopandas as gpd
import io
import gzip
from zipfile import ZipFile
from io import StringIO
from pathlib import Path
client = boto3.client('s3') 
import pyarrow.parquet as pq
from datetime import date, timedelta
import dask.dataframe as dd

session = boto3.session.Session(profile_name='yse-bioecon')
client = boto3.client('s3') 

"""
FUNCTIONS I WROTE WHICH ARE PART OF A LARGER FILE ON OUR PRIVATE GITHUB
WILL NOT RUN WITHOUT THE OTHER FUNCTIONS OR ACCESS TO OUR DATA

"""


#rewritten 04/26/22 to fix no-in-county-contacts bug 
def pull_raw_um_dc(xbucket, StateDate, xsql):
    umfile = 'third/'+ StateDate
    localkey = get_s3_keys(xbucket, umfile)
    umdata = []
    for jkey in localkey:
        try:
            key = jkey
            tumdata = s3selecttsv(xbucket, key, xsql)
            tumdata.columns = [
                'crap',  
                'lat',
                'lon',
                'distance_m',
                'cbg',
                'device_id_1',
                      'unixtime_1' ,
                'datasource_id_1',
                'cel_distance_m_1',
                'local_time_1',
                'speed_kph_1',
                'device_id_2', 
                'unixtime_2', 
                'datasource_id_2',
                'cel_distance_m_2',
                'local_time_2',
                'speed_kph_2'       
                    ]
            tumdata['date'] = umfile[len(umfile)-8: len(umfile)]
            umdata.append(tumdata)
        except:
            continue
    if len(umdata) == 0:
        umdata = pd.DataFrame({
              'crap': [1.0],
              'lat': [1.0],
              'lon': [1.0],
              'distance_m': [1],
              'cbg': [1],
              'device_id_1': ['foo'],
              'unixtime_1': [1] ,
              'datasource_id_1': [1],
              'cel_distance_m_1': [1.0],
              'local_time_1': ['foo'],
              'speed_kph_1': [1.0],
              'device_id_2': ['foo'], 
              'unixtime_2': [1], 
              'datasource_id_2': [1],
              'cel_distance_m_2':[1.0],
              'local_time_2': ['foo'],
              'speed_kph_2':[1.0],
              'date': ['foo']})
    else:
        umdata = pd.concat(umdata)             
            #if isdask == 1:
            #umdata = dd.from_pandas(umdata)
    return(umdata)
    
#Serves same purpose as pull_raw_um_dc(), used as initial declump data fix but was too slow
def pull_contacts_given_devices(xbucket, StateDate, devices):
    umfile = 'third/' + StateDate
    
    cdate = StateDate.split('=')[-1]
    
    localkey = get_s3_keys(xbucket, umfile)
    umdata = []
    for jkey in localkey:
        key = "s3://" + xbucket + "/" + jkey
        tmp_umdata = pd.read_csv(key, sep = '\t', compression = 'gzip')
        tmp_umdata['scbg'] = tmp_umdata['cbg'].apply(str)
        tmp_umdata['scbg'] = [x if len(x) == 12 else '0' + x for x in tmp_umdata['scbg'] ]

        q = tmp_umdata.query("(device_id_1 in @devices) or (device_id_2 in @devices)")
        umdata.append(q)
        
    mp_contacts = pd.concat(umdata, ignore_index=True)
    mp_contacts.date = cdate
    return mp_contacts
