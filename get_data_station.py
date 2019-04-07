import sys
import ssl
import datetime as dt
import numpy as np
#from sqlalchemy.types import String, Integer
from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import threading
from pysolar.solar import *
import time
import urllib.request
import io
import os
import boto3
import json
import decimal

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

table = dynamodb.Table('giro-dev')

URL_TIMEOUT = os.getenv('URL_TIMEOUT', 60)

def get_data(s, URL_TIMEOUT):

    start_time = time.time()

    # lock to serialize console output
    lock = threading.Lock()

    stationdf = s;
  
    #print("s is {}".format(s))

    """

    stationId = s['stationId']
    name = s['name']
    longitude = station['longitude']
    latitude = station['latitude'] 
     
    print("station is {}".format(station))
    """

    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ])

    logger = logging.getLogger()

    logger.info('{} get_data.py start {}'.format(dt.datetime.now(), stationdf['name']))

    now = dt.datetime.now()

    try:
        _create_unverified_https_context = ssl._create_unverified_context
    except AttributeError:
        # Legacy Python that doesn't verify HTTPS certificates by default
        pass
    else:
        # Handle target environment that doesn't support HTTPS verification
        ssl._create_default_https_context = _create_unverified_https_context

    fromDate = str((dt.datetime.now() - dt.timedelta(minutes=int(20))).strftime('%Y-%m-%dT%H:%M:%S'))
    toDate = str((dt.datetime.now() + dt.timedelta(minutes=int(5))).strftime('%Y-%m-%dT%H:%M:%S'))
    urlfrom = '&fromDate=' + fromDate
    urlto = '&toDate=' + toDate
    urldates = urlfrom + urlto

    #get data from GIRO, save to stationdata
    urlpt1 = "http://lgdc.uml.edu/common/DIDBGetValues?ursiCode="
    urlpt2 = "&charName=MUFD,hmF2,TEC,scaleF2,hF2,hmF1,hmE,foF2,foF1,foE,foEs,fbEs,yF1,hE,yF2&DMUF=3000"
    #urlpt2 = "&charName=MUFD,hmF2,TEC,foF2,foE,foEs&DMUF=3000" #standard parameters
    df_list = []
    #for index, row in stationdf.iterrows():
    logger.info('{} read_csv {} {}{}{}{}'.format(dt.datetime.now(), stationdf['stationId'], urlpt1, stationdf['stationId'], urlpt2, urldates))
    with urllib.request.urlopen(urlpt1 + stationdf['stationId'] + urlpt2 + urldates, timeout=URL_TIMEOUT) as conn:
        pagecontent = conn.read()
        #print(pagecontent)

        df = pd.read_csv(io.StringIO(pagecontent.decode('utf-8')),
            comment='#',
            delim_whitespace=True,
            parse_dates=[0],
            names = ['time', 'cs', 'fof2', 'qd', 'fof1', 'qd', 'mufd', 'qd', 'foes', 'qd', 'foe', 'qd', 'hf2', 'qd', 'he', 'qd', 'hme', 'qd', 'hmf2', 'qd', 'hmf1', 'qd', 'yf2', 'qd', 'yf1', 'qd', 'tec', 'qd', 'scalef2', 'qd', 'fbes', 'qd'])\
            .assign(stationId=stationdf['stationId'])
            #names = ['time', 'cs', 'fof2', 'qd', 'mufd', 'qd', 'foes', 'qd', 'foe', 'qd', 'hmf2', 'qd', 'tec', 'qd'])\ #standard parameters
        df_list.append(df)
    stationdata=pd.concat(df_list)
    stationdata = stationdata.iloc[[-1]]
    logger.info('{} read_csv complete {}'.format(dt.datetime.now(),s))
    stationdata = stationdata[['stationId', 'time', 'cs', 'fof2', 'fof1', 'mufd', 'foes', 'foe', 'hf2', 'he', 'hme', 'hmf2', 'hmf1', 'yf2',  'yf1',  'tec', 'scalef2', 'fbes']]
    #stationdata = stationdata[['stationId', 'time', 'cs', 'fof2', 'mufd', 'foes', 'foe', 'hmf2', 'tec']] #standard parameters

    stationdata.cs = stationdata.cs.astype(str)
    stationdata = stationdata[stationdata.cs.str.contains("No") == False]

    #added processing 

    #get same datatypes before concat
    stationdata[['time']] = stationdata[['time']].apply(pd.to_datetime)

    stationdata['altitude'] = np.nan

    concatted = stationdata #[pd.isnull(stationdata['altitude'])]
    #logger.info ('row count: combined {}'.format(concatted.count()))
    
    stationdf = pd.DataFrame([stationdf])

    #print("stationdf is {}".format(stationdf)) 
    #print("stationdf longitude is {}".format(stationdf['longitude']))

    #stationdf['longitude'] = stationdf['longitude'].astype(float)
    stationdf['latitude'] = stationdf['latitude'].astype(float)
   
    stationdf.longitude = stationdf.longitude.astype(float)
 
     #stationdf.longitude.iloc[0]  
    #to shift 0 to 360 to -180 to 180 values for correct mapping, sun altitude
    stationdf.loc[stationdf.longitude > 180, 'longitude'] = stationdf.longitude - 360

    #merge to get station data
    stationdf.reset_index(inplace=True)
    #concatted['stationId'] = concatted['stationId'].astype(int)
    unprocessed = stationdf.merge(concatted, left_on='stationId', right_on='stationId', how='right')

    #set --- to nan
    #unprocessed = unprocessed.applymap(lambda x: x.replace('---','') if type(x) is str else x)

    # round fof2 to 3 decimals
    #print("unprocessed is {}".format(unprocessed)); 
    unprocessed['fof2'] = unprocessed['fof2'].astype(float)
    unprocessed['fof2'] = unprocessed['fof2'].apply(lambda x: round(x, 3))

    #filter out errors
    #unprocessed = unprocessed[unprocessed.time != 'ERROR:']

    #unprocessed[['mufd']] = unprocessed[['mufd']].apply(pd.to_numeric)
    unprocessed[['time']] = unprocessed[['time']].apply(pd.to_datetime)
    unprocessed.longitude = unprocessed.longitude.astype(float)
    unprocessed.latitude = unprocessed.latitude.astype(float)

    #calculate sun altitude
    unprocessed['altitude']=np.nan
    n = 0
    for index, row in unprocessed.iterrows():
        if pd.isnull(row['altitude']):
            n = n +1
            date = row['time']
            tz = dt.timezone.utc
            date = date.replace(tzinfo=tz)
            unprocessed.loc[index, 'altitude'] = get_altitude( row['latitude'],row['longitude'], date)
    end_time = time.time()
    unprocessed.sort_values(by=['time'], inplace=True)

    unprocessed = unprocessed[['time','cs','fof2','fof1','mufd','foes','foe','hf2','he','hme','hmf2','hmf1','yf2','yf1','tec','scalef2','fbes','altitude', 'stationId']]
    #unprocessed = unprocessed[['time','cs','fof2','fof1','mufd','foes','foe','hf2','he','hme','hmf2','hmf1','yf2','yf1','tec','scalef2','fbes','altitude', 'stationId']] #standard parameters

    unprocessed[['altitude']] = unprocessed[['altitude']].round({'altitude': 1})
    #logger.info('{} to_sql start {}'.format(dt.datetime.now(),s))
    #unprocessed.to_sql('measurement', con=engine, if_exists='append', index=False)
   
    #print("unprocessed is {}".format(unprocessed)) 
   
    unprocessed = unprocessed.fillna(0)
    unprocessed = unprocessed.applymap(str)
    #unprocessed.replace(r'\s+', np.nan, regex=True)
 
    #unprocessed.time = unprocessed.time.astype(str)
    #unprocessed.mufd = unprocessed.mufd.astype(str)
    
 
    response = table.update_item(
        Key={
            'stationId': unprocessed.stationId.iloc[0],
        },
        UpdateExpression="set updated = :t, mufd =:m, cs = :cs, fof2 = :fof2, fof1 = :fof1, foes = :foes, hf2 = :hf2, he = :he, hme = :hme, hmf2 = :hmf2, hmf1 = :hmf1, yf2 = :yf2, yf1 = :yf1, tec = :tec, scalef2 = :scalef2, fbes = :fbes, altitude = :altitude",
        ExpressionAttributeValues={
            ':t': unprocessed.time.iloc[0],
            ':m': unprocessed.mufd.iloc[0],
            ':cs' : unprocessed.cs.iloc[0],
            ':fof2' : unprocessed.fof2.iloc[0],
            ':fof1' : unprocessed.fof1.iloc[0],
            ':foes' : unprocessed.foes.iloc[0],
            ':hf2' : unprocessed.hf2.iloc[0],
            ':he' : unprocessed.he.iloc[0],
            ':hme' : unprocessed.hme.iloc[0],
            ':hmf2' : unprocessed.hmf2.iloc[0],
            ':hmf1' : unprocessed.hmf1.iloc[0],
            ':yf2' : unprocessed.yf2.iloc[0],
            ':yf1' : unprocessed.yf1.iloc[0],
            ':tec' : unprocessed.tec.iloc[0],
            ':scalef2' : unprocessed.scalef2.iloc[0],
            ':fbes' : unprocessed.fbes.iloc[0],
            ':altitude' : unprocessed.altitude.iloc[0]
        },
        ReturnValues="UPDATED_NEW"
    )
   
    #print("UpdateItem succeeded:")
    #print(json.dumps(response, indent=4))
     
    end_time = time.time()
    #logger.info('{} to_sql complete {}'.format(dt.datetime.now(), s))
    #print ('complete {} {} new records'.format(s, len(unprocessed.index)))
    logger.info('{} processed {} rows for {} in {} seconds'.format(dt.datetime.now(),len(unprocessed), s,  round(end_time - start_time, 2)))
    
