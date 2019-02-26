import sys
import ssl
import datetime as dt
import numpy as np
from sqlalchemy.types import String, Integer
from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import threading
from pysolar.solar import *
import time
import psycopg2
import urllib.request
import io
import os

DB_USER=os.getenv("DB_USER")
DB_PASSWORD=os.getenv("DB_PASSWORD")
DB_HOST=os.getenv("DB_HOST")
DB_NAME=os.getenv("DB_NAME")
DB_TIMEOUT= os.getenv('DB_TIMEOUT', 5000)
URL_TIMEOUT = os.getenv('URL_TIMEOUT', 60)

def get_data(s, URL_TIMEOUT):

    start_time = time.time()

    # lock to serialize console output
    lock = threading.Lock()

    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ])

    logger = logging.getLogger()

    logger.info('{} get_data.py start {}'.format(dt.datetime.now(), s))

    try:
        con = psycopg2.connect("dbname={} user={} host={} password={} options='-c statement_timeout={}'".format(DB_NAME, DB_USER, DB_HOST, DB_PASSWORD, DB_TIMEOUT))
    except:
        logger.error("I am unable to connect to the database")

    now = dt.datetime.now()

    from sqlalchemy import create_engine
    engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(DB_USER, DB_PASSWORD, DB_HOST, DB_NAME))

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

    #logger.info('{} getting data for {} from {} to {}'.format(dt.datetime.now(),s, fromDate, toDate)) 

    #get station_id given code
    stationdf = pd.read_sql_query("SELECT id, code, longitude, latitude FROM station WHERE code = '{}'".format(s), con)
    ss = pd.Series(stationdf['id'])
    ss = int(ss)
    #only get records from sql that we're getting from didbase to save resources
    #since = datetime.now() - timedelta(days=n+1)
    #nn = n+2

    #get data from GIRO, save to stationdata
    urlpt1 = "http://lgdc.uml.edu/common/DIDBGetValues?ursiCode="
    urlpt2 = "&charName=MUFD,hmF2,TEC,scaleF2,hF2,hmF1,hmE,foF2,foF1,foE,foEs,fbEs,yF1,hE,yF2&DMUF=3000"
    #urlpt2 = "&charName=MUFD,hmF2,TEC,foF2,foE,foEs&DMUF=3000" #standard parameters
    df_list = []
    for index, row in stationdf.iterrows():
        logger.info('{} read_csv {} {}{}{}{}'.format(dt.datetime.now(), row['code'], urlpt1, row['code'], urlpt2, urldates))
        with urllib.request.urlopen(urlpt1 + row['code'] + urlpt2 + urldates, timeout=URL_TIMEOUT) as conn:
            pagecontent = conn.read()
            #print(pagecontent)

        df = pd.read_table(io.StringIO(pagecontent.decode('utf-8')),
            comment='#',
            delim_whitespace=True,
            parse_dates=[0],
            names = ['time', 'cs', 'fof2', 'qd', 'fof1', 'qd', 'mufd', 'qd', 'foes', 'qd', 'foe', 'qd', 'hf2', 'qd', 'he', 'qd', 'hme', 'qd', 'hmf2', 'qd', 'hmf1', 'qd', 'yf2', 'qd', 'yf1', 'qd', 'tec', 'qd', 'scalef2', 'qd', 'fbes', 'qd'])\
            .assign(station_id=row['id'])
            #names = ['time', 'cs', 'fof2', 'qd', 'mufd', 'qd', 'foes', 'qd', 'foe', 'qd', 'hmf2', 'qd', 'tec', 'qd'])\ #standard parameters
        df_list.append(df)
    stationdata=pd.concat(df_list)
    logger.info('{} read_csv complete {}'.format(dt.datetime.now(),s))
    stationdata = stationdata[['station_id', 'time', 'cs', 'fof2', 'fof1', 'mufd', 'foes', 'foe', 'hf2', 'he', 'hme', 'hmf2', 'hmf1', 'yf2',  'yf1',  'tec', 'scalef2', 'fbes']]
    #stationdata = stationdata[['station_id', 'time', 'cs', 'fof2', 'mufd', 'foes', 'foe', 'hmf2', 'tec']] #standard parameters

    #logger.info('{} getting processed records for {} from {} to {}'.format(dt.datetime.now(),s, fromDate, toDate))
    processed = pd.read_sql("SELECT * FROM measurement WHERE station_id = '{}'".format(ss), con)

    stationdata.cs = stationdata.cs.astype(str)
    stationdata = stationdata[stationdata.cs.str.contains("No") == False]

    #added processing 

    #get same datatypes before concat
    processed[['time']] = processed[['time']].apply(pd.to_datetime)
    stationdata[['time']] = stationdata[['time']].apply(pd.to_datetime)

    logger.info('{} row count: stationdata {} processed {}'.format(s, len(stationdata), len(processed)))
    concatted = pd.concat([processed,stationdata], ignore_index=True, sort=False).drop_duplicates(subset=['station_id', 'time'])
    concatted = concatted[pd.isnull(concatted['altitude'])]
    #logger.info ('row count: combined {}'.format(concatted.count()))

    #to shift 0 to 360 to -180 to 180 values for correct mapping, sun altitude
    stationdf.loc[stationdf.longitude > 180, 'longitude'] = stationdf.longitude - 360

    #merge to get station data
    stationdf.reset_index(inplace=True)
    concatted['station_id'] = concatted['station_id'].astype(int)
    unprocessed = stationdf.merge(concatted, left_on='id', right_on='station_id', how='right')

    #set --- to nan
    unprocessed = unprocessed.applymap(lambda x: x.replace('---','') if type(x) is str else x)

    # round fof2 to 3 decimals
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

    unprocessed = unprocessed[['time','cs','fof2','fof1','mufd','foes','foe','hf2','he','hme','hmf2','hmf1','yf2','yf1','tec','scalef2','fbes','altitude', 'station_id']]
    #unprocessed = unprocessed[['time','cs','fof2','fof1','mufd','foes','foe','hf2','he','hme','hmf2','hmf1','yf2','yf1','tec','scalef2','fbes','altitude', 'station_id']] #standard parameters

    df['column'] = df['column'].apply(lambda x: round(x, decimals))
    unprocessed[['altitude']] = unprocessed[['altitude']].round({'altitude': 1})
    #logger.info('{} to_sql start {}'.format(dt.datetime.now(),s))
    unprocessed.to_sql('measurement', con=engine, if_exists='append', index=False)
    end_time = time.time()
    #logger.info('{} to_sql complete {}'.format(dt.datetime.now(), s))
    #print ('complete {} {} new records'.format(s, len(unprocessed.index)))
    logger.info('{} processed {} rows for {} in {} seconds'.format(dt.datetime.now(),len(unprocessed), s,  round(end_time - start_time, 2)))
    
