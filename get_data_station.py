import sys
import os
from pysolar.solar import *
import time
import psycopg2
import urllib.request
import io

cwd = os.getcwd()

def get_data(s, n=1):

    #import sys
    import ssl
    import datetime as dt
    import fileinput
    import numpy as np
    import logging
    from sqlalchemy.types import String, Integer
    from datetime import datetime
    from datetime import timedelta
    from logging.handlers import RotatingFileHandler
    import pandas as pd

    # lock to serialize console output
    import threading
    lock = threading.Lock()

    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        #logging.FileHandler("{0}/{1}.log".format(logPath, fileName)),
        logging.StreamHandler(sys.stdout)
    ])

    logger = logging.getLogger()

    #logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    logger.info('{} get_data.py start {}'.format(dt.datetime.now(), s))

    try:
        con = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='mysecretpassword'")
    except:
        logger.error("I am unable to connect to the database")

    now = dt.datetime.now()

    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres:mysecretpassword@localhost:5432/postgres')

    #added 4/21 for SSL error workaround
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
    since = datetime.now() - timedelta(days=n+1)
    nn = n+2

    #get data from GIRO, save to stationdata
    urlpt1 = "http://lgdc.uml.edu/common/DIDBGetValues?ursiCode="
    urlpt2 = "&charName=MUFD,hmF2,TEC,scaleF2,hF2,hmF1,hmE,foF2,foF1,foE,foEs,fbEs,yF1,hE,yF2&DMUF=3000"
    df_list = []
    for index, row in stationdf.iterrows():
        logger.info('{} read_csv {} {}{}{}{}'.format(dt.datetime.now(), row['code'], urlpt1, row['code'], urlpt2, urldates))
        
        #to avoid hanging if target unavailable fetch page with urllib using 10 second timeout then handoff to pandas
        with urllib.request.urlopen(urlpt1 + row['code'] + urlpt2 + urldates, timeout=10) as conn:
            pagecontent = conn.read()
            
        df = pd.read_table(io.StringIO(pagecontent.decode('utf-8')),
            comment='#',
            delim_whitespace=True,
            parse_dates=[0],
            names = ['time', 'cs', 'fof2', 'qd', 'fof1', 'qd', 'mufd', 'qd', 'foes', 'qd', 'foe', 'qd', 'hf2', 'qd', 'he', 'qd', 'hme', 'qd', 'hmf2', 'qd', 'hmf1', 'qd', 'yf2', 'qd', 'yf1', 'qd', 'tec', 'qd', 'scalef2', 'qd', 'fbes', 'qd'])\
            .assign(station_id=row['id'])
        df_list.append(df)
    stationdata=pd.concat(df_list)
    logger.info('{} read_csv complete {}'.format(dt.datetime.now(),s))
    stationdata = stationdata[['station_id', 'time', 'cs', 'fof2', 'fof1', 'mufd', 'foes', 'foe', 'hf2', 'he', 'hme', 'hmf2', 'hmf1', 'yf2',  'yf1',  'tec', 'scalef2', 'fbes']]

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

    #filter out errors
    #unprocessed = unprocessed[unprocessed.time != 'ERROR:']

    #unprocessed[['mufd']] = unprocessed[['mufd']].apply(pd.to_numeric)
    unprocessed[['time']] = unprocessed[['time']].apply(pd.to_datetime)
    unprocessed.longitude = unprocessed.longitude.astype(float)
    unprocessed.latitude = unprocessed.latitude.astype(float)

    #calculate sun altitude
    unprocessed['altitude']=np.nan
    start_time = time.time()
    n = 0
    for index, row in unprocessed.iterrows():
        if pd.isnull(row['altitude']):
            n = n +1
            date = row['time']
            tz = dt.timezone.utc
            date = date.replace(tzinfo=tz)
            unprocessed.loc[index, 'altitude'] = get_altitude( row['latitude'],row['longitude'], date)
    end_time = time.time()
    #logger.info('{} pysolar processed {} rows for {} in {} seconds'.format(dt.datetime.now(),n, s,  round(end_time - start_time, 2)))
    unprocessed.sort_values(by=['time'], inplace=True)

    unprocessed = unprocessed[['time','cs','fof2','fof1','mufd','foes','foe','hf2','he','hme','hmf2','hmf1','yf2','yf1','tec','scalef2','fbes','altitude', 'station_id']]

    unprocessed[['altitude']] = unprocessed[['altitude']].round({'altitude': 1})
    #logger.info('{} to_sql start {}'.format(dt.datetime.now(),s))
    unprocessed.to_sql('measurement', con=engine, if_exists='append', index=False)
    #logger.info('{} to_sql complete {}'.format(dt.datetime.now(), s))
    #print ('complete {} {} new records'.format(s, len(unprocessed.index)))
    logger.info('{} processed {} rows for {} in {} seconds'.format(dt.datetime.now(),len(unprocessed), s,  round(end_time - start_time, 2)))

#get_data(sys.argv[1])
