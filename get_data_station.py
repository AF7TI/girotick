import sys
import os
from pysolar.solar import *
import time
import psycopg2

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

    try:
        con = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='mysecretpassword'")
    except:
        print ("I am unable to connect to the database")

    now = dt.datetime.now()
    logging.basicConfig(filename='sys.stdout',level=logging.INFO)
    #logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    #handler = RotatingFileHandler("get_data.log", maxBytes=2000000, backupCount=10)
    #logging.addHandler(handler)
    logging.info('{} get_data.py start {}'.format(dt.datetime.now(), s))

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

    if n == '1':
        fromDate = str((dt.datetime.now() - dt.timedelta(days=int(1))).strftime('%Y-%m-%d'))
    else:
        fromDate = str((dt.datetime.now() - dt.timedelta(days=int(n))).strftime('%Y-%m-%d'))
    toDate = str((dt.datetime.now() + dt.timedelta(days=1)).strftime('%Y-%m-%d'))
    urlfrom = '&fromDate=' + fromDate
    urlto = '&toDate=' + toDate
    urldates = urlfrom + urlto

    #logging.info('{} getting data for {} from {} to {}'.format(dt.datetime.now(),s, fromDate, toDate)) 

    #get station_id given code
    print ('selecting station from stationdf')
    stationdf = pd.read_sql_query("SELECT * FROM station WHERE code = '{}'".format(s), con)
    print(stationdf)
    ss = pd.Series(stationdf['id'])
    ss = int(ss)
    #only get records from sql that we're getting from didbase to save resources
    since = datetime.now() - timedelta(days=n+1)
    nn = n+2

    #get data from GIRO, save to stationdata
    urlpt1 = "http://lgdc.uml.edu/common/DIDBGetValues?ursiCode="
    urlpt2 = "&charName=mufd,hmF2,TEC,scaleF2,hF2,hmF1,hmE,foF2,foF1,foE,foEs,fbEs,yF1,hE,yF2&DMUF=3000"
    df_list = []
    for index, row in stationdf.iterrows():
        #print (row['code'])
        logging.info('{} read_csv {} {}{}{}{}'.format(dt.datetime.now(), row['code'], urlpt1, row['code'], urlpt2, urldates))
        df=pd.read_csv(urlpt1 + row['code'] + urlpt2 + urldates,
            comment='#',
            delim_whitespace=True,
            parse_dates=[0],
            names = ['time', 'cs', 'fof2', 'qd', 'fof1', 'qd', 'mufd', 'qd', 'foes', 'qd', 'foe', 'qd', 'hf2', 'qd', 'he', 'qd', 'hme', 'qd', 'hmf2', 'qd', 'hmf1', 'qd', 'yf2', 'qd', 'yf1', 'qd', 'tec', 'qd', 'scalef2', 'qd', 'fbes', 'qd'])\
            .assign(station_id=row['id'])
        df_list.append(df)
    stationdata=pd.concat(df_list)
    logging.info('{} read_csv complete {}'.format(dt.datetime.now(),s))
    stationdata = stationdata[['station_id', 'time', 'cs', 'fof2', 'fof1', 'mufd', 'foes', 'foe', 'hf2', 'he', 'hme', 'hmf2', 'hmf1', 'yf2',  'yf1',  'tec', 'scalef2', 'fbes']]

    #logging.info('{} getting processed records for {} from {} to {}'.format(dt.datetime.now(),s, fromDate, toDate))
    #processed = pd.read_sql(session.query(Measurement).filter(Measurement.station_id == ss).filter(Measurement.time > since).statement,session.bind, parse_dates=['time'])
    #station_data_df = pd.read_sql("select * FROM measurement where time > datetime('now', '-7 day')" ,con, parse_dates=['time'],  index_col=['time'])
    #processed = pd.read_sql("SELECT * FROM measurement WHERE station_id = '{}' and time > datetime('now', '-{} day')".format(ss, nn), con)
    processed = pd.read_sql("SELECT * FROM measurement WHERE station_id = '{}'".format(ss), con)
    print ('### {} processed query complete {}'.format(ss, processed.count()))
    logging.info('{} retrieved processed records for {}'.format(dt.datetime.now(),s))
    #processed = pd.read_sql(session.query(Measurement).filter(Measurement.station_id == stationid).statement,session.bind)

    processed = processed[['station_id', 'time', 'cs', 'fof2', 'fof1', 'mufd', 'foes', 'foe', 'hf2', 'he', 'hme', 'hmf2', 'hmf1', 'yf2',  'yf1',  'tec', 'scalef2', 'fbes', 'altitude']]

    #stationdata.to_csv('stationdata.csv')
    #for col in stationdata.select_dtypes([np.object]).columns[1:]:
    #    stationdata = stationdata[stationdata.time.str.contains("ERROR") == False]
    stationdata.cs = stationdata.cs.astype(str)
    stationdata = stationdata[stationdata.cs.str.contains("No") == False]

    #added processing 

    #get same datatypes before concat
    processed[['time']] = processed[['time']].apply(pd.to_datetime)
    stationdata[['time']] = stationdata[['time']].apply(pd.to_datetime)

    print ('row count: stationdata {} processed {}'.format(stationdata.count(), processed.count()))
    concatted = pd.concat([processed,stationdata], ignore_index=True).drop_duplicates(subset=['station_id', 'time'])
    concatted = concatted[pd.isnull(concatted['altitude'])]
    print ('finish concat unprocessed and processed')
    print ('row count: combined {}'.format(concatted.count()))

    #to shift 0 to 360 to -180 to 180 values for correct mapping, sun altitude
    stationdf.ix[stationdf.longitude > 180, 'longitude'] = stationdf.longitude - 360

    #merge to get station data
    stationdf.reset_index(inplace=True)
    #stationdf['id'] = stationdf['id'].astype(int)
    #concatted.to_csv('concatted.csv')
    concatted['station_id'] = concatted['station_id'].astype(int)
    unprocessed = stationdf.merge(concatted, left_on='id', right_on='station_id', how='right')

    #set --- to nan
    unprocessed = unprocessed.applymap(lambda x: x.replace('---','') if type(x) is str else x)

    #filter out errors
   # unprocessed = unprocessed[unprocessed.time != 'ERROR:']

    unprocessed[['mufd']] = unprocessed[['mufd']].apply(pd.to_numeric)
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
            #print (row)
            #merged.loc[index, 'altitude'] = get_altitude( row['Latitude'],row['Longitude'], row['Time'])
            date = row['time']
            tz = dt.timezone.utc
            date = date.replace(tzinfo=tz)
            unprocessed.loc[index, 'altitude'] = get_altitude( row['latitude'],row['longitude'], date)
            #row['altitude'] = get_altitude( row['Latitude'],row['Longitude'], row['Time'])
    end_time = time.time()
    #logging.info('{} pysolar processed {} rows for {} in {} seconds'.format(dt.datetime.now(),n, s,  round(end_time - start_time, 2)))
    unprocessed.sort_values(by=['time'], inplace=True)

    unprocessed = unprocessed[['time','cs','fof2','fof1','mufd','foes','foe','hf2','he','hme','hmf2','hmf1','yf2','yf1','tec','scalef2','fbes','altitude', 'station_id']]

    #unprocessed['time'] = unprocessed['time'].dt.strftime('%Y-%m-%d %H:%M')
    unprocessed[['altitude']] = unprocessed[['altitude']].round({'altitude': 1})
    #logging.info('{} to_sql start {}'.format(dt.datetime.now(),s))
    #unprocessed.drop_duplicates(inplace=True)
    unprocessed.to_sql('measurement', con=engine, if_exists='append', index=False)
    #logging.info('{} to_sql complete {}'.format(dt.datetime.now(), s))
    logging.info('{} processed {} rows for {} in {} seconds'.format(dt.datetime.now(),n, s,  round(end_time - start_time, 2)))
    #sys.stdout = old_stdout

    #log_file.close()
#get_data(sys.argv[1])
                                                                                                                                                             
