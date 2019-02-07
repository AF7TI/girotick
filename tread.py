import concurrent.futures
import time
import datetime as dt
import sys
import logging
import pandas as pd
import psycopg2
import get_data_station
from get_data_station import get_data

DB_NAME='postgres'
DB_USER='postgres'
DB_HOST='localhost'
DB_PASSWORD='mysecretpassword'
DB_TIMEOUT=5000 #postgres timeout

MAX_WORKERS=4 #threadcount
URL_TIMEOUT=10 #urllib timeout for giro page fetch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        #logging.FileHandler("{0}/{1}.log".format(logPath, fileName)),
        logging.StreamHandler(sys.stdout)
    ])

logger = logging.getLogger()

logger.info('{} tread.py start'.format(dt.datetime.now()))

try:
    con = psycopg2.connect("dbname={} user={} host={} password={} options='-c statement_timeout={}'".format(DB_NAME, DB_USER, DB_HOST, DB_PASSWORD, DB_TIMEOUT))
except:
    logger.error("I am unable to connect to the database")

#get list of station codes from database
stationdf = pd.read_sql('Select code from station', con)
stationdf = pd.Series(stationdf['code'].values)

treadstart = time.time()

# We can use a with statement to ensure threads are cleaned up promptly
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Start the load operations and mark each future with its URL
    future_to_url = {executor.submit(get_data, url, URL_TIMEOUT): url for url in stationdf}
    for future in concurrent.futures.as_completed(future_to_url):
        url = future_to_url[future]
        try:
            data = future.result()
        except Exception as exc:
            print('%r generated an exception: %s' % (url, exc))
        #else:
        #    print('%r page is %d bytes' % (url, len(data)))
    logger.info('{} tread.py completed in {} seconds'.format(dt.datetime.now(), round(time.time() - treadstart, 2)))
