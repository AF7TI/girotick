#!python3
import os
import threading
from queue import Queue
import time
import datetime as dt
import get_data_station
from get_data_station import get_data
#from myflaskapp import *
import sys
import logging
import pandas as pd

import psycopg2

try:
    con = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='mysecretpassword'")
except:
    print ("I am unable to connect to the database")

cwd = os.getcwd()

days = int(sys.argv[1])
threadcount = int(sys.argv[2])


logging.basicConfig(filename='sys.stdout',level=logging.INFO)
logging.info('{} tread.py start'.format(dt.datetime.now()))

# lock to serialize console output
lock = threading.Lock()

def do_work(item):
    #time.sleep(1) # pretend to do some lengthy work.
    # Make sure the whole print completes or threads can mix up output in one line.
    print ('item is: ' + item)
    logging.info('{} do_work item {}'.format(dt.datetime.now(), item))
    get_data(item, days)
#    with lock:
 #       print(threading.current_thread().name,item)
##        logging.info('{} do_work item {} {}'.format(dt.datetime.now(), threading.current_thread().name, item))
# The worker thread pulls an item from the queue and processes it
def worker():
    while True:
        item = q.get()
        logging.info('{} worker got item {} from queue'.format(dt.datetime.now(), item))
        do_work(item)
        q.task_done()
        logging.info('{} worker done w item {}'.format(dt.datetime.now(), item))
# Create the queue and thread pool.
q = Queue()
logging.info('{} ### creating queue with tread pool count {} ###'.format(dt.datetime.now(), threadcount))
for i in range(threadcount):
     t = threading.Thread(target=worker)
     t.daemon = True  # thread dies when main thread (only non-daemon thread) exits.
     t.start()

# stuff work items on the queue.
start = time.perf_counter()
stationdf = pd.read_sql('Select code from station', con)
stationdf = pd.Series(stationdf['code'].values)
for item in stationdf:
    q.put(item)

q.join()       # block until all tasks are done

print('time:',time.perf_counter() - start)
