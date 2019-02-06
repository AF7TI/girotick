import subprocess
import time
import os
import sys
import logging
import psycopg2

cwd = os.getcwd()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        #logging.FileHandler("{0}/{1}.log".format(logPath, fileName)),
        logging.StreamHandler(sys.stdout)
    ])

logger = logging.getLogger()

try:
    con = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='mysecretpassword'")
except:
    logger.error("Unable to connect to the database")

cursor = con.cursor()

try:
    cursor.execute(open("dbsetup.sql", "r").read())
except:
    logger.error("Unable to execute database cursor")

cursor.close()
con.close()

starttime=time.time()
while True:
  # GIRO would like us to start polls at 8 minutes past a 15-minute mark, i.e. 8/23/38/53 minutes after the hour.
  wait = 900.0 - ((time.time() - 480.0) % 900.0)
  logger.info("sleeping %.1f seconds", (wait))
  time.sleep(wait)
  logger.info("start tick")
  subprocess.call(['python', cwd + '/tread.py', '1', '4'])
