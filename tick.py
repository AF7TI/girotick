import subprocess
import time
import os
import sys
import logging
import psycopg2

DB_NAME=os.getenv("DB_NAME")
DB_USER=os.getenv("DB_USER")
DB_HOST=os.getenv("DB_HOST")
DB_PASSWORD=os.getenv("DB_PASSWORD")
DB_TIMEOUT= os.getenv('DB_TIMEOUT', 5000)

cwd = os.getcwd()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ])

logger = logging.getLogger()

try:
    con = psycopg2.connect("dbname={} user={} host={} password={} options='-c statement_timeout={}'".format(DB_NAME, DB_USER, DB_HOST, DB_PASSWORD, DB_TIMEOUT))
    logger.warning("connected to database")
except:
    logger.error("Unable to connect to the database")

cursor = con.cursor()

try:
    schema = cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    for table in cursor.fetchall():
        logger.warning(table)
except:
    logger.error("Unable to get database schema")

try:
    cursor.execute(open("dbsetup.sql", "r").read())
except:
    logger.error("Unable to execute dbsetup.sql does schema already exist?")

cursor.close()
con.close()

starttime=time.time()
while True:
  # GIRO would like us to start polls at 8 minutes past a 15-minute mark, i.e. 8/23/38/53 minutes after the hour.
  wait = 900.0 - ((time.time() - 480.0) % 900.0)
  logger.info("sleeping %.1f seconds", (wait))
  time.sleep(wait)
  logger.info("start tick")
  subprocess.call(['python', cwd + '/tread.py'])
