import subprocess
import time
import os

cwd = os.getcwd()

import psycopg2

try:
    con = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='mysecretpassword'")
except:
    print ("Unable to connect to the database")

cursor = con.cursor()

try:
    cursor.execute(open("test.sql", "r").read())
except:
    print ("Unable to execute database cursor")

starttime=time.time()
while True:
  print ("tick")
  subprocess.call(['python', cwd + '/tread.py', '1', '4'])
  time.sleep(300.0 - ((time.time() - starttime) % 300.0))
