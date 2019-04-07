import subprocess
import time
import os
import sys
import logging
import psycopg2

from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

table = dynamodb.Table('giro-dev')

starttime=time.time()
while True:
  # GIRO would like us to start polls at 8 minutes past a 15-minute mark, i.e. 8/23/38/53 minutes after the hour.
  wait = 900.0 - ((time.time() - 480.0) % 900.0)
  logger.info("sleeping %.1f seconds", (wait))
  time.sleep(wait)
  logger.info("start tick")
  subprocess.call(['python', cwd + '/tread.py'])
