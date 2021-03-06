# -*- coding: utf-8 -*-
"""
Created on Sun Apr 29 13:38:43 2018

@author: ASUS
"""

import os

import redis
from rq import Worker, Queue, Connection
import dbConnector as db
from datetime import datetime, timedelta

listen = ['high', 'default', 'low']

#redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')
redis_url=os.environ.get("REDIS_URL")

conn = redis.from_url(redis_url)
#pool = redis.ConnectionPool(host=redis_url)
#conn = redis.Redis(connection_pool=pool)

def timeConverter(dtVal, convert=True):
    if convert:
        dtVal=dtVal+timedelta(hours=8)
    
    return dtVal.strftime("%d/%m/%Y, %H:%M:%S")

def standard_handler(job, exc_type, exc_value, traceback):
    jobid=job.id
    error=(str(exc_type)+'----'+str(exc_value))
    error=error.replace("'", "")
    error=error.replace('"',"")
    
    print("error---:", error)
    db.editRow('jobs',['lastchecked', 'jobstatus'],[timeConverter(datetime.now()),error],'jobid', jobid)


#def startWorker():
#    global conn
#    conn = redis.from_url(redis_url)
#    with Connection(conn):
#        worker = Worker(map(Queue, listen), exception_handlers=[standard_handler])
#        worker.work()
#
#def stopWorker():
#    global conn
#    conn.close
        
if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(map(Queue, listen), exception_handlers=[standard_handler])
        worker.work()
        