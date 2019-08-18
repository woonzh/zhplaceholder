# -*- coding: utf-8 -*-
"""
Created on Sun Feb 10 15:50:07 2019

@author: ASUS
"""

import sgx
import dbConnector as db
from rq import Queue
from worker import conn

import time

def getTime(prev):
    cur=time.time()
    return cur, str((time.time()-prev)/60)

def updateSGXPrice():
    start=time.time()
    df, df2=sgx.crawlSummary()
    cur, elapsed=getTime(start)
    print('crawled %s stocks in %s secs'%(str(len(df)), elapsed))
    vals=sgx.processData(df)
    cur, elapsed=getTime(cur)
    print('processed %s stocks in %s secs'%(str(len(df)), elapsed))
    db.updateDB(vals)
    cur, elapsed=getTime(cur)
    print('updated %s stocks to db in %s secs'%(str(len(df)), elapsed))
#    return df, df2, vals

def updateSGXPriceBackground():
    q = Queue(connection=conn)
    result=q.enqueue(updateSGXPrice)
    print(result)
    
def getCompanyInfo():
    df, df2=sgx.crawlSummary()
    companyFullInfo=sgx.collateCompanyInfo(df)
    return companyFullInfo

print('runne')