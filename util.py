# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 11:21:59 2019

@author: ASUS
"""

import pandas as pd
try:
    from rq import Queue
    from rq.job import Job
    from worker import conn
except:
    t=1
from datetime import datetime, timedelta
import dbConnector as db
from threading import Timer
from threading import Barrier
import requests
import time
import string
import random

def stringGenerator(length=15):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def timeConverter(dtVal=datetime.now(), convert=True):
    if convert:
        dtVal=dtVal+timedelta(hours=8)
    
    return dtVal.strftime("%d/%m/%Y, %H:%M:%S")

def durCalculator(start,end):
    startTime=datetime.strptime(start, "%d/%m/%Y, %H:%M:%S")
    endTime=datetime.strptime(end, "%d/%m/%Y, %H:%M:%S")
    diff=str(endTime-startTime)
    return diff

def updateJobStatus(intJobId, stat="Completed"):
    curTime=timeConverter()
    start=db.runquery("SELECT jobstart FROM joblist WHERE intjobid='%s'"%(intJobId), True)['result'][0][0]
    duration=durCalculator(start, curTime)
    db.editRow('jobs',['lastchecked', 'jobstatus', 'jobend', 'duration'],[curTime,stat, curTime, duration],'intjobid', intJobId)

class timeClass:
    def __init__(self):
        self.startTime=None
        self.curTime=None
        self.history=pd.DataFrame(columns=['event', 'time', 'elapsed'])
        
    def startTimer(self):
        self.startTime=datetime.now()
        self.curTime=self.startTime
        self.history.loc[len(self.history)]=['start', self.startTime, 0]
        print('time started')
    
    def getTimeSplit(self, event):
        now=datetime.now()
        try:
            split=(now - self.curTime)/60
        except:
            split='Nil'
        
        try:
            elapsed=(now-self.startTime)/60
        except:
            elapsed = 'Nil'
        
        self.history.loc[len(self.history)]=[event, now, split]
        
        print(event + ': %s mins - %s mins'%(str(split), str(elapsed)))
        
        self.curTime=now
    
    def stopTime(self):
        now=datetime.now()
        split=(now-self.startTime)/60
        self.history.loc[len(self.history)]=['end', now, split]
        
        print('total time taken: %s mins'%(str(split)))    
    
    def getCurTime(self):
        now=datetime.now()
        timeElapsed=(now-self.startTime)/60
        return timeElapsed
    
    def getCurDate(self):
        now=datetime.now()
        return now.strftime('%d/%m/%Y')

class workerClass:
    def __init__(self):
        self.q=Queue(connection=conn, default_timeout=40000)
    
    def timeConverter(self, dtVal, convert=True):
        if convert:
            dtVal=dtVal+timedelta(hours=8)
        
        return dtVal.strftime("%d/%m/%Y, %H:%M:%S")
        
    def queueFunc(self, jobName, func, params=None, intId=""):
        returnVal={
            'result':None,
            'error': None
                }
        try:
            if params==None:
                returnVal['result']=self.q.enqueue(func).get_id()
            else:
                returnVal['result']=self.q.enqueue(func, params).get_id()
            
            db.insertRow('jobs',[returnVal['result'], jobName, self.timeConverter(datetime.now()),"","",self.timeConverter(datetime.now()),"","", intId])
            
        except BaseException as e:
            returnVal['error']=e
        
        return returnVal
    
    def getResult(self, jobId):
        job=Job.fetch(jobId, connection=conn)
        result={
            'status':None,
            'result':None,
            'start': None,
            'end':None,
            'duration':None,
            'query':None
            }
        result['status']=job.get_status()
        if result['status']=='finished':
            result['result']=job.result
            result['start']=self.timeConverter(job.started_at)
            result['end'] = self.timeConverter(job.ended_at)
            result['duration']=str(job.ended_at-job.started_at)
            result['query']=db.editRow('jobs', ['lastchecked', 'jobstatus','jobstart', 'jobend', 'duration'], \
                       [self.timeConverter(datetime.now()),'finished', result['start'], \
                        result['end'], result['duration']], 'jobid', jobId)
        else:
            result['query']=db.editRow('jobs',['lastchecked', 'jobstatus'],[self.timeConverter(datetime.now()),result['status']],'jobid', jobId)
        
        return result
    
def pollFunc(pollTime):
    timer=Timer(pollTime,pollFunc,[pollTime])
    timer.start()
    url='https://zhplaceholder.herokuapp.com/keepalive'
    result=requests.get(url)
    print("refreshed")

#useless will be overwritten 
def actFunc(t=1234):
    print(t)
        
def runFunc(actFunc, pollFunc=pollFunc, pollTime=5*60, actFuncParams=()):
    pollFunc(pollTime)
    if len(actFuncParams)>0:
        actFunc(*actFuncParams)
    else:
        actFunc()

def currentDate():
    now=datetime.now()
    return now.strftime("%b_%d_%Y").lower()

#runFunc(actFunc=actFunc,pollTime=5, actFuncParams=('err'))

#b = Barrier(2, timeout=3)
#
#def sleepCheck():
#    time.sleep(3)
#    print("slept")
#
#sleepCheck()
#b.wait()
#print("check")