# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 11:21:59 2019

@author: ASUS
"""

import pandas as pd
from rq import Queue
from rq.job import Job
from worker import conn
from datetime import datetime, timedelta
import dbConnector as db

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
        split=(now - self.curTime)/60
        self.history.loc[len(self.history)]=[event, now, split]
        
        print(event + ': %s mins'%(str(split)))
        
        self.curTime=now
    
    def stopTime(self):
        now=datetime.now()
        split=(now-self.startTime)/60
        self.history.loc[len(self.history)]=['end', now, split]
        
        print('total time taken: %s mins'%(str(split)))    

class workerClass:
    def __init__(self):
        self.q=Queue(connection=conn, default_timeout=18000)
    
    def timeConverter(self, dtVal, convert=True):
        if convert:
            dtVal=dtVal+timedelta(hours=8)
        
        return dtVal.strftime("%d/%m/%Y, %H:%M:%S")
        
    def queueFunc(self, jobName, func, params=None):
        returnVal={
            'result':None,
            'error': None
                }
        try:
            if params==None:
                returnVal['result']=self.q.enqueue(func).get_id()
            else:
                returnVal['result']=self.q.enqueue(func, params).get_id()
            
            db.insertRow('jobs',[returnVal['result'], jobName, self.timeConverter(datetime.now()),"","","","",""])
            
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
        