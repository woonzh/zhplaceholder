# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 11:21:59 2019

@author: ASUS
"""

import datetime
import pandas as pd
from rq import Queue
from rq.job import Job
from worker import conn

class timeClass:
    def __init__(self):
        self.startTime=None
        self.curTime=None
        self.history=pd.DataFrame(columns=['event', 'time', 'elapsed'])
        
    def startTimer(self):
        self.startTime=datetime.datetime.now()
        self.curTime=self.startTime
        self.history.loc[len(self.history)]=['start', self.startTime, 0]
        print('time started')
    
    def getTimeSplit(self, event):
        now=datetime.datetime.now()
        split=(now - self.curTime)/60
        self.history.loc[len(self.history)]=[event, now, split]
        
        print(event + ': %s mins'%(str(split)))
        
        self.curTime=now
    
    def stopTime(self):
        now=datetime.datetime.now()
        split=(now-self.startTime)/60
        self.history.loc[len(self.history)]=['end', now, split]
        
        print('total time taken: %s mins'%(str(split)))

class workerClass:
    def __init__(self):
        self.q=Queue(connection=conn)
        
    def queueFunc(self, func, params=None):
        returnVal={
            'result':None,
            'error': None
                }
        try:
            if params==None:
                returnVal['result']=self.q.enqueue(func).get_id()
            else:
                returnVal['result']=self.q.enqueue(func, params).get_id()
            
        except BaseException as e:
            returnVal['error']=e
        
        return returnVal
    
    def getResult(self, jobId):
        job=Job.fetch(jobId, connection=conn)
        result={}
        result['status']=job.get_status()
        result['result']=job.result
        
        return result
        