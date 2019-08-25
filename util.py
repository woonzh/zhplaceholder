# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 11:21:59 2019

@author: ASUS
"""

import datetime
import pandas as pd

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
        