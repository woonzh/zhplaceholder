# -*- coding: utf-8 -*-
"""
Created on Sun Sep  1 14:53:35 2019

@author: ASUS
"""

#import sgx
from rq import Queue
from rq.job import Job
from worker import conn

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

wc=workerClass()
    
def testFunc(num):
    r=10*3
    return r

wc=workerClass()
result=wc.queueFunc(testFunc, 1)