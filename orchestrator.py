# -*- coding: utf-8 -*-
"""
Created on Sun Sep  1 14:53:35 2019

@author: ASUS
"""

#import sgx
from rq import Queue
from rq.job import Job
from worker import conn
from util import workerClass

wc=workerClass()
    
def testFunc(num):
    r=10*3
    return r

#result=wc.queueFunc('test', testFunc, 1)