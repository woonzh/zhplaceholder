# -*- coding: utf-8 -*-
"""
Created on Sun Sep  1 14:53:35 2019

@author: ASUS
"""

import sgx
from rq import Queue
from rq.job import Job
from worker import conn
from util import workerClass
import util
import test2

wc=workerClass()

def runSGXFull(jobId=""):
    params=(0, False, 'cloud', jobId)
    util.runFunc(actFunc=sgx.getFullDetails, actFuncParams=params)
    
def runFriar(jobId=""):
    util.runFunc(actFunc=test2.runProg)
    
#def testFunc():
#    r=10*3
#    return r

#result=wc.queueFunc('test', testFunc, 1)