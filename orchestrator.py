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

wc=workerClass()

def runSGXFull():
    util.runFunc(sgx.getFullDetails)
    
#def testFunc():
#    r=10*3
#    return r

#result=wc.queueFunc('test', testFunc, 1)