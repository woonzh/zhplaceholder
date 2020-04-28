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
import hkex
import nasdaq

wc=workerClass()

def runSGXFull(jobId=""):
    params=(0, False, 'cloud', jobId)
    util.runFunc(actFunc=sgx.getFullDetails, actFuncParams=params)
    
def runSGXUpdate(dragIndex=None, sumTries=None,jobId=''):
    params=(dragIndex)
    util.runFunc(actFunc=sgx.updateCompanyInfo, actFuncParams=params)
    
def runFriar(mode=1, jobId=''):
    params=(mode, jobId)
    util.runFunc(actFunc=test2.runProg, actFuncParams=params)
    
def runHKEXFull(dragIndex=None, sumTries=None,jobId=''):
    util.runFunc(actFunc=hkex.run)
    
def runHKEXUpdateDetails(dragIndex=None, sumTries=None,jobId=''):  
    util.runFunc(actFunc=hkex.updateDetails)

def runHKEXUpdateBasic(dragIndex=None, sumTries=None,jobId=''):  
    util.runFunc(actFunc=hkex.updateBasic)
    
def runNasdaqFull(jobId=''):
    util.runFunc(actFunc=nasdaq.run)
    
def runNasdaqDetailsUpdate(jobId=''):
    util.runFunc(actFunc=nasdaq.updateDetails)

def runNasdaqBasicUpdate(jobId=''):
    util.runFunc(actFunc=nasdaq.updateBasics)
    
#def testFunc():
#    r=10*3
#    return r

#result=wc.queueFunc('test', testFunc, 1)