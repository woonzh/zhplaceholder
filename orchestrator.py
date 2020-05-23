# -*- coding: utf-8 -*-
"""
Created on Sun Sep  1 14:53:35 2019

@author: ASUS
"""

import sgx
from util import workerClass
import util
import test2
import hkex
import nasdaq
import USStocks
import analysis
import pandas as pd

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

def runHKEXUpdateBasic(quandlBool=0):
    print('orc -%s'%(str(quandlBool)))
    params=(quandlBool)
    util.runFunc(actFunc=hkex.updateBasic,actFuncParams=params)
    
def runNasdaqFull(userAgentNum=0):
    print('orc -%s'%(str(userAgentNum)))
    params=(userAgentNum)
    util.runFunc(actFunc=nasdaq.run,actFuncParams=params)
    
def runNasdaqDetailsUpdate(userAgentNum=0):
    print('orc -%s'%(str(userAgentNum)))
    params=(userAgentNum)
    util.runFunc(actFunc=nasdaq.updateDetails,actFuncParams=params)

def runNasdaqBasicUpdate(userAgentNum=0):
    print('orc -%s'%(str(userAgentNum)))
    params=(userAgentNum)
    util.runFunc(actFunc=nasdaq.updateBasics,actFuncParams=params)
    
def runIEXDetails(start_end_params=(0,0)):
    print('orc -%s'%(str(start_end_params)))
    params=start_end_params
    util.runFunc(actFunc=USStocks.updateKeyStats,actFuncParams=params)
    
def runIEXBasics(start_end_params=(0,0)):
    print('orc -%s'%(str(start_end_params)))
    params=start_end_params
    util.runFunc(actFunc=USStocks.updateQuote,actFuncParams=params)
    
def runAnalytics(country='sg', pw='', clean=False):
    print('orc analytics -%s-%s-%s'%(country, pw, clean))
    correctPw='P@ssw0rd'
    
    nullVal=pd.DataFrame()
    
    if pw!=correctPw:
        return nullVal
    
    if country=='sg':
        df=analysis.run(cloud=True, clean=clean)
    
    if country=='hk':
        df=hkex.fullCloudAnalytics(clean=clean)
    
    return df

#df=runAnalytics(country='hk',pw='P@ssw0rd')