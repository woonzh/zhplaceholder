# -*- coding: utf-8 -*-
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from crawler import crawler

import time
import pandas as pd

import dbConnector as db
import hkexDict
import statistics
from logger import logger
import sys
from quandlClass import quandlClass

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

url="https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities?sc_lang=en"
hkSum='data/HKsummary.csv'
hkSumEngine='data/HKsummaryEngineered.csv'
dbName='hksummary'

currencyCols=['price','yearhigh', 'yearlow', 'dayhigh', 'daylow']
numericCols=['turnover','market_cap','pe','dividend', 'volume']
percenCols=['percen_traded', 'downside','upside']
comDict=hkexDict.companyTag
log=logger()
quan=quandlClass()

def run():
    local=False
    crawl=crawler(local)
    crawl.urlDirect(url)
    
    df=crawl.crawlHKEXSummary()
    crawl.store(df, hkSum, dbName)
    
#    df=df.loc[[0,1]]
    
    df=crawl.getHKEXDetails(df=df, dbname=dbName)
    crawl.store(df, hkSum, dbName)
    
    crawl.closeDriver()   
    return df

def updateDetails():
    local=False
    crawl=crawler(local)
    crawl.urlDirect(url)
    
    df=crawl.getHKEXDetails(df=None, dbname=dbName)
    crawl.store(df, hkSum, dbName)
    
    crawl.closeDriver()
    
    return df

def updateBasic(quandl=0):
    local=False
    crawl=crawler(local)
    
    quandl=int(quandl)
    
    if quandl==0:
        quandlBool=False
    else:
        quandlBool=True
    
    print('hkex quandl bool- %s-%s'%(quandl, quandlBool))
    
    success=True
#    
    if quandlBool:
        df=quan.updateHKEXData(dbname=dbName)
        df2=crawl.updatePriceQuandl(df=df,dbname=dbName)
    else:
        crawl.urlDirect(url)
        df=crawl.crawlHKEXSummary()
        if len(df)>5:
            df2=crawl.updatePrice(df=df, dbname=dbName)
        else:
            success=False
    
    if success:
        crawl.store(df2, hkSum, dbName, write='cloud')
        
        df3=crawl.updateHighLow(df2)
        crawl.store(df3, hkSum, dbName, write='cloud')
        df=df3
    
    crawl.closeDriver()
    
    return df

def dataEngineer(df):
    df=df.copy(deep=True)
    df['downside']=[(x-y)/x if (x!=0 and y!=0 and x!='nan' and y!='nan') \
      else 0 for x,y in zip(df['price'],df['yearlow'])]
    df['upside']=[(y-x)/x if (x!=0 and y!=0 and x!='nan' and y!='nan') \
      else 0 for x,y in zip(df['price'],df['yearhigh'])]
    
    df['prev_price']=[x-y if x!=0 else 0 for x,y in zip(df['price'],df['day_priceinc'])]
    
    df['day_volatility']=[100*((x-y)/y) if (x!=0 and y!=0) else 0 for x,y in zip(df['dayhigh'],df['daylow'])]
    df['day_volatility_weighted']=[abs(x)-abs(y) for x, y in zip(df['day_volatility'],df['day_perceninc'])]
    
    for col in percenCols:
        df[col]=[round(x*100,5) if x>0 else 0 for x in df[col]]
    
    return df

def removeNonUpdatedRows(df):
    store=pd.DataFrame()
    store['dates']=list(set(df['date_update']))
    store['count']=[sum(df['date_update']==x) for x in store['dates']]
    store=store.sort_values(by=['count'], ascending=False)
    
    curDate=store.iloc[0,0]
    
    return df[df['date_update']==curDate]

def cleanData(df):
    df=df.copy(deep=True)
    
    df=removeNonUpdatedRows(df)
    
    for col in currencyCols:
        newColVal=[float(x.split('$')[1].replace(',','')) if len(str(x).split('$'))>1 else 0 \
                   for x in df[col]]
        df[col]=newColVal
    
    for col in numericCols:
#        print(col)
        lst=[]
        for count,val in enumerate(df[col]):
#            print('%s-%s'%(count,val))
            val=str(val).strip().replace('*','').replace(',','').replace('x','')
            if str(val) == '-' or str(val)=='nan':
                lst.append(0)
            else:
                try:
                    numericVal=float(val)
                except:
                    try:
                        hit=False
                        if 'B' in val:
                            numericVal=float(val[:-1])
                            numericVal=numericVal * pow(10,9)
                            hit=True
                        if 'M' in val:
                            numericVal=float(val[:-1])
                            numericVal=numericVal * pow(10,6)
                            hit=True
                        if 'K' in val:
                            numericVal=float(val[:-1])
                            numericVal=numericVal * pow(10,3)
                            hit=True
                        if hit==False:
                            numericVal=float(val[:-1])
                    except:
                        numericVal=0
                lst.append(numericVal)
        df[col]=lst
    
    df['day_priceinc']=[float(x.split(' ')[0].replace('+','')) if str(x) !='nan' and str(x)!='' else 0 for x \
      in df['upval']]
    df['day_perceninc']=[float(x.split(' ')[1].replace("(",'').replace(')','').replace('%',''))\
      if str(x) !='nan' and str(x)!='' else 0 for x in df['upval']]
    df=df.drop('upval', axis=1)
    
    df['abs_day_perceninc']=[abs(x) for x in df['day_perceninc']]
    
    df['shares_oustanding']=[y/x if (x >0 and y>0 )else 0 for x,y in zip(df['price'],df['market_cap'])]
    
    df['percen_traded']=[x/y if (x >0 and y>0 )else 0 for x,y in zip(df['volume'],df['shares_oustanding'])]
    
    
    
    return df

def sieveData(df, filters=None):
    nonullcols=['turnover','pe']
    
    for col in nonullcols:
        df=df[df[col]>0]
    
    if filters is None:
        filters={
            'price':['>',0.1,'price'],
            'pe1':['>',1,'pe'],
            'pe2':['<',30,'pe'],
    #        'marketcap':['>',10*pow(10,8),'market_cap'],
    ##        'turnover':['>',5*pow(10,7),'turnover'],
            'upside':['>',30,'upside'],
            'downside':['<',30,'downside'],
            'percen_traded':['>',0,'percen_traded'],
            'suspended':['=','N','suspended']
                }
        
    for i in filters:
        if filters[i][0]=='>':
            df=df[df[filters[i][2]]>filters[i][1]]
        if filters[i][0]=='<':
            df=df[df[filters[i][2]]<filters[i][1]]
        if filters[i][0]=='=':
            df=df[df[filters[i][2]]==filters[i][1]]
        if filters[i][0]=='!=':
            df=df[df[filters[i][2]]!=filters[i][1]]
    
    return df

def checkNum(txt):
    try:
        a=float(txt)
        return True
    except:
        return False

def getStats(df):
    cols=list(df)
    store={}
    
    percen=[0.1,0.25,0.5,0.75,0.9]
    
    for col in cols:
        try:
            temDf=pd.DataFrame()
            temDf['store']=[float(x) if checkNum(x)==True else 0 for x in df[col]]
            tem=list(temDf[temDf['store']>0]['store'])
            tem.sort()
            lst={}
            lst['avg']=statistics.mean(tem)
            for p in percen:
                ind=int(round(p*len(tem),0))
                lst[str(p)]=tem[ind]
            
                store[col]=lst
        except:
            store[col]=''
    
    return store

def analytics(download=False, cloud=False):
    if cloud:
        df=db.extractTable(dbName)['result']
        return df
    if download:
        df=db.extractTable(dbName)['result']
        df.to_csv(hkSum, index=False)
    df=pd.read_csv(hkSum)
    return df

def fullCloudAnalytics(clean=False):
    df=analytics(cloud=True)
    dfClean=cleanData(df)
    dfEngine=dataEngineer(dfClean)
    
    if clean:
        dfEngine=filterView(dfEngine, cloud=True)
    
    return dfEngine

def getIndustryCompany(df, industry='bank'):
    lst=comDict[industry]
    boolCheck=[True if x.lower() in lst else False for x in df['com_name']]
    return df[boolCheck]

def filterView(df, cloud=False):
    cols_to_show=['com_name', 'price', 'day_volatility', 'day_volatility_weighted','day_perceninc','day_perceninc_5_day_sum','downside', 'upside',\
                  'turnover', 'market_cap', 'pe', 'dividend', 'percen_traded', 'day_perceninc_sum']
    if cloud:
        cols_to_show=['com_name', 'price', 'day_volatility', 'day_volatility_weighted','day_perceninc','downside', 'upside',\
                  'turnover', 'market_cap', 'pe', 'dividend', 'percen_traded']

    return df[cols_to_show]

def findCompany(df, comName=None, code=None):
    if comName is not None:
        return df[df['com_name']==comName]
    if code is not None:
        return df[df['code']==code]
    
    return df

def runLogger(df, run=False, calStats=False):
    dfNew=df.copy(deep=True)
    if run:
        store=log.update('hkex',df, 'code','com_name')
        store=log.calStats('hkex')
        log.save()
    else:
        if calStats:
            store=log.calStats('hkex')
            log.save()
    table=log.compileTable('hkex')
    
    dfNew=pd.merge(dfNew, table, how='left', left_on='code', right_on='symbol')
    dfNew.drop(['symbol','company'],axis=1, inplace=True)
    
    return dfNew, table

#df=run()
    
dayChange={
    'price':['>',1,'price'],
    'pe1':['>',1,'pe'],
#    'market_cap':['>',pow(10,7),'market_cap'],
    'pe2':['<',50,'pe'],
#    'day_perceninc':['<',-3,'day_perceninc'],
#    'day_perceninc2':['>',3,'day_perceninc'],
    'abs_day_perceninc':['>',3,'abs_day_perceninc'],
    'percen_traded':['>',0,'percen_traded'],
    'suspended':['=','N','suspended']
        }

upside={
    'price':['>',2,'price'],
#    'pe1':['>',1,'pe'],
#    'pe2':['<',40,'pe'],
    'upside':['>',30,'upside'],
#    'downside':['<',30,'downside'],
    'percen_traded':['>',0,'percen_traded'],
    'suspended':['=','N','suspended'],
#    'day_perceninc':['>',1,'day_perceninc'],
    'day_perceninc_5_day_sum':['>',0.5,'day_perceninc_5_day_sum'],
#    'market_cap':['>',pow(10,8),'market_cap']
        }

#df=updateBasic(quandl=0)
#log.updateFromPrevLog('store/backup/20200522.json')
#
#df=analytics(download=False)
#dfClean=cleanData(df)
#dfEngine=dataEngineer(dfClean)
#dfEngine,table=runLogger(dfEngine, run=False, calStats=False)
#
#dfEngineView=filterView(dfEngine)
#stats=getStats(dfEngine)
###
#dfDayChange=sieveData(dfEngine,filters=dayChange)
#dfDayChangeView=filterView(dfDayChange)
###
#dfUpside=sieveData(dfEngine,filters=upside)
#dfUpsideView=filterView(dfUpside)
#
#dfSieve=sieveData(dfEngine)
#dfView=filterView(dfSieve)
##
#dfCom=findCompany(dfEngine, comName='CHINA MOBILE')
#dfComView=filterView(dfCom)
##
#dfInd=getIndustryCompany(dfEngine, industry='telecom')
#dfIndView=filterView(dfInd)
#comDf=engineDf[engineDf['com_name']=='ICBC']

#store,table=runLogger(dfEngine)






