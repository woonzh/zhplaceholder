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

url="https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities?sc_lang=en"
hkSum='data/HKsummary.csv'
hkSumEngine='data/HKsummaryEngineered.csv'
dbName='hksummary'

currencyCols=['price','yearhigh', 'yearlow']
numericCols=['turnover','market_cap','pe','dividend', 'volume']
percenCols=['percen_traded', 'downside','upside']
comDict=hkexDict.companyTag

def run():
    crawl=crawler()
    crawl.urlDirect(url)
    
    df=crawl.crawlHKEXSummary()
    crawl.store(df, hkSum, dbName)
    
#    df=df.loc[[0,1]]
    
    df=crawl.getHKEXDetails(df=df, dbname=dbName)
    crawl.store(df, hkSum, dbName)
    
    crawl.closeDriver()
    
    return df

def updateDetails():
    crawl=crawler()
    crawl.urlDirect(url)
    
    df=crawl.getHKEXDetails(df=None, dbname=dbName)
    crawl.store(df, hkSum, dbName)
    
    crawl.closeDriver()
    
    return df

def updateBasic():
    crawl=crawler()
    crawl.urlDirect(url)
    
    df=crawl.crawlHKEXSummary()
    df2=crawl.updatePrice(df=df, dbname=dbName)
    crawl.store(df2, hkSum, dbName)
    
    df3=crawl.updateHighLow(df2)
    crawl.store(df3, hkSum, dbName)
    
    crawl.closeDriver()
    
    return df,df2

def dataEngineer(df):
    df=df.copy(deep=True)
    df['downside']=[(x-y)/x if (x!=0 and y!=0 and x!='nan' and y!='nan') \
      else 0 for x,y in zip(df['price'],df['yearlow'])]
    df['upside']=[(y-x)/x if (x!=0 and y!=0 and x!='nan' and y!='nan') \
      else 0 for x,y in zip(df['price'],df['yearhigh'])]
    
    for col in percenCols:
        df[col]=[round(x*100,5) if x>0 else 0 for x in df[col]]
    
    return df

def cleanData(df):
    df=df.copy(deep=True)
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
    
    df['percen_traded']=[x/y if (x >0 and y>0 )else 0 for x,y in zip(df['volume'],df['market_cap'])]
    
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

def analytics(download=False):
    if download:
        df=db.extractTable(dbName)['result']
        df.to_csv(hkSum, index=False)
    df=pd.read_csv(hkSum)
    return df

def getIndustryCompany(df, industry='bank'):
    lst=comDict[industry]
    boolCheck=[True if x.lower() in lst else False for x in df['com_name']]
    return df[boolCheck]

def filterView(df):
    cols_to_show=['com_name', 'price', 'day_priceinc', 'day_perceninc', 'downside', 'upside',\
                  'turnover', 'market_cap', 'pe', 'dividend', 'percen_traded']
    
    return df[cols_to_show]

def findCompany(df, comName=None, code=None):
    if comName is not None:
        return df[df['com_name']==comName]
    if code is not None:
        return df[df['code']==code]
    
    return df

#df=run()
    
dayChange={
    'price':['>',1,'price'],
    'pe1':['>',1,'pe'],
    'pe2':['<',50,'pe'],
    'day_perceninc':['<',-3,'day_perceninc'],
#    'day_perceninc2':['>',3,'day_perceninc'],
    'percen_traded':['>',0,'percen_traded'],
    'suspended':['=','N','suspended']
        }

upside={
    'price':['>',1,'price'],
    'pe1':['>',1,'pe'],
    'pe2':['<',30,'pe'],
    'upside':['>',30,'upside'],
    'downside':['<',30,'downside'],
    'percen_traded':['>',0,'percen_traded'],
    'suspended':['=','N','suspended']
        }

df=analytics(download=False)
dfClean=cleanData(df)
dfEngine=dataEngineer(dfClean)
dfEngineView=filterView(dfEngine)
stats=getStats(dfEngine)

dfDayChange=sieveData(dfEngine,filters=dayChange)
dfDayChangeView=filterView(dfDayChange)

dfUpside=sieveData(dfEngine,filters=upside)
dfUpsideView=filterView(dfUpside)

#dfSieve=sieveData(dfEngine)
#dfView=filterView(dfSieve)

#comDf=findCompany(dfEngine, comName='STANCHART')
#
#indDf=getIndustryCompany(dfEngine, industry='energy')
#indDf2=filterView(indDf)
#comDf=engineDf[engineDf['com_name']=='ICBC']







