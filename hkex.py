# -*- coding: utf-8 -*-
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from crawler import crawler

import time
import pandas as pd

import dbConnector as db
import hkexDict
#import analysis

url="https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities?sc_lang=en"
hkSum='data/HKsummary.csv'
hkSumEngine='data/HKsummaryEngineered.csv'
dbName='hksummary'

currencyCols=['price','yearhigh', 'yearlow']
numericCols=['turnover','market_cap','pe','dividend']
comDict=hkexDict.companyTag

def run():
    crawl=crawler()
    crawl.startDriver(url)
    
    df=crawl.crawlHKEXSummary()
    crawl.store(df, hkSum, dbName)
    
    df=df.loc[[0,1]]
    
    df=crawl.getHKEXDetails(df=df, dbname=dbName)
    crawl.store(df, hkSum, dbName)
    
    crawl.closeDriver()
    
    return df

def updateDetails():
    crawl=crawler()
    crawl.startDriver(url)
    
    df=crawl.getHKEXDetails(df=None, dbname=dbName)
    crawl.store(df, hkSum, dbName)
    
    crawl.closeDriver()
    
    return df

def updateBasic():
    crawl=crawler()
    crawl.startDriver(url)
    
    df=crawl.crawlHKEXSummary()
    df2=crawl.updatePrice(df=df, dbname=dbName)
    crawl.store(df2, hkSum, dbName)
    
    df3=crawl.updateHighLow(df2)
    crawl.store(df3, hkSum, dbName)
    
    crawl.closeDriver()
    
    return df,df2

def dataEngineer(df):
    df=df.copy(deep=True)
    df['downside']=[round((x-y)/x,2) if (x!=0 and y!=0 and x!='nan' and y!='nan') \
      else 0 for x,y in zip(df['price'],df['yearlow'])]
    df['upside']=[round((y-x)/x,2) if (x!=0 and y!=0 and x!='nan' and y!='nan') \
      else 0 for x,y in zip(df['price'],df['yearhigh'])]
    
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
                        if 'B' in val:
                            numericVal=float(val[:-1])
                            numericVal=numericVal * pow(10,9)
                        if 'M' in val:
                            numericVal=float(val[:-1])
                            numericVal=numericVal * pow(10,6)
                        if 'K' in val:
                            numericVal=float(val[:-1])
                            numericVal=numericVal * pow(10,3)
                    except:
                        numericVal=0
                lst.append(numericVal)
        df[col]=lst
    
    df['day_priceinc']=[float(x.split(' ')[0].replace('+','')) if str(x) !='nan' and str(x)!='' else 0 for x \
      in df['upval']]
    df['day_perceninc']=[float(x.split(' ')[-1].replace('+','').replace("(",'').replace(')','')\
      .replace('%','')) if str(x) !='nan' and str(x)!='' else 0 for x in df['upval']]
    df=df.drop('upval', axis=1)   
    return df

def sieveData(df):
    nonullcols=['turnover','pe']
    
    for col in nonullcols:
        df=df[df[col]>0]
    
    filters={
        'pe1':['>',1,'pe'],
        'pe2':['<',30,'pe'],
        'turnover':['>',5*pow(10,7),'turnover'],
        'upside':['>',0.4,'upside']
#        'downside':['<',0.3,'downside']
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

def analytics(download=True):
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
                  'turnover', 'market_cap', 'pe', 'dividend']
    
    return df[cols_to_show]

#df=run()
#a=updateBasic()

#df=analytics(download=False)
#cleanDf=cleanData(df)
#engineDf=dataEngineer(cleanDf)
#engineDf.to_csv(hkSumEngine, index=False)
#sievedDf=sieveData(engineDf)
#viewDf=filterView(sievedDf)
#
#indDf=getIndustryCompany(engineDf, industry='oil')
#comDf=engineDf[engineDf['com_name']=='ICBC']

#crawl=crawler()
#crawl.startDriver(url)
#
#df=crawl.crawlHKEXSummary()
#df2=crawl.updatePrice(df=df, dbname=dbName)

#crawl.closeDriver()








