# -*- coding: utf-8 -*-
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from crawler import crawler

import time
import pandas as pd

import dbConnector as db
#import analysis

url="https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities?sc_lang=en"
hkSum='data/HKsummary.csv'
hkSumEngine='data/HKsummaryEngineered.csv'
dbName='hksummary'

currencyCols=['price', 'yearhigh','yearlow']
numericCols=['turnover','market_cap','pe','dividend']

def run():
    crawl=crawler()
    crawl.startDriver(url)
    
    df=crawl.crawlHKEXSummary()
    crawl.store(df, hkSum, dbName)
    
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
    
    df=crawl.getPriceUpdate(df=None, dbname=dbName)
    crawl.store(df, hkSum, dbName)
    
    crawl.closeDriver()

def dataEngineer(df):
    df['price_divide_yearlow']=[round((x-y)/x,2) if x!=0 and y!=0 and x!='nan' and y!='nan' \
      else 0 for x,y in zip(df['price'],df['yearlow'])]
    df['yearhigh_divide_price']=[round((y-x)/x,2) if x!=0 and y!=0 and x!='nan' and y!='nan' \
      else 0 for x,y in zip(df['price'],df['yearhigh'])]
    
    return df

def cleanData(df):
    
    for col in currencyCols:
        newColVal=[float(x.split('$')[1].replace(',','')) if len(str(x).split('$'))>1 else 0 \
                   for x in df[col]]
        df[col]=newColVal
    
    for col in numericCols:
        lst=[]
        for val in df[col]:
            val=val.strip().replace('*','').replace(',','')
            if val == '-':
                lst.append(0)
            else:
                numericVal=float(val[:-1])
                if 'B' in val:
                    numericVal=numericVal * pow(10,9)
                if 'M' in val:
                    numericVal=numericVal * pow(10,6)
                if 'K' in val:
                    numericVal=numericVal * pow(10,3)
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
        'pe1':['>',2,'pe'],
        'pe2':['<',40,'pe'],
        'turnover':['>',10*pow(10,6),'turnover']
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
    else:
        df=pd.read_csv(hkSum)
    
    return df
    
#df=run()

#df=analytics(download=False)
#cleanDf=cleanData(df)
#engineDf=dataEngineer(cleanDf)
#engineDf.to_csv(hkSumEngine, index=False)
#sievedDf=sieveData(engineDf)