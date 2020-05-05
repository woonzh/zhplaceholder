# -*- coding: utf-8 -*-
"""
Created on Fri Feb 22 21:54:43 2019

@author: ASUS
"""

import os
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.neighbors import LocalOutlierFactor
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import scipy.cluster.hierarchy as spc
import dbConnector as db
import statistics
import json
from logger import logger

file='data/companyInfo.csv'
newFile='data/companyInfo(cleaned).csv'
cmpFile='data/companyInfo(cmp).csv'
summaryFName='data/summary.csv'
filedir='logs'
metadata = 'D:\stuff\scrapy\sgx\logs\metadata.txt'
pca = PCA(n_components=3,#len(list(dfProcess)),
         random_state = 123,
         svd_solver = 'auto'
         )
clf = LocalOutlierFactor(contamination=0.3)
log=logger()

def replace(string, old, new):
    string.replace(old, new)

def dfCleaner(df, cleanCol='prevclosedate', exceptions=[]):
    dfNew=df[df[cleanCol]!='-']
    dfDel=df[df[cleanCol]=='-']
    print('%s with empty values deleted.'%(len(dfDel)))
    
    df=dfNew.copy(deep=True)
    
    summary=pd.DataFrame(columns=['name','count', 'percen'])
    
    for header in list(df):
        if header not in exceptions:
#            print(header)
            lst=list(df[header])
            newLst=[]
            for i in lst: 
                i=str(i)   
                newVal=i.replace(',', '')                
                if 'M' in i or 'B' in i:
#                    newVal=i.replace(',', '')
                    if 'M' in newVal:
                        newVal=newVal.replace('M', '')
                        try:
                            newVal=float(newVal)*1000000
                        except:
                            newVal=0
                    else:
                        newVal=newVal.replace('B', '')
                        try:
                            newVal=float(newVal)*1000000000
                        except:
                            newVal=0
                        
                else:
                    newVal=newVal.replace('USD', '')
                    newVal=newVal.replace('SGD', '')
                    try:
                        newVal=float(newVal)
                    except:
                        newVal=0
                
                newLst.append(newVal)
            df[header]=newLst
            errorCount=sum(df[header]==0)
            summary.loc[len(summary)]=[header, errorCount,round(errorCount/len(df),1)]
        else:
            newLst=[x if (str(x)!='nan' and str(x)!='-') else '' for x in df[header]]
            df[header]=newLst
            
    return df, dfDel, dfNew, summary

def replaceCurrency(itm):
    currencies=['USD','HKD','JPY', 'AUD', 'CNY']
    for cur in currencies:
        itm=itm.replace(cur,'')
    return itm

def featuresEngineering(df, details):
    eps=df['normalizedeps']
    yearhighlow=df['yearhighlow']
    price=df['openprice']
    income=df['netincome']
    cash=df['cash']
    assets=df['assets']
    debt=df['totaldebt']
    shortdebt=df['shortdebt']
    enterpriseVal=df['enterprisevalue']
    sharesOutstanding=df['sharesoutstanding']
    industry=df['industry']
    marketcap=df['marketcap']
    marketcap=sharesOutstanding*price
    div=df['dividend']
    div_5=df['divident_5_yr_avg']
    revGrowth=df['revenue_per_share_5_yr_growth']
    earningGrowth=df['eps_per_share_5_yr_growth']
    tradedVal=df['tradedval']
    tradedVol=df['tradedvol']
    high_low=df['high_low']
    day_high=df['day_high']
    day_low=df['day_low']
    
#    #aquirer multiple
    df['aquirer multiple']=[x/y if (x!=0 and y!=0) else 0 for x, y in zip(income, enterpriseVal) ]
    
#    #aquirer multiple against price
    df['normalized aquirer multiple']=[y/x if (x!=0 and y!=0) else 0 for x,y in zip(df['aquirer multiple'], price)]
    
    # get the % of shares traded
    df['volume traded %']=[x/(y) if (x!=0 and y!=0) else 0 for x,y in zip(df['avgvolume'], sharesOutstanding)]
    
    df['debt_assets_ratio']=[x/y if (x!=0 and y!=0) else 0 for x,y in zip(debt,assets)]
    
    df['p_nav']=[a/(x+y-z) if (x!=0 and y!=0 and z!=0 and a!=0) else 0 for x,y,z,a in zip(cash, assets, debt, marketcap)]
    df['type']=['reit' if ('reits' in str(x).lower()) else 'others' for x in industry]
    df['div_val']=[x if x !=0 else y for x,y in zip(div,div_5)]
    df['profitMarginGrowth']=[y/x if (x!=0 and y!=0) else 0 for x,y in zip(revGrowth, earningGrowth)]
    df['cash_percen']=[x/y if (x!=0 and y!=0) else 0 for x,y in zip(cash, assets)]
    
    df['shortdebt_over_profit']=[x/y if (x!=0 and y!=0) else 0 for x,y in zip(shortdebt, income)]
    
    yearhighsplit=[x.split('-') if len(x.split('-'))>1 else ['0','0'] for x in yearhighlow]
    df['yearhigh']=[float(x[0].replace(',','').replace(' ','')) for x in yearhighsplit]
    df['yearlow']=[float(x[1].replace(',','').replace(' ','')) for x in yearhighsplit]
    
    df['dayVolatility']=[round(100*(x-y)/z,2) if (x!=0 and y!=0 and z!=0) else 0 for x,y,z in zip(day_high, day_low, price)]
    df['weightedDayVolatility']=[abs(x)-abs(y) for x,y in zip(df['dayVolatility'], df['percenchange'])]
    
    df['yearVolatility']=[(x-y)/y if (x!=0 and y!=0) else 0 for x,y in zip(df['yearhigh'], df['yearlow'])]
    df['upside']=[(x-y)/y if (x!=0 and y!=0) else 0 for x,y in zip(df['yearhigh'], price)]
    df['downside']= [(y-x)/y if (x!=0 and y!=0) else 0 for x,y in zip(price, df['yearlow'])]
    
    df['dayVolume']=[float(x/y) if (x!=0 and y!=0) else 0 for x,y in zip(tradedVol, sharesOutstanding)]
    
    return df

def removeNull(df, inclusions=[]):
    newDf=pd.DataFrame(columns=list(df))
    if inclusions==[]:
        inclusions=list(range(len(list(df))))
    
    for i in list(df.index):
        val=list(df.loc[i][inclusions])
        if 0 not in val:
            newDf.loc[i]=df.loc[i]
            
    newDf=newDf.reset_index(drop=True)
    return newDf

def pcaFiles(df):
    df_pca_main = pd.DataFrame(pca.fit_transform(df))
    df_pca = df_pca_main.values
    return df_pca

def removeOutlier(df, orgDf):
    clf = LocalOutlierFactor(contamination=0.3)
    y_pred = clf.fit_predict(df)
    
    newdf2=pd.DataFrame(columns=list(orgDf))
    for count, val in enumerate(list(y_pred)):
        if val==1:
            newdf2.loc[count]=list(orgDf.loc[count])
    
    return newdf2

def plotKMeans(df, clusters=1):
    kmeans = KMeans(n_clusters=clusters)
    kmeans.fit(df)
    
    # appropriate cluster labels of points in ds
    data_labels = kmeans.labels_
    # coordinates of cluster centers
    cluster_centers = kmeans.cluster_centers_
    
    colors = ['b', 'g']
    plt.scatter(df[:, 0], df[:, 1],
                            c=[colors[i] for i in data_labels], s=1)
    plt.scatter(cluster_centers[:, 0], cluster_centers[:, 1], color = "k")
    plt.show()

#check infoName
def cleanAndProcess(sumName=summaryFName, infoName=file, newFileName=newFile):
    if type(sumName)==type('string'):
        details=pd.read_csv(sumName)
    else:
        details=sumName
        
    if type(infoName)==type('string'):
        df=pd.read_csv(infoName)
    else:
        df=infoName
    
    dfMain, dfDel, dfCheck, summary=dfCleaner(df, exceptions=['names', 'prevclosedate', 'p_float', 'industry', 'financial_info', 'yearhighlow', 'high_low'])
    print('cleaned')
    dfNew=featuresEngineering(dfMain, details)
    
    f, fsummary=processFinancialInfo(dfNew)
    dfNew=pd.merge(dfNew, fsummary, on='names')
    
    dfNew.to_csv(newFileName, index=False)
        
    return dfMain, dfDel, dfCheck, summary, dfNew, f

def addToMatrix(df, a, b):
    cols=list(df)
    indexs=list(df.index)
    
    if len(cols)==0:
        df=pd.DataFrame(columns=[a])
        df.loc[a]=[0]
        cols=list(df)
        indexs=list(df)
    
    if a not in cols:
        df[a]=[0]*len(indexs)
        df.loc[a]=[0]*(len(cols)+1)
        cols=list(df)
        indexs=list(df.index)
    
    if b not in cols:
        df[b]=[0]*len(indexs)
        df.loc[b]=[0]*(len(cols)+1)
        cols=list(df)
        indexs=list(df.index)
    
    cola=cols.index(a)
    colb=cols.index(b)
    indexa=indexs.index(a)
    indexb=indexs.index(b)
    
    df.iloc[cola, indexb]=df.iloc[cola, indexb]+1
    df.iloc[colb, indexa]=df.iloc[colb, indexa]+1
    
    return df

def extractIndustries(fname=newFile, df=None):
    if df is None:
        df=pd.read_csv(fname)
    industry=list(df['industry'])
    store={}
    dfStore=pd.DataFrame()
    
    for count, line in enumerate(industry):
        tem=[x.strip() for x in str(line).split('/')]
        for cur in tem:
            if cur!='':
                try:
                    store[cur]=store[cur]+1
                except:
                    store[cur]=1
                    
                for nextCur in tem:
                    if nextCur!=cur and nextCur!='':
                        dfStore=addToMatrix(dfStore, cur, nextCur)
                            
    corr=dfStore.corr()

    pdist = spc.distance.pdist(corr)
    linkage = spc.linkage(pdist, method='complete')
    idx = spc.fcluster(linkage, 0.5 * pdist.max(), 'distance')
    
    clusters=pd.DataFrame()
    clusters['industry']=list(corr.index)
    clusters['cluster']=list(idx)
    
    return store, dfStore, clusters

def train(x,y):
    clf = LinearRegression().fit(x, y)
    return clf.predict(x)

def filterData(fname=newFile, industry=[], df=None, filters=None, name=None):
    if filters is None:
        filters={
            'peratio':['!=','nan',True],
#            'openprice':['>',0.2,True],
#            'industry':['!=','',True],
#            'openprice':['>',1,False],
#            'net_profit_margin':['>', 10],
            'volume traded %':['>', 0.0001,False],
#            'p_nav':['<',1, True],
            'type':['=','others',True]
#            'revenue':['>',0]
#            'debt_assets_ratio':['<',0.4],
#            'operating_margin':['>',10],
#            'profitMarginGrowth':['>',0]
#            'cash_percen':['>',0.05],
#            'downside':['<',-0.4],
#            'upside':['>',0.3],
#            'dayVolume':['>',0,True]
#            'revenue':['>',100*pow(10,6)],
#            'Accumulated Depreciation, Total growth':['<',1]
                }
    stats={
        'Consumer':{
            'peratio':1,
            'openprice': 0.2,
            'net_profit_margin': 8
                }
            }
    if df is None:
        df=pd.read_csv(fname)
        dfStore=pd.read_csv(fname)
    else:
        dfStore=df.copy(deep=True)
        
    if len(industry)>0:
        industries=[str(x) for x in df['industry']]
        keepList=[]
        for count, val in enumerate(industries):
            val=[x.strip() for x in val.split('/')]
            for ind in industry:
                if ind in val:
                    keepList.append(count)
                    
        df=df.loc[keepList]
        df=df.reset_index(drop=True)
        dfStore=df.copy(deep=True)
        
    if name is not None:
        name=name.lower()
        names=[x.lower() for x in df['names']]
        keepList=[]
        for count,val in enumerate(names):
            if name in val:
                keepList.append(count)
        df=df.loc[keepList]
        df=df.reset_index(drop=True)
    
#    print(len(df))
        
    for i in filters:
        if filters[i][0]=='>':
            df=df[df[i]>filters[i][1]]
        if filters[i][0]=='<':
            df=df[df[i]<filters[i][1]]
        if filters[i][0]=='=':
            df=df[df[i]==filters[i][1]]
            dfStore=dfStore[dfStore[i]==filters[i][1]]
        if filters[i][0]=='!=':
            df=df[df[i]!=filters[i][1]]
            dfStore=dfStore[dfStore[i]!=filters[i][1]]
        
        if type(filters[i][1])!=type('s'):
            if filters[i][2]:
                temDf=dfStore[dfStore[i]==0]
                df=df.append(temDf)
                df.drop_duplicates(subset='names', inplace=True)
            dfStore=df.copy(deep=True)
    
#    df=df[(df['peratio']>1)&(df['openprice']>0.2)&(df['net_profit_margin']>5)&(df['volume traded %']>0.01)]
    return df

def extractFileFromDB(fname=file, dbName='rawData'):
    df=db.extractTable(dbName)['result']
    return df
    
def getFilteredResult(industry=[], cloud=True, filters=None):
    if cloud:
        rawData=extractFileFromDB()
        summary=extractFileFromDB(dbName='summary')
    dfMain, dfDel, dfCheck, summary, dfNew, f=cleanAndProcess(summary, rawData, newFile)
    df=filterData(industry=industry, df=dfNew, filters=None)
    return df

def cleanCols(df):
    cols_to_drop=['high_low', 'close','prevclosedate','p_float','avgvolume','normalizedeps',\
              'mthvwap','unadjvwap','adjvwap', 'dividend','divident_5_yr_avg','debt', \
              'long_term_debt_equity','ebit','operating_income','netincome', 'ebita',\
              'sharesoutstanding', 'pricebookvalue', 'type', 'industry', 'enterprisevalue', \
              'assets', 'cash', 'capex', 'financial_info']
    
    display=['names','openprice','industry','upside','downside','day_high','day_low','dayVolatility','percenchange','weightedDayVolatility','volume traded %','dayVolume','peratio','revenue','div_val','marketcap',\
             'operating_margin','net_profit_margin','debt_assets_ratio','shortdebt_over_profit',\
             'p_nav','profitMarginGrowth','cash_percen',\
             'Revenue growth', 'Operating Income growth', 'Net Income growth', 'Cash growth', \
             'Total Receivables, Net growth', 'Accumulated Depreciation, Total growth', \
             'Total Assets growth', 'Total Current Liabilities growth', \
             'Total Long Term Debt growth', 'Total Debt growth', 'Common Stock, Total growth', \
             'receiveables over revenue', 'cash over assets', 'long term debt over debt']
    
#    df=df.drop(cols_to_drop, axis=1)
    df=df[display]
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

def cleanFinancial(info):
    tem=json.loads(info)
    for t in tem:
        lst=tem[t]
        newLst=[]
        for i in lst:
            i=i.replace(',','')
            try:
                a=float(i)
            except:
                a=0
            newLst.append(a)
        tem[t]=newLst
    return tem

def getGrowth(lst):
    lst=[(x-y)/y if (x!=0 and y!=0 ) else 0 for x,y in zip(lst[1:],lst[:-1])]
    return lst

def getRatio(lst1, lst2):
    return [x/y if (x!=0 and y!=0) else 0 for x,y in zip(lst1, lst2)]

def getMean(lst):
    count=0
    for i in lst:
        if i !=0:
            count+=1
    if count>0:
        return round(sum(lst)/count,2)
    else:
        return 0
    
def getCompany(df, nameCol, nameExc):
    store=None
    for count, name in enumerate(df[nameCol]):
        name=name.lower()
        nameExc=nameExc.lower()
        
        if nameExc in name:
            if store is None:
                store=df.loc[[count]]
            else:
                store.loc[len(store)]=df.loc[count]
    
    return store

def processFinancialInfo(df):
    financialInfo=list(df['financial_info'])
    names=list(df['names'])
    
    growths=['Revenue','Operating Income','Net Income', 'Cash', 'Total Receivables, Net', \
             'Accumulated Depreciation, Total', 'Total Assets','Total Current Liabilities',\
             'Total Long Term Debt', 'Total Debt', 'Common Stock, Total']
    ratio={
        'receiveables over revenue':['Total Receivables, Net','Revenue'],
        'cash over assets':['Cash', 'Total Assets'],
        'long term debt over debt':['Total Long Term Debt', 'Total Debt'],
        'debt over revenue':['Total Debt','Net Income']
        }
    
    addName=' growth'
    summary=pd.DataFrame(columns=['names']+[x+addName for x in growths]+list(ratio))
    store={}
    for count, info in enumerate(financialInfo):
        if info!='' and str(info)!='nan' and str(info)!='{}':
            lst=[names[count]]
            store2={}
            store2['info']=cleanFinancial(info)
#            print(names[count])
            
            tem={}
            for g in growths:
                tem[g+addName]=getGrowth(store2['info'][g])
                lst.append(getMean(tem[g+addName]))
            store2['growth']=tem
            
            tem2={}
            for r in ratio:
                tem2[r]=getRatio(tem[ratio[r][0]+addName],tem[ratio[r][1]+addName])
                lst.append(getMean(tem2[r]))
            store2['ratio']=tem2
            store[names[count]]=store2
            
            summary.loc[len(summary)]=lst
        else:
            summary.loc[len(summary)]=[names[count]]+[0]*(len(list(summary))-1)
    
    return store, summary

def runLogger(df, run=False):
    if run:
        store=log.update('sgx',df, 'names','names')
        store=log.calStats()
        log.save()
    
    table=log.compileTable('sgx')
    
    df=pd.merge(df, table, how='outer', left_on='names', right_on='company')
    df.drop(['symbol','company'],axis=1, inplace=True)
    
    return df,table
    
def run(pullFromDB=False):
    if pullFromDB:
        df=extractFileFromDB()
        df.to_csv(file, index=False)
    df=pd.read_csv(file)
#    return df
    dfMain, dfDel, dfCheck, summary, dfNew, f=cleanAndProcess(summaryFName, file, newFile)
    return df,dfNew, dfMain, f

dailyChangeFilter={
    'peratio':['!=','nan',True],
    'industry':['!=','',True],
    'openprice':['>',0.25,False],
    'volume traded %':['>', 0.0001,False]}

upsideFilter={
    'peratio':['<',20,True],
#    'peratio':['<',15,True],
    'industry':['!=','',True],
    'openprice':['>',0.25,False],
    'net_profit_margin':['>', 0,True],
    'volume traded %':['>', 0.00001,False],
#    'dayVolume':['>', 0,False],
#    'p_nav':['>',1],
    'type':['=','others',True],
#    'revenue':['>',100*pow(10,6),True],
#    'operating_margin':['>',10],
    'downside':['>',-0.6,True],
    'upside':['>',0.3,True]
        }

#df=run(False)

#df,dfNew, dfMain, financial = run(False)
#dfNew, table=runLogger(dfNew, False)
#stats=getStats(dfNew)
#dfNewCmp=cleanCols(dfNew)
#####
#dfDailyChange=filterData(filters=dailyChangeFilter,df=dfNew)
#dfDailyChangeCmp=cleanCols(dfDailyChange)
###
#dfUpside=filterData(filters=upsideFilter,df=dfNew)
#dfUpsideCmp=cleanCols(dfUpside)
##
#industry=['Electrical Components & Equipment', 'Electronic Equipment & Parts', 'Industrial Machinery & Equipment', 'Semiconductors']
#dfFilter=filterData(df=dfNew, industry=industry)
#dfCmp=cleanCols(dfFilter)
##dfCmp.to_csv(cmpFile, index=False)
#
#company=getCompany(dfNew,'names', 'hyphens pharma')
##
#store, dfStore, clusters=extractIndustries(df=dfNew)
#
#f, fsummary=processFinancialInfo(dfNew)
#fstats=getStats(fsummary)

