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

file='data/companyInfo.csv'
newFile='data/companyInfo(cleaned).csv'
summaryFName='data/summary.csv'
filedir='logs'
metadata = 'D:\stuff\scrapy\sgx\logs\metadata.txt'
pca = PCA(n_components=3,#len(list(dfProcess)),
         random_state = 123,
         svd_solver = 'auto'
         )
clf = LocalOutlierFactor(contamination=0.3)

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
                if 'M' in i or 'B' in i:
                    newVal=i.replace(',', '')
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
                    newVal=i.replace('USD', '')
                    newVal=i.replace('SGD', '')
                    try:
                        newVal=float(newVal)
                    except:
                        newVal=0
                
                newLst.append(newVal)
            df[header]=newLst
            errorCount=sum(df[header]==0)
            summary.loc[len(summary)]=[header, errorCount,round(errorCount/len(df),1)]
    return df, dfDel, dfNew, summary

def featuresEngineering(df, details):
    eps=df['normalizedeps']
    price=df['openprice']
    income=df['netincome']
    cash=df['cash']
    assets=df['assets']
    debt=df['debt']
    enterpriseVal=df['enterprisevalue']
    sharesOutstanding=df['sharesoutstanding']
    industry=df['industry']
    marketcap=df['marketcap']
    marketcap=sharesOutstanding*price
    div=df['dividend']
    div_5=df['divident_5_yr_avg']
    
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
    df['profitGrowth']=df['div_val']
    
    cols_to_drop=['high_low', 'close','prevclosedate','p_float','avgvolume','normalizedeps',\
                  'mthvwap','unadjvwap','adjvwap', 'dividend','divident_5_yr_avg','debt', \
                  'long_term_debt_equity','operating_income','netincome'
                  ]
    
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
    
    dfMain, dfDel, dfCheck, summary=dfCleaner(df, exceptions=['names', 'prevclosedate', 'p_float', 'industry'])
    dfNew=featuresEngineering(dfMain, details)
    
    dfNew.to_csv(newFileName, index=False)
    
    dfCompare=dfNew[['names','openprice', 'normalizedeps', 'peratio', 'new PE ratio', 'netincome', 'operating_margin', 'net_profit_margin','aquirer multiple','normalized aquirer multiple', 'cash', 'assets', 'roe', 'new roe', 'roa', 'new roa', 'price_sales', 'price_cf','long_term_debt_equity', 'revenue_per_share_5_yr_growth', 'eps_per_share_5_yr_growth']]
    
    return dfMain, dfDel, dfCheck, summary, dfNew, dfCompare

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
            'peratio':['!=','nan'],
            'openprice':['>',0.1],
#            'net_profit_margin':['>', 20],
#            'volume traded %':['>', 0.01],
#            'p_nav':['<',1],
            'type':['=','others'],
#            'revenue':['>',0]
#            'debt_assets_ratio':['<',0.4],
            'operating_margin':['>',10]
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
        if filters[i][0]=='!=':
            df=df[df[i]!=filters[i][1]]
    
#    df=df[(df['peratio']>1)&(df['openprice']>0.2)&(df['net_profit_margin']>5)&(df['volume traded %']>0.01)]
    return df

def extractFileFromDB(fname=file, dbName='rawData'):
    df=db.extractTable(dbName)['result']
    return df
    
def getFilteredResult(industry=[], cloud=True, filters=None):
    if cloud:
        rawData=extractFileFromDB()
        summary=extractFileFromDB(dbName='summary')
    dfMain, dfDel, dfCheck, summary, dfNew, dfCompare=cleanAndProcess(summary, rawData, newFile)
    df=filterData(industry=industry, df=dfNew, filters=None)
    return df

if __name__ == "__main__":
    pullFromDB=True
    if pullFromDB:
        df=extractFileFromDB()
        df.to_csv(file, index=False)
        
    dfMain, dfDel, dfCheck, summary, dfNew, dfCompare=cleanAndProcess(summaryFName, file, newFile)
#
#industries, industriesDf, clusters=extractIndustries()
#a=filterData(industry=[])
#b=a[['names','marketcap','openprice','peratio','dividend','divident_5_yr_avg','revenue','operating_margin','net_profit_margin', 'p_nav']]
#
#x=a[['div_val','roe','roa','operating_margin','net_profit_margin', 'p_nav', 'eps']]
#y=a['peratio']
#
#result=train(x,y)
#
#z=x.copy(deep=True)
#z['y']=y
#corr=z.corr()
#
#b['pred'] = result
#b['diff']=(b['pred']-b['peratio'])/b['peratio']

#b=dfNew[dfNew['names']=='Wilmar Intl']
#a=getFilteredResult()
