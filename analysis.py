# -*- coding: utf-8 -*-
"""
Created on Fri Feb 22 21:54:43 2019

@author: ASUS
"""

import os
import tensorflow as tf
from tensorflow.contrib.tensorboard.plugins import projector
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.neighbors import LocalOutlierFactor
import matplotlib.pyplot as plt
import scipy.cluster.hierarchy as spc

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

def dfCleaner(df, cleanCol='prevCloseDate', exceptions=[]):
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
    eps=df['normalizedEPS']
    price=df['openPrice']
    income=df['netincome']
    cash=df['cash']
    assets=df['assets']
    debt=df['debt']
    enterpriseVal=df['enterpriseValue']
    sharesOutstanding=df['sharesOutstanding']
    floatv=[float(x.split('%')[0])/100 if x!='-' else 0 for x in list(df['float'])]
    
    #new PEratio
#    df['new PE ratio']=[x/(y/z) if (x!=0 and y!=0 and z!=0) else 0 for x, y,z in zip(price,income, sharesOutstanding)]
    df['new PE ratio']=[x/y if (x!=0 and y!=0) else 0 for x, y in zip(price, eps)]
    
#    #pricebook value
#    df['pbvratio']=[c/((x+y+z-a)/b) if (x!=0 and y!=0 and z!=0 and a!=0 and b!=0 and c!=0) else 0 \
#      for x, y, z, a, b, c in zip(cash, assets, enterpriseVal, debt, outstandingshares, closePrice)]
#    
#    #enterprice value over ebita
#    newebita=[x if x!=0 else y for x, y in zip(ebita, income)]
#    df['newevebita']=[x/y if (x!=0 and y!=0) else 0 for x, y in zip(enterpriseVal, newebita)]
#    
#    #ROE
    df['new roe']=[x/(y+z-a) if (x!=0 and y!=0 and z!=0 and a!=0) else 0 \
      for x, y, z, a in zip(income, cash, assets, debt)]

#    #ROA
    df['new roa']=[x/y if (x!=0 and y!=0) else 0 \
      for x, y in zip(income, assets)]
#    #aquirer multiple
    df['aquirer multiple']=[x/y if (x!=0 and y!=0) else 0 for x, y in zip(income, enterpriseVal) ]
    
#    #aquirer multiple against price
    df['normalized aquirer multiple']=[y/x if (x!=0 and y!=0) else 0 for x,y in zip(df['aquirer multiple'], price)]
    
    # get the % of shares traded
    df['volume traded %']=[x/(z*y) if (x!=0 and y!=0) else 0 for x,y, z in zip(df['avgVolume'], sharesOutstanding, floatv)]
    
    df=pd.merge(df, details[['names','address']], how='left',left_on='name',right_on='names')
    
    df=df.drop(['names'], axis=1)
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
    details=pd.read_csv(sumName)
    df=pd.read_csv(infoName)
    
    dfMain, dfDel, dfCheck, summary=dfCleaner(df, exceptions=['name', 'prevCloseDate', 'float', 'industry'])
    dfNew=featuresEngineering(dfMain, details)
    
    dfNew.to_csv(newFileName, index=False)
    dfNew=dfNew.set_index(dfNew['name'], drop=True)
    
    dfCompare=dfNew[['name', 'address','openPrice', 'normalizedEPS', 'peratio', 'new PE ratio', 'netincome', 'operating_margin', 'net_profit_margin','aquirer multiple','normalized aquirer multiple', 'cash', 'assets', 'roe', 'new roe', 'roa', 'new roa', 'price/Sales', 'price/CF','long term debt/equity', 'revenue_per_share_5_yr_growth', 'eps_per_share_5_yr_growth']]
    a=dfCompare[(dfCompare['new PE ratio']>0)&(dfCompare['openPrice']>0.2)]
    a['compare']=a['new PE ratio']-a['peratio']
    
    return dfMain, dfDel, dfCheck, summary, dfNew, dfCompare, a

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

def extractIndustries(fname=newFile):
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

def filterData(fname=newFile, industry=[]):
    stats={
        'Consumer':{
            'peratio':1,
            'openPrice': 0.2,
            'net_profit_margin': 8
                }
            }
    df=pd.read_csv(fname)
    industries=[str(x) for x in df['industry']]
    keepList=[]
    for count, val in enumerate(industries):
        val=[x.strip() for x in val.split('/')]
        for ind in industry:
            if ind in val:
                keepList.append(count)
                
    df=df.loc[keepList]
    df=df[(df['peratio']>1)&(df['openPrice']>0.2)&(df['net_profit_margin']>5)&(df['volume traded %']>0.01)]
    return df

if __name__ == "__main__":
    dfMain, dfDel, dfCheck, summary, dfNew, dfCompare, a=cleanAndProcess(summaryFName, file, newFile)

industries, industriesDf, clusters=extractIndustries()
a=filterData(industry=['Services'])
a.to_csv('to_explore.csv')

#df_pca = pcaFiles(industriesDf)
#clusters.to_csv(metadata, sep='\t')
#tf_data = tf.Variable(df_pca)
#
#with tf.Session() as sess:
#    saver = tf.train.Saver([tf_data])
#    sess.run(tf_data.initializer)
#    saver.save(sess, filedir+'/tf_data.ckpt')
#    config = projector.ProjectorConfig()
#    embedding = config.embeddings.add()
#    embedding.tensor_name = tf_data.name
#    embedding.metadata_path = metadata
#    projector.visualize_embeddings(tf.summary.FileWriter(filedir), config)

#df_pca = pcaFiles(dfProcess)
#
#newdf2=removeOutlier(df_pca, df2)
#newdfProcess=newdf2[['dividend', 'pricebookvalue','new PE ratio', 'margin', 'newevebita', 'roe']]
#newdf2.to_csv()
#
#df_pca = pcaFiles(newdfProcess)
#
#plotKMeans(df_pca)
#
#compare=dfNew[['name','peratio', 'new PE ratio', 'enterpriseValue', 'EBIT','aquirer multiple', 'ebita', 'netincome', 'roe']]
#compare=compare[(compare['aquirer multiple']!=0) & (compare['EBIT']>0)]

#tf_data = tf.Variable(df_pca)
#
#with tf.Session() as sess:
#    saver = tf.train.Saver([tf_data])
#    sess.run(tf_data.initializer)
#    saver.save(sess, filedir+'/tf_data.ckpt')
#    config = projector.ProjectorConfig()
#    embedding = config.embeddings.add()
#    embedding.tensor_name = tf_data.name
#    embedding.metadata_path = metadata
#    projector.visualize_embeddings(tf.summary.FileWriter(filedir), config)