# -*- coding: utf-8 -*-
"""
Created on Thu Nov 22 23:11:08 2018

@author: ASUS
"""

import os
from urllib import parse
import psycopg2 as ps
from psycopg2.extras import execute_batch
from datetime import datetime
import pandas as pd
import numpy as np

connList=None

dbList={
    'summary': 'summary',
    'rawData': 'rawdata',
    'jobs':'joblist',
    'friar':'friar'
    }

def connectToDatabase():
    global connList
    connList={}
    url='postgres://gwbzmyoayqpank:52e661da399acf81878730af4b33746d838e3193fbe69b5794af3c1bbe0e05a8@ec2-54-221-198-156.compute-1.amazonaws.com:5432/dd11g91285ue9f'

    os.environ['DATABASE_URL'] = url
               
    parse.uses_netloc.append('postgres')
    url=parse.urlparse(os.environ['DATABASE_URL'])
    
    conn=ps.connect(
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
            )
    
    cur=conn.cursor()
    
    connList['cur']=cur
    connList['conn']=conn

def runquery(query, resultExpected=False, valList=(), close=True):
    global connList
    if connList is None:
        connectToDatabase()
    cur=connList['cur']
    conn=connList['conn']
        
    result={
        'msg':None,
        'result':None,
        'error':None}
    try:
        if len(valList)==0:
            cur.execute(query)
        else:
            print("batch")
            execute_batch(cur, query, valList)
        
        result['msg']=cur.statusmessage
#        print(result['msg'])
        if resultExpected:
            result['result']=cur.fetchall()
        
        if close==True:
            closeConn()
            
    except BaseException as e:
        result['error']=[e]
        print(e)
        closeConn()
    
#    print(result)
    return result

def closeConn():
    global connList
    
    cur=connList['cur']
    conn=connList['conn']
    
    cur.close()
    conn.commit()
    connList=None

def dtypeConverter(df, calType=1):
    ref={
        'object': 'varchar(255)',
        'float64':'float',
        'int64': 'int'
            }
    
    ref2={
        'object': '%s',
        'float64':'%s',
        'int64': '%s'
            }
    
    if calType==1:
        types=df.dtypes
        tIndex=list(types.index)
        tType=[x.name for x in list(types)]
        
        store=''
        
        for count, val in enumerate(tIndex):
            store+='%s %s, '%(val.replace(' ', '_'), ref[tType[count]])
        
        return store[:-2]
    
    if calType==2:
        store=''
        for i in df.dtypes:
            store+=ref2[i.name]
            store+=', '
        
        return store[:-2]
    

def recreateTable(dbName, df, cont=0):
    tblResult=findTable(dbName)
    if tblResult['error'] is not None:
        return tblResult
    else:
        tblName=tblResult['result']
        
    queries={
        'dropTbl':'drop table %s' %(tblName),
        'createTbl':'CREATE TABLE %s (%s)' % (tblName,dtypeConverter(df))
        }
    
#    print(queries)
    
#    return queries['createTbl']
    indexes=list(queries)
    
    try:
        for ind in range(cont, len(queries)):
            query=queries[indexes[ind]]
            result=runquery(query, close=False)
            print(indexes[ind], '--', result)
        
        closeConn()
    except:
        print(indexes[ind], '-- error --')
        if ind<(len(queries)-1):
            recreateTable(tblName, df, cont=(ind+1))
        else:
            closeConn()

def rewriteTable(dbName, df):
    tblResult=findTable(dbName)
    if tblResult['error'] is not None:
        return tblResult
    else:
        tblName=tblResult['result']
        
    result=getColumnName(tblName)
    if len(result)==0:
        recreateTable(dbName, df, cont=1)
        
    cols=str([x.replace(' ','_') for x in list(df)]).replace("'","")[1:-1]
    vals=[]
    for i in df.index:
#        vals.append(str(list(df.loc[i]))[1:-1])
        lst=list(df.loc[i])
        lst=[int(x) if type(x)==np.int64 else x for x in lst]
        vals.append(lst)
        
    query='INSERT INTO %s (%s) VALUES(%s)'%(tblName, cols, dtypeConverter(df, calType=2))
    print(query)
    print(vals)
#    return query, vals
    
    result = runquery(query, valList=vals, close=False)
    print("result ok")
    
    try:
        closeConn()
    except BaseException as e:
        print(e)
        
    print('rewrite table %s --'%(tblName), result)
    return result

def getColumnName(tblName, close=True):
    query="SELECT column_name FROM information_schema.columns WHERE table_name = '%s'"%(tblName)
    qresult=runquery(query, resultExpected=True, close=close)
    
    if qresult['error'] is not None:
        return None
    
    result=[x[0] for x in qresult['result']]
    
    print('extract cols of %s --- '%(tblName), str(qresult['msg']), str(qresult['error']))
    
    return result

def extractTable(dbName):
    tblResult=findTable(dbName)
    if tblResult['error'] is not None:
        return tblResult
    else:
        tblName=tblResult['result']
        
    result={
        'msg':None,
        'error':None,
        'result':None
            }
    
    tblCols=getColumnName(tblName, close=False)
    
    if len(tblCols)==0:
        result['error']='tbl not found'
        return result
    
    query='SELECT * FROM %s'%(tblName)
    qresult=runquery(query, resultExpected=True)
    
    result['msg']=qresult['msg']
    result['error']=qresult['error']
    
    if qresult['error'] is None:
        df=pd.DataFrame(columns=tblCols)
        for val in qresult['result']:
            df.loc[len(df)]=[x for x in val]
        
        result['result']=df
        
    print('extract %s --- '%(tblName), str(qresult['msg']), str(qresult['error']))
    
    return result

def findTable(dbName):
    result={
        'msg':None,
        'result':None,
        'error':None}
    try:
        result['result']=dbList[dbName]
    except:
        result['msg']='tbl Not found'
        result['error']='tbl Not found'
    
    return result

def insertRow(dbName, lst):
    tblResult=findTable(dbName)
    if tblResult['error'] is not None:
        return tblResult
    else:
        tblName=tblResult['result']
        
    result={
        'msg':None,
        'result':None,
        'error':None}
    
    cols=getColumnName(tblName, close=False)
    
    if cols is None:
        closeConn()
        result['error']= 'table dun exist'
        return result
    else:
        cols=str(cols).replace("'","")[1:-1]
    
    query='INSERT INTO %s (%s) VALUES(%s)' %(tblName, cols, str(lst)[1:-1])
#    print(query)
    
    result=runquery(query)
    
    print('insert row to %s --- '%(tblName), str(result['msg']), str(result['error']))
    
    return result

def editRow(dbName, edColNames, edColVals, colName, colVal):
    tblResult=findTable(dbName)
    if tblResult['error'] is not None:
        return tblResult
    else:
        tblName=tblResult['result']
        
    result={
        'msg':None,
        'result':None,
        'error':None}
    
    cols=getColumnName(tblName, close=False)
    
    if cols is None:
        closeConn()
        result['error']= 'table dun exist'
        return result
    else:
        cols=cols
#        cols=str(cols).replace("'","")[1:-1]
    
    store=""
    for count, val in enumerate(edColNames):
        store+="%s = '%s', "%(val, edColVals[count])
    store=store[:-2]
    
    query="UPDATE %s SET %s WHERE %s='%s'"%(tblName, store,colName, colVal)
    print(query)
    
    result=runquery(query)
#    
    return result
    
#a=runquery("SELECT intjobid from joblist WHERE jobid ='%s'"%('test2'),True)

#df=pd.read_csv('data/summary.csv')
#df['testing']=([1]*len(df))
#a=recreateTable('summary', df)
#b=rewriteTable('summary', df)
#df2=extractTable('summary')
#a=insertRow('summary', ['test', 'test','test', 'test', 'test'])
#a=editRow('jobs',['tc', 'te'], [1,2], 'jobid', 'test')