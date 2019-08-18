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

queryLst=0

def connectToDatabase():
    url='postgres://bmmoobheozofxs:ac8ba0f76a53e13844126695d8bad3d6826d1e087773b87bef85cebc43664f30@ec2-54-225-196-122.compute-1.amazonaws.com:5432/d20apms1nhd8do'

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
    
    return cur, conn

def runquery(query, lst=True, connected=False, valList=(), connList={}):
    if connected:
        cur=connList['cur']
        conn=connList['conn']
    else:
        cur, conn=connectToDatabase()
        
    result=None
    try:
        if len(valList)==0:
            cur.execute(query)
        else:
            print("batch")
            execute_batch(cur, query, valList)
            
        if lst:
            result=list(cur)
        else:
            result=['success']
    except ps.Error as e:
        result=['error']
        print(e)
    except ValueError as e:
        result=['error']
        print(e)
    except:
        result=['error']
#        print(ps.Error)
    
    print(result)
    
    if connected==False:
        cur.close()
        conn.commit()
        
    return result

def updateCompanyRows(lst, connList):    
    lst.sort()
    
    #get current company names and new names to add
    query="SELECT name from PRICE"
    names = [i[0] for i in runquery(query, connected=True, connList=connList)]
    names.sort()
    
    addList=[]
    
    for i in lst:
        try:
            names.remove(i)
        except:
            addList.append(i)
            
    print(addList)
    
    #get all column names
    query="SELECT column_name FROM information_schema.columns WHERE table_name = 'price'"
    colNames=[i[0] for i in runquery(query, connected=True, connList=connList)]
    
    valList=""
    count=0
    for i in addList:
        if count == 0:
            valList="('%s'%s %s)"%(i, ((len(colNames)>1)* ", "), str([0]*(len(colNames)-1))[1:-1])
        else:
            valList= valList +", ('%s'%s %s)"%(i, ((len(colNames)>1)* ", "), str([0]*(len(colNames)-1))[1:-1])
        count += 1
        print(valList)
        
    print(valList)
    
    #create new row
    if len(addList)>0:
        query="INSERT INTO price(%s) VALUES %s" % (str(colNames)[1:-1].replace("'",""), valList)
        print(query)
        result=runquery(query,lst=False, connected=True, connList=connList)
    else:
        result=["table already up to date."]
    
    return result

def createNewRow(connList):
    curDate=datetime.now()
    colName="date%s%s%s"%(format(curDate.day, '02d'), format(curDate.month, '02d'), curDate.year)
    query="ALTER TABLE price ADD %s float"%(colName)
    
    print(query)
    
    result=runquery(query, lst=False, connected=True, connList=connList)
    return result, colName

def updatePrice(df, connList, colName):
    global queryLst
    queryLst=()
    query="UPDATE price SET %s"%(colName)+""" = %s WHERE name = %s"""
#    query="UPDATE price SET date9122018 = 110.0 WHERE name = 'test'"
    print(query)
    try:
        for i in range(len(df.index)):
            queryLst+=((float(df.iloc[i,1]), df.iloc[i,0]),)
    except:
        print(df.iloc[i,0])
        print(i)
#    print(queryLst)
    result=runquery(query, lst=False, connected=True, valList=queryLst, connList=connList)
#    result=runquery(query, lst=False, connected=True, connList=connList)
    
    return result

def updateDB(df):
    cur, conn=connectToDatabase()
    conn.autocommit=True
    connList={
        'cur': cur,
        'conn': conn
        }
    try:
        comNames=list(df['name'])
        result=updateCompanyRows(comNames, connList)
        if result[0] =='error':
            return False, 'error updating / checking companies in DB'
        else:
            result, colName=createNewRow(connList)
            result2=updatePrice(df, connList, colName) 
    except ValueError as e:
        print(e)
        cur.close()
        conn.commit()  
    except:
        cur.close()
        conn.commit()
    
    cur.close()
    conn.commit()

#df=pd.DataFrame(columns=['name', 'price'])
#df.loc[0]=['def2', '110.0']
#df.loc[1]=['test', '112.0']
#updateDB(df)