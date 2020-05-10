import requests
import json
import pandas as pd
import util
import dbConnector as db

class iex:
    def __init__(self, mode='prod', host='local'):
        
        self.prodUrl='https://cloud.iexapis.com/stable'
        self.sandUrl='https://sandbox.iexapis.com/stable'

        self.prod_pub_key='pk_c416027ee8454209af696c16ba1f031c'
        self.prod_secret_key='sk_e2f7dd2b47864935a6d50db3b83f1c4a'
        
        self.sand_pub_key='Tpk_6f9f528cefc14639a415f535c7a12c69'
        self.sand_secret_key='Tsk_fe87c12722964ca3854a3005c9e79c2e'
        
        self.timec=util.timeClass()
        self.USDbName='nasdaqsummary'
        
        if mode=='prod':
            self.url=self.prodUrl
            self.pub_key=self.prod_pub_key
            self.secret_key=self.prod_secret_key
        else:
            self.url=self.sandUrl
            self.pub_key=self.sand_pub_key
            self.secret_key=self.sand_secret_key
            
        self.apiBatch=50
        
        self.baseParams={
            'token':self.secret_key
                }
            
        self.refs={
            'symbols': {
                'extension':'/ref-data/symbols',
                'store':'data/USSymbols.csv'
                },
            'key_stats':{
                'extension':'/stock/%s/stats',
                'store':'data/key_stats.csv'
                    },
            'quote':{
                'extension':'/stock/%s/quote'
                    }
            }
            
        self.symbolFilter={
            'exchange':['NAS','NYS'],
            'type':['cs','rt']
                }
        self.symbolColsToKeep=['symbol','exchange','name','type']
        self.quoteColsToDel=['symbol','latestPrice','latestVolume','latestUpdate','latestTime','change',\
                              'changePercent', 'open','close', 'previousClose', 'high','low',\
                              'marketCap','avgTotalVolume','week52High','week52Low','ytdChange',\
                              'peRatio']
            
    def callAPI(self, url, params=None):
        if params is None:
            req=requests.get(url)
        else:
            req=requests.get(url, params=params)
        
        if str(req.status_code)=='200':
            data=json.loads(req.text)
        else:
            print(req.text)
            data=None
        
        return data
    
    def filterData(self, df, cols, colsToKeep=None):
        baseDf=df.copy(deep=True)
        for col in cols:
            temDf=None
            lst=cols[col]
            for itm in lst:
                if temDf is None:
                    temDf=baseDf[baseDf[col]==itm]
                else:
                    temDf=temDf.append(baseDf[baseDf[col]==itm])
            baseDf=temDf.copy(deep=True)
        
        baseDf=baseDf[colsToKeep]
        
        return baseDf
    
    def convertLstToDf(self, data):
        df=None

        for itm in data:
            if df is None:
                df=pd.DataFrame(columns=list(itm))
            df.loc[len(df)]=list(itm.values())
        
        return df
            
    def store(self, df, ref=None, dbname=None):
        if ref is not None:
            try:
                df.to_csv(self.refs[ref]['store'], index=False)
            except:
                print('save local failed')
        if dbname is not None:
            db.recreateTable(dbname, df)
            db.rewriteTable(dbname, df)
            
    def convertDataFrame(self,df):
        newDf=pd.DataFrame()
        
        for col in list(df):
            newDf[col.lower()]=df[col]
        
        return newDf
            
    def updateData(self,df, dbname=None, mainDf=None,overwrite=False, idCol=None):
        if mainDf is None:
            results=db.extractTable(dbname)['result']
        else:
            results=mainDf.copy(deep=True)
        
        results=self.convertDataFrame(results)
            
        df=df.copy(deep=True)
        df=self.convertDataFrame(df)
            
        if len(results)==0 or overwrite==True:
            return df
        else:
            newCols=[x.lower() for x in list(df)]
            oldCols=[x.lower() for x in list(results)]
            
            for col in newCols:
                if col not in oldCols:
                    results[col]=['']*len(results)
            
            for ind in list(df.index):
                row=df.loc[ind]
                identifier=row[idCol]
#                print(identifier)
#                print(results)
                
                rowFound=results[results[idCol]==identifier]
                
                
                if len(rowFound)>0:
                    rowFoundInd=rowFound.index[0]
                    for col in list(row.index):
                        rowFound[col]=row[col]
                    results.loc[rowFoundInd]=list(rowFound.values[0])
#                    return 
                else:
                    newInd=len(results)
                    results.loc[newInd]=['']*len(list(results))
                    newRow=results.loc[newInd]
                    for col in list(row.index):
                        newRow[col]=row[col]
                    results.loc[newInd]=list(newRow.values)
#                    return results
        
        return results
    
    def getSymbolsList(self, apiExtract=False):
        if apiExtract:
            print('extracting symbols list from api')
            url=self.url+self.refs['symbols']['extension']
            params=self.baseParams
            data=self.callAPI(url,params)
            
            if data is not None:
                df=self.convertLstToDf(data)
        else:
            print('extracting symbols list from file')
            df=pd.read_csv(self.refs['symbols']['store'])
        
        df=self.filterData(df,self.symbolFilter, self.symbolColsToKeep)
            
        return df
    
    def getKeyStats(self, symbol):
        url=(self.url+self.refs['key_stats']['extension'])%(symbol)
        params=self.baseParams
#        print(url, params)
        
        data=self.callAPI(url, params)
        
        return data
        
    
    def getAllKeyStats(self, dbname, df=None, start=0, end=0, overwrite=False):
        print('iex - start:%s / end:%s'%(start,end))
        print('get all key stats')
        self.timec.startTimer()
        if df is None:
            df=db.extractTable(dbname)['result']
            
        if overwrite==False:
            df2=df[df['week52change']=='']
            
        print('symbol count: %s' %(str(len(df2))))
        
        if end==0:
            symbols=df2['symbol'][end:]
        else:
            symbols=df2['symbol'][start:end]
        
        lst=[]
        
        for count, symbol in enumerate(symbols):
            data=self.getKeyStats(symbol)
            data['symbol']=symbol
            print('%s - %s'%(symbol,str(data)))
            lst.append(data)
            
            if count%self.apiBatch==0 and count>0:
                self.timec.getTimeSplit(str(count)+'-api called')
                updatedTable=self.convertLstToDf(lst)
                df=self.updateData(df=updatedTable,mainDf=df, idCol='symbol')
                self.timec.getTimeSplit(str(count)+'-updated')
                self.store(df,dbname=dbname)
                lst=[]
        
        self.timec.getTimeSplit(str(count)+'-api called')
        updatedTable=self.convertLstToDf(lst)
        df=self.updateData(df=updatedTable,mainDf=df, idCol='symbol')
        
        self.timec.getTimeSplit(str(count)+'-updated')
        self.store(df,dbname=dbname)
        
        self.timec.stopTime()
        
        return df
    
    def getStockQuote(self, symbol):
        url=(self.url+self.refs['quote']['extension'])%(symbol)
        params=self.baseParams
        
        data=self.callAPI(url,params)
        
        return data
    
    def getAllStockQuotes(self, dbname=None,df=None, start=0, end=0):
        print('iex - start:%s / end:%s'%(start,end))
        self.timec.startTimer()
        if df is None:
            df=db.extractTable(dbname)['result']
        
        if end==0:
            symbols=df['symbol'][start:]
        else:
            symbols=df['symbol'][start:end]
        
        lst=[]
        
        for count, symbol in enumerate(symbols):
            data=self.getStockQuote(symbol)
            lst.append(data)
            
            if count%self.apiBatch==0 and count>0:
                self.timec.getTimeSplit(str(count)+'-api called')
                updatedTable=self.convertLstToDf(lst)
                df=self.updateData(df=updatedTable,mainDf=df, idCol='symbol')
                self.timec.getTimeSplit(str(count)+'-updated')
                self.store(df,dbname=dbname)
                lst=[]
        
        self.timec.getTimeSplit(str(count)+'-api called')
        updatedTable=self.convertLstToDf(lst)
        df=self.updateData(df=updatedTable,mainDf=df, idCol='symbol')
        self.timec.getTimeSplit(str(count)+'-updated')
        self.store(df,dbname=dbname)
        self.timec.stopTime()  
        
        return df
        
#a=iex()
#symbols=a.getSymbolsList()

#symbols=['A','AAAU','FIS']
#b=a.getAllKeyStats(symbols)
#d=a.convertLstToDf(b)
