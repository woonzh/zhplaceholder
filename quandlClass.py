import quandl
import json
import util
import pandas as pd
import dbConnector as db

class quandlClass:
    def __init__(self):
        self.api="LRtmHtP86nCxsnkgA5Db"
        self.api2='yNGwxvnSsuRKrtSSWpNF'
        self.timec=util.timeClass()
        quandl.ApiConfig.api_key = "LRtmHtP86nCxsnkgA5Db"
        self.curDate=self.timec.getCurDate(quandl=True)
        self.oldDate=self.timec.getCurDate(quandl=True, daydelta=-4)
        self.hkexConvert={
            'Nominal Price':'price',
            'Share Volume (000)':'volume',
            'updated_date':'date_update',
            'High':'dayhigh',
            'Low':'daylow',
            'symbol':'code',
            'com_name':'com_name'}
        
    def getHKEXSingleData(self, symbol):
        dataset='HKEX/%s'%(str(symbol))
        data=quandl.get(dataset, rows=1)
        data['updated_date']=data.index[0].strftime('%Y-%m-%d')
        return data
    
    def engineerHKEXData(self, df):
        newDf=pd.DataFrame()
        for col in self.hkexConvert:
            newDf[self.hkexConvert[col]]=df[col]
        
        change=[round(x-y,2) if x!=0 and y!=0 else 0 for x,y in zip(df['Nominal Price'],df['Previous Close'])]
        changePercen=[round(100*x/y,2) if x!=0 and y!=0 else 0 for x,y in zip(change,df['Previous Close'])]
        
        newUpVal=[str(x)+" ("+str(y)+'%)' if x!=0 and y!=0 else 0 for x,y in zip(change,changePercen)]
        
        newDf['upval']=newUpVal
        
        return newDf
    
    def updateHKEXData(self,symbolLst=None, companyLst=None, dbname=None):
        print('quandl start')
        self.timec.startTimer()
        
        if symbolLst is None:
            orgDf=db.extractTable(dbname)['result']
            symbolLst=list(orgDf['code'][:2])
            companyLst=list(orgDf['com_name'][:2])
        
        print('quandl start-%s to call'%(len(symbolLst)))
        
        symbolLst=[x.zfill(5) for x in symbolLst]
            
        df=None
        count=0
        suspendedCount=0
        for count,symbol in enumerate(symbolLst):
            if 'suspended' not in symbol.lower():
                try:
                    data=self.getHKEXSingleData(symbol)
                    if count==0:
                        df=data.copy(deep=True)
                        df.insert(0,'symbol', symbol)
                        df.insert(1,'com_name', companyLst[count])
                        df.reset_index(inplace=True, drop=True)
                    else:
                        lst=[symbol, companyLst[count]]
                        lst+=data.values.tolist()[0]
                        df.loc[len(df)]=lst
                    count+=1
                except:
                    print('%s-%s not found'%(symbol,companyLst[count]))
                
                if count>0 and count%50==0:
                    self.timec.getTimeSplit('quandl-%s'%(str(count)))
            else:
                suspendedCount+=1
        print('quandl done %s/%s %s - suspended %s - not found'%(count,len(symbolLst),suspendedCount,len(symbolLst)-count-suspendedCount))
        self.timec.stopTime()
        print('df length - %s'%(len(df)))
        
        df2=self.engineerHKEXData(df)
        
        return df2
                
#a=quandlClass()
#symbolLst=['03988','00883']
#companyLst=['test1','test2']
#d,d2=a.updateHKEXData(symbolLst, companyLst)