from crawler import crawler
import pandas as pd
import dbConnector as db
import time
import scipy.cluster.hierarchy as spc

url='https://www.nasdaq.com/market-activity/stocks/screener'
symbolUrl='https://www.nasdaq.com/market-activity/stocks/%s'
nasdaqFile='data/nasdaqSummary.csv'
nasdaqDetailsFile='data/nasdaqDetails.csv'
dbname='nasdaqsummary'
detailDbname='nasdaqdetails'

currencyCols=['price', 'one_yr_tar', 'eps', 'div','day_high','day_low','year_high','year_low']
numericCols=['change', 'volume', 'market_cap', 'pe_ratio', 'forward_pe', 'fifty_day_vol', 'beta']
percenCols=['percen_change', 'yield']

def run(userAgentNum=0, local=False):
    print('run nasdaq full')
    print('nasdaq -%s'%(str(userAgentNum)))
    crawl=crawler(local=local,userAgentNum=userAgentNum)
    
    df=crawl.getNasdaqPrice(dbname=dbname,url=url,addRemainCols=True)
    
    if len(df)>10:
        crawl.store(df, fileLoc=nasdaqFile, dbName=dbname, write='cloud')
        
        df2=crawl.getNasdaqDetails(url=symbolUrl,dbName=dbname)
        crawl.store(df2, fileLoc=nasdaqDetailsFile, dbName=dbname, write='cloud')
    
    crawl.closeDriver()
    return df

def updateBasics(userAgentNum=0, local=False):
    print('run nasdaq update Basics')
    print('nasdaq -%s'%(str(userAgentNum)))
    crawl=crawler(local=local,userAgentNum=userAgentNum)
    
    summary=crawl.getNasdaqPrice(dbname=None,url=url)
    
    if len(summary)>10:
        df=crawl.updateDetails(summary, dbname)
        crawl.store(df, fileLoc=nasdaqFile, dbName=dbname, write='cloud')
    else:
        df=summary
    
    crawl.closeDriver()
    return df

def updateDetails(userAgentNum=0, local=False):
    print('run nasdaq full')
    print('nasdaq -%s'%(str(userAgentNum)))
    crawl=crawler(local=local,userAgentNum=userAgentNum)
    
    success, df=crawl.getNasdaqDetails(url=symbolUrl,dbName=dbname)
    if success:
        crawl.store(df, fileLoc=nasdaqDetailsFile, dbName=dbname, write='cloud')
    else:
        print('access denied')
    
    crawl.closeDriver()
    return df

def extractIndustries(df, colName):
    industry=list(df[colName])
    store={}
    
    for count, line in enumerate(industry):
        tem=[x.strip() for x in str(line).split('/')]
        for cur in tem:
            if cur!='':
                try:
                    store[cur]=store[cur]+1
                except:
                    store[cur]=1
    
    return store

def dataCleaning(df):
    df=df.drop_duplicates(subset='symbol')
    for col in list(df):
        df[col]=[x if str(x)!='-' and str(x)!='nan' and str(x)!='--' else '' for x in df[col]]
    
    df['day_high']=[x.split('/')[0] if str(x)!='' else '' for x in df['day_high_low']]
    df['day_low']=[x.split('/')[1] if str(x)!='' else '' for x in df['day_high_low']]
    df['year_high']=[x.split('/')[0] if str(x)!='' else '' for x in df['year_high_low']]
    df['year_low']=[x.split('/')[1] if str(x)!='' else '' for x in df['year_high_low']]
    df.drop('day_high_low',axis=1,inplace=True)
    df.drop('year_high_low',axis=1,inplace=True)
    
    for col in currencyCols:
        if col in list(df):
            df[col]=[float(str(x).replace('$','').replace(',','')) if str(x) !='' else 0 for x in df[col]]
    for col in numericCols:
        if col in list(df):
            df[col]=[float(str(x).replace(',','')) if str(x) !='' else 0 for x in df[col]]
    for col in percenCols:
        if col in list(df):
            df[col]=[float(str(x).replace('%','')) if str(x) !='' else 0 for x in df[col]]
        
    return df

def dataEngineering(df):
    df=df.copy(deep=True)
    df['pe']=[x/y if x!=0 and y!=0 else 0 for x,y in zip(df['price'],df['eps'])]
    df['shares_oustanding']=[x/y if x!=0 and y!=0 else 0 for x,y in zip(df['market_cap'],df['price'])]
    df['day_percen_traded']=[x/y if x!=0 and y!=0 else 0 for x,y in zip(df['volume'],df['shares_oustanding'])]
    df['fifty_day_percen_traded']=[x/y if x!=0 and y!=0 else 0 for x,y in zip(df['fifty_day_vol'],df['shares_oustanding'])]
    df['upside']=[round((x-y)*100/y,2) if x!=0 and y!=0 else 0 for x,y in zip(df['year_high'],df['price'])]
    df['downside']=[round((y-x)*100/y,2) if x!=0 and y!=0 else 0 for x,y in zip(df['year_low'],df['price'])]
    df['dayVolatility']=[round((x-y)*100/z,2) if x!=0 and y!=0 and z!=0 else 0 for x,y,z in zip(df['day_high'],df['day_low'],df['price'])]
    
    return df

def cleanView(df):
    cols=['company','price','percen_change', 'market_cap', 'pe_ratio', 'yield', \
          'upside', 'downside', 'dayVolatility','day_percen_traded','fifty_day_percen_traded', 'sector','industry']
    return df[cols]

def sieveData(df, filters=None, industryCol=None, industries=[]):
    nonullcols=['price','volume']
    df=df.copy(deep=True)
    
    for col in nonullcols:
        df=df[df[col]>0]
        
        
    if industryCol is not None:
        for ind in list(df.index):
            data=df.loc[ind][industryCol]
            keep=False
            for industry in industries:
                if industry in data:
                    keep=True
            if keep==False:
                df.drop(ind,axis=0,inplace=True)
                
    if filters is None:            
        filters={
            'price':['>',1,'price'],
            'pe_ratio':['<',40,'pe_ratio'],
            'day_percen_traded':['>',0,'day_percen_traded'],
            'upside':['>',30,'upside']
            
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

def analytics(download=True):
    if download:
        df=db.extractTable(dbname)['result']
        df.to_csv(nasdaqFile, index=False)
        
    df=pd.read_csv(nasdaqFile)
    
    return df

df=analytics(False)
dfClean=dataCleaning(df)
dfEngine=dataEngineering(dfClean)
breakdownIndustry=extractIndustries(df,'industry')
breakdownSector=extractIndustries(df,'sector')
#
dayChange={
    'price':['>',1,'price'],
    'pe_ratio1':['>',2,'pe_ratio'],
    'pe_ratio2':['<',40,'pe_ratio'],
    'day_percen_traded':['>',0,'day_percen_traded'],
    'upside':['>',30,'upside']
        }

upside={
    'price':['>',1,'price'],
    'pe_ratio1':['>',2,'pe_ratio'],
#    'pe_ratio2':['<',40,'pe_ratio'],
    'day_percen_traded':['>',0,'day_percen_traded']
#    'upside':['>',30,'upside']
        }

#dfDayVol=sieveData(dfEngine,filters=dayChange)
#dfDayVolView=cleanView(dfDayVol)
#
#dfUpside=sieveData(dfEngine, filters=upside)
#dfUpsideView=cleanView(dfUpside)
##
#dfFilter=sieveData(dfEngine)
#dfView=cleanView(dfFilter)
#
#dfCmp=getCompany(dfEngine,'company', 'standard')
