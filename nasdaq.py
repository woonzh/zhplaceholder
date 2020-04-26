from crawler import crawler
import pandas as pd

url='https://www.nasdaq.com/market-activity/stocks/screener'
symbolUrl='https://www.nasdaq.com/market-activity/stocks/%s'
nasdaqFile='data/nasdaqSummary.csv'
nasdaqDetailsFile='data/nasdaqDetails.csv'
dbname='nasdaqsummary'
detailDbname='nasdaqdetails'

def run(local=False):
    print('run nasdaq full')
    crawl=crawler(local)
    df=crawl.getNasdaqPrice(dbname=dbname,url=url)
    crawl.store(df, fileLoc=nasdaqFile, dbName=dbname, write='cloud')
    df2=crawl.getNasdaqDetails(symbolUrl,df=df, dbname=detailDbname)
    crawl.store(df2, fileLoc=nasdaqDetailsFile, dbName=detailDbname, write='cloud')
    
    crawl.closeDriver()
    return df

def updateDetails(local=False):
    print('run nasdaq full')
    crawl=crawler(local)
    df=crawl.getNasdaqDetails(symbolUrl, readdbname=dbname,storedbname=detailDbname)
    crawl.store(df, fileLoc=nasdaqDetailsFile, dbName=detailDbname, write='cloud')
    
    crawl.closeDriver()
    return df

def analytics(download=False):
    if download:
        summary=db.extractTable(dbname)['result']
        df=db.extractTable(detailDbname)['result']
        summary.to_csv(nasdaqFile, index=False)
        df.to_csv(nasdaqDetailsFile, index=False)
        
    summary=pd.read_csv(nasdaqFile)
    df=pd.read_csv(nasdaqDetailsFile)
    
    return summary,df

#df=run(local=True)

#crawl=crawler(local=True)
#df=crawl.getNasdaqPrice(url)
#df2=crawl.getNasdaqDetails(symbolUrl,df=df)
#crawl.store(df, fileLoc=nasdaqFile, dbName=dbname, write='cloud')
#store=crawl.getSymbolData('msft',symbolUrl)

#crawl.closeDriver()
    
#indexes=[0]
#
#for col in a:
#    df[col]=['']*len(df)
#
#for ind in indexes:
#    row=df.loc[ind]
#    symbol=row['symbol']
#    data=store
#    for itm in data:
#        row[itm]=data[itm]
#    
#    df.loc[ind]=list(row)