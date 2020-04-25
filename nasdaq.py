from crawler import crawler

url='https://www.nasdaq.com/market-activity/stocks/screener'
symbolUrl='https://www.nasdaq.com/market-activity/stocks/%s'
nasdaqFile='data/nasdaqSummary.csv'
dbname='nasdaqsummary'

def run(local=False):
    print('run nasdaq full')
    crawl=crawler(local)
    df=crawl.getNasdaqPrice(url)
    crawl.store(df, fileLoc=nasdaqFile, dbName=dbname, write='cloud')
    #store=crawl.getSymbolData('msft',symbolUrl)
    
    crawl.closeDriver()
    return df

crawl=crawler(local=True)
df=crawl.getNasdaqPrice(url)
df2=crawl.getNasdaqDetails(symbolUrl,df=df)
#crawl.store(df, fileLoc=nasdaqFile, dbName=dbname, write='cloud')
#store=crawl.getSymbolData('msft',symbolUrl)

#crawl.closeDriver()