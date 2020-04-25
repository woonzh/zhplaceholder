from crawler import crawler

url='https://www.nasdaq.com/market-activity/stocks/screener'
symbolUrl='https://www.nasdaq.com/market-activity/stocks/%s'
nasdaqFile='data/nasdaqSummary.csv'
dbname='nasdaqsummary'

def run():
    print('run nasdaq full')
    crawl=crawler()
    df=crawl.getNasdaqPrice(url)
    crawl.store(df, fileLoc=nasdaqFile, dbName=dbname, write='cloud')
    #store=crawl.getSymbolData('msft',symbolUrl)
    
    crawl.closeDriver()
    return df

#crawl=crawler()
#df=crawl.getNasdaqPrice(url)
#crawl.store(df, fileLoc=nasdaqFile, dbName=dbname, write='cloud')
#store=crawl.getSymbolData('msft',symbolUrl)

#crawl.closeDriver()