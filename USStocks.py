from iexclass import iex

dbname='nasdaqsummary'
iexc=iex()

def updateSymbolList(apiExtract=False):
    symbols=iexc.getSymbolsList(apiExtract)
    df=iexc.updateData(symbols,dbname=dbname, idCol='symbol')
    iexc.store(df,ref='symbols', dbname=dbname)
    
    return df

def updateKeyStats():
    df=iexc.getAllKeyStats(dbname=dbname)
    
    return df

def updateQuote():
    df=iexc.getAllStockQuotes(dbname=dbname)
    return df

def test(start=-1, end=-1):
    print('start:%s / end:%s'%(start,end))
    
#symbols=updateSymbolList()
#keyStats=updateKeyStats()
    
#quotes=updateQuote()