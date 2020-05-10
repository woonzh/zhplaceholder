from iexclass import iex

dbname='nasdaqsummary'
iexc=iex()

def updateSymbolList(apiExtract=False):
    symbols=iexc.getSymbolsList(apiExtract)
    df=iexc.updateData(symbols,dbname=dbname, idCol='symbol')
    iexc.store(df,ref='symbols', dbname=dbname)
    
    return df

def updateKeyStats(start=0, end=0):
    print('usstocks - start:%s / end:%s'%(start,end))
    df=iexc.getAllKeyStats(dbname=dbname, start=int(start), end=int(end))
    
    return df

def updateQuote(start=0, end=0):
    print('usstocks - start:%s / end:%s'%(start,end))
    df=iexc.getAllStockQuotes(dbname=dbname, start=int(start), end=int(end))
    return df

def test(start=-1, end=-1):
    print('start:%s / end:%s'%(start,end))


#data=iexc.getKeyStats('CYCN')
#symbols=updateSymbolList()
#keyStats=updateKeyStats()
    
#quotes=updateQuote()