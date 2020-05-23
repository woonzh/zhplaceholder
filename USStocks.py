from iexclass import iex
import dbConnector as db
import pandas as pd

dbname='nasdaqsummary'
file='data/USStocks.csv'
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
    
def run(pullFromDB=False):
    if pullFromDB:
        df=db.extractTable(dbname)['result']
        df.to_csv(file, index=False)
    df=pd.read_csv(file)
    return df

#df=run(True)

#a=iexc.convertLstToDf(lst)
#a['symbol']=['CYRN','CYRX']
#    
#b=iexc.updateData(df=a,mainDf=results, idCol='symbol')

#data=iexc.getKeyStats('CYCN')
#symbols=updateSymbolList()
#keyStats=updateKeyStats()
    
#quotes=updateQuote()