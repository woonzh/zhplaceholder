from iexclass import iex
import dbConnector as db
import pandas as pd
import statistics
from datetime import datetime, date
from logger import logger

dbname='nasdaqsummary'
file='data/USStocks.csv'
iexc=iex()
log=logger()

colsToRemove=['exchange','type', 'ttmdividendrate', 'primaryexchange','calculationprice',\
              'opentime', 'opensource','close', 'closetime', 'closesource','hightime', \
              'highsource','lowtime', 'lowsource','latestsource','latestupdate','delayedprice', \
              'delayedpricetime', 'oddlotdelayedprice', 'oddlotdelayedpricetime', 'extendedprice',\
              'extendedchange', 'extendedchangepercent', 'extendedpricetime','iexopen', \
              'iexopentime', 'iexclose', 'iexclosetime', 'ytdchange', 'lasttradetime', \
              'isusmarketopen','companyname','nextdividenddate', 'exdividenddate',\
              'iexrealtimeprice', 'iexrealtimesize', 'iexlastupdated','iexmarketpercent', \
              'iexvolume','iexbidprice', 'iexbidsize', 'iexaskprice', 'iexasksize',\
              'maxchangepercent', 'year5changepercent', 'year2changepercent', \
              'year1changepercent', 'ytdchangepercent', 'month6changepercent', \
              'month3changepercent', 'month1changepercent', 'day30changepercent', \
              'day5changepercent']
viewCols=['name','latestprice','changepercent', 'changepercent_5_day_sum','day_volatility',\
          'day_volatility_average','upside','downside','traded%','traded%_5_day_avg','traded_val',\
          'traded_val_5_day_avg','avg_traded%', 'avg_traded_val', 'dividendyield','latesttime',\
          'marketcap', 'float_percen','next_earnings_daydiff', 'nextearningsdate']

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

def dataEngineer(df):
    df=df.copy(deep=True)
    roundDegree=4
    
    price=df['latestprice']
    yearhigh=df['week52high']
    yearlow=df['week52low']
    marketcap=df['marketcap']
    volume=df['latestvolume']
    dayhigh=df['high']
    daylow=df['low']
    avgVolume=df['avgtotalvolume']
    changePercen=df['changepercent']
    floatCount=df['float']
    sharesOutstanding=df['sharesoutstanding']
    
    df['float_percen']=[round(100*x/y,roundDegree) if x>0 and y>0 else 0 for x, y in zip(floatCount, sharesOutstanding)]
    
    df['upside']=[round(100*(y-x)/x,roundDegree) if x>0 and y>0 else 0 for x,y in zip(price, yearhigh)]
    df['downside']=[round(100*(x-y)/x,roundDegree) if x>0 and y>0 else 0 for x,y in zip(price, yearlow)]
    df['traded%']=[round(100*x/y,roundDegree) if x>0 and y>0 else 0 for x,y in zip(volume,df['float_percen'])]
    df['traded_val']=[x*y if x>0 and y>0 else 0 for x,y in zip(volume,price)]
    df['avg_traded%']=[round(100*x/y,roundDegree) if x>0 and y>0 else 0 for x,y in zip(avgVolume,df['float_percen'])]
    df['avg_traded_val']=[x*y if x>0 and y>0 else 0 for x,y in zip(avgVolume,price)]
    
    df['high_close']=[round(100*(y-x)/x,roundDegree) if x>0 and y>0 else 0 for x,y in zip(price,dayhigh)]
    df['abs_change']=[round(100*(y-x)/x,roundDegree) if x>0 and y>0 else 0 for x,y in zip(price,dayhigh)]
    df['day_volatility']=[round(100*(y-x)/y,roundDegree) if x>0 and y>0 else 0 for x,y in zip(dayhigh, daylow)]
    
    df['changepercent']=[round(100*x,roundDegree) if x!=0 else 0 for x in changePercen]
    df['absChangePercen']=[abs(x) if x!=0 else 0 for x in df['changepercent']]
    
    tday=datetime.now()
    df['next_earnings_daydiff']=[(datetime.strptime(str(x), '%Y-%m-%d')-tday).days if str(x)!='nan' and x!=0 else 0 for x in df['nextearningsdate']]
    
    return df

def cleanCols(df):
    df=df.copy(deep=True)
    for col in colsToRemove:
        df.drop(col,axis=1,inplace=True)
        
    for col in list(df):
        colLst=df[col]
        try:
            colLst=[x if str(x)!='nan' else 0 for x in colLst]
            df[col]=colLst
        except:
            t=1
    
    return df

def colView(df):    
    return df[viewCols]

def checkNum(txt):
    try:
        a=float(txt)
        return True
    except:
        return False

def getStats(df):
    cols=list(df)
    store={}
    
    percen=[0.1,0.25,0.5,0.75,0.9]
    
    for col in cols:
        try:
            temDf=pd.DataFrame()
            temDf['store']=[float(x) if checkNum(x)==True else 0 for x in df[col]]
            tem=list(temDf[temDf['store']>0]['store'])
            tem.sort()
            lst={}
            lst['avg']=statistics.mean(tem)
            for p in percen:
                ind=int(round(p*len(tem),0))
                lst[str(p)]=tem[ind]
            
                store[col]=lst
        except:
            store[col]=''
    
    return store

def runLogger(df, run=False, calStats=False):
    dfNew=df.copy(deep=True)
    if run:
        store=log.update('USStocks',dfNew, 'symbol','name')
        store=log.calStats('USStocks')
        log.save()
    else:
        if calStats:
            store=log.calStats('USStocks')
            log.save()
    table=log.compileTable('USStocks')
    
    dfNew=pd.merge(dfNew, table, how='left', left_on='symbol', right_on='symbol')
    dfNew.drop(['symbol','company'],axis=1, inplace=True)
    
    return dfNew, table

def sieveData(df, filters):
    df=df.copy(deep=True)
    nonullcols=['latestprice','volume']
    
    for col in nonullcols:
        df=df[df[col]>0]
        
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

dayChangeFilter={
    'price':['>',1,'latestprice'],
    'traded%':['>',1,'traded%'],
    'traded_val':['>',5*pow(10,6),'traded_val'],
    'avg_traded%':['>',0.7,'traded%'],
    'avg_traded_val':['>',5*pow(10,6),'avg_traded_val'],
    'absChangePercen':['>',3,'absChangePercen']
        }

upsideFilter={
    'price':['>',1,'latestprice'],
    'traded%':['>',1,'traded%'],
    'traded_val':['>',5*pow(10,6),'traded_val'],
    'avg_traded%':['>',0.7,'traded%'],
    'avg_traded_val':['>',5*pow(10,6),'avg_traded_val'],
    'upside':['>',40,'upside']
        }

weekEarningsFilter={
    'price':['>',1,'latestprice'],
    'traded%':['>',1,'traded%'],
    'traded_val':['>',5*pow(10,6),'traded_val'],
    'avg_traded%':['>',0.7,'traded%'],
    'avg_traded_val':['>',5*pow(10,6),'avg_traded_val'],
    'next_earnings_daydiff':['<',7,'next_earnings_daydiff']
        }

#df=run(False)
#dfClean=cleanCols(df)
#dfEngine=dataEngineer(dfClean)
#dfNew, table=runLogger(dfEngine, run=False, calStats=False)

#dfView=colView(dfNew)
#stats=getStats(dfNew)
#
#dfDayChange=sieveData(dfNew, dayChangeFilter)
#dfDayChangeView=colView(dfDayChange)
#
#dfUpside=sieveData(dfNew, upsideFilter)
#dfUpsideView=colView(dfUpside)
#
#dfWeekEarnings=sieveData(dfNew, weekEarningsFilter)
#dfWeekEarningsView=colView(dfWeekEarnings)

#a=iexc.convertLstToDf(lst)
#a['symbol']=['CYRN','CYRX']
#    
#b=iexc.updateData(df=a,mainDf=results, idCol='symbol')

#data=iexc.getKeyStats('CYCN')
#symbols=updateSymbolList()
#keyStats=updateKeyStats()
    
#quotes=updateQuote()