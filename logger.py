import json
import util
from functools import reduce
import pandas as pd
import numpy as np

class logger:
    def __init__(self):
        self.storeLoc='store/store.json'
        self.backUpLoc='store/backup/%s.json'
        self.tables={}
        self.reference={
            'sgx':{
                'metrics':{
                    'openprice':'price',
                    'upside':'upside',
                    'downside':'downside',
                    'dayVolatility':'day_volatility',
                    'weightedDayVolatility':'day_volatility_weighted',
                    'dayVolume':'day_volume',
                    'percenchange':'percenchange',
                    'high_close':'high_close'
                        }
                    },
            'hkex':{
                'metrics':{
                    'price':'price',
                    'upside':'upside',
                    'downside':'downside',
                    'day_volatility':'day_volatility',
                    'day_volatility_weighted':'day_volatility_weighted',
                    'day_perceninc':'day_perceninc'
                        }
                    },
            'nasdaq':{
                'metrics':{
                    'upside':'upside',
                    'downside':'downside'
                        }
                    }
                }
        self.stats=['average', 'sum', '5_day_sum']#, '5_day_avg', '30_day_avg']
        self.load()
        self.timec=util.timeClass()
        self.curDate=self.timec.getCurDateNumeric()
    
    def load(self):
        try:
            with open(self.storeLoc) as rfile:
                self.store=json.load(rfile)
        except:
            self.store={}
            for cat in self.reference:
                self.store[cat]={
                    }
                
    def save(self):
        wfile=open(self.storeLoc,'w')
        wfile.write(json.dumps(self.store))
        wfile.close()
#        with open(self.storeLoc,'w') as wfile:
#            wfile.write(json.dumps(self.store))
        
        backupFile=self.backUpLoc%(self.curDate)        
        jfile=open(backupFile,'w')
        jfile.write(json.dumps(self.store))
        jfile.close()
#        backupFile=self.backUpLoc%(self.curDate)
#        with open(backupFile,'w') as jfile:
#            jfile.write(json.dumps(self.store))
    
    def nonzerocheck(self,lst):
        count=0
        for i in lst:
            if i!=0:
                count+=1
        return round(sum(lst)/count,4)
            
    def calStats(self, xchange):
        storexchange=self.store[xchange]
        for symbol in storexchange:
            storesymbol=storexchange[symbol]
            for metric in storesymbol['metrics']:
                storemetric=storesymbol['metrics'][metric]
                valLst=[storemetric['datedvals'][x] for x in storemetric['datedvals']]
                valLst2=[1+(x/100) if x!=0 else 1 for x in valLst]
                storemetric['stats']={
                        'average':round(float(sum(valLst)/len(valLst)),4),
                        '5_day_avg':round(float(sum(valLst[-5:])/len(valLst[-5:])),4),
                        '30_day_avg':round(float(sum(valLst[-22:])/len(valLst[-22:])),4),
                        'sum': round(float((np.prod(valLst2)-1)*100),4),
                        '5_day_sum':round(float((np.prod(valLst2[-5:])-1)*100),4),
                        '30_day_sum':round(float((np.prod(valLst2[-22:])-1)*100),4)
                        }
                storesymbol['metrics'][metric]=storemetric
            storexchange[symbol]=storesymbol
        self.store[xchange]=storexchange
        return self.store
    
    def compileTable(self, xchange):
        self.timec.startTimer()
        if len(self.store[xchange])>0:
            cols=['symbol','company']
            for metric in self.reference[xchange]['metrics']:
                cols+=[self.reference[xchange]['metrics'][metric]+'_'+x for x in self.stats]
            df=pd.DataFrame(columns=cols)
            df.loc[0]=['']*len(list(df))
#            print(df)
            
            storexchange=self.store[xchange]
            for code in storexchange:
                storecode=storexchange[code]['metrics']
                loc=df.loc[0]
                loc['symbol']=code
                loc['company']=storexchange[code]['company']
                for metric in storecode:
                    storemetric=storecode[metric]
                    stats=storemetric['stats']
                    for stat in stats:
                        if stat in self.stats:
                            loc[metric+'_'+stat]=stats[stat]
#                    print(loc)
                df.loc[len(df)]=list(loc)
#                    if len(df)%100==0:
#                        self.timec.getTimeSplit(str(len(df)))
        self.tables[xchange]=df
        self.timec.stopTime()
        return df
                        
    
    def update(self, exchange, df, symbolCol, companyCol):
        df=df.reset_index(drop=True)
        store=self.store[exchange]
        metrics=self.reference[exchange]['metrics']
        
        for ind in list(df.index):
            row=df.loc[ind]
            symbol=row[symbolCol]
            company=row[companyCol]
            try:
                storeSymbol=store[symbol]
                storeSymbol['company']=company
                storeSymbol=store[symbol]['metrics']
            except:
                store[symbol]={
                    'company':company,
                    'metrics':{}
                        }
                storeSymbol=store[symbol]['metrics']
            
            for metric in metrics:
                metricVal=row[metric]
                metricInd=metrics[metric]
                try:
                    metricDict=storeSymbol[metricInd]
                except:
                    storeSymbol[metricInd]={
                        'datedvals':{},
                        'stats':{}
                            }
                    metricDict=storeSymbol[metricInd]
                metricDict['datedvals'][self.curDate]=metricVal
                storeSymbol[metricInd]=metricDict
            store[symbol]['metrics']=storeSymbol
            
        self.store[exchange]=store
        return self.store