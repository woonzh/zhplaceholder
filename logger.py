import json
import util
from functools import reduce
import pandas as pd

class logger:
    def __init__(self):
        self.storeLoc='store/store.json'
        self.backUpLoc='store/backup/%s.json'
        self.reference={
            'sgx':{
                'metrics':{
                    'price':'price',
                    'upside':'upside',
                    'downside':'downside'
                        }
                    },
            'hkex':{
                'metrics':{
                    'price':'price',
                    'upside':'upside',
                    'downside':'downside'
                        }
                    },
            'nasdaq':{
                'metrics':{
                    'upside':'upside',
                    'downside':'downside'
                        }
                    }
                }
        self.stats=['average', '5_day_avg', '30_day_avg']
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
        with open(self.storeLoc,'w') as wfile:
            json.dump(self.store, wfile)
            
        backupFile=self.backUpLoc%(self.curDate)
        with open(backupFile,'w') as wfile:
            json.dump(self.store, wfile)
    
    def nonzerocheck(self,lst):
        count=0
        for i in lst:
            if i!=0:
                count+=1
        return round(sum(lst)/count,4)
            
    def calStats(self):
        for xchange in self.store:
            storexchange=self.store[xchange]
            for symbol in storexchange:
                storesymbol=storexchange[symbol]
                for metric in storesymbol['metrics']:
                    storemetric=storesymbol['metrics'][metric]
                    valLst=[storemetric['datedvals'][x] for x in storemetric['datedvals']]
                    storemetric['stats']={
                            'average':round(float(sum(valLst)/len(valLst)),4),
                            '5_day_avg':round(float(sum(valLst[-5:])/len(valLst[-5:])),4),
                            '30_day_avg':round(float(sum(valLst[-22:])/len(valLst[-2:])),4)
                            }
                    storesymbol['metrics'][metric]=storemetric
                storexchange[symbol]=storesymbol
            self.store[xchange]=storexchange
        return self.store
    
    def compileTable(self):
        tables={}
        for xchange in self.store:
            if len(self.store[xchange])>0:
                cols=['symbol','company']
                for metric in self.reference[xchange]['metrics']:
                    cols+=[metric+'_'+x for x in self.stats]
                df=pd.DataFrame(columns=cols)
                df.loc[0]=['sample']+(['']*(len(list(df))-1))
#                print(df)
                
                storexchange=self.store[xchange]
                for code in storexchange:
                    storecode=storexchange[code]['metrics']
                    loc=df[df['symbol']=='sample']
                    loc['symbol']=code
                    loc['company']=storexchange[code]['company']
                    for metric in storecode:
                        storemetric=storecode[metric]
                        stats=storemetric['stats']
                        for stat in stats:
                            loc[metric+'_'+stat]=stats[stat]
#                    print(loc)
                    df.loc[len(df)]=list(loc)
                    if len(df)%10==0:
                        print(len(df))
                tables[xchange]=df
        self.tables=tables
        return tables
                        
    
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