import util
from selenium import webdriver
from selenium.webdriver import ActionChains
import os
import time
import pandas as pd
import dbConnector as db

class crawler:
    def __init__(self):
        self.version='linux'
        self.host='cloud'
        self.cloud=(self.host=='cloud')
        self.batchUpload=10
        
        self.timec=util.timeClass()
        self.dateStr=self.timec.getCurDate(cloud=self.cloud)
        
        self.subClassNames={
            'code':""".//td[@class="code"]""",
            'com_name':""".//td[@class="name"]""",
            'price':""".//td[@class="price"]//bdo""",
            'upval':""".//td[@class="price"]//div""",
            'turnover':""".//td[@class="turnover"]""",
            'market_cap':""".//td[@class="market"]""",
            'pe':""".//td[@class="pe"]""",
            'dividend':""".//td[@class="dividend"]""",
            'yearhigh':'', 
            'yearlow':''}
        
        self.chromepath=""
        if self.version =='windows':
            self.chromepath='chromedriver/chromedriver.exe'
        else:
            self.chromepath='chromedriver(linux)/chromedriver'
        
        if self.host == 'local':
            self.capabilities = webdriver.DesiredCapabilities.CHROME
            self.options=webdriver.ChromeOptions()
            self.options.add_argument('--headless')
        else:
            self.GOOGLE_CHROME_BIN=os.environ.get('GOOGLE_CHROME_BIN', None)
            self.CHROMEDRIVER_PATH=os.environ.get('CHROMEDRIVER_PATH', None)
            
            self.chrome_options = webdriver.ChromeOptions()
            self.chrome_options.binary_location = self.GOOGLE_CHROME_BIN
            self.chrome_options.add_argument('--headless')
            self.chrome_options.add_argument('--disable-gpu')
            self.chrome_options.add_argument('--no-sandbox')
            
    def startDriver(self, url=None):
        if self.host=='local':
            self.driver = webdriver.Chrome(self.chromepath, chrome_options=self.options)
        else:
            self.driver = webdriver.Chrome(executable_path=self.CHROMEDRIVER_PATH, chrome_options=self.chrome_options)
        
        self.actions = ActionChains(self.driver)
        self.driver.maximize_window()
        
        if url is not None:
            self.urlDirect(url)
            
    def urlDirect(self, url):
        self.driver.get(url)
        time.sleep(3)
        
##update price and highlows
    
    def updatePrice(self,df, dbname):
        orgDf=db.extractTable(dbname)['result']
        
        colLst=['price','code','upval','market_cap','pe','dividend']
        
        for count in range(len(df)):
            code=df['code'][count] 
            filterdf=orgDf[orgDf['code']==code]
            
            if len(filterdf)>0:
                ind=filterdf.index[0]
                for col in colLst:
                    colNum=list(orgDf).index(col)
                    val=df[col][count]
                    orgDf.iloc[ind,colNum]=val

#missing high low update
                
                dateColNum=list(orgDf).index('date_update')
                orgDf.iloc[ind,dateColNum]=self.dateStr
            else:                   
#                high_lows=self.crawlHKEXDetails(code)
#                for data in high_lows:
#                    filterdf[data]=[high_lows[data]]
                
                lst=list(df.loc[count])
                orgDf.loc[len(orgDf)]=lst
        
        return orgDf
    
    def updateHighLow(self,df):
        df=df.copy(deep=True)
        data={
            'price':df['price'],
            'yearhigh':df['yearhigh'],
            'yearlow':df['yearlow']
            }
        
        for d in data:
            lst=[]
            lst2=[]
            for x in data[d]:
                try:
                    lst.append(float(x[3:]))
                    lst2.append(x)
                except:
                    lst.append(0)
                    lst2.append('-')
            df[d]=lst2
            data[d]=lst
        
        for i in range(len(data['price'])):
            price=data['price'][i]
            high=data['yearhigh'][i]
            low=data['yearlow'][i]
            
            priceInd=list(df).index('price')
            highInd=list(df).index('yearhigh')
            lowInd=list(df).index('yearlow')
            
            if price > high:
                df.iloc[i,highInd]=df.iloc[i,priceInd]
            
            if price <low:
                df.iloc[i,lowInd]=df.iloc[i,priceInd]
        
        return df

#crawl details
    
    def crawlHKEXDetails(self, symbol):
        newUrl='https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote?sym=symbol&sc_lang=en'
        url=url=newUrl.replace('symbol',symbol)
        self.urlDirect(url)
        lst={}
        lst['yearhigh']=self.driver.find_element_by_class_name("col_high52").text
        lst['yearlow']=self.driver.find_element_by_class_name("col_low52").text
        return lst
    
    def getHKEXDetails(self, df=None, dbname=None):
        if df is None:
            df=db.extractTable(dbname)['result']
        
        symbols=df['code']
        for count, symbol in enumerate(symbols):
            try:
                newData=self.crawlHKEXDetails(symbol)
                print('%s-%s'%(symbol, newData))
                for data in newData:
                    ind=list(df).index(data)
                    df.iloc[count,ind]=newData[data]
                self.timec.getTimeSplit('%s-%s data'%(str(count),symbol))
            except:
                t=1
            
            if count !=0 and count%100 == 0:
                self.store(df,None,dbname)
        
        self.df=df
        
        return df

#Recrawl summary
    
    def extractSum(self, df, rows):        
        for row in rows:
            lst=[]
            for sub in self.subClassNames:
                if self.subClassNames[sub]!='':
                    try:
                        lst.append(row.find_element_by_xpath(self.subClassNames[sub]).get_attribute('innerText'))
                    except:
                        lst.append('')
                else:
                    lst.append('')
            df.loc[len(df)]=lst
        
        return df
            
    def crawlHKEXSummary(self):
        self.timec.startTimer()
        eleLoc=0
        count=0
        ele=self.driver.find_element_by_class_name("load")
        print(ele.location)
        
        cols=[x for x in self.subClassNames]
        df=pd.DataFrame(columns=cols)
        
        while ele.location['y']>eleLoc and count < 1:
            count+=1
            print(count*20)
            eleLoc=ele.location['y']
            self.actions.move_to_element(ele).perform()
            time.sleep(1)
            
            rows=self.driver.find_elements_by_class_name("datarow")
            df=self.extractSum(df,rows[len(df):])
            
            ele.click()
            time.sleep(1)
            
            self.timec.getTimeSplit(str(count))
        
        df['date_update']=[self.timec.getCurDate(cloud=self.cloud)]*len(df)
        self.timec.stopTime()
        self.df=df
        
        return df
    
    def store(self, df, fileLoc=None, dbName=None):
        if self.host=='local':
            df.to_csv(fileLoc, index=False)
        else:
            db.recreateTable(dbName, df)
            db.rewriteTable(dbName, df)
    
    def closeDriver(self):
        self.driver.quit()