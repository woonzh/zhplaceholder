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
        self.batchUpload=10
        
        self.timec=util.timeClass()
        
        self.subClassNames={
            'code':""".//td[@class="code"]""",
            'com_name':""".//td[@class="name"]""",
            'price':""".//td[@class="price"]//bdo""",
            'upval':""".//td[@class="price"]//div[@class="upval"]""",
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
        
##update price
    
    def updatePrice(self,df, updateCount, rows):
        for row in rows:
            code=self.driver.find_element_by_xpath(self.subClassNames['code']).text
            price=self.driver.find_element_by_xpath(self.subClassNames['price']).text
            
            filterdf=df[df['code']==code]
            if len(filterdf)>0:
                ind=filterdf.index[0]
                colNum=list(df).index('price')
                dateColNum=list(df).index('date_update')
                lowColNum=list(df).index('yearlow')
                highColNum=list(df).index('highlow')
                
                if price<df.iloc[ind,lowColNum]:
                    df.iloc[ind,lowColNum]=price
                
                if price>df.iloc[ind,highColNum]:
                    df.iloc[ind,highColNum]=price
                
                df.iloc[ind, colNum]=price
                df.iloc[ind,dateColNum]=self.dateStr
            else:
                for sub in self.subClassNames:
                    val=self.driver.find_element_by_xpath(self.subClassNames[sub]).text
                    filterdf[sub]=[val]
                    
                high_lows=self.crawlHKEXDetails(code)
                for data in high_lows:
                    filterdf[data]=[high_lows[data]]
                
                filterdf['date_update']=[self.dateStr]
                df.loc[len(df)]=list(filterdf)
            
            updateCount+=1
        
        return df, updateCount
    
    def getPriceUpdate(self, df=None, dbname='hksummary'):
        self.timec.startTimer()
        eleLoc=0
        count=0
        ele=self.driver.find_element_by_class_name("load")
        updateCount=0
        
        self.dateStr=self.timec.getCurDate()
        
        if df is None:
            df=db.extractTable(dbname)['result']
        
        while ele.location['y']>eleLoc and count <1:
            count+=1
            print(count*20)
            eleLoc=ele.location['y']
            self.actions.move_to_element(ele).perform()
            time.sleep(1)
            
            rows=self.driver.find_elements_by_class_name("datarow")
            df, updateCount=self.updatePrice(df,updateCount,rows[updateCount:])
            
            ele.click()
            time.sleep(1)
            
            self.timec.getTimeSplit(str(count))
        
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
        
        while ele.location['y']>eleLoc:
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
        
        df['date_update']=[self.timec.getCurDate()]*len(df)
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