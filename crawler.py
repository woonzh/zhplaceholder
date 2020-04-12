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
    
    def crawlHKEXDetails(self, url):
        self.urlDirect(url)
        lst=[]
        lst.append(self.driver.find_element_by_class_name("col_high52").text)
        lst.append(self.driver.find_element_by_class_name("col_low52").text)
        return lst
    
    def getHKEXDetails(self, df=None, dbname=None):
        newUrl='https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote?sym=symbol&sc_lang=en'
        if df is None:
            df=db.extractTable(dbname)['result']
        
        symbols=df['code']
        for count, symbol in enumerate(symbols):
            try:
                url=newUrl.replace('symbol',symbol)
                newData=self.crawlHKEXDetails(url)
                df.loc[count]=list(df.loc[count])[:-len(newData)]+newData
                self.timec.getTimeSplit('%s-%s data'%(str(count),symbol))
            except:
                t=1
            
            if count !=0 and count%20 == 0:
                self.store(df,None,dbname)
        
        self.df=df
        
        return df
            
    def crawlHKEXSummary(self):
        self.timec.startTimer()
        eleLoc=0
        count=0
        ele=self.driver.find_element_by_class_name("load")
        print(ele.location)
        
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