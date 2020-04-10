import util
from selenium import webdriver
from selenium.webdriver import ActionChains
import os
import time
import pandas as pd
import database as db

class crawler:
    def __init__(self):
        self.version='linux'
        self.host='cloud'
        self.batchUpload=10
        
        self.timec=util.timeClass()
        self.timec.startTimer()
        
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
            self.driver.get(url)
            time.sleep(3)
            
    def crawlHKEXSummary(self):
        eleLoc=0
        count=0
        ele=self.driver.find_element_by_class_name("load")
        print(ele.location)
        
        while ele.location['y']>eleLoc:
            count+=1
            print(count*20)
            eleLoc=ele.location['y']
            self.actions.move_to_element(ele).perform()
            time.sleep(1)
            ele.click()
            time.sleep(1)
        
        subClassNames={
                'code':""".//td[@class="code"]""",
                'com_name':""".//td[@class="name"]""",
                'price':""".//td[@class="price"]//bdo""",
                'upval':""".//td[@class="price"]//div[@class="upval"]""",
                'turnover':""".//td[@class="turnover"]""",
                'market_cap':""".//td[@class="market"]""",
                'pe':""".//td[@class="pe"]""",
                'dividend':""".//td[@class="dividend"]"""}
        
        rows=self.driver.find_elements_by_class_name("datarow")
        
        cols=[x for x in subClassNames]
        df=pd.DataFrame(columns=cols)
        
        for row in rows:
            lst=[]
            for sub in subClassNames:
                try:
                    lst.append(row.find_element_by_xpath(subClassNames[sub]).get_attribute('innerText'))
                except:
                    lst.append('')
            df.loc[len(df)]=lst
        
        return rows,df
    
    def store(self, df, fileLoc=None, dbName=None):
        if self.host=='local':
            df.to_csv(fileLoc, index=False)
        else:
            db.recreateTable(dbName, df)
            db.rewriteTable(dbName, df)
    
    def closeDriver(self):
        self.driver.quit()