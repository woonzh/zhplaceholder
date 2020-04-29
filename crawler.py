import util
from selenium import webdriver
from selenium.webdriver import ActionChains
import os
import time
import pandas as pd
import dbConnector as db

class crawler:
    def __init__(self, local=False, userAgentNum=0):
        print('crawler -%s'%(str(userAgentNum)))
        if local:
            self.version='windows'
            self.host='local' 
        else:
            self.version='linux'
            self.host='cloud'
        self.cloud=(self.host=='cloud')
        self.batchUpload=10
        
        self.timec=util.timeClass()
        self.timec.startTimer()
        self.dateStr=self.timec.getCurDate(cloud=self.cloud)
        self.dbBatch=50
        
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
            'yearlow':'',
            'volume':'',
            'dayhigh':'',
            'daylow':'',
            'suspended':""".//td[@class="code"]"""}
        
        self.nasdaqDetailsHeaders={
            'market_cap':'Market Cap', 
            'sector':'Sector', 
            'industry':'Industry',
            'pe_ratio':'P/E Ratio', 
            'forward_pe':'Forward P/E 1 Yr.',
            'one_yr_tar':'1 Year Target',
            'eps':'Earnings Per Share(EPS)',
            'day_high_low':"Today's High/Low",
            'div':"Annualized Dividend",
            'yield':'Current Yield',
            'ex_div_date':'Ex Dividend Date',
            'div_pay_date':'Dividend Pay Date',
            'fifty_day_vol':'50 Day Average Vol.',
            'year_high_low':'52 Week High/Low',
            'beta':'Beta'}
        
        self.chromepath=""
        if self.version =='windows':
            self.chromepath='chromedriver/chromedriver.exe'
        else:
            self.chromepath='chromedriver(linux)/chromedriver'
        
        if self.host == 'local':
            self.capabilities = webdriver.DesiredCapabilities.CHROME
            self.options=webdriver.ChromeOptions()
            self.options.add_argument('--headless')
            self.options.add_argument('--window-size=1920,1080')
            self.options.add_argument("--start-maximized")
            self.options.add_argument("--lang=en-us")
            self.options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36")
        else:
            self.GOOGLE_CHROME_BIN=os.environ.get('GOOGLE_CHROME_BIN', None)
            self.CHROMEDRIVER_PATH=os.environ.get('CHROMEDRIVER_PATH', None)
            self.useragentlist=['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',\
                                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',\
                                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36',\
                                'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',\
                                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',\
                                'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36',\
                                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',\
                                'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36']
            self.useragent=self.useragentlist[int(userAgentNum)]
            self.chrome_options = webdriver.ChromeOptions()
            self.chrome_options.binary_location = self.GOOGLE_CHROME_BIN
            self.chrome_options.add_argument('--headless')
            self.chrome_options.add_argument('--disable-gpu')
            self.chrome_options.add_argument('--no-sandbox')
            self.chrome_options.add_argument("--lang=en-us")
            self.chrome_options.add_argument('--disable-dev-shm-usage')
#            self.chrome_options.add_argument("--start-maximized")
            self.chrome_options.add_argument("--user-agent=%s"%(self.useragent))
        
        self.startDriver()
            
    def startDriver(self, url=None):
        if self.host=='local':
            self.driver = webdriver.Chrome(executable_path=self.chromepath, chrome_options=self.options)
        else:
            self.driver = webdriver.Chrome(executable_path=self.CHROMEDRIVER_PATH, chrome_options=self.chrome_options)
        
        self.actions = ActionChains(self.driver)
        self.driver.maximize_window()
        
        if url is not None:
            self.urlDirect(url)
            
    def urlDirect(self, url):
        self.driver.get(url)
        time.sleep(3)
        source=self.driver.page_source[:300]
        print(source)
        if 'Access Denied' in source:
            return False
        else:
            return True
        
###get nasdaq price
    
    def closeCookies(self):
        buttonlinks=["""//button[@id="_evh-ric-c"]""","""//button[@id="_evidon-decline-button"]""",\
                     """//button[@id="_evidon-accept-button"]""","""//svg[@class="evidon-banner-icon"]"""]
        cont=True
        for link in buttonlinks:
            try:
                but=self.driver.find_element_by_xpath(link)
                but.click()
                cont=False
            except:
                t=1
        
        time.sleep(2)
        
        if cont:
            print('close cookies failed')
        else:
            print('closed cookies')
            
    def convertNums(self, currency):
        currency=str(currency).replace('$','').replace(',','').replace('%','')
        try:
            currency=float(currency)
        except:
            currency=0
            
        return currency
    
    def updateCol(self, ratio, colLst, suffix):
        newColLst=[]
        
        for count, itm in enumerate(colLst):
            try:
                ratioVal=ratio[count]
                if ratioVal!=0:
                    newItm=round(itm*ratio[count],2)
                else:
                    newItm=0
            except:
                newItm=0
                
            newItm=str(newItm)+suffix
            newColLst.append(newItm)
        
        return newColLst
            
            
    def updateDetails(self, summary, detailDbName):
        df=db.extractTable(detailDbName)['result']
#        df=detailDbName
        
        oldPrice=[self.convertNums(x) for x in df['price']]
        newPrice=[self.convertNums(x) for x in summary['price']]
        
        ratio=[x/y if (x!=0 and y!=0) else 0 for x,y in zip(newPrice, oldPrice)]
        
        cols_to_update={
            'pe_ratio':[ratio,''],
            'forward_pe':[ratio,''],
            'yield':[[1/x if x!=0 else 0 for x in ratio],'%']}
        
        for col in cols_to_update:
            colLst=[self.convertNums(x) for x in df[col]]
            df[col]=self.updateCol(cols_to_update[col][0],colLst,cols_to_update[col][1])
        
        for col in list(summary):
            df[col]=summary[col]
        
        return df
    
    def getNasdaqData(self,df):
        time.sleep(1)        
        self.closeCookies()
        
        ele=self.driver.find_element_by_xpath("""//div[@class="featured-symbols__header"]""")
        self.actions.move_to_element(ele).perform()
        time.sleep(1)
        
        rows=self.driver.find_elements_by_xpath("""//tr[@class="symbol-screener__row"]""")
        for row in rows:
            lst=[]
            for ind in self.data:
                if len(self.data[ind])>0:
                    try:
                        path=self.data[ind][0]
                        lst.append(row.find_element_by_xpath(path).text)
                    except:
                        try:
                            path=self.data[ind][1]
                            lst.append(row.find_element_by_xpath(path).text)
                        except:
                            lst.append('--')
                else:
                    lst.append(self.timec.getCurDate())
            df.loc[len(df)]=lst
            
        df.drop_duplicates(subset='symbol', inplace=True)
        
        return df
    
    def getNasdaqPrice(self, dbname,url=None, addRemainCols=False):
        self.data={
            'symbol':[""".//th[@class="symbol-screener__cell symbol-screener__cell--ticker"]"""],
            'company':[""".//td[@class="symbol-screener__cell symbol-screener__cell--company"]"""],
            'price':[""".//td[@class="symbol-screener__cell"]"""],
            'change':[""".//td[@class="symbol-screener__cell symbol-screener__cell--change--up"]""",\
                      """.//td[@class="symbol-screener__cell symbol-screener__cell--change--down"]"""],
            'percen_change':[""".//td[@class="symbol-screener__cell symbol-screener__cell--percent--up"]""",\
                             """.//td[@class="symbol-screener__cell symbol-screener__cell--percent--down"]"""], 
            'volume':[""".//td[@class="symbol-screener__cell symbol-screener__cell--volume"]"""],
            'date_updated':[]
                }
        df=pd.DataFrame(columns=list(self.data))
        
        self.urlDirect(url)
        time.sleep(3)
        
#        self.closeCookies()
        
        cont=True
        count=0
        
        while cont==True:
            try:
                df=self.getNasdaqData(df)
                nextBut=self.driver.find_element_by_xpath("""//li[@class="next"]//a""")
                nextBut.click()
                time.sleep(3)
                print('success')
            except:
                print('fail')
                cont=False
            count+=1
            self.timec.getTimeSplit('%s-%s data'%(str(count),len(df)))
            
            if count%self.dbBatch==0 and count > 1:
                self.store(df,dbName=dbname)
        
        if addRemainCols:
            for col in self.nasdaqDetailsHeaders:
                df[col]=['']*len(df)
            
        return df
    
    def getNasdaqDetails(self, url, df=None, dbName=None):
        if df is None:
            df=db.extractTable(dbName)['result']
        else:
            df=df.copy(deep=True)
            
        indexes=list(df.index)
        
#        indexes=[0,1,2]
        
        print('scraping details-%s'%(str(len(indexes))))
        
        for count, ind in enumerate(indexes):
            row=df.loc[ind]
            symbol=row['symbol']
            try:
                success, data=self.getSymbolData(symbol,url)
                if success==False:
                    return success, None
            except:
                print('get data failed - %s'%(symbol))
                data={}
            for itm in data:
                row[itm]=data[itm]
            
#            print(row)
            df.loc[ind]=list(row)
            self.timec.getTimeSplit('%s-%s data'%(str(count),symbol))
            
            if count%self.dbBatch==0 and count >1:
                self.store(df,dbName=dbName)
        return df
    
    def getSymbolData(self, symbol, url):
        url=url%(symbol)
        
        try:
            success=self.urlDirect(url)
            if success==False:
                return success, None
            time.sleep(2)
            self.closeCookies()
            time.sleep(3)
        except:
            print(Exception)
            print('unable to get url')
            return False, {}
        
        headers={}
        for itm in self.nasdaqDetailsHeaders:
            headers[self.nasdaqDetailsHeaders[itm]]=itm
        
        headerLst=list(headers)
        
        store={}
        for itm in self.nasdaqDetailsHeaders:
            store[itm]=''
            
        cont=True
        count=0
        
        while cont and count < 10:
            self.driver.execute_script("window.scrollBy(0,200)")
            time.sleep(1)
            modules=self.driver.find_elements_by_xpath("""//h2[@class="module-header"]""")
            for mod in modules:
                if 'Key Data' in mod.text:
                    cont=False
            count+=1
        self.driver.execute_script("window.scrollBy(0,500)")
        time.sleep(2)
            
#        modules=self.driver.find_elements_by_xpath("""//h2[@class="module-header"]""")
#        check=False
#        for mod in modules:
#            if 'Key Data' in mod.text:
#                check=True
#                
        if cont==False:
            print('navigate to key data success')
        else:
            print('navigate to key data fail')
        time.sleep(6)
        
        rows=self.driver.find_elements_by_xpath("""//tr[@class="summary-data__row"]""")
        print('rows-%s'%(str(len(rows))))
        count=0
        for row in rows:
            header=row.find_element_by_xpath(""".//td[@class="summary-data__cellheading"]""").text
            data=row.find_element_by_xpath(""".//td[@class="summary-data__cell"]""").text
            if header in headerLst:
                store[headers[header]]=data
                count+=1
        
        print(store)
        
        return success, store
        
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
        lst['volume']=self.driver.find_element_by_class_name('col_volume').text
        lst['dayhigh']=self.driver.find_element_by_class_name('col_high').text
        lst['daylow']=self.driver.find_element_by_class_name('col_low').text
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
                        itm=row.find_element_by_xpath(self.subClassNames[sub]).get_attribute('innerText')
                        if sub=='code' and 'suspended' in itm:
                            itm=itm.split('\n')[1]
                        if sub=='suspended':
                            if 'suspended' in itm:
                                itm='Y'
                            else:
                                itm='N'
                        lst.append(itm)                                
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
        
        df['date_update']=[self.timec.getCurDate(cloud=self.cloud)]*len(df)
        self.timec.stopTime()
        self.df=df
        
        return df
    
    def store(self, df, fileLoc=None, dbName=None, write='local'):
#        print(write)
        if self.host=='local' and write=='local':
            df.to_csv(fileLoc, index=False)
        else:
#            print('db')
            db.recreateTable(dbName, df)
            db.rewriteTable(dbName, df)
    
    def closeDriver(self):
        self.driver.quit()