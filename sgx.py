# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By

import time
import pandas as pd
import os
import argparse
import util
import json

import dbConnector as db
#import analysis

version='linux'
host='cloud'
cloud=(host=='cloud')
batchUpload=10

timec=util.timeClass()
timec.startTimer()

chromepath=""
if version =='windows':
    chromepath='chromedriver/chromedriver.exe'
else:
    chromepath='chromedriver(linux)/chromedriver'

if host == 'local':
    capabilities = webdriver.DesiredCapabilities.CHROME
    options=webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('log-level=3')
    driver = webdriver.Chrome(chromepath, chrome_options=options)
else:
    GOOGLE_CHROME_BIN=os.environ.get('GOOGLE_CHROME_BIN', None)
    CHROMEDRIVER_PATH=os.environ.get('CHROMEDRIVER_PATH', None)
    
    chrome_options = webdriver.ChromeOptions()
    chrome_options.binary_location = GOOGLE_CHROME_BIN
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    
    driver = webdriver.Chrome(executable_path=CHROMEDRIVER_PATH, chrome_options=chrome_options)

driver.maximize_window()
actionChains = ActionChains(driver)

mainURL="https://www.sgx.com/securities/securities-prices?code=all"
summaryFName='data/summary.csv'
companyInfoFName='data/companyInfo.csv'
companyUpdatedInfoFName='data/companyInfo(updated).csv'
infoLogs='data/logs/companyInfo_'
priceHistFName='data/priceHist.csv'
dragIndex=2
clickCount=5
maxSummaryTries=15
overwrite={
    'financial_info': 'text'
        }

def retrieveText(lst, attribute="innerText"):
    store=[]
    for i in lst:
        string=i.get_attribute(attribute)
#        if attribute=="href":
#            string=mainURL+string
        store.append(string)
    
    return store

def extractData(df2):
    global lst
    
    names=driver.find_elements_by_class_name('sgx-table-cell-security-name')
    lastPrice=driver.find_elements_by_xpath('//sgx-table-cell-number[@data-column-id="lt"]')
    vol=driver.find_elements_by_xpath("""//sgx-table-cell-number[@data-column-id="vl"]""")
    valTraded=driver.find_elements_by_xpath('//sgx-table-cell-number[@data-column-id="v"]')
    changeVal=driver.find_elements_by_xpath('//sgx-table-cell-number[@data-column-id="c"]')
    changePercen=driver.find_elements_by_xpath('//sgx-table-cell-number[@data-column-id="p"]')
    dayHigh=driver.find_elements_by_xpath('//sgx-table-cell-number[@data-column-id="h"]')
    dayLow=driver.find_elements_by_xpath('//sgx-table-cell-number[@data-column-id="l"]')
    lst=vol
    
    df=pd.DataFrame()
    df['names']=retrieveText(names)
    df['last price']=retrieveText(lastPrice)
    df['change']=retrieveText(changeVal)
    df['changePercen']=retrieveText(changePercen)
    df['vol']=retrieveText(vol)
    df['valTraded']=retrieveText(valTraded)
    df['dayHigh']=retrieveText(dayHigh)
    df['dayLow']=retrieveText(dayLow)
    df['address']=retrieveText(names, "href")
    df['update_date']=timec.getCurDate()
    
    df2=df2.append(df)
    df2=df2[df2['names']!='']
    
    df2.drop_duplicates(subset ="names", keep = 'first', inplace = True)
    
    return df2, df

def closeAlerts():
    try:
        driver.find_element_by_xpath("//button[text()='OK']").click()
    except:
        print('cannot click ok')
    time.sleep(0.1)
    driver.execute_script("window.scrollBy(0,450)")
    time.sleep(0.5)
    try:
        driver.find_element_by_xpath("""//button[@data-i18n="sgx-consent-banner.accept-button"]""").click()
    except:
        print('cannot accept cookies')
    
def crawlSummary():
    global dragIndex
    global maxSummaryTries
    driver.get(mainURL)
    time.sleep(3)
    closeAlerts()
    time.sleep(2)
#    driver.execute_script("document.body.style.zoom='70%'")
    time.sleep(1)
    
    lst=[]
    df=pd.DataFrame(columns=['names', 'last price', 'change','changePercen','vol', 'valTraded', 'dayHigh','dayLow','address', 'update_date'])
    
    df, df2 = extractData(df)
    lst.append(df2)
    
#    option=driver.find_element_by_class_name("vertical-scrolling-bar")
    downBut=driver.find_element_by_xpath("""//div[@class="sgx-scroll-shield-button--down sgx-icon--before"]""")
    
    cont=True
    curCount=0
    consecSameCount=0
    count=0
    
    try:
        while cont==True:
            count+=1
            print(count)
            
            #org 15
            for ind in range(clickCount):
                downBut.click()
                time.sleep(0.1)
#            actionChains.click_and_hold(option).move_by_offset(0,dragIndex).release().perform()
            if host=='cloud':
                time.sleep(2)
            else:
                time.sleep(0.2)
            
        #    new_height = driver.execute_script("return arguments[0].scrollHeight", cont)
        #    print(new_height)
            df, df2 = extractData(df)
            lst.append(df2)
            print('df length: %s'%(str(len(df))))
            
            if len(df)==curCount:
                consecSameCount+=1
            else:
                consecSameCount=0
            
            if consecSameCount >=maxSummaryTries:
                cont=False
            
            curCount=len(df)
    except:
        print("stopped at: %s, size: %s" %(str(count), str(len(df))))
        print(len(df))
        print(len(df2))
        
    df.drop_duplicates(subset ="names", keep = 'first', inplace = True)
    
    df.set_index(pd.Series(list(range(0,len(df)))))
    
    return df, lst

def processData(df):
    vals=pd.DataFrame()
    vals['names']=df['names']
    vals['price']=df['last price']
    
    for i in range(len(vals)):
        try:
            a=float(vals.iloc[i,1])
        except:
            vals.iloc[i,1]=0
    return vals

def processString(info):        
    if info.find('%')==-1 and info!='-':
        symbols=['mm', 'S$', ' ', '(', ')', ',']
        newStr=info
        for i in symbols:
            newStr=newStr.replace(i, '')
            
        try:
            newStr=float(newStr)
        except:
            print('conversion error "%s"'%(info))
            return info
        
        if info.find('mm')>-1:
            newStr=newStr*(10**6)
        
        if info.find('(')>-1:
            newStr=-newStr
    else:
        return info
    
    return newStr

def getCompanyInfo(name, url):
    driver.get(url)

    time.sleep(1)
    #
    try:
        driver.find_element_by_xpath("//button[text()='OK']").click()
    except:
        print('click ok failed')
    
    time.sleep(1)
    
    try:
        driver.find_element_by_xpath("//button[text()='Accept Cookies']").click()
    except:
        print('click accept cookies failed')
    #
    time.sleep(2)
    #
    driver.execute_script("window.scrollBy(0,700)")
    time.sleep(3)
#    return driver
    #
#    driver.switch_to.frame(driver.find_element_by_tag_name("iframe"))
##basic info
    headerDict={
        'openPrice':'Previous Open Price',
        'high_low':'Previous Day High/Low',
        'close':'Previous Close',
        'prevCloseDate':'Previous Close Date',
        'yearHighLow':'52 Week High/Low',
        'beta':'5-Year Beta',
        'marketCap':'Total Market Cap',
        'sharesOutstanding':'Shares Outstanding',
        'p_float':'Float',
        'avgVolume':'Average 3-month Volume',
        'normalizedEPS':'Normalised Diluted EPS',
        'mthvwap':'Average 3-month Volume',
        'unadjVWAP':'Unadj. 6-month VWAP',
        'adjVWAP':'Adj. 6-month VWAP',
        'peratio':'P/E RatioThis ratio is calculated by dividing the current Price by the sum of the Diluted Earnings Per Share including discontinued operations AFTER Extraordinary Items and Accounting Changes over the last four interim periods.',
        'price_Sales':'Price/SalesPrice divided by the sales per share for the trailing 12 months.',
        'price_CF':'Price/CFPrice divided by cash flow per share for the trailing 12 months.',
        'pricebookvalue':'Price/Book ValueThis is the Current Price divided by the latest interim period Book Value Per Share.',
        'dividend':'Dividend YieldThis value is the current percentage dividend yield based on the present cash dividend rate. It is calculated as the Indicated Annual Dividend divided by the current Price, multiplied by 100.',
        'divident_5_yr_avg':'Dividend Yield 5-yr avgAverage of the dividend yield over the last 60 months',
        'shortDebt':'Net DebtSum of all short term debt, notes payable, long term debt and preferred equity minus the total cash and equivalents and short term investments for the most recent interim period. Only reported for industrial, utility and banking companies.',
        'totalDebt':'Total Debt',
        'long term debt_equity':'Long Term Debt / Equity',
        'enterpriseValue':'Enterprise Value',
        'assets':'Total Assets',
        'cash':'Cash & Short Term Investments',
        'roe':'Return on Equity (ROE)This value is calculated as the Income Available to Common Stockholders for the most recent fiscal year divided by the Average Common Equity and is expressed as a percentage. Average Common Equity is the average of the Common Equity at the beginning and the end of the year.',
        'roa':'Return on Assets (ROA)Income after tax for the trailing 12 months divided by the average total assets.',
        'capEx':'CapExCapital expenditures (CapEx) refer to funds that are used by a company for the purchase, improvement, or maintenance of long-term assets to improve the efficiency or capacity of the company.',
        'EBIT':'',
        'revenue':'Total RevenueThis is the sum of all revenue (sales) reported for all operating divisions for the most recent trailing 12 month period.',
        'operating_income': 'Operating Income',
        'operating_margin':'Operating MarginPercent of revenue remaining after paying all operating expenses for the trailing 12 months.',
        'netincome':'Net Income',
        'net_profit_margin':'Net Profit MarginReturn on sales for the trailing 12 months.',
        'revenue_per_share_5_yr_growth':'Revenue/share 5 yr growthCompound annual growth rate of revenues per share over the last 5 years.',
        'eps_per_share_5_yr_growth':'EPS 5 yr growthCompound annual growth rate of earnings per share excluding extraordinary items and discontinued operations over the last 5 years.',
        'ebita':"EBITDAEarnings Before Interest, Taxes for the trailing 12 months plus the same period's Depreciation and Amortization expenses from the Statement of Cash Flows."
            }
    
    cont=True
    counter=0
    while cont==True:
        try:
            info=driver.find_element_by_xpath("""//div[@data-tabs-data="overview"]""").find_element_by_xpath("""//div[@class="sgx-content-table-scroll-container"]""").find_element_by_xpath("""//tbody""")
            th=info.find_elements_by_xpath("""//th""")
            td=info.find_elements_by_xpath("""//td[@class="sgx-content-table-cell--right-align"]""")
            cont=False
        except:
            if counter<4:
                driver.execute_script("window.scrollBy(0,100)")
                time.sleep(2)
                counter+=1
            else:
                df=pd.DataFrame()
                df['names']=[name]
                for i in headerDict:
                    df[i]=['-']
                return df
    
    th2=[]
    for i in th:
        text=i.get_attribute('innerText')
        if text!='':
            texts=text.split('\n')
            th2.append(texts[0])
    
    td2=[]
    for i in td:
        text=i.get_attribute('innerText')
        if text!='':
            td2.append(text)
            
#    return th2,td2
            
    
    df=pd.DataFrame()
    df['names']=[name]
    
    for i in headerDict:
        header=headerDict[i]
        df[i]=['-']
        found=False
        count=0
        while found == False and count<len(th2):
            if th2[count]==header:
                df[i]=[td2[count]]
                del th2[count]
                del td2[count]
                found=True
            count+=1
    
    generalDict={
        'industry':"""//span[@class="widget-stocks-header-tags-container"]//span[@class="website-tag"]"""#, """//span[@class="website-tag"]"""]
            }
    
    for i in generalDict:
        info=driver.find_elements_by_xpath(generalDict[i])
#        print(info)
        store=''
        for j in range(len(info)):
            store+=info[j].get_attribute('innerText')
            store+=' / '
    
        df[i]=store
        
##financial info
    try:
        time.sleep(1)
        cont=True
        counter=0
        while cont and counter <8:
            try:
                ele=driver.find_element_by_xpath("""//span[@class="sgx-accordion-expandAll-btn"]""")
                time.sleep(1)
                ele.click()
                cont=False
            except:
                counter+=1
                driver.execute_script("window.scrollBy(0,300)")
                time.sleep(1)
        
        driver.execute_script("window.scrollBy(0,300)")
        print("%s-%s"%(cont, counter))
        
        store={}
        if cont==False:
            tables=driver.find_elements_by_xpath("""//table[@class="website-content-table"]""")
            for table in tables:
                try:
                    if table.text!='':
                        header=table.find_element_by_xpath(""".//thead""")
                        headerEle=header.find_elements_by_xpath(""".//th""")
                        headerEleTitle=headerEle[0].text
                        if headerEleTitle!='':
                            store[headerEleTitle]=[x.text for x in headerEle][1:]
                        
                        contents=table.find_element_by_xpath(""".//tbody""")
                        rows=contents.find_elements_by_xpath(""".//tr""")
                        
                        for row in rows:
                            rowHeader=row.find_element_by_xpath(""".//th""").text
    #                        print(rowHeader)
                            rowContent=row.find_elements_by_xpath(""".//td""")
                            if rowHeader!='':
                                store[rowHeader]=[x.text for x in rowContent]
                except:
                    print("read one table error")
        
        df['financial_info']=json.dumps(store)
    except:
        df['financial_info']=''
        print("click expand error")
    
    return df

def getTime(prev):
    cur=time.time()
    return cur, str((time.time()-prev)/60)

def collateCompanyInfo(comList, fname=[companyInfoFName], start=0, host=host, batchUpload=batchUpload):
    if start!=0:
        if host=='local':
            store=pd.read_csv(fname)
        else:
            result=db.extractTable('rawData')
            if result['error'] is not None:
                start=0
                store=''
            else:
                store=result['result']
    else:
        store=''
    
    uploadTrack=0
    print("start:", start)
    for i in range(start, len(comList)):
        name=comList.iloc[i,0]
        url=comList.iloc[i,4]
        companyinfo=getCompanyInfo(name, url)
        if i ==0:
            store=companyinfo
            print(store)
            if host=='cloud':
                db.recreateTable('rawData', store, overwrite=overwrite)
        else:
            store.loc[i]=companyinfo.loc[0]
            print(store.loc[i])
            print(len(comList), start, i)
        
        if host=='local':
            for name in fname:
                store.to_csv(name, index=False)
        else:
            if (i%batchUpload)==batchUpload-1:
                df=store.loc[list(range(i-batchUpload+1, i+1))]
                db.rewriteTable('rawData', df)
                uploadTrack=i+1
        timec.getTimeSplit('%s extracted:'%(name))
        
    if host=='cloud':
        df=store.loc[list(range(uploadTrack, i+1))]
        if len(df)>0:
            db.rewriteTable('rawData', df)
    
    return store

def extractSummary(fname, store=False, dbName=None):   
    df, df2=crawlSummary()
    df=df.reset_index(drop=True)
    if host!='cloud':
        df.to_csv(summaryFName, index=False)
        
    if store:
        db.recreateTable(dbName, df)
        db.rewriteTable(dbName, df)
        
    timec.getTimeSplit('summary extracted')
    return df, df2

def getFullDetails(index=0, summaryBool=False, host=host, intJobId=''):
    dbName='summary'
    if summaryBool==False:
        if host=='cloud':
            result=db.extractTable(dbName)
            if result['error'] is not None:
                df, df2=extractSummary(summaryFName)
                db.recreateTable(dbName, df)
                db.rewriteTable(dbName, df)
            else:
                df=result['result']
        else:
            try:
                df=pd.read_csv(summaryFName)
            except:
                df, df2=extractSummary(summaryFName)
    else:
        df, df2=extractSummary(summaryFName)
        if host=='cloud':
            db.recreateTable(dbName, df)
            db.rewriteTable(dbName, df)
        print('done updating summary')
    
    
#    df=df.loc[list(range(0,1))]
#    print(df)
    companyFullInfo=collateCompanyInfo(df, start=index, host=host)
#    results=analysis.cleanAndProcess(infoName=companyInfoFName)
    timec.stopTime()
    if intJobId != '':
        util.updateJobStatus(intJobId=intJobId)
    return df, companyFullInfo
    
def updatePriceHist(df, companyFullInfo,updateDatabase=True):
    tdayDate=util.currentDate()
    if updateDatabase:
        db.recreateTable('summary', df)
        db.rewriteTable('summary', df)
        db.recreateTable('rawData', companyFullInfo, overwrite=overwrite)
        db.rewriteTable('rawData', companyFullInfo)
#        result=db.extractTable('history')
#        
#        if result['error'] is None:
#            hist=result['result']
#            
#            temLst=['']*len(hist)
#            nameLst=list(hist['names'])
#            for count, name in enumerate(df['names']):
#                if name in nameLst:
#                    rowCount=nameLst.index(name)
#                    temLst[rowCount]=df['last price'][count]
#                else:
#                    hist.loc[len(hist)]=[name]+['']*(len(list(hist))-1)
#                    temLst.append(df['last price'][count])
#            hist[tdayDate]=temLst
#             
#        else:
#            hist=pd.DataFrame()
#            hist['names']=df['names']
#            hist[tdayDate]=df['last price']
#        
#        db.rewriteTable('history', hist, True)
    #update local csv
    t=1
        
def isInt(val):
    try:
        a=float(val)
        return True
    except:
        return False
    
def cleanCurrency(itm):
#    print(itm)
    itm=itm.strip().split(' ')
    if len(itm)>1:
        itm=itm[1]
    else:
        itm=itm[0]
    
    itm=itm.replace(',','').replace('USD','').replace('HKD','').replace('AUD','').strip()
    itm=float(itm)
    return itm
        
def updateRatios(companyInfo):
    ratios=[float(x)/float(y) if (isInt(x) and isInt(y)) else 1 for x, y in zip(companyInfo['last price'], companyInfo['openprice'])]
    colList=['marketcap','peratio','price_sales','price_cf','price_sales', 'dividend', 'divident_5_yr_avg']
    for col in colList:
        companyInfo[col]=[float(x)*float(y) if (isInt(x)==True and isInt(y)==True) else x for x,y in zip(companyInfo[col], ratios)]
    
    year_high=[cleanCurrency(x.split('-')[0]) if (str(x)!= '-' and str(x)!='NaN' and str(x)!='nan') else 0 for x in companyInfo['yearhighlow']]
    year_low=[cleanCurrency(x.split('-')[1]) if (str(x)!= '-' and str(x)!='NaN' and str(x)!='nan') else 0 for x in companyInfo['yearhighlow']]
#    print('done with high low')
    price=[cleanCurrency(x) if ((str(x).strip())!= '﹣' and str(x)!='-' and str(x)!='NaN'and str(x)!='nan') else 0 for x in companyInfo['last price']]
    
    for count in range(len(price)):
        if year_high[count]>0 and year_low[count]>0 and price[count]>0:
            if price[count] > year_high[count]:
                year_high[count]=price[count]
            if price[count] < year_low[count]:
                year_low[count]=price[count]
    
    new_year=[(str(x)+' - '+str(y)) if (x>0 and y>0) else'-' for x,y in zip(year_high,year_low)]
    companyInfo['yearhighlow']=new_year
    
    
    
    return companyInfo
    
def updateCompanyInfo(dragCount=None, sumTries=None, downloadData=True):
    now=timec.getCurDate(cloud=cloud)
    global dragIndex
    global maxSummaryTries
    
#    if dragCount is not None:
#        dragIndex=dragCount
#        
#    if sumTries is not None:
#        maxSummaryTries=sumTries
    
    print('dragIndex: %s, sumTries:%s'%(str(dragIndex),str(maxSummaryTries)))
    
    df,df2=extractSummary(summaryFName)
    
    if downloadData:
        result=db.extractTable('rawData')
        if result is None:
            companyFullInfo=result['result']
        else:
            companyFullInfo=pd.read_csv(companyInfoFName)
    else:
        companyFullInfo=pd.read_csv(companyInfoFName)
        
    companyFullInfo=pd.merge(companyFullInfo, df[['names','last price','change','changePercen','vol', 'valTraded', 'dayHigh','dayLow','update_date']], how='outer', left_on='names', right_on='names')
    companyFullInfo=updateRatios(companyFullInfo)
    
    replace={
        'openprice':'last price',
        'pricechange': 'change',
        'percenchange': 'changePercen',
        'tradedvol': 'vol',
        'tradedval':'valTraded',
        'prevclosedate':'update_date',
        'day_high':'dayHigh',
        'day_low':'dayLow'
            }
    
    for itm in replace:
        companyFullInfo[itm]=companyFullInfo[replace[itm]]
        companyFullInfo.drop(replace[itm], axis=1, inplace=True)

    companyFullInfo['prevclosedate']=[now]*len(companyFullInfo)
    print('done')
    
#    if host=='local':
#        companyFullInfo.to_csv(companyUpdatedInfoFName, index=False)
#        companyFullInfo.to_csv(infoLogs+now+'.csv', index=False)
    
    updatePriceHist(df, companyFullInfo)
    
    return df, companyFullInfo
    
#    results=analysis.cleanAndProcess(infoName=companyUpdatedInfoFName)
    
#    return results

def closeDriver():
    driver.quit()

#df=crawlSummary()

#df, companyFullInfo=updateCompanyInfo()
#closeDriver()