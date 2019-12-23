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

import dbConnector as db
#import analysis

version='linux'
host='cloud'
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

mainURL="https://www2.sgx.com/securities/securities-prices"
summaryFName='data/summary.csv'
companyInfoFName='data/companyInfo.csv'
companyUpdatedInfoFName='data/companyInfo(updated).csv'
infoLogs='data/logs/companyInfo_'
priceHistFName='data/priceHist.csv'

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
    
    lst=vol
    
    df=pd.DataFrame()
    df['names']=retrieveText(names)
    df['last price']=retrieveText(lastPrice)
    df['vol']=retrieveText(vol)
    df['val traded']=retrieveText(valTraded)
    df['address']=retrieveText(names, "href")
    
    df2=df2.append(df)
    df2=df2[df2['names']!='']
    return df2, df

def closeAlerts():
    try:
        driver.find_element_by_xpath("//button[text()='OK']").click()
    except:
        print('cannot click ok')
    time.sleep(0.1)
    try:
        driver.find_element_by_xpath("//button[text()='Accept Cookies']").click()
    except:
        print('cannot accept cookies')
    driver.execute_script("window.scrollBy(0,300)")
    
def crawlSummary():
    
    driver.get(mainURL)
    time.sleep(1)
    
    closeAlerts()
    
    time.sleep(0.5)
    
    lst=[]
    df=pd.DataFrame(columns=['names', 'last price', 'vol', 'val traded', 'address'])
    
    df, df2 = extractData(df)
    lst.append(df2)
    
    option=driver.find_element_by_class_name("vertical-scrolling-bar")
    
    for j in range(55):#55
        actionChains = ActionChains(driver)
        #org 8
        # 8.5 694
        #9, 10, 12 13 14 15 16 694
        print(j)
        actionChains.click_and_hold(option).move_by_offset(0,16).release().perform()
        time.sleep(0.2)
        
    #    new_height = driver.execute_script("return arguments[0].scrollHeight", cont)
    #    print(new_height)
        df, df2 = extractData(df)
        lst.append(df2)
        
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
        driver.find_element_by_xpath("//button[text()='Accept Cookies']").click()
    except:
        print('click failed')
    #
    time.sleep(2)
    #
    driver.execute_script("window.scrollBy(0,700)")
    time.sleep(2)
#    return driver
    #
#    driver.switch_to.frame(driver.find_element_by_tag_name("iframe"))
    
    headerDict={
        'openPrice':'Previous Open Price',
        'high_low':'Previous Close',
        'close':'Previous Close',
        'prevCloseDate':'Previous Close Date',
        'marketCap':'Total Market Cap',
        'sharesOutstanding':'Shares Outstanding',
        'p_float':'Float',
        'avgVolume':'Average 3-month Volume',
        'normalizedEPS':'Normalised Diluted EPS',
        'mthvwap':'Average 3-month Volume',
        'unadjVWAP':'Unadj. 6-month VWAP',
        'adjVWAP':'Adj. 6-month VWAP',
        'peratio':'P/E Ratio',
        'price_Sales':'Price/Sales',
        'price_CF':'Price/CF',
        'pricebookvalue':'Price/Book Value',
        'dividend':'Dividend Yield',
        'divident_5_yr_avg':'Dividend Yield 5-yr avg',
        'debt':'Net Debt',
        'long term debt_equity':'Long Term Debt / Equity',
        'enterpriseValue':'Enterprise Value',
        'assets':'Total Assets',
        'cash':'Cash & Short Term Investments',
        'roe':'Return on Equity (ROE)',
        'roa':'Return on Assets (ROA)',
        'capEx':'CapEx',
        'EBIT':'',
        'revenue':'Total Revenue',
        'operating_income': 'Operating Income',
        'operating_margin':'Operating Margin',
        'netincome':'Net Income',
        'net_profit_margin':'Net Profit Margin',
        'revenue_per_share_5_yr_growth':'Revenue/share 5 yr growth',
        'eps_per_share_5_yr_growth':'EPS 5 yr growth',
        'roe':'Return on Equity (ROE)',
        'roa':'Return on Assets (ROA)',
        'ebita':''
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
    
    return df
    
    overviewDict={
        'openPrice':"""//td[@data-bind="text: companyInfo.openPrice, formatNonZeroValue: 'dollars'"]""",
        'high_low':"""//span[@data-bind="text: companyInfo.lowPrice != null ? companyInfo.lowPrice : '', format: 'dollars'"]""",
        'close':"""//td[@data-bind="text: companyInfo.previousClosePrice, formatNonZeroValue: 'dollars'"]""",
        'prevCloseDate':"""//td[@data-bind="text: companyInfo.previousCloseDate, format: 'date'"]""",
        'marketCap':"""//td[@data-bind="text: companyInfo.marketCap, formatNonZeroValue: 'millions'"]""",
        'mthvwap':"""//span[@data-bind="text: companyInfo.adjustedVolWeightedAvgPrice, formatNonZeroValue: 'number'"]""",
        'sharesOutstanding':"""//td[@data-bind="text: companyInfo.sharesOutstanding, formatNonZeroValue: 'volume'"]""",
        'normalizedEPS':"""//td[@data-bind="text: companyInfo.eps != null ? companyInfo.eps : '-', formatNonZeroValue: 'dollars'"]""",
        'unadjVWAP':"""//td[@data-bind="visible: !companyInfo.hasOwnProperty('volWeightedAvgPrice') || companyInfo.volWeightedAvgPrice == null"]""",
        'adjVWAP':"""//td[@data-bind="visible: !companyInfo.hasOwnProperty('adjustedVolWeightedAvgPrice') || companyInfo.adjustedVolWeightedAvgPrice == null"]""",
        'avgVolume':"""//td[@data-bind="visible: !companyInfo.hasOwnProperty('adjustedVolWeightedAvgPrice') || companyInfo.adjustedVolWeightedAvgPrice == null"]"""
        }
    
    valuationDict={
        'peratio':"""//td[@data-bind="text: companyInfo.peRatio != null ? companyInfo.peRatio : '-', formatNonZeroValue: 'number'"]""",
        'evebita':"""//td[@data-bind="text: companyInfo.evEbitData != null ? companyInfo.evEbitData : '-', formatNonZeroValue: 'number'"]""",
        'pricebookvalue':"""//td[@data-bind="text: companyInfo.priceToBookRatio != null ? companyInfo.priceToBookRatio : '-', formatNonZeroValue: 'number'"]""",
        'dividend':"""//td[@data-bind="text: companyInfo.dividendYield != null ? companyInfo.dividendYield : '-', formatNonZeroValue: 'percent'"]"""
        }
    
    financialsDict={
        'debtebita':"""//td[@data-bind="text: companyInfo.totalDebtEbitda != null ? companyInfo.totalDebtEbitda : '-', formatNonZeroValue: 'number'"]""",
        'debt':"""//td[@data-bind="text: companyInfo.totalDebt != null ? companyInfo.totalDebt : '-', formatNonZeroValue: 'millions'"]""",
        'enterpriseValue':"""//td[@data-bind="text: companyInfo.enterpriseValue != null ? companyInfo.enterpriseValue : '-', formatNonZeroValue: 'millions'"]""",
        'assets':"""//td[@data-bind="text: companyInfo.totalAssets != null ? companyInfo.totalAssets : '-', formatNonZeroValue: 'millions'"]""",
        'cash':"""//td[@data-bind="text: companyInfo.cashInvestments != null ? companyInfo.cashInvestments : '-', formatNonZeroValue: 'millions'"]""",
        'capEx':"""//td[@data-bind="text: companyInfo.capitalExpenditures != null ? companyInfo.capitalExpenditures : '-', formatNonZeroValue: 'millions'"]""",
        'EBIT':"""//td[@data-bind="text: companyInfo.ebit != null ? companyInfo.ebit : '-', formatNonZeroValue: 'millions'"]""",
        'revenue':"""//td[@data-bind="text: companyInfo.totalRevenue != null ? companyInfo.totalRevenue : '-', formatNonZeroValue: 'millions'"]""",
        'netincome':"""//td[@data-bind="text: companyInfo.netIncome != null ? companyInfo.netIncome : '-', formatNonZeroValue: 'millions'"]""",
        'ebita':"""//td[@data-bind="text:  companyInfo.ebitda != null ? companyInfo.ebitda : '-', formatNonZeroValue: 'millions'"]"""
        }
    
    generalDict={
        'industry':"""//span[@class="widget-stocks-header-tags-container"]"""
            }
    
    allDict={
        'overview':overviewDict,
        'valuation':valuationDict,
        'financials':financialsDict
        }
    
    dividendsDict={
        'date':"""//td[@data-bind="text: dividendExDate, format: 'date'"]""",
        'dividend':"""//td[@data-bind="text: dividendPrice, formatNonZeroValue: 'cents'"]"""
            }
    
#    store={}
#    processedStore={}
#    storeDF=pd.DataFrame()
#    storeDF['name']=[name]
#    
#    for j in allDict:
#        oneDict=allDict[j]
#        tem={}
#        tem2={}
#        for i in oneDict:
#            try:
#                info=driver.find_element_by_xpath(oneDict[i]).get_attribute('innerText')
#            except:
#                info='-'
#            tem[i]=info
#            tem2[i]=processString(info)
#            storeDF[i]=[tem2[i]]
#        store[j]=tem
#        processedStore[j]=tem2
#    
#    dates=driver.find_elements_by_xpath(dividendsDict['date'])
#    dividends=driver.find_elements_by_xpath(dividendsDict['dividend'])
#    
#    dividendList=pd.DataFrame(columns=['date', 'dividend'])
#    for i in range(len(dates)):
#        date=dates[i].get_attribute('innerText')
#        dividend=dividends[i].get_attribute('innerText')
#        dividendList.loc[i]=[date, dividend]
#        if i == 0:
#            storeDF['dividend']=dividend
#        
#    return storeDF, store, processedStore, dividendList

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
                db.recreateTable('rawData', store)
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

def extractSummary(fname):   
    df, df2=crawlSummary()
    df=df.reset_index(drop=True)
    if host!='cloud':
        df.to_csv(summaryFName, index=False)
        
    timec.getTimeSplit('summary extracted')
    return df, df2

def getFullDetails(index=0, summaryBool=False, host=host, intJobId=''):
    
    if summaryBool==False:
        if host=='cloud':
            dbName='summary'
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
            db.recreateTable(dbName)
            db.rewriteTable(dbName, df)
    
    
#    df=df.loc[list(range(0,3))]
#    print(df)
    companyFullInfo=collateCompanyInfo(df, start=index, host=host)
#    results=analysis.cleanAndProcess(infoName=companyInfoFName)
    timec.stopTime()
    if intJobId != '':
        util.updateJobStatus(intJobId=intJobId)
#    return df, companyFullInfo
    
def updatePriceHist(df, companyFullInfo,updateDatabase=True):
    tdayDate=util.currentDate()
    if updateDatabase:
        db.rewriteTable('rawData', companyFullInfo)
        result=db.extractTable('history')
        if result['error'] is None:
            hist=result['result']
            temLst=['']*len(hist)
            nameLst=hist['names']
            for count, name in enumerate(df['names']):
                if name in nameLst:
                    rowCount=nameLst.index(name)
                    temLst[rowCount]=df['last price'][count]
                else:
                    hist.loc[len(hist)]=[name]+['']*(len(list(hist))-1)
                    temLst.append(df['last price'][count])
            hist[tdayDate]=temLst
             
        else:
            hist=pd.DataFrame()
            hist['names']=df['names']
            hist[tdayDate]=df['last price']
        
        db.rewriteTable('history', hist)
    #update local csv
    t=1
        
def isInt(val):
    try:
        a=float(val)
        return True
    except:
        return False
        
def updateRatios(companyInfo):
    ratios=[float(x)/float(y) if (isInt(x) and isInt(y)) else 1 for x, y in zip(companyInfo['last price'], companyInfo['openprice'])]
    colList=['marketcap','peratio','price_sales','price_cf','price_sales']
    for col in colList:
        companyInfo[col]=[float(x)*float(y) if (isInt(x)==True and isInt(y)==True) else x for x,y in zip(companyInfo[col], ratios)]
    
    return companyInfo
    
def updateCompanyInfo(downloadData=True):
    now=util.currentDate()
    
    df,df2=extractSummary(summaryFName)
    
    if downloadData:
        result=db.extractTable('rawData')
        if result is None:
            companyFullInfo=result['result']
        else:
            companyFullInfo=pd.read_csv(companyInfoFName)
    else:
        companyFullInfo=pd.read_csv(companyInfoFName)
        
    companyFullInfo=pd.merge(companyFullInfo, df[['names','last price']], how='outer', left_on='names', right_on='names')
    companyFullInfo=updateRatios(companyFullInfo)
    companyFullInfo['openprice']=companyFullInfo['last price']
    companyFullInfo.drop(['last price'], axis=1, inplace=True)
    companyFullInfo['prevclosedate']=[now]*len(companyFullInfo)
    print('done')
    
    companyFullInfo.to_csv(companyUpdatedInfoFName, index=False)
    companyFullInfo.to_csv(infoLogs+now+'.csv', index=False)
    
    updatePriceHist(df, companyFullInfo)
    
#    return df, companyFullInfo
    
#    results=analysis.cleanAndProcess(infoName=companyUpdatedInfoFName)
    
#    return results

def closeDriver():
    driver.quit()
    
#df, companyFullInfo= updateCompanyInfo()
#updatePriceHist(df, companyFullInfo)

#if __name__ == "__main__":
#    parser = argparse.ArgumentParser("simple_example")
#    parser.add_argument("--index", help="index to start.", type=int, default=0)
#    parser.add_argument("--function", help="0 to crawl full summary and details. 1 to crawl summary for price update.", type=int, default=0)
#    parser.add_argument("--summaryBool", help="0 to crawl re-crawl summary as well. 1 to not re-crawl summary", type=int, default=1)
#    args = parser.parse_args()
#    if args.function==0:
#        print(args.index)
#        getFullDetails(args.index, args.summaryBool==0)
#    else:
#        results=updateCompanyInfo()
#        print('to just do summary')
#        
#        a=updateCompanyInfo()
#        df,df2=extractSummary(summaryFName)
#    
#    driver.quit()
    
#else:
#    index=0
#    function=1
#    if function ==0:
#        getFullDetails(index)
#    else:
#        updateCompanyInfo()