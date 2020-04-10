# -*- coding: utf-8 -*-
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from crawler import crawler

import time
import pandas as pd

import dbConnector as db
#import analysis

url="https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities?sc_lang=en"
hkSum='data/HKsummary.csv'
dbName='hksummary'

def run():
    crawl=crawler()
    crawl.startDriver(url)
    
    rows,df=crawl.crawlHKEXSummary()
    
    crawl.store(df, hkSum, dbName)
    
    crawl.closeDriver()