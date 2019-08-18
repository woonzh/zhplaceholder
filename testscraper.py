from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By

import time
import os

version='windows'
host='cloud'

chromepath=""
if version =='windows':
    chromepath='chromedriver/chromedriver.exe'

else:
    chromepath='chromedriver(linux)/chromedriver'

if host == 'local':
    capabilities = webdriver.DesiredCapabilities.CHROME
    options=webdriver.ChromeOptions()
    options.add_argument('--headless')
else:
    GOOGLE_CHROME_BIN=os.environ.get('GOOGLE_CHROME_BIN', None)
    CHROMEDRIVER_PATH=os.environ.get('CHROMEDRIVER_PATH', None)
    
    chrome_options = webdriver.ChromeOptions()
    chrome_options.binary_location = GOOGLE_CHROME_BIN
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')

def test():
    if host=="local":
        driver = webdriver.Chrome(chromepath, chrome_options=options)
    else:
        driver = webdriver.Chrome(executable_path=CHROMEDRIVER_PATH, chrome_options=chrome_options)
    driver.maximize_window()
    
    mainURL="https://www.webscraper.io/test-sites/e-commerce/allinone"
    
    driver.get(mainURL)
    time.sleep(1)
    val=driver.find_element_by_xpath('//div[@class="jumbotron"]//h1')
    
    text=val.get_attribute('innerText')
    
    driver.close()
    
    return text