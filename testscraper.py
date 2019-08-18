from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By

import time

version='windows'

if version =='windows':
    chromepath='chromedriver/chromedriver.exe'

else:
    chromepath='chromedriver(linux)/chromedriver'
    
capabilities = webdriver.DesiredCapabilities.CHROME
options=webdriver.ChromeOptions()
options.add_argument('--headless')

def test():
    driver = webdriver.Chrome(chromepath, chrome_options=options)
    driver.maximize_window()
    
    mainURL="https://www.webscraper.io/test-sites/e-commerce/allinone"
    
    driver.get(mainURL)
    time.sleep(1)
    val=driver.find_element_by_xpath('//div[@class="jumbotron"]//h1')
    
    text=val.get_attribute('innerText')
    
    driver.close()
    
    return text