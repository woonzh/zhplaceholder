import requests
import json
import threading
import pandas as pd

url='https://zhplaceholder.herokuapp.com/iexupdatedetails'
url='https://zhplaceholder.herokuapp.com/iexupdatebasics'
params={
    'start': 10,
    'end':20
        }
result=requests.get(url, params=params)

#url='https://zhplaceholder.herokuapp.com/testWorker'
#params={
#    'params': 10
#        }
#t=1
#url='https://zhplaceholder.herokuapp.com/hkexWorker'
#url='https://zhplaceholder.herokuapp.com/hkexUpdateBasic'
#url='https://zhplaceholder.herokuapp.com/hkexUpdateDetails'
#params={
#    'quandl': 0
#        }
#result=requests.get(url, params=params)
#result=requests.get(url)

#url='https://zhplaceholder.herokuapp.com/nasdaqfull'
#url='https://zhplaceholder.herokuapp.com/nasdaqupdatedetails'
#url='https://zhplaceholder.herokuapp.com/nasdaqupdatebasic'
#params={
#    'useragent': 4
#        }
#result=requests.get(url, params=params)
#result=requests.get(url)

#url='https://zhplaceholder.herokuapp.com/sgxWorker'
#result=requests.get(url)

#url='https://zhplaceholder.herokuapp.com/sgxUpdate'
#params={
#    'dragIndex':5,
#    'sumTries':10
#        }
#url='https://zhplaceholder.herokuapp.com/rawdata'
#result=requests.get(url)

#ans=json.loads(result.text)
#jobid=ans['answer']['result']

#
#url='https://zhplaceholder.herokuapp.com/workerResult'
#params={
#    'jobId':jobid
#        }
#result=requests.get(url, params)
#print(result.text)
