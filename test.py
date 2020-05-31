import requests
import json
import threading
import pandas as pd

#url='https://zhplaceholder.herokuapp.com/iexupdatedetails'
#url='https://zhplaceholder.herokuapp.com/iexupdatebasics'
#params={
#    'start': 0,
#    'end':0
#        }
#result=requests.get(url, params=params)

url='https://zhplaceholder.herokuapp.com/sgxUpdate'
#result=requests.get(url)

#url='https://zhplaceholder.herokuapp.com/hkexWorker'
url='https://zhplaceholder.herokuapp.com/hkexUpdateBasic'
#url='https://zhplaceholder.herokuapp.com/hkexUpdateDetails'
params={
    'quandl': 0
        }
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
#
#ans=json.loads(result.text)
#jobid=ans['answer']['result']

#url='https://zhplaceholder.herokuapp.com/getAnalytics'
#url='http://localhost/getAnalytics'
#params={
#    'pw':'P@ssw0rd',
#    'country':'sg'
#        }
#result=requests.get(url, params=params)

#ans=json.loads(result.text)
#jobid=ans['answer']['result']


#
#url='https://zhplaceholder.herokuapp.com/workerResult'
#url='https://zhplaceholder.herokuapp.com/cancelworker'
#params={
#    'jobId':jobid
#        }
#result2=requests.get(url, params)
#print(result2.text)
