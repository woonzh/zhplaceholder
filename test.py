import requests
import json
import threading
import pandas as pd

#url='https://zhplaceholder.herokuapp.com/getFilterResult'
#filters={
#    'peratio':['>',1],
#    'openprice':['>',0.2],
#    'net_profit_margin':['>', 5],
#    'volume traded %':['>', 0.01]
#        }
#params={
#    'filters':filters
#        }
#result=requests.get(url, params)
#ans=json.loads(result.text)
#raw=ans['answer']
#test=json.loads(raw)
#df=pd.DataFrame(columns=test['columns'], data=test['data'])
#jobid=ans['answer']['result']

#url='https://zhplaceholder.herokuapp.com/testWorker'
#params={
#    'params': 10
#        }
#
#url='https://zhplaceholder.herokuapp.com/sgxWorker'
#
#result=requests.get(url)
#ans=json.loads(result.text)
#jobid=ans['answer']['result']

url='https://zhplaceholder.herokuapp.com/workerResult'
params={
    'jobId':jobid
        }
result=requests.get(url, params)
print(result.text)

#def test():
#    print("test")
#    
#def test2():
#    print("test2")
#
#timer=threading.Timer(3.0,test)
#timer.start()
#test2()