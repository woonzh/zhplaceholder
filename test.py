import requests
import json

#url='https://zhplaceholder.herokuapp.com/testWorker'
#params={
#    'params': 10
#        }

#result=requests.get(url)
#ans=json.loads(result.text)
#jobid=ans['answer']['result']

#url='https://zhplaceholder.herokuapp.com/sgxWorker'
#result=requests.get(url)
#print(result.text)

url='https://zhplaceholder.herokuapp.com/workerResult'
params={
    'jobId':'638abc54-e8c3-4607-8fde-565fe86366bb'
        }
result=requests.get(url, params)
print(result.text)