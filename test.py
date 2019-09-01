import requests
import json

url='https://zhplaceholder.herokuapp.com/testWorker'
#params={
#    'params': 10
#        }

#result=requests.get(url)
#ans=json.loads(result.text)
#jobid=ans['answer']['result']

url='https://zhplaceholder.herokuapp.com/workerResult'
params={
    'jobId':jobid
        }
result=requests.get(url, params)
print(result.text)