import requests
import json

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
    'jobId':'dee61d86-9cb1-4f72-b114-5573aa865d7e'
        }
result=requests.get(url, params)
print(result.text)