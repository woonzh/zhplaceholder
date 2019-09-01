import requests

url='https://zhplaceholder.herokuapp.com/testWorker'
params={
    'params': 10
        }

result=requests.get(url, params)
jobid=result['answer']['result']

#url='https://zhplaceholder.herokuapp.com/workerResult'
#params={
#    'jobId':jobid
#        }
#result=requests.get(url, params)