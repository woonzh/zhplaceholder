import requests
import json
import threading

#url='https://zhplaceholder.herokuapp.com/testWorker'
#params={
#    'params': 10
#        }
#
url='https://zhplaceholder.herokuapp.com/sgxWorker'

result=requests.get(url)
ans=json.loads(result.text)
jobid=ans['answer']['result']

#url='https://zhplaceholder.herokuapp.com/workerResult'
#params={
#    'jobId':jobid
#        }
#result=requests.get(url, params)
#print(result.text)

#def test():
#    print("test")
#    
#def test2():
#    print("test2")
#
#timer=threading.Timer(3.0,test)
#timer.start()
#test2()