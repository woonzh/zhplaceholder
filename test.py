import requests

url='https://zhplaceholder.herokuapp.com/testWorker'
params={
    'params': 10
        }

result=requests.get(url, params)