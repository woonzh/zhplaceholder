import requests
import json

apiKey='HF2N7DEDWYMQLWQR'
url='https://www.alphavantage.co/query'

params={
    'function':'TIME_SERIES_INTRADAY',
    'symbol':'883',
    'interval':'5min',
    'outputsize':'compact',
    'apikey':apiKey
    }

ans=requests.get(url, params=params)
reply=json.loads(ans.text)