import requests
import json

url='https://fmpcloud.io/api/v3/quote/AAPL?apikey=03c3cdf11f96675a6fe0b2c20a590dc4'
url2='https://fmpcloud.io/stock/list?apikey=03c3cdf11f96675a6fe0b2c20a590dc4'

ans=requests.get(url)
data=json.loads(ans.text)