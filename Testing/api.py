from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
import os
from time import sleep
from time import time

def api_runner():
    global df
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest' 
    #Original Sandbox Environment: 'https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
      'start':'1',
      'limit':'25',
      'convert':'USD'
    }
    headers = {
      'Accepts': 'application/json',
      'X-CMC_PRO_API_KEY': '14432547-016c-4b22-84c2-c061f595c891',
    }

    session = Session()
    session.headers.update(headers)

    try:
      response = session.get(url, params=parameters)
      data = json.loads(response.text)
      
    except (ConnectionError, Timeout, TooManyRedirects) as e:
      print(e)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.float_format',lambda x: '%.5f'% x)
    df=pd.json_normalize(data['data'])
    df['timestamp']=pd.to_datetime('now')
    #df2 = df.groupby('name',sort=False)
   # df2 = df.dropna(axis = 1)
    #df2 = df.drop(['date_added','last_updated','quote.USD.volume_24h'],axis=0, inplace=True)
    #a = df.isnull().sum()
    columns_to_delete = ['quote.USD.last_updated','quote.USD.volume_24h','num_market_pairs','date_added','circulating_supply','last_updated','platform','max_supply','self_reported_circulating_supply','self_reported_market_cap','platform.id','platform.name','platform.symbol','platform.slug','platform.token_address']
    df.drop(columns_to_delete, inplace=True,axis=1)
    #df.dropna(subset=['self_reported_circulating_supply','self_reported_market_cap','platform.id','platform.name','platform.symbol','platform.slug','platform.token_address'],inplace=True)
    df

    if not os.path.isfile(r'/Users/divyeshpatil/Desktop/Api_Automation/crypto_numeric_data.csv'):
        df.to_csv(r'/Users/divyeshpatil/Desktop/Api_Automation/crypto_numeric_data.csv', header='column_names')
    else:
        df.to_csv(r'/Users/divyeshpatil/Desktop/Api_Automation/crypto_numeric_data.csv', mode='a', header=False)
        

for i in range(250):
    api_runner()
    print('API Running')
    sleep(300) #sleep for 5 minute
exit()