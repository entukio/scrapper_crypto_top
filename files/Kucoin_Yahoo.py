import logging
import numpy as np
import pandas as pd
import requests
from datetime import datetime
import yfinance as yf

def getYahooData(symbol,start_date,end_date):
        symbol = symbol.replace('USD','')     
        try:
            y_data = yf.download(f'{symbol}-USD', start=start_date, end=end_date)
            if y_data.empty:
                return [0]
            else:
                y_data.reset_index(inplace=True)
                new_data = y_data[['Date','Close']].copy()
                new_data['EMA23'] = np.nan
                new_data['EMA56'] = np.nan
                new_data['SMA200'] = np.nan

                return [1,new_data]
                
        except Exception as e:
            logging.error(f"Error downloading data for symbol {symbol}: {e}")


def getKucoinData(symbol,start_date,end_date):

    # example:
    # symbol = 'GMT'
    # start = dateToSec('2024-04-01')
    # end = dateToSec('2024-04-03')

    symbol = symbol.replace('USD','')

    def dateToSec(date_string):
        # Convert the date string to a datetime object
        date_object = datetime.strptime(date_string, '%Y-%m-%d')

        # Convert the datetime object to seconds since the Unix epoch
        unix_timestamp = int(date_object.timestamp())

        return unix_timestamp
    
    start = dateToSec(start_date)
    end = dateToSec(end_date)
    
    endpoint = f'https://api.kucoin.com/api/v1/market/candles?type=1day&symbol={symbol}-USDT&startAt={start}&endAt={end}'
    response = requests.get(endpoint) #, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        if 'msg' not in response.json():
            def secToDate(seconds):
                # Convert seconds since 1970 to a datetime object
                date_object = datetime.fromtimestamp(int(seconds))
                return date_object
            df = pd.DataFrame(response.json().get('data',None))
            df = df[[0,2]].copy()
            df.columns=['Date','Close']
            df['Date'] = df['Date'].astype(int)
            df['Close'] = df['Close'].astype(float)
            df["Date"] = df["Date"].apply(secToDate)
            df['EMA23'] = np.nan
            df['EMA56'] = np.nan
            df['SMA200'] = np.nan
            return [True,df]
        else:
            return[False]
    
    else:
        # Print the error message if the request failed
        print(f"Kucoin Download Error: {response.status_code} - {response.text}")
        return [False]
