import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import logging
from fear_greed import get_fear_greed
import requests
from bs4 import BeautifulSoup

files_path = '/home/entukio/projects/scrapper_crypto_top/files/'

logging.basicConfig(filename='MARKET_SUMMARY.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

conn=sqlite3.connect('/home/entukio/projects/scrapper_crypto_top/files/top_500_with_mcap_stablecoins_excluded.db')

df_final=pd.read_sql_query('SELECT * FROM top_500_with_mcap_stablecoins_excluded',conn)

conn.close()

scraping_date = df_final['Date'][0]

df_final['current_Middle_Trend_Up'] = df_final['current_Middle_Trend_Up'].apply(lambda x: int.from_bytes(x, byteorder='big') if x is not None else None)
df_final['current_Long_Trend_Up'] = df_final['current_Long_Trend_Up'].apply(lambda x: int.from_bytes(x, byteorder='big') if x is not None else None)

df_final['current_Middle_Trend_Up'] = df_final['current_Middle_Trend_Up'].apply(lambda x: bool(x) if x is not None else None)
df_final['current_Long_Trend_Up'] = df_final['current_Long_Trend_Up'].apply(lambda x: bool(x) if x is not None else None)


### Summary stats ###


Total_coins = len(df_final) #############################################

In_Uptrend = len(df_final[(df_final['current_Middle_Trend_Up'] == True) & (df_final['middle_flip_date'].notna())])
In_Downtrend = len(df_final[(df_final['current_Long_Trend_Up'] == False) & (df_final['middle_flip_date'].notna())])
No_Trend_Info = Total_coins - In_Uptrend - In_Downtrend

Above_200_MA = len(df_final[(df_final['current_Long_Trend_Up'] == True) & (df_final['long_flip_date'].notna())])
Below_200_MA = len(df_final[(df_final['current_Long_Trend_Up'] == False) & (df_final['long_flip_date'].notna())])
No_200_MA_info = Total_coins - Above_200_MA - Below_200_MA

Fear_Greed = None
Fear_Greed_class = None
Fear_Greed_update_time = None

Fear_greed_fetch_object = get_fear_greed()
if Fear_greed_fetch_object[0] == 1:
    try:
        Fear_Greed = Fear_greed_fetch_object[1]['fear_greed_value']
        Fear_Greed_class = Fear_greed_fetch_object[1]['fear_greed_class']
        Fear_Greed_update_time = Fear_greed_fetch_object[1]['fear_greed_update_time']
    except Exception as e:
        logging.error(f'Fear and greed error: {e}')
elif Fear_greed_fetch_object[0] == 0:
    logging.error(f'Fear and greed error: {Fear_greed_fetch_object[1]}')

In_Uptrend_Perc = f'{round((In_Uptrend / Total_coins)*100,2)}%'
In_Downtrend_Perc = f'{round((In_Downtrend / Total_coins)*100,2)}%'
No_Trend_Perc = f'{round((No_Trend_Info / Total_coins)*100,2)}%'

Above_200_MA_Perc = f'{round((Above_200_MA / Total_coins)*100,2)}%'
Below_200_MA_Perc = f'{round((Below_200_MA / Total_coins)*100,2)}%'
No_200_MA_info_Perc = f'{round((No_200_MA_info / Total_coins)*100,2)}%'

df_summary = pd.DataFrame({
    'Date': [scraping_date],
    'Total_coins': [Total_coins],
    'In_Uptrend': [In_Uptrend],
    'In_Downtrend': [In_Downtrend],
    'No_Trend_Info': [No_Trend_Info],
    'Above_200_MA': [Above_200_MA],
    'Below_200_MA': [Below_200_MA],
    'No_200_MA_info': [No_200_MA_info],
    'Fear_Greed': [Fear_Greed],
    'Fear_Greed_update_time': [Fear_Greed_update_time],
    'Fear_Greed_class': [Fear_Greed_class],
    'In_Uptrend_Perc': [In_Uptrend_Perc],
    'In_Downtrend_Perc': [In_Downtrend_Perc],
    'No_Trend_Perc': [No_Trend_Perc],
    'Above_200_MA_Perc': [Above_200_MA_Perc],
    'Below_200_MA_Perc': [Below_200_MA_Perc],
    'No_200_MA_info_Perc': [No_200_MA_info_Perc]
})

conn1=sqlite3.connect('/home/entukio/projects/scrapper_crypto_top/files/market_overview.db')

df_summary.to_sql(f'market_overview',conn1,if_exists='append',index=False)

def get_totalMcaps(df_final):
    try:
        site = f'https://www.slickcharts.com/currency'
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
        res = requests.get(site,headers=headers)
        soup = BeautifulSoup(res.text,'html.parser')
        total1 = float(soup.find('h5',class_="text-center").text.split('$')[1].replace(',',''))
        btc_mcap = df_final[df_final['Id'] == "Bitcoin_BTC"]['Mcap'].iloc[0]    
        eth_mcap = df_final[df_final['Id'] == "Ethereum_ETH"]['Mcap'].iloc[0]
        total2 = total1 - btc_mcap
        total3 = total2 - eth_mcap
        return [total1,total2,total3]
    except Exception as e:
        print('fail 1')
        logging.error(f'TOTAL MCAP DOWNLOAD FAILED: {e}')
        return [None,None,None]

def update_mcap_csv():
    total = pd.read_csv(f'{files_path}coin-dance-market-cap-historical.csv',delimiter=";")
    new_total = get_totalMcaps(df_final)
    pr = {"Date":[pd.to_datetime(scraping_date)],"Total1":[new_total[0]],"Total2":[new_total[1]],"Total3":[new_total[2]]}
    new_row = pd.DataFrame(pr)
    new_df = pd.concat([total,new_row],ignore_index=True)
    new_df.to_csv(f'{files_path}coin-dance-market-cap-historical.csv',index=False,sep=";")

try:
    update_mcap_csv()
    print('updated')
except Exception as e:
    logging.error(f'mcap file not updated: {e}')
    print('fail 2')
