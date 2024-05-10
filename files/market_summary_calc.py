import pandas as pd
import sqlite3
from datetime import datetime, timedelta

conn=sqlite3.connect('/home/entukio/projects/scrapper_crypto_top/files/top_500_with_Mcap_stablecoins_excluded.db')

df_final=pd.read_sql_query('SELECT * FROM top_500_with_Mcap_stablecoins_excluded',conn)

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

Fear_Greed = 76 ############################################

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
    'In_Uptrend_Perc': [In_Uptrend_Perc],
    'In_Downtrend_Perc': [In_Downtrend_Perc],
    'No_Trend_Perc': [No_Trend_Perc],
    'Above_200_MA_Perc': [Above_200_MA_Perc],
    'Below_200_MA_Perc': [Below_200_MA_Perc],
    'No_200_MA_info_Perc': [No_200_MA_info_Perc]
})

conn1=sqlite3.connect('/home/entukio/projects/scrapper_crypto_top/files/market_overview.db')

df_summary.to_sql(f'market_overview',conn1,if_exists='append',index=False)
