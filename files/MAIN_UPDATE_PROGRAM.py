import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime
import numpy as np
from Kucoin_Yahoo import getKucoinData,getYahooData
import json

# # # # # # # # # #  # # # # #  # # # # # #
# # # # # # # # # #  # # # # #  # # # # # 
# # # # # 1. special functions # # # # # #
# # # # # # # # # #  # # # # #  # # # # # #
# # # # # # # # # #  # # # # #  # # # # # #

def Data_Validation(coin_db,src='Internal'):
    coin_db['Date'] = coin_db['Date'].apply(lambda x : pd.to_datetime(x).date())
    
    if src=='Internal':
        # Validation 2: Check for same price for more than 3+ days - ONLY TO BE CHECKED FOR SCRAPPED DB!
        if (coin_db['Close'].diff().fillna(0) == 0).rolling(window=3).sum().max() >= 3:
            print('2 failed')
            return False
        
        # Validation 1: Check for price change exceeding 3x or 95% drop
        price_change = coin_db['Close'].pct_change()
        if ((price_change > 3) | (price_change < -0.95)).any():
            print('1 failed')
            return False
    
        # Validation 3: Check for price values containing 0.0
        if (coin_db['Close'] == 0.0).any():
            print('3 failed')
            return False
    
    
        # Validation 4: Check if length is less than 300 days
        if len(coin_db) < 201:
            print('4 failed')
            return False
    
    '''    
    # Validation 5: Check for missing dates
    expected_dates = pd.date_range(start=coin_db['Date'].min(), end=coin_db['Date'].max())
    actual_dates = coin_db['Date']
    
    # Convert the 'actual_dates' column to a set for faster lookup
    actual_dates_set = set(actual_dates)
    
    # Find missing dates by checking which expected dates are not in the actual dates set
    missing_dates = [date for date in expected_dates if date not in actual_dates_set]

    if len(missing_dates) > 1:
        print('missing_dates:')
        print(missing_dates)
        return False
    '''
    print('DATA VALIDATION PASSED!')
    return True


def add_yahoo_data(identifier,symbol,db_name,coin_db,yahoo_data):
    k_data = yahoo_data[1]
    k_data.sort_values(by=['Date'],ascending=True,inplace=True)
    k_data['Date'] = k_data['Date'].apply(remove_hour)
    coin_db = pd.concat([coin_db,k_data],ignore_index=True)

    if Data_Validation(coin_db,src='External') == True:
        TA_Calculate(coin_db,db_name,symbol)
        logging.info(f"Saved to DB: {identifier}, length of data added: {k_data}")
        return True
    else:
        return [f'Data_Validation for {symbol} not passed']

def duplicates(id):
    print(' inicjacja duplicates')
    # todo: remove id duplicates
    if remove_duplicates(id):
        print(' remove duplicates sprawdzone')
        return [False,None,None]
    else:
        if id in mapping_ids:
            print(' trying matcchh')
            match = [i for i in mapping if i['Id'] == id]
            print(' matched')
            print(match)
            download_symbol = match[0].get('kucoin',None)
            print('download_symbol')
            print(download_symbol)
            source = 'Kucoin'
            if download_symbol is None:
                download_symbol = match[0].get('yahoo',None) 
                source = 'Yahoo'
            if download_symbol is None:
                download_symbol = match[0].get('Id')
                source = 'Internal'
            print(' returning 35')
            return [True,download_symbol,source]
        else:
            return [False]

def remove_duplicates(id):
    matched_rows_with_duplicates = top_500_with_mcap_stablecoins_excluded[top_500_with_mcap_stablecoins_excluded['Id'] == id]    
    # Find duplicate rows among those where "name" is "Akash network"
    duplicate_rows = matched_rows_with_duplicates[matched_rows_with_duplicates.duplicated(subset=['Id'], keep='first')]
    if len(duplicate_rows) > 1:

        identifier = duplicate_rows.iloc[0]['Id']
    
        # Get the indices of duplicate rows
        duplicate_indices = duplicate_rows.index
        
        # Remove the second and subsequent occurrences of duplicates
        top_500_with_mcap_stablecoins_excluded.drop(duplicate_indices[1:], inplace=True)
        
        # Reset the index after removal
        top_500_with_mcap_stablecoins_excluded.reset_index(drop=True, inplace=True)
        
        logging.info(f'Duplicate removed: {identifier}')

        return True
    else:
        return False

def check_flip_date(coin_db,last_trends):
    broken = False
    start = last_trends[0]
    for i in range(len(last_trends)):
        if last_trends[i] != start:
            date1 = coin_db.iloc[-1-i+1]['Date']
            date1 = pd.to_datetime(date1).date() # correct the date
            broken = True
            break  
        else:
            continue
    if broken == False:
        return None
    else:
        return date1

def TA_Calculate(coin_db,db_name,symbol):
    try:
        conn = sqlite3.connect(f'/home/entukio/projects/scrapper_crypto_top/files/prices/{db_name}')
        name = db_name.split('.')[0]
        coin_db['EMA23'] = coin_db['Close'].ewm(span=23, adjust=False,min_periods=23).mean()
        coin_db['EMA56'] = coin_db['Close'].ewm(span=56, adjust=False,min_periods=56).mean()
        coin_db['SMA200'] = coin_db['Close'].rolling(window=200).mean()
        coin_db['Long_Trend_Up'] = coin_db['Close'] > coin_db['SMA200']
        coin_db['Middle_Trend_Up'] = coin_db['EMA23'] > coin_db['EMA56']
        coin_db.to_sql(f'db_{symbol}',conn,if_exists='replace',index=False)
        print(f'successfully converted {name}')
    except Exception as e:
        err = (f'error in saving {db_name}: {e}')
        print(err)
        logging.error(err)

def dates_between(start_date,end_date):
    # Get yesterday's date

    # Initialize an empty list to store the dates
    date_list = []

    # Loop through the dates from start_date to yesterday's date - skipping yesterday, because it was already added before to the prices array
    current_date = start_date + pd.Timedelta(days=1)
    while current_date <= end_date - pd.Timedelta(days=1):
        date_list.append(current_date)
        current_date += pd.Timedelta(days=1)

    return date_list

def remove_hour(x):
    try:
        dt = pd.to_datetime(x).date()
    except:
        dt = None
    return dt

# # # # # # # # # #  # # # # #  # # # # # #
# # # # # # # # # #  # # # # #  # # # # # 
# # # # # 2.  Configuration     # # # # # #
# # # # # # # # # #  # # # # #  # # # # # #
# # # # # # # # # #  # # # # #  # # # # # #

logging.basicConfig(filename='UPDATER.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

available_databases = os.listdir('/home/entukio/projects/scrapper_crypto_top/files/prices/') # with historic price info

# Scrapper database - with all scraped data - THE LAST DATE SHOULD BE YESTERDAY TO CORRECTLY RUN THE SCRIPT
conn=sqlite3.connect('/home/entukio/projects/scrapper_crypto_top/files/scrapper_crypto_database.db')
df_all_scrapped = pd.read_sql('SELECT * FROM top_crypto',conn)

df_all_scrapped['Date'] = df_all_scrapped['Date'].apply(remove_hour)
df_all_scrapped['Price'] = df_all_scrapped['Price'].str.replace(',', '.').astype(float)
df_all_scrapped['Price'] = df_all_scrapped['Price'].astype(float)


LATEST_TOP_500 = df_all_scrapped[df_all_scrapped['Date']==df_all_scrapped.iloc[-1]['Date']]

# IMPORTANT NOTE: DATABASE LAST RECORD NEEDS TO BE YESTERDAY TO RUN THIS SCRIPT: THE CLOSE PRICE SHOULD BE RECEIVED AT 23:59 LAST DAY UTC-0

query='''
SELECT 
    t1.*, 
    t1.rank - t2.rank AS rank_1dΔ, 
    t1.rank - t3.rank AS rank_3dΔ, 
    t1.rank - t4.rank AS rank_7dΔ, 
    t1.rank - t5.rank AS rank_14dΔ, 
    t1.mcap - t6.mcap AS mcap_1dΔ, 
    t1.mcap - t7.mcap AS mcap_3dΔ, 
    t1.mcap - t8.mcap AS mcap_7dΔ, 
    t1.mcap - t9.mcap AS mcap_14dΔ
FROM 
    top_crypto AS t1
LEFT JOIN 
    top_crypto AS t2 
    ON t1.name = t2.name AND DATE(t1.date, '-1 day') = DATE(t2.date, 'start of day')
LEFT JOIN 
    top_crypto AS t3 
    ON t1.name = t3.name AND DATE(t1.date, '-3 days') = DATE(t3.date, 'start of day')
LEFT JOIN 
    top_crypto AS t4 
    ON t1.name = t4.name AND DATE(t1.date, '-7 days') = DATE(t4.date, 'start of day')
LEFT JOIN 
    top_crypto AS t5 
    ON t1.name = t5.name AND DATE(t1.date, '-14 days') = DATE(t5.date, 'start of day')
LEFT JOIN 
    top_crypto AS t6 
    ON t1.name = t6.name AND DATE(t1.date, '-1 day') = DATE(t6.date, 'start of day')
LEFT JOIN 
    top_crypto AS t7 
    ON t1.name = t7.name AND DATE(t1.date, '-3 days') = DATE(t7.date, 'start of day')
LEFT JOIN 
    top_crypto AS t8 
    ON t1.name = t8.name AND DATE(t1.date, '-7 days') = DATE(t8.date, 'start of day')
LEFT JOIN 
    top_crypto AS t9 
    ON t1.name = t9.name AND DATE(t1.date, '-14 days') = DATE(t9.date, 'start of day')
WHERE 
    DATE(t1.date, 'start of day') = DATE('now', '-1 day', 'localtime');
'''
top_500_with_mcap = pd.read_sql_query(query,conn)

top_500_with_mcap['Date'] = top_500_with_mcap['Date'].apply(remove_hour)
top_500_with_mcap['Price'] = top_500_with_mcap['Price'].astype(float)

# exclude usd coins
top_500_with_mcap_stablecoins_excluded = top_500_with_mcap[~top_500_with_mcap['Symbol'].str.contains('USD')].copy()

top_500_with_mcap_stablecoins_excluded['current_Middle_Trend_Up'] = None
top_500_with_mcap_stablecoins_excluded['current_Long_Trend_Up'] = None
top_500_with_mcap_stablecoins_excluded['middle_flip_date'] = None
top_500_with_mcap_stablecoins_excluded['long_flip_date'] = None

top_500_with_mcap_stablecoins_excluded = top_500_with_mcap_stablecoins_excluded.copy()

# The last top 500 list
asset_book = top_500_with_mcap_stablecoins_excluded[['Symbol','Name','Id']]

# load duplicate mapping dictionary

with open('mapping.json', 'r') as json_file:
    mapping = json.load(json_file)
mapping_ids = [i.get('Id') for i in mapping]

# Placeholders for new up and down trends 


# # # # # # # # # #  # # # # #  # # # # # #
# # # # # # # # # #  # # # # #  # # # # # 
# # # # # 3.  Main Mapper Loop     # # # # # #
# # # # # # # # # #  # # # # #  # # # # # #
# # # # # # # # # #  # # # # #  # # # # # #



def MapperLooper(asset_book):
    for i in range(len(asset_book)):
        #####################################
        identifier = f'{asset_book.iloc[i]["Id"]}'
        db_name = f'{identifier}.db'
        symbol = f'{asset_book.iloc[i]["Symbol"]}USD'
        #####################################
        try:
            print('trying')
            duplicate_info = duplicates(identifier)
            print('duplicate info')
            print(duplicate_info)
            if duplicate_info[0] == True or (i in mapping_ids):
                print('i in mapping ids')
                Coin_Object = Mapper(identifier,duplicate_info[1],db_name,duplicate_info[2])
                print(Coin_Object.identifier)
                print('success!!!')
            else:
                Coin_Object = Mapper(identifier,symbol,db_name)
                print(Coin_Object.identifier)
                print(Coin_Object.prices)
                print('success ya')
            
            try:
                if Coin_Object.updateDatabase() == True:
                    Coin_Object.loadUpdatedDataExecute()
      
            except Exception as e:
                logging.error(f'PROGRAM ERROR - {identifier}:{e}')

        except Exception as e:
            logging.error(f'FATAL error in Mapper program for {identifier},{e}')


# # # # # # # # # #  # # # # #  # # # # # #
# # # # # # # # # #  # # # # #  # # # # # 
# # # # # 4.   Mapper Class     # # # # # #
# # # # # # # # # #  # # # # #  # # # # # #
# # # # # # # # # #  # # # # #  # # # # # #


class Mapper:
    #map class to handle every top500 crypto coin functions: loop - create Mapper for every one
    def __init__(self,identifier,symbol,db_name,source='Kucoin'):
        self.identifier = identifier
        self.symbol = symbol
        self.prices = []
        self.db_name = db_name
        self.source = source
        
        # ATTRIBUTES - the last data will be available by default. 
        coin = top_500_with_mcap_stablecoins_excluded[top_500_with_mcap_stablecoins_excluded['Id']==self.identifier]
        coin.reset_index(drop=True, inplace=True)
        
        prices = [{
        'Date':coin['Date'][0],
        'Price':coin['Price'][0]
        }]

        self.prices.extend(prices)

    

    def loadUpdatedDataExecute(self):
        conn = sqlite3.connect(f'/home/entukio/projects/scrapper_crypto_top/files/prices/{self.db_name}')
        coin_db = pd.read_sql_query(f'SELECT * FROM db_{self.symbol}',conn)
        current_Middle_Trend_Up = coin_db.iloc[-1]['Middle_Trend_Up']
        current_Long_Trend_Up = coin_db.iloc[-1]['Long_Trend_Up']
        Middle_Trends = coin_db['Middle_Trend_Up'].tolist()
        Middle_Trends.reverse()
        Long_Trends = coin_db['Long_Trend_Up'].tolist()
        Long_Trends.reverse()
        middle_flip_date = check_flip_date(coin_db,Middle_Trends)
        long_flip_date = check_flip_date(coin_db,Long_Trends)
        print('NECESSARRRY AAAA START')
        print(current_Middle_Trend_Up)
        print(current_Long_Trend_Up)
        print(middle_flip_date)
        print(long_flip_date)
        print('NECESSARRRY AAAA END')

        top_500_with_mcap_stablecoins_excluded.loc[(top_500_with_mcap_stablecoins_excluded['Id']==self.identifier),'current_Middle_Trend_Up'] = current_Middle_Trend_Up
        print('BELOW SHOULD BE CURRENT MIDDLE TRENDUP')
        print(top_500_with_mcap_stablecoins_excluded.loc[(top_500_with_mcap_stablecoins_excluded['Id']==self.identifier)])

        top_500_with_mcap_stablecoins_excluded.loc[(top_500_with_mcap_stablecoins_excluded['Id']==self.identifier),'current_Long_Trend_Up'] = current_Long_Trend_Up

        top_500_with_mcap_stablecoins_excluded.loc[(top_500_with_mcap_stablecoins_excluded['Id']==self.identifier),'middle_flip_date'] = middle_flip_date

        top_500_with_mcap_stablecoins_excluded.loc[(top_500_with_mcap_stablecoins_excluded['Id']==self.identifier),'long_flip_date'] = long_flip_date


    def updateDatabase(self):
        print('UWAGA')
        print(f'db_{self.symbol}')
        conn = sqlite3.connect(f'/home/entukio/projects/scrapper_crypto_top/files/prices/{self.db_name}')
        try:
            coin_db = pd.read_sql_query(f'SELECT * FROM db_{self.symbol}',conn)
            print('COINDBBB')
            print(coin_db)
            last_scrapper_date = pd.to_datetime(self.prices[-1]['Date']).date()
            last_db_date_data = pd.to_datetime(coin_db.iloc[-1]['Date']).date()
            print('last_db_date_data')
            print(last_db_date_data)
            print(type(last_db_date_data))
            missing_dates = dates_between(last_db_date_data,last_scrapper_date)
            #add the latest available date
            missing_dates.append(self.prices[0]['Date'])
            new_data_from_scrapper = df_all_scrapped[(df_all_scrapped['Id'] == self.identifier) & (df_all_scrapped['Date'].isin(missing_dates))]
            for row in range(len(new_data_from_scrapper)):
                Coin_Object = {
                    'Date':new_data_from_scrapper.iloc[row]['Date'],
                    'Price':new_data_from_scrapper.iloc[row]['Price']
                }
                self.prices.append(Coin_Object)
            self.prices = sorted(self.prices, key=lambda x: x['Date'])
            print('self prices')
            print(self.prices)

            if len(new_data_from_scrapper) == len(missing_dates):
                print(f'YAAAAAA! len(new_data_from_scrapper) == len(missing_dates)')
                try:
                    rows = []
                    for i in range(len(new_data_from_scrapper)):
                        dt = new_data_from_scrapper.iloc[i]
                        new_row = [dt['Date'],dt['Price'],np.nan,np.nan,np.nan]
                        rows.append(new_row)
                    new_data = pd.DataFrame(rows)
                    new_data.columns = ['Date','Close','EMA23','EMA56','SMA200']
                    
                    new_data['places'] = -np.floor(np.log10(new_data['Close'].astype(float)))
                    last_dec_places_5 = 5.0 in new_data.iloc[-4:-1]['places'].tolist()
                    last_closes = new_data.iloc[-4:-1]['Close'].tolist()
                    
                    # condition - must not have 5 decimal places and last must 3 numbers must not be equal
                    condition = (last_dec_places_5) & all(x == last_closes[0] for x in last_closes)
                    
                    if (not condition):
                        new_data.drop(columns=['places'],inplace=True)
                        coin_db = pd.concat([coin_db,new_data],ignore_index=True)
                        print('all good, can add')
                        print(coin_db)
                        if Data_Validation(coin_db)  == True:
                            TA_Calculate(coin_db,self.db_name,self.symbol)
                            logging.info(f"Saved to DB: {self.identifier}, length of data added: {new_data}")
                            return True
                        else: 
                            logging.info(f'Data validation from scrapper db not passed. Downlading from kucoin {self.symbol}')
                            print('Downloading from Kucoin')
                            start = datetime.today().date() - pd.Timedelta(days=401)
                            end = datetime.today().date()
                            start_date = datetime.strftime(start,'%Y-%m-%d')
                            end_date = datetime.strftime(end,'%Y-%m-%d')
                            kucoin_data = getKucoinData(self.symbol,start_date,end_date)
                            if kucoin_data[0] == True:
                                k_data = kucoin_data[1]
                                k_data.sort_values(by=['Date'],ascending=True,inplace=True)
                                k_data['Date'] = k_data['Date'].apply(remove_hour)
                                coin_db = pd.concat([coin_db,k_data],ignore_index=True)
                                if Data_Validation(coin_db,src='External')  == True:
                                    TA_Calculate(coin_db,self.db_name,self.symbol)
                                    logging.info(f"Saved to DB: {self.identifier}, length of data added: {k_data}")
                                    return True
                                else:
                                    print('DBDDDD')
                                    print(coin_db)
                                    logging.error(f'data valiadation fail for Kucoin {self.symbol}')
                            else:
                                logging.error(f"Error in Kucoin updating DB: {self.identifier}")

                    else:
                        if self.source == 'Kucoin':
                            print('Downloading from Kucoin')
                            start_date = datetime.strftime(self.prices[0]['Date'],'%Y-%m-%d')
                            end_date = datetime.strftime(self.prices[-1]['Date']+pd.Timedelta(days=1),'%Y-%m-%d')
                            kucoin_data = getKucoinData(self.symbol,start_date,end_date)
                            if kucoin_data[0] == True:
                                k_data = kucoin_data[1]
                                k_data.sort_values(by=['Date'],ascending=True,inplace=True)
                                k_data['Date'] = k_data['Date'].apply(remove_hour)
                                coin_db = pd.concat([coin_db,k_data],ignore_index=True)
                                if Data_Validation(coin_db,src='External')  == True:
                                    TA_Calculate(coin_db,self.db_name,self.symbol)
                                    logging.info(f"Saved to DB: {self.identifier}, length of data added: {k_data}")
                                    return True
                                else:
                                    logging.error(f'data valiadation fail for Kucoin {self.symbol}, trying to get all values')
                                    start = datetime.today().date() - pd.Timedelta(days=401)
                                    end = datetime.today().date() - pd.Timedelta(days=1)
                                    start_date = datetime.strftime(start,'%Y-%m-%d')
                                    end_date = datetime.strftime(end,'%Y-%m-%d')
                                    kucoin_data = getKucoinData(self.symbol,start_date,end_date)
                                    if kucoin_data[0] == True:
                                        k_data = kucoin_data[1]
                                        k_data.sort_values(by=['Date'],ascending=True,inplace=True)
                                        k_data['Date'] = k_data['Date'].apply(remove_hour)
                                        coin_db = pd.concat([coin_db,k_data],ignore_index=True)
                                        if Data_Validation(coin_db,src='External')  == True:
                                            TA_Calculate(coin_db,self.db_name,self.symbol)
                                            logging.info(f"Saved to DB: {self.identifier}, length of data added: {k_data}")
                                            return True
                                        else:
                                            logging.error(f'data valiadation fail for Kucoin {self.symbol}')
                                    else:
                                        logging.error(f"Error in Kucoin updating DB: {self.identifier}")
                            else:
                                logging.error(f"Error in Kucoin updating DB: {self.identifier}")
                        elif self.source == 'Yahoo':
                            # get from yahoo
                            start_date = datetime.strftime(self.prices[0]['Date'],'%Y-%m-%d')
                            end_date = datetime.strftime(self.prices[-1]['Date']+pd.Timedelta(days=1),'%Y-%m-%d')
                            yahoo_data = getYahooData(self.symbol,start_date,end_date)
                            if yahoo_data[0] == True:
                                try:
                                    result = add_yahoo_data(self.identifier,self.symbol,self.db_name,coin_db,yahoo_data)
                                    if result != 1:
                                        logging.error(result[0])

                                except Exception as e:
                                    print(f'add_yahoo_data error 425 {e}')

                            else:
                                logging.error(f"Error in Yahoo updating DB: {self.identifier}")
                            
                        else:
                            logging.error(f'No data available for {self.identifier}')
                                                
                except Exception as e:
                    print(e)
            else:
                # there are no available prices for all missing dates
                print('there are no available prices for all missing dates')
                if self.source == 'Kucoin':
                    print('Downloading from Kucoin')
                    start_date = datetime.strftime(self.prices[0]['Date'],'%Y-%m-%d')
                    end_date = datetime.strftime(self.prices[-1]['Date']+pd.Timedelta(days=1),'%Y-%m-%d')
                    kucoin_data = getKucoinData(self.symbol,start_date,end_date)
                    if kucoin_data[0] == True:
                        k_data = kucoin_data[1]
                        k_data.sort_values(by=['Date'],ascending=True,inplace=True)
                        k_data['Date'] = k_data['Date'].apply(remove_hour)
                        coin_db = pd.concat([coin_db,k_data],ignore_index=True)
                        if Data_Validation(coin_db,src='External') == True:
                            TA_Calculate(coin_db,self.db_name,self.symbol)
                            logging.info(f"Saved to DB: {self.identifier}, length of data added: {k_data}")
                            return True
                        else:
                            logging.error(f'data validation fail kucoin {self.symbol}')
                    else:
                        logging.error(f"Error in Kucoin updating DB: {self.identifier}")
                elif self.source == 'Yahoo':
                    # get from yahoo
                    start_date = datetime.strftime(self.prices[0]['Date'],'%Y-%m-%d')
                    end_date = datetime.strftime(self.prices[-1]['Date']+pd.Timedelta(days=1),'%Y-%m-%d')
                    yahoo_data = getYahooData(self.symbol,start_date,end_date)
                    if yahoo_data[0] == True:
                        k_data = yahoo_data[1]
                        k_data.sort_values(by=['Date'],ascending=True,inplace=True)
                        k_data['Date'] = k_data['Date'].apply(remove_hour)
                        coin_db = pd.concat([coin_db,k_data],ignore_index=True)
                        if Data_Validation(coin_db,src='External') == True:
                            TA_Calculate(coin_db,self.db_name,self.symbol)
                            logging.info(f"Saved to DB: {self.identifier}, length of data added: {k_data}")
                            return True
                        else:
                            logging.error(f'data validation fail kucoin {self.symbol}')

                    else:
                        logging.error(f"Error in Yahoo updating DB: {self.identifier}")

            conn.close()

        except Exception as e:
            print('CRITICAL 1')
            print(e)
            logging.info(f'no db loaded for {self.db_name}, error: {e}, getting it from external source')
            try:
                start = datetime.today().date() - pd.Timedelta(days=401)
                end = datetime.today().date() - pd.Timedelta(days=1)
                start_date = datetime.strftime(start,'%Y-%m-%d')
                end_date = datetime.strftime(end,'%Y-%m-%d')
                coin_db = getKucoinData(self.symbol,start_date,end_date)
                if coin_db[0] == True:
                    new_data = coin_db[1]
                    new_data.sort_values(by=['Date'],ascending=True,inplace=True)
                    new_data['Date'] = new_data['Date'].apply(remove_hour)
                    if Data_Validation(new_data) == True:
                        TA_Calculate(new_data,self.db_name,self.symbol)
                        logging.info(f"Saved to DB: {self.identifier}, length of data added: {k_data}")
                        return True
                    else:
                        logging.error(f'data validation fail kucoin {self.symbol}')

                    
            except Exception as e:
                logging.error(f'no data loaded from kuCoin for {self.db_name}, error: {e}')
                try:
                    # get from yahoo
                    start = datetime.today().date() - pd.Timedelta(days=401)
                    end = datetime.today().date()
                    yahoo_data = getYahooData(self.symbol,start_date,end_date)
                    if yahoo_data[0] == True:
                        k_data = yahoo_data[1]
                        k_data.sort_values(by=['Date'],ascending=True,inplace=True)
                        k_data['Date'] = k_data['Date'].apply(remove_hour)
                        if Data_Validation(new_data) == True:
                            TA_Calculate(new_data,self.db_name,self.symbol)
                            logging.info(f"Saved to DB: {self.identifier}, length of data added: {k_data}")
                            return True
                        else:
                            logging.error(f'data validation fail kucoin {self.symbol}')

                        
                    else:
                        logging.error(f"Error in Yahoo updating DB: {self.identifier}")
                except Exception as e:
                    logging.error(f'No data from Yahoo for {self.db_name}')


# # # # # # # # # #  # # # # #  # # # # # #
# # # # # # # # # #  # # # # #  # # # # # 
# # 5.   Output Databases for Streamlit # # # 
# # # # # # # # # #  # # # # #  # # # # # #
# # # # # # # # # #  # # # # #  # # # # # #



if __name__ == "__main__":
    MapperLooper(asset_book)
    # asset_book is taken from top_500_with_mcap_stablecoins_excluded, which is taken from scrapper_crypto_database
    OUTPUT_DB_1 = top_500_with_mcap_stablecoins_excluded


    conn_top_500 = sqlite3.connect('/home/entukio/projects/scrapper_crypto_top/files/top_500_with_mcap_stablecoins_excluded.db')

    OUTPUT_DB_1.to_sql('top_500_with_mcap_stablecoins_excluded',conn_top_500,if_exists='replace',index=False)



    conn_top_500.close()
