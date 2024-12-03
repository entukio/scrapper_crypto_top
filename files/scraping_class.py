import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import time
import sqlite3

class CryptoScraping:
    def __init__(self):
        self.scrapped_data=[]
    def scrap(self,i):
        print('Scrapping...',i)
        self.site = f'https://cryptoslate.com/coins/page/{i}'
        self.headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
        res = requests.get(self.site,headers=self.headers)
        try:
            soup = BeautifulSoup(res.text,'html.parser')
            tbody_element = soup.find('tbody')
            tr_elements = tbody_element.find_all('tr')
            p_elements_array = []
            for tr_html in tr_elements:
                # Parse the <tr> element as HTML
                #tr_soup = BeautifulSoup(tr_html, 'html.parser')
                # Find all <p> elements with class that includes "chakra-text" inside the <tr>
                p_elements = tr_html.find_all('td')
                # Convert the <p> elements to strings and append to the array
                p_elements_array.append([p for p in p_elements])
            for coin in range(len(p_elements_array)):
                # name
                name = p_elements_array[coin][1].text.strip()
                # price 
                price = p_elements_array[coin][2].text.strip()
                # mcap
                mcap = p_elements_array[coin][6].text.strip()
                #vol 24h
                vol24h = p_elements_array[coin][7].text.strip()
                #% ath
                from_ath = p_elements_array[coin][-1].text.strip()
                date = datetime.now()
                self.scrapped_data.append({'name':name, 'price':price, 'mcap':mcap, 'vol24h':vol24h, 'from_ath':from_ath, 'date':date})
                print(name, ': done')
        except Exception as e:
            print(e)
    def scrap_all(self):
        print('scrapping 6  pages...')
        for i in range(1,7):
            self.scrap(i)
            time.sleep(2)

    def save_to_database(self):
        print('saving to database')
        df = pd.DataFrame(self.scrapped_data)
        def format_finanace(x):
            return round(float(x.replace('$','').replace(',','')),10)
        df['price'] = df['price'].apply(format_finanace)
        df['mcap'] = df['mcap'].apply(format_finanace)
        df['vol24h'] = df['vol24h'].apply(format_finanace)
	#####
        df['id'] = df['name'].apply(lambda x: x.replace(' ','_'))
        df['symbol'] = df['name'].apply(lambda x: x.split(' ')[-1])
        df['date'] = df['date'].apply(lambda x : pd.to_datetime(x).date())

        #####
        df.sort_values(by=['mcap'],ascending=False,inplace=True)
        df['rank'] = [i+1 for i in range(len(df))]
        conn = sqlite3.connect('/home/entukio/projects/scrapper_crypto_top/files/scrapper_crypto_database.db')
        df.to_sql('top_crypto',conn,if_exists='append',index=False)
