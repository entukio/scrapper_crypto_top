from scraping_class import CryptoScraping
import time
def scrappy():
    scraper = CryptoScraping()
    scraper.scrap_all()
    scraper.save_to_database()
try:
    scrappy()
except:
    time.sleep(15)
    try:
        scrappy()
    except Exception as e:
        with open('log_scrapper_errors.txt','a') as f:
            f.write(str(e)+'\n')
