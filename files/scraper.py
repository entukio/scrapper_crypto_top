from scraping_class import CryptoScraping

scraper = CryptoScraping()
scraper.scrap_all()
scraper.save_to_database()
