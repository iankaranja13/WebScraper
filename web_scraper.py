import logging
import mysql.connector
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import schedule
import time
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)

# MySQL database connection setup
def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host="localhost",      # e.g., "localhost" or IP address of the MySQL server
            user="root",           # e.g., "root"
            password="",           # your MySQL password
            database="stock_data"  # your database name (stock_data)
        )
        if connection.is_connected():
            logging.debug("Connected to the MySQL database")
            return connection
    except mysql.connector.Error as err:
        logging.error(f"Error: {err}")
        return None

# Function to store stock data into MySQL
def store_stock_data(stock_code, description, amount):
    connection = connect_to_db()
    if connection:
        cursor = connection.cursor()
        try:
            # Insert stock data into stock_prices table
            cursor.execute("""
                INSERT INTO stock_prices (date, stock_code, description, amount)
                VALUES (%s, %s, %s, %s)
            """, (datetime.now(), stock_code, description, amount))
            connection.commit()
            logging.debug("Stock data inserted into MySQL database.")
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
        finally:
            cursor.close()
            connection.close()

# Add a list of stock symbols to fetch
STOCKS_TO_FETCH = [
    {"symbol": "AAPL", "exchange": "NASDAQ"},  # Apple
    {"symbol": "GOOGL", "exchange": "NASDAQ"},  # Alphabet (Google)
    {"symbol": "MSFT", "exchange": "NASDAQ"},  # Microsoft
    {"symbol": "AMZN", "exchange": "NASDAQ"},  # Amazon
    {"symbol": "TSLA", "exchange": "NASDAQ"},  # Tesla
]

def fetch_stock_data_for_symbol(stock_symbol, exchange):
    logging.debug(f"Fetching stock data for {stock_symbol} on {exchange}...")

    # Set up options for headless mode
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Set up ChromeDriver path
    service = ChromeService(executable_path='C:/Users/Ian Karanja/.wdm/drivers/chromedriver/win64/132.0.6834.83/chromedriver-win32/chromedriver.exe')
    
    # Initialize WebDriver
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        # Generate URL for the stock
        url = f"https://www.google.com/finance/quote/{stock_symbol}:{exchange}?hl=en"
        logging.debug(f"Fetching URL: {url}")
        driver.get(url)

        # Use WebDriverWait to wait until the elements are present
        company_name = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.zzDege'))
        ).text
        logging.debug(f"Company Name: {company_name}")

        stock_price_str = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.YMlKec.fxKbKc'))
        ).text
        logging.debug(f"Stock Price: {stock_price_str}")

        # Clean the stock price to remove non-numeric characters and convert to float
        stock_price = ''.join(filter(str.isdigit, stock_price_str))  # Remove non-numeric characters
        stock_price = float(stock_price) / 100  # Convert cents to dollars if necessary

        # Use XPath to handle dynamic class names for the change element
        try:
            change = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//span[contains(@class, 'P2Luy') and contains(@class, 'Ez2loe') and contains(@class, 'ZYVHBb')]"))
            ).text
        except:
            change = "N/A"
            logging.debug("Stock change element not found.")
        
        logging.info(f"Company: {company_name}, Price: {stock_price}, Change: {change}")
        
        # Store the cleaned data in MySQL
        store_stock_data(stock_symbol, company_name, stock_price)
    
    except Exception as e:
        logging.error(f"Error fetching data for {stock_symbol}: {e}")
    
    finally:
        driver.quit()

def fetch_all_stock_data():
    logging.debug("Starting to fetch data for all stocks...")
    for stock in STOCKS_TO_FETCH:
        fetch_stock_data_for_symbol(stock["symbol"], stock["exchange"])
    logging.debug("Finished fetching data for all stocks.")

def main():
    # Schedule the task to run daily
    schedule.every().day.at("11:48").do(fetch_all_stock_data)
    
    logging.debug("Scheduled task initialized. Waiting for execution...")
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
