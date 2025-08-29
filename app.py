import os
import logging
import random
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException
from openpyxl import Workbook, load_workbook
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime
import pandas as pd


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

# Configuration
CONFIG = {
    'TIMEOUT': 30,              # Timeout for page loading
    'MAX_RETRIES': 3,           # Maximum number of retries
    'WAIT_TIME': 5,             # Wait time for page to load completely
    'OUTPUT_FILE': 'euka_brands_data.xlsx'
}

def save_to_excel(data):
    """
    Saves brand data to Excel file.
    """
    try:
        wb = Workbook()
        ws = wb.active
        
        # Add headers
        ws.append(['Brand Name', 'Number of Products', 'Total Sales', 'Extraction Time'])
        
        # Add data with timestamp
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for brand_name, num_products, total_sales in data:
            ws.append([brand_name, num_products, total_sales, current_time])
            
        wb.save(CONFIG['OUTPUT_FILE'])
        logging.info(f"Saved {len(data)} brands to {CONFIG['OUTPUT_FILE']}")

    except Exception as e:
        logging.error(f"Error saving Excel file: {str(e)}")
        raise



def setup_driver():
    """
    Sets up and returns a configured Chrome WebDriver using Chrome profile 6.
    """
    # Use the ChromeDriver in the project folder
    driver_path = os.path.join(os.path.dirname(__file__), "chromedriver-win64", "chromedriver.exe")
    
    if not os.path.exists(driver_path):
        raise Exception(f"ChromeDriver not found at: {driver_path}")
    
    logging.info(f"Using ChromeDriver: {driver_path}")
    
    service = Service(driver_path)
    options = webdriver.ChromeOptions()
    
    # Create a separate user data directory for this script to avoid conflicts
    script_user_data_dir = os.path.join(os.path.dirname(__file__), "chrome_profile_6")
    os.makedirs(script_user_data_dir, exist_ok=True)
    
    # Copy profile 6 from the main Chrome user data if it exists
    main_user_data_dir = os.path.expanduser("~\\AppData\\Local\\Google\\Chrome\\User Data")
    main_profile_6_dir = os.path.join(main_user_data_dir, "Profile 6")
    script_profile_6_dir = os.path.join(script_user_data_dir, "Profile 6")
    
    if os.path.exists(main_profile_6_dir) and not os.path.exists(script_profile_6_dir):
        import shutil
        try:
            shutil.copytree(main_profile_6_dir, script_profile_6_dir)
            logging.info(f"Copied Profile 6 from main Chrome to script directory")
        except Exception as e:
            logging.warning(f"Could not copy Profile 6: {str(e)}")
    
    # Use the script-specific user data directory
    options.add_argument(f"--user-data-dir={script_user_data_dir}")
    options.add_argument("--profile-directory=Profile 6")
    
    # Other Chrome options
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-features=TranslateUI")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-blink-features=AutomationControlled")  # Hide automation
    options.add_experimental_option("excludeSwitches", ["enable-automation"])  # Hide automation
    options.add_experimental_option('useAutomationExtension', False)  # Hide automation
    options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90, 100)}.0.0.0 Safari/537.36")
    
    logging.info(f"Using Chrome profile 6 from: {script_user_data_dir}")
    
    return webdriver.Chrome(service=service, options=options)



def scrape_euka_brands(url):
    """
    Scrapes brand information from Euka website.
    """
    driver = None
    retry_count = 0
    
    while retry_count < CONFIG['MAX_RETRIES']:
        try:
            driver = setup_driver()
            logging.info(f"Starting scraping for Euka brands from: {url}")
            
            # Load page with retries
            try:
                driver.get(url)
                logging.info("Page loaded, waiting for content to appear...")
                
                # Wait for the page to load completely
                time.sleep(CONFIG['WAIT_TIME'])
                
                # Wait for table rows to be present
                WebDriverWait(driver, CONFIG['TIMEOUT']).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr.group"))
                )
                
                logging.info("Page loaded successfully, extracting brand data...")
                
            except TimeoutException:
                retry_count += 1
                logging.warning(f"Timeout loading page, retry {retry_count}/{CONFIG['MAX_RETRIES']}")
                if retry_count == CONFIG['MAX_RETRIES']:
                    raise Exception("Failed to load page after maximum retries")
                continue
            
            # Extract brand data
            brands_data = []
            
            # Find all table rows
            table_rows = driver.find_elements(By.CSS_SELECTOR, "tr.group")
            logging.info(f"Found {len(table_rows)} brand rows")
            
            for row in table_rows:
                try:
                    # Extract brand name (first td with button)
                    brand_button = row.find_element(By.CSS_SELECTOR, "td button")
                    brand_name = brand_button.text.strip()
                    
                    # Extract number of products (second td)
                    tds = row.find_elements(By.CSS_SELECTOR, "td")
                    if len(tds) >= 2:
                        num_products = tds[1].text.strip()
                    else:
                        num_products = "N/A"
                    
                    # Extract total sales (third td)
                    if len(tds) >= 3:
                        total_sales = tds[2].text.strip()
                    else:
                        total_sales = "N/A"
                    
                    if brand_name:
                        brands_data.append((brand_name, num_products, total_sales))
                        logging.info(f"Extracted: {brand_name} - {num_products} products - {total_sales}")
                    
                except Exception as e:
                    logging.warning(f"Error extracting data from row: {str(e)}")
                    continue
            
            logging.info(f"Successfully extracted data for {len(brands_data)} brands")
            
            # Save to Excel
            if brands_data:
                save_to_excel(brands_data)
                return len(brands_data)
            else:
                raise Exception("No brand data found on the page")
                
        except Exception as e:
            retry_count += 1
            logging.error(f"Error scraping {url} (attempt {retry_count}): {str(e)}")
            if retry_count < CONFIG['MAX_RETRIES']:
                logging.info("Retrying...")
                time.sleep(5)
            else:
                logging.error(f"Max retries reached for {url}")
                raise
        finally:
            if driver:
                driver.quit()

# URL to scrape
url = "https://app.euka.ai/social-intelligence/categories/7"

if __name__ == "__main__":
    logging.info("Starting Euka brand extraction process")
    logging.info(f"Target URL: {url}")
    
    start_time = time.time()
    
    try:
        count = scrape_euka_brands(url)
        logging.info(f"Successfully extracted data for {count} brands")
    except Exception as e:
        logging.error(f"Failed to scrape brands: {str(e)}")
    
    end_time = time.time()
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = total_time % 60
    
    logging.info(f"\nTotal execution time: {minutes}m {seconds:.2f}s")