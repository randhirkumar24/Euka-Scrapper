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
    'BATCH_SIZE': 50,           # Reduced batch size for more frequent saves
    'SCROLL_WAIT': 1.5,         # Increased base wait time
    'MAX_RETRIES': 5,           # Maximum number of retries
    'TIMEOUT': 20,              # Increased timeout
    'MAX_WORKERS': 4,           # Maximum number of parallel workers
    'MASTER_FILE': 'master_localities.xlsx',
    'SCROLL_STEP': 500,         # Reduced scroll step for better reliability
    'MAX_SCROLL_ATTEMPTS': 5,
    'SCROLL_MULTIPLIER': 1.5,
    'PROGRESS_FILE': 'scraping_progress.json',
    'RECOVERY_WAIT': 60,        # Increased recovery wait time
    'MAX_ERRORS': 5,           # Increased max errors
    'RATE_LIMIT_PAUSE': 300,    # 5 minutes pause if rate limited
    'SCROLL_PAUSE_INTERVAL': 10,  # Save progress every 10 scrolls
    # City-specific minimum localities thresholds
    'CITY_THRESHOLDS': {
        'mumbai': 3000,    # Mumbai has 3000+ localities
        'delhi': 2500,     # Delhi has 2500+ localities
        'bangalore': 2000, # Bangalore has 2000+ localities
        'default': 500     # Default threshold for other cities
    }
}

def load_progress():
    """
    Load progress from the progress file.
    """
    try:
        if os.path.exists(CONFIG['PROGRESS_FILE']):
            with open(CONFIG['PROGRESS_FILE'], 'r') as f:
                return json.load(f)
    except Exception as e:
        logging.warning(f"Could not load progress file: {str(e)}")
    return {'completed_urls': [], 'partial_data': {}}

def save_progress(completed_urls, current_url=None, seen_titles=None):
    """
    Save progress to the progress file.
    """
    try:
        progress = {
            'completed_urls': completed_urls,
            'partial_data': {}
        }
        if current_url and seen_titles:
            progress['partial_data'] = {
                'url': current_url,
                'seen_titles': list(seen_titles)
            }
        with open(CONFIG['PROGRESS_FILE'], 'w') as f:
            json.dump(progress, f)
    except Exception as e:
        logging.error(f"Error saving progress: {str(e)}")

def save_to_excel(data, folder_name, file_index, city_name):
    """
    Saves data to both individual Excel file and master Excel file.
    """
    try:
        # Save to individual city file
        file_name = os.path.join(folder_name, f"localities_{file_index}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        wb = Workbook()
        ws = wb.active
        
        # Add headers
        ws.append(['Locality Name', 'City', 'Extraction Time'])
        
        # Add data with timestamp and city name
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for row in data:
            ws.append([row, city_name, current_time])
            
        wb.save(file_name)
        logging.info(f"Saved {len(data)} entries to {file_name}")

        # Update master file
        update_master_file(data, city_name)

    except Exception as e:
        logging.error(f"Error saving Excel file: {str(e)}")
        raise

def update_master_file(data, city_name):
    """
    Updates the master Excel file with new locality data.
    """
    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        new_data = [[locality, city_name, current_time] for locality in data]
        
        if os.path.exists(CONFIG['MASTER_FILE']):
            # Load existing master file
            df_existing = pd.read_excel(CONFIG['MASTER_FILE'])
            df_new = pd.DataFrame(new_data, columns=['Locality Name', 'City', 'Extraction Time'])
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            
            # Remove duplicates keeping the latest entry
            df_combined = df_combined.sort_values('Extraction Time').drop_duplicates(
                subset=['Locality Name', 'City'], 
                keep='last'
            )
        else:
            # Create new master file
            df_combined = pd.DataFrame(new_data, columns=['Locality Name', 'City', 'Extraction Time'])
        
        # Save master file
        df_combined.to_excel(CONFIG['MASTER_FILE'], index=False)
        logging.info(f"Updated master file with {len(data)} entries for {city_name}")
        
    except Exception as e:
        logging.error(f"Error updating master file: {str(e)}")
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

def check_for_rate_limit(driver):
    """
    Check if we're being rate limited or blocked.
    """
    try:
        # Check for common rate limit indicators
        rate_limit_indicators = [
            "too many requests",
            "rate limit exceeded",
            "please try again later",
            "access denied",
            "blocked"
        ]
        page_source = driver.page_source.lower()
        for indicator in rate_limit_indicators:
            if indicator in page_source:
                return True
                
        # Check if the page is blank or missing expected elements
        if not driver.find_elements(By.CLASS_NAME, "loc-card__title"):
            return True
            
        return False
    except Exception:
        return False

def scroll_page(driver):
    """
    Simulates pressing End key for very fast scrolling.
    """
    try:
        # Get current height
        current_height = driver.execute_script("return document.body.scrollHeight")
        
        # Scroll to bottom of page (like pressing End key)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # Very short delay to allow content to load
        time.sleep(0.5)
        
        # Get new height after scroll and content load
        new_height = driver.execute_script("return document.body.scrollHeight")
        
        # Return True if the page height increased (meaning more content loaded)
        return new_height > current_height
    except Exception as e:
        logging.warning(f"Scroll operation failed: {str(e)}")
        return False

def get_min_localities_threshold(city_name):
    """
    Get the minimum expected localities for a specific city.
    """
    city_key = city_name.lower()
    return CONFIG['CITY_THRESHOLDS'].get(city_key, CONFIG['CITY_THRESHOLDS']['default'])

def reset_progress():
    """
    Reset the progress file to start fresh.
    """
    if os.path.exists(CONFIG['PROGRESS_FILE']):
        os.remove(CONFIG['PROGRESS_FILE'])
        logging.info("Reset progress file for fresh start")

def merge_excel_files(folder_name, city_name):
    """
    Merges all Excel files in the folder into a single merged file.
    """
    try:
        all_data = []
        merged_file = os.path.join(folder_name, f"{city_name}_merged_localities.xlsx")
        
        # Read all Excel files in the folder
        for file in os.listdir(folder_name):
            if file.startswith("localities_") and file.endswith(".xlsx"):
                file_path = os.path.join(folder_name, file)
                df = pd.read_excel(file_path)
                all_data.extend(df.values.tolist())
        
        # Remove duplicates while preserving order
        seen = set()
        unique_data = []
        for item in all_data:
            if item[0] not in seen:  # Check first column (locality name)
                seen.add(item[0])
                unique_data.append(item)
        
        # Create new Excel file with unique data
        df_merged = pd.DataFrame(unique_data, columns=['Locality Name', 'City', 'Extraction Time'])
        df_merged.to_excel(merged_file, index=False)
        logging.info(f"Created merged file with {len(unique_data)} unique localities: {merged_file}")
        
    except Exception as e:
        logging.error(f"Error merging Excel files: {str(e)}")

def save_and_merge_data(data, folder_name, city_name):
    """
    Saves new data directly to a single merged file, replacing the old one.
    """
    try:
        merged_file = os.path.join(folder_name, f"{city_name}_localities.xlsx")
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        new_data = [[locality, city_name, current_time] for locality in data]
        
        if os.path.exists(merged_file):
            # Load existing data and append new data
            df_existing = pd.read_excel(merged_file)
            df_new = pd.DataFrame(new_data, columns=['Locality Name', 'City', 'Extraction Time'])
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            
            # Remove duplicates keeping the latest entry
            df_combined = df_combined.sort_values('Extraction Time').drop_duplicates(
                subset=['Locality Name'], 
                keep='last'
            )
        else:
            # Create new file
            df_combined = pd.DataFrame(new_data, columns=['Locality Name', 'City', 'Extraction Time'])
        
        # Save merged file
        df_combined.to_excel(merged_file, index=False)
        logging.info(f"Updated merged file with {len(data)} new entries. Total unique localities: {len(df_combined)}")
        
        # Update master file
        update_master_file(data, city_name)
        
    except Exception as e:
        logging.error(f"Error saving/merging data: {str(e)}")
        raise

def scrape_locality(url, folder_name, resume_data=None):
    """
    Scrapes locality information with continuous merging of data.
    """
    driver = None
    retry_count = 0
    error_count = 0
    city_name = url.split("localities-in-")[-1].replace("/", "").capitalize()
    min_expected = get_min_localities_threshold(city_name)

    # Initialize or resume progress
    seen_titles = set(resume_data) if resume_data else set()
    batch_data = []
    scroll_count = 0
    consecutive_same_height = 0

    while error_count < CONFIG['MAX_ERRORS']:
        try:
            driver = setup_driver()
            logging.info(f"Starting/Resuming scraping for {city_name}")
            logging.info(f"Already collected localities: {len(seen_titles)}")
            logging.info(f"Minimum expected localities for {city_name}: {min_expected}")

            os.makedirs(folder_name, exist_ok=True)

            # Load page with retries
            while retry_count < CONFIG['MAX_RETRIES']:
                try:
                    driver.get(url)
                    WebDriverWait(driver, CONFIG['TIMEOUT']).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, "loc-card__title"))
                    )
                    break
                except TimeoutException:
                    retry_count += 1
                    logging.warning(f"Timeout loading page, retry {retry_count}/{CONFIG['MAX_RETRIES']}")
                    if retry_count == CONFIG['MAX_RETRIES']:
                        raise

            no_new_data_count = 0
            last_count = len(seen_titles)
            last_height = driver.execute_script("return document.body.scrollHeight")

            while no_new_data_count < 5:  # Increased tolerance for no new data
                # Extract locality titles
                titles = driver.find_elements(By.CLASS_NAME, "loc-card__title")
                initial_count = len(seen_titles)

                for title in titles:
                    title_text = title.text.strip()
                    if title_text and title_text not in seen_titles:
                        seen_titles.add(title_text)
                        batch_data.append(title_text)

                # Save and merge batch if size reached
                if len(batch_data) >= CONFIG['BATCH_SIZE']:
                    save_and_merge_data(batch_data, folder_name, city_name)
                    batch_data = []
                    save_progress([], url, seen_titles)
                    logging.info(f"Progress saved. Current localities: {len(seen_titles)}")

                # Scroll to bottom and check if more content loaded
                more_content = scroll_page(driver)
                scroll_count += 1

                # Get current height
                new_height = driver.execute_script("return document.body.scrollHeight")

                # Check if we're really at the bottom
                if new_height == last_height:
                    consecutive_same_height += 1
                    if consecutive_same_height >= 3:  # If height hasn't changed in 3 attempts
                        if len(seen_titles) == initial_count:  # And no new data
                            no_new_data_count += 1
                else:
                    consecutive_same_height = 0
                    no_new_data_count = 0

                last_height = new_height

                # Show progress
                if len(seen_titles) > last_count:
                    logging.info(f"Found {len(seen_titles)} localities in {city_name}")
                    last_count = len(seen_titles)

                # If we haven't found any new data in a while, try refreshing the page
                if no_new_data_count == 3:
                    logging.info("No new data found, refreshing page...")
                    driver.refresh()
                    time.sleep(2)  # Wait for page to reload
                    WebDriverWait(driver, CONFIG['TIMEOUT']).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, "loc-card__title"))
                    )

            # Save remaining data
            if batch_data:
                save_and_merge_data(batch_data, folder_name, city_name)

            # Verify we got enough localities
            if len(seen_titles) < min_expected:
                logging.warning(f"Found only {len(seen_titles)} localities for {city_name}, expected at least {min_expected}. Retrying...")
                raise Exception(f"Insufficient localities found for {city_name}")

            logging.info(f"Completed scraping for {city_name}. Total localities found: {len(seen_titles)}")
            return len(seen_titles)

        except Exception as e:
            error_count += 1
            logging.error(f"Error scraping {url} (attempt {error_count}): {str(e)}")
            if error_count < CONFIG['MAX_ERRORS']:
                logging.info(f"Waiting {CONFIG['RECOVERY_WAIT']} seconds before retrying...")
                time.sleep(CONFIG['RECOVERY_WAIT'])
            else:
                logging.error(f"Max errors reached for {url}, skipping...")
                raise
        finally:
            if driver:
                driver.quit()

def scrape_multiple_localities_sequential(urls):
    """
    Scrapes multiple localities sequentially with resume capability.
    """
    results = {}
    failed_urls = []
    total_urls = len(urls)
    
    # Load previous progress
    progress = load_progress()
    completed_urls = set(progress.get('completed_urls', []))
    partial_data = progress.get('partial_data', {})

    for index, url in enumerate(urls, 1):
        if url in completed_urls:
            logging.info(f"Skipping already completed URL: {url}")
            continue

        try:
            logging.info(f"\n=== Processing URL {index}/{total_urls} ===")
            logging.info(f"Current URL: {url}")
            
            city_name = url.split("localities-in-")[-1].replace("/", "").capitalize()
            folder_name = f"localities_in_{city_name}"
            
            # Add delay between URLs
            if index > 1:
                logging.info("Waiting 5 seconds before processing next URL...")
                time.sleep(5)
            
            # Resume from partial data if available
            resume_data = None
            if partial_data.get('url') == url:
                resume_data = partial_data.get('seen_titles', [])
                logging.info(f"Resuming from previous session with {len(resume_data)} localities")

            # Process the URL
            count = scrape_locality(url, folder_name, resume_data)
            results[url] = count
            completed_urls.add(url)
            save_progress(list(completed_urls))
            
            # Show progress
            logging.info(f"\n--- Progress Update ---")
            logging.info(f"Completed: {index}/{total_urls} URLs")
            logging.info(f"Success rate: {len(results)}/{index}")
            logging.info(f"Latest URL: {url} - Found {count} localities")
            
        except Exception as e:
            failed_urls.append(url)
            logging.error(f"Failed to scrape {url}: {str(e)}")
            logging.info(f"Moving to next URL...")

    # Final report
    logging.info("\n=== Final Scraping Summary ===")
    logging.info(f"Total URLs processed: {total_urls}")
    logging.info(f"Successfully scraped: {len(results)} URLs")
    logging.info(f"Failed to scrape: {len(failed_urls)} URLs")
    
    if results:
        logging.info("\nSuccessful Results:")
        for url, count in results.items():
            city = url.split("localities-in-")[-1].replace("/", "").capitalize()
            logging.info(f"- {city}: {count} localities")
    
    if failed_urls:
        logging.info("\nFailed URLs:")
        for url in failed_urls:
            logging.info(f"- {url}")
    
    return results, failed_urls

# List of URLs to scrape
urls = [
        "https://www.magicbricks.com/localities-in-gurgaon/",

        
    # Add more URLs here
]

if __name__ == "__main__":
    logging.info("Starting locality extraction process")
    logging.info(f"Total cities to process: {len(urls)}")
    
    # Reset progress to start fresh
    reset_progress()
    
    start_time = time.time()
    results, failed_urls = scrape_multiple_localities_sequential(urls)
    
    end_time = time.time()
    total_time = end_time - start_time
    hours = int(total_time // 3600)
    minutes = int((total_time % 3600) // 60)
    seconds = total_time % 60
    
    logging.info(f"\nTotal execution time: {hours}h {minutes}m {seconds:.2f}s")