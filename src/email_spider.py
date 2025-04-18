import argparse
import csv
import re
import time
from urllib.parse import urlparse, urljoin
import scrapy
from bs4 import BeautifulSoup
import requests
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import signal
import sys
import logging
import os

# Configure structured logging with timestamps and thread names
logging.basicConfig(
    format='[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

# Thread-local storage for per-thread Selenium drivers
thread_local = threading.local()
# Keep track of all drivers to quit at the end
selenium_drivers = []

# Graceful shutdown: quit all Selenium drivers on termination signals
def handle_exit(signum, frame):
    logging.info(f"Received signal {signum}, shutting down...")
    for drv in selenium_drivers:
        try:
            drv.quit()
        except Exception:
            pass
    sys.exit(0)

# Register signal handlers for clean exit
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

def get_selenium_driver():
    """Return a thread-local Selenium driver, creating it on first access."""
    if not hasattr(thread_local, 'driver'):
        driver = setup_selenium()
        thread_local.driver = driver
        selenium_drivers.append(driver)
    return thread_local.driver

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Standalone spider class for potential Scrapy integration.
class EmailSpider(scrapy.Spider):
    name = "email_spider"

def is_valid_url(url):
    """Validate a URL by checking its scheme and network location."""
    try:
        result = urlparse(url)
        return bool(result.scheme and result.netloc)
    except Exception as e:
        logging.error(f"URL validation error: {e}")
        return False

def get_provider_website(row):
    """
    Return the provider website URL from column 13 if available.
    It must be non-empty, start with 'http', not contain 'google.com', and pass validation.
    """
    if len(row) > 13:
        candidate = row[13].strip()
        if candidate:
            logging.debug(f"Found candidate URL in column 13: {candidate}")
        if candidate and candidate.startswith("http") and "google.com" not in candidate.lower() and is_valid_url(candidate):
            logging.info(f"Using provider website: {candidate}")
            return candidate
    logging.warning("No valid provider website found in the expected column.")
    return ""

def setup_selenium():
    """Initialize and return a headless Selenium Chrome WebDriver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--ignore-certificate-errors")
    # Optionally, add --no-sandbox if needed:
    # chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def extract_emails_from_html(html):
    """Extract emails from HTML while ignoring common image extensions."""
    emails = set()
    # Refined regex: ensure we match typical email patterns and exclude common image extensions.
    email_regex = r'(?i)\b(?!.*\.(?:png|jpg|jpeg|gif|bmp))([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})\b'
    for match in re.findall(email_regex, html):
        # Additional filtering: check that email does not include suspicious parts (e.g., 'logo', 'icon')
        if not any(word in match.lower() for word in ['logo', 'icon', 'banner']):
            emails.add(match)
    # Check for mailto links
    soup = BeautifulSoup(html, 'html.parser')
    for a in soup.find_all('a', href=re.compile(r'^mailto:', re.I)):
        email = a.get('href').split("mailto:")[-1].split('?')[0]
        if email and not any(word in email.lower() for word in ['logo', 'icon']):
            emails.add(email)
    return emails

def click_contact_page(driver):
    """Attempt to locate and click a 'contact' or 'about' link."""
    try:
        # Search for links with text containing 'contact' or 'about'
        links = driver.find_elements(By.XPATH, "//*[contains(translate(text(),'CONTACT','contact'),'contact') or contains(translate(text(),'ABOUT','about'),'about')]")
        if links:
            for link in links:
                candidate = link.get_attribute('href')
                if candidate and is_valid_url(candidate):
                    logging.info(f"Clicking link: {candidate}")
                    driver.get(candidate)
                    try:
                        WebDriverWait(driver, 10).until(
                            lambda d: d.execute_script("return document.readyState") == "complete"
                        )
                    except Exception:
                        pass
                    return driver.page_source
    except Exception as e:
        logging.error(f"Error clicking contact/about link: {e}")
    # Fallback: try appending /contact to base URL
    try:
        base_url = driver.current_url.rstrip('/')
        fallback_url = base_url + "/contact"
        logging.info(f"Trying fallback URL: {fallback_url}")
        driver.get(fallback_url)
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except Exception:
            pass
        return driver.page_source
    except Exception as e:
        logging.error(f"Fallback error: {e}")
    return ""


def scrape_emails_with_selenium(driver, url):
    """
    Load the base page with Selenium and attempt to extract emails.
    If no emails are found, try navigating to a contact page.
    """
    emails_found = set()
    try:
        logging.info(f"Loading base page: {url}")
        driver.get(url)
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except Exception:
            pass
    except Exception as e:
        logging.error(f"Error loading base page {url}: {e}")
        return emails_found

    base_html = driver.page_source
    logging.debug(f"Base page length: {len(base_html)}")
    emails_found.update(extract_emails_from_html(base_html))
    if emails_found:
        logging.info(f"Emails found on base page: {emails_found}")
    else:
        logging.info("No emails found on base page. Trying to click a Contact link...")
        contact_html = click_contact_page(driver)
        if contact_html:
            emails_found.update(extract_emails_from_html(contact_html))
            if emails_found:
                logging.info(f"Emails found on contact page: {emails_found}")
            else:
                logging.info("No emails found on contact page.")
        else:
            logging.info("No contact page was detected.")
    return emails_found

def scrape_emails_from_website(website):
    """
    Use Selenium exclusively to load the provider website,
    attempt to extract emails from the base page,
    and if necessary, navigate to a Contact page.
    """
    emails_found = set()
    driver = None
    try:
        driver = setup_selenium()
        emails_found = scrape_emails_with_selenium(driver, website)
    except Exception as e:
        logging.error(f"Error with Selenium for {website}: {e}")
    finally:
        if driver is not None:
            driver.quit()
    return ";".join(emails_found) if emails_found else ""

def fetch_emails_with_requests(url, timeout=10):
    """Attempt to fetch the page via HTTP and extract emails without rendering."""
    try:
        logging.info(f"Fetching via requests: {url}")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "  
                "AppleWebKit/537.36 (KHTML, like Gecko) "  
                "Chrome/88.0.4324.96 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=timeout)
        html = resp.text
        logging.debug(f"Requests page length: {len(html)}")
        emails = extract_emails_from_html(html)
        if emails:
            logging.info(f"Emails found via requests: {emails}")
        else:
            logging.info("No emails found via requests.")
        return emails
    except Exception as e:
        logging.error(f"Error fetching via requests for {url}: {e}")
        return set()

def get_emails_for_website(url):
    """Try HTTP fetch for speed, then fall back to Selenium rendering if needed."""
    emails = fetch_emails_with_requests(url)
    if emails:
        return emails
    # Fallback to Selenium using a thread-local driver
    driver = get_selenium_driver()
    return scrape_emails_with_selenium(driver, url)

def process_csv(input_file, output_file, max_workers=3, force=False):
    """Read the input CSV (tab-delimited), process in parallel threads, and write results."""
    # Load existing results if not forcing a full scrape
    output_rows = []
    processed_websites = set()
    if not force and os.path.exists(output_file):
        try:
            with open(output_file, newline='', encoding='utf-8') as outf:
                reader_out = csv.DictReader(outf)
                existing_rows = list(reader_out)
            output_rows = existing_rows
            processed_websites = set(r['website'] for r in existing_rows if r.get('website'))
            logging.info(f"Loaded {len(existing_rows)} existing row(s) from '{output_file}', will skip these websites on resume.")
        except Exception as e:
            logging.error(f"Error loading existing output '{output_file}': {e}")
    else:
        if force:
            logging.info("Force mode enabled: ignoring existing output and re-scraping all entries.")
    # Read and prepare tasks, skipping already processed entries
    tasks = []
    with open(input_file, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        headers = next(reader, None)
        if headers is None:
            logging.warning("Input CSV is empty.")
            return
        for row in reader:
            if not any(row) or len(row) < 14:
                logging.warning(f"Skipping row due to insufficient data: {row}")
                continue
            website = get_provider_website(row)
            if not website:
                logging.warning(f"Skipping row because no valid non-Google website found: {row}")
                continue
            clinic_name = row[1].strip()
            address = row[8].strip()
            phone = row[12].strip()
            # Skip if already processed
            if website in processed_websites:
                logging.debug(f"Skipping already processed website: {website}")
                continue
            tasks.append((clinic_name, website, address, phone))
    logging.info(f"Starting threaded processing with {max_workers} workers and {len(tasks)} tasks...")
    output_rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single, t): t for t in tasks}
        for future in concurrent.futures.as_completed(futures):
            try:
                rows = future.result()
                output_rows.extend(rows)
            except Exception as e:
                logging.error(f"Error in task {futures[future]}: {e}")
    # Clean up any Selenium drivers created
    for drv in selenium_drivers:
        try:
            drv.quit()
        except Exception:
            pass
    # Write results to CSV
    with open(output_file, "w", newline='', encoding='utf-8') as outfile:
        fieldnames = ["clinic_name", "website", "address", "phone", "email_addresses"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in output_rows:
            writer.writerow(row)
    logging.info(f"Processed {len(output_rows)} valid row(s) and saved to '{output_file}'.")

def process_single(task):
    clinic_name, website, address, phone = task
    logging.info(f"Starting processing {clinic_name} ({website})")
    start_time = time.time()
    emails_set = get_emails_for_website(website)
    duration = time.time() - start_time
    emails_scraped = ";".join(emails_set) if emails_set else ""
    logging.info(
        f"Finished processing {clinic_name} ({website}) in {duration:.2f}s, emails: {emails_scraped or 'none'}"
    )
    emails_list = [e.strip() for e in emails_scraped.split(';') if e.strip()]
    rows_out = []
    if emails_list:
        for email in emails_list:
            rows_out.append({
                "clinic_name": clinic_name,
                "website": website,
                "address": address,
                "phone": phone,
                "email_addresses": email
            })
    else:
        rows_out.append({
            "clinic_name": clinic_name,
            "website": website,
            "address": address,
            "phone": phone,
            "email_addresses": ""
        })
    return rows_out

def main():
    parser = argparse.ArgumentParser(
        description="Aggregate clinic CSV data and scrape emails from provider websites using an advanced Selenium approach."
    )
    parser.add_argument("input_csv", type=str, help="Input CSV file with clinic entries")
    parser.add_argument("-o", "--output", dest="output_csv", type=str,
                        default="scraped_emails.csv",
                        help="Output CSV file for scraped emails (default: scraped_emails.csv)")
    parser.add_argument("-f", "--force", dest="force", action="store_true",
                        help="Force re-scraping all entries, ignoring existing output file")
    parser.add_argument("-w", "--workers", dest="workers", type=int, default=3,
                        help="Number of parallel worker threads (default: 3)")
    args = parser.parse_args()
    try:
        process_csv(
            args.input_csv,
            args.output_csv,
            max_workers=args.workers,
            force=args.force
        )
    except KeyboardInterrupt:
        # Handle Ctrl+C from user
        handle_exit(signal.SIGINT, None)

if __name__ == '__main__':
    main()
