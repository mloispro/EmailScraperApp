import argparse
import csv
import re
import time
from urllib.parse import urlparse, urljoin
try:
    import scrapy
except ImportError:
    scrapy = None
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None
try:
    import requests
except ImportError:
    requests = None
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import signal
import sys
import logging
import os
import json
# Import parse_address and is_valid_url from utils
try:
    from src.utils import parse_address, is_valid_url
except ImportError:
    from utils import parse_address, is_valid_url

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

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
except ImportError:
    webdriver = None
    Options = None
    By = None
    WebDriverWait = None
    EC = None

# Standalone spider class for potential Scrapy integration.
if scrapy:
    class EmailSpider(scrapy.Spider):
        name = "email_spider"
else:
    EmailSpider = None


def get_provider_website(row):
    """
    Return the provider website URL from column 13 if available.
    It must be non-empty, start with 'http', not contain 'google.com', and pass validation.
    """
    if len(row) > 13:
        candidate = row[13].strip()
        if candidate and candidate.startswith("http") and "google.com" not in candidate.lower():
            return candidate
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

# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC

def scrape_google_maps_address(driver, maps_url):
    """
    Load a Google Maps place URL and return its full formatted address
    (as shown in the “Copy address” tooltip).
    """
    driver.get(maps_url)
    # wait until the “Copy address” button appears
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'button[data-tooltip="Copy address"]'))
    )
    btn = driver.find_element(By.CSS_SELECTOR, 'button[data-tooltip="Copy address"]')
    aria = btn.get_attribute("aria-label")  
    # aria-label is like "Copy address: 1915 Lyndale Ave S, Minneapolis, MN 55403, USA"
    if aria and ":" in aria:
        return aria.split(":", 1)[1].strip()
    return None



# precompile all our detectors
_maps_re     = re.compile(r'https?://(?:www\.)?google\.[^/]+/maps/place', re.I)
_url_re      = re.compile(r'https?://\S+', re.I)
_phone_re    = re.compile(r'\(?\d{3}\)?[ \-]?\d{3}[ \-]?\d{4}')
_email_re    = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.I)
_address_re  = re.compile(
    r'\d+\s+\d*\s*(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Way|Pl|Place)\b',
    re.I
)

def parse_row(row):
    """Return (maps_url, clinic_name, website, address, phone, email) from any CSV row."""
    maps_url = clinic_name = website = address = phone = email = ''
    seen = set()

    # first pass: pick URLs, phone, email, address
    for cell in row:
        text = cell.strip()
        if not text:
            continue

        if not maps_url and _maps_re.search(text):
            maps_url = text; seen.add(text); continue

        if not website and _url_re.match(text) and 'google.com/maps' not in text:
            website = text; seen.add(text); continue

        if not phone:
            m = _phone_re.search(text)
            if m:
                phone = m.group(); seen.add(text); continue

        if not email:
            m = _email_re.search(text)
            if m:
                email = m.group(); seen.add(text); continue

        if not address and _address_re.search(text):
            address = text; seen.add(text); continue

    # second pass: clinic name = first leftover cell
    for cell in row:
        text = cell.strip()
        if text and text not in seen:
            clinic_name = text
            break

    return maps_url, clinic_name, website, address, phone, email


def process_csv(input_file, output_file, max_workers=3, force=False):
    """Read CSV of unknown shape, auto-detect fields, then scrape."""
    output_rows = []
    processed_websites = set()

    if not force and os.path.exists(output_file):
        try:
            with open(output_file, newline='', encoding='utf-8') as outf:
                existing = list(csv.DictReader(outf))
            output_rows = existing
            processed_websites = {r['WEBSITE'] for r in existing if r.get('WEBSITE')}
            logging.info(f"Loaded {len(existing)} existing row(s).")
        except Exception as e:
            logging.error(f"Error loading '{output_file}': {e}")
    elif force:
        logging.info("Force mode enabled.")

    tasks = []
    with open(input_file, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        next(reader, None)  # skip header

        for row in reader:
            if not any(row):
                continue

            maps_url, clinic_name, website, address, phone, email = parse_row(row)
            if not maps_url or not clinic_name or not website:
                logging.warning(f"Skipping row (missing maps, name, or site): {row}")
                continue
            if website in processed_websites:
                logging.debug(f"Already processed: {website}")
                continue

            tasks.append((clinic_name, maps_url, website, address, phone, email))

    logging.info(f"Queuing {len(tasks)} tasks with {max_workers} workers.")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single, t): t for t in tasks}
        for fut in concurrent.futures.as_completed(futures):
            try:
                output_rows.extend(fut.result())
            except Exception as e:
                logging.error(f"Task {futures[fut]} error: {e}")

    for drv in selenium_drivers:
        try: drv.quit()
        except: pass

    # now include CITY in the header
    with open(output_file, "w", newline='', encoding='utf-8') as outf:
        fieldnames = ["FIRSTNAME", "WEBSITE", "ADDRESSS", "CITY", "PHONE_NUMBER", "EMAIL"]
        writer = csv.DictWriter(outf, fieldnames=fieldnames)
        writer.writeheader()
        for r in output_rows:
            writer.writerow(r)

    logging.info(f"Saved {len(output_rows)} rows to '{output_file}'.")

                                

def process_single(task):
    """
    task = (clinic_name, maps_url, website, address, phone, csv_email)
    returns a list of dicts to write out, now including CITY.
    """
    clinic_name, maps_url, website, address, phone, csv_email = task
    driver = get_selenium_driver()

    # enrich the address via Google Maps, if possible
    try:
        full_address = scrape_google_maps_address(driver, maps_url)
        address = full_address or address
    except Exception as e:
        logging.warning(f"Couldn’t scrape address for {maps_url}: {e}")

    logging.info(f"{clinic_name}: using address → {address}")

    # parse street, city, state
    street, city, state = parse_address(address)

    # decide whether to use the CSV-parsed email or scrape
    if csv_email:
        emails_set = {csv_email}
        logging.info(f"{clinic_name}: using parsed email {csv_email}")
    else:
        logging.info(f"{clinic_name}: scraping emails from {website}")
        start = time.time()
        emails_set = get_emails_for_website(website)
        dur = time.time() - start
        logging.info(f"  → found {emails_set or 'none'} in {dur:.1f}s")

    rows_out = []
    if emails_set:
        for e in emails_set:
            rows_out.append({
                "FIRSTNAME":    clinic_name,
                "WEBSITE":      website,
                "ADDRESSS":     address,
                "CITY":         city,
                "PHONE_NUMBER": phone,
                "EMAIL":        e
            })
    else:
        rows_out.append({
            "FIRSTNAME":    clinic_name,
            "WEBSITE":      website,
            "ADDRESSS":     address,
            "CITY":         city,
            "PHONE_NUMBER": phone,
            "EMAIL":        ""
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
