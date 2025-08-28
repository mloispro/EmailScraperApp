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
    format="[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
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
    if not hasattr(thread_local, "driver"):
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
    email_regex = r"(?i)\b(?!.*\.(?:png|jpg|jpeg|gif|bmp))([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})\b"
    for match in re.findall(email_regex, html):
        # Additional filtering: check that email does not include suspicious parts (e.g., 'logo', 'icon')
        if not any(word in match.lower() for word in ["logo", "icon", "banner"]):
            emails.add(match)
    # Check for mailto links
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=re.compile(r"^mailto:", re.I)):
        email = a.get("href").split("mailto:")[-1].split("?")[0]
        if email and not any(word in email.lower() for word in ["logo", "icon"]):
            emails.add(email)
    return emails


def click_contact_page(driver):
    """Attempt to locate and click a 'contact' or 'about' link."""
    try:
        # Search for links with text containing 'contact' or 'about'
        links = driver.find_elements(
            By.XPATH,
            "//*[contains(translate(text(),'CONTACT','contact'),'contact') or contains(translate(text(),'ABOUT','about'),'about')]",
        )
        if links:
            for link in links:
                candidate = link.get_attribute("href")
                if candidate and is_valid_url(candidate):
                    logging.info(f"Clicking link: {candidate}")
                    driver.get(candidate)
                    try:
                        WebDriverWait(driver, 10).until(
                            lambda d: d.execute_script("return document.readyState")
                            == "complete"
                        )
                    except Exception:
                        pass
                    return driver.page_source
    except Exception as e:
        logging.error(f"Error clicking contact/about link: {e}")
    # Fallback: try appending /contact to base URL
    try:
        base_url = driver.current_url.rstrip("/")
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


def scrape_google_maps_data(driver, maps_url):
    """
    Enhanced function to scrape phone, website, and full address from Google Maps page.
    Returns (phone, website, full_address)
    """
    phone = website = full_address = ""

    try:
        logging.info(f"Scraping Google Maps data from: {maps_url}")
        driver.get(maps_url)

        # Wait for the page to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, '[data-value="Directions"]')
            )
        )

        # Get full address from "Copy address" button
        try:
            btn = driver.find_element(
                By.CSS_SELECTOR, 'button[data-tooltip="Copy address"]'
            )
            aria = btn.get_attribute("aria-label")
            if aria and ":" in aria:
                full_address = aria.split(":", 1)[1].strip()
                logging.info(f"Found address: {full_address}")
        except Exception as e:
            logging.debug(f"Could not extract address: {e}")

        # Extract phone number using multiple selectors
        phone_selectors = [
            'button[data-tooltip="Call"]',
            'button[aria-label*="Call"]',
            '[data-value="phone"]',
            "span[data-phone-number]",
            'a[href^="tel:"]',
        ]

        for selector in phone_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    # Try aria-label first
                    aria_label = elem.get_attribute("aria-label")
                    if aria_label and "Call" in aria_label:
                        # Extract phone from "Call (218) 736-6987" format
                        phone_match = re.search(
                            r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", aria_label
                        )
                        if phone_match:
                            phone = phone_match.group()
                            logging.info(f"Found phone via aria-label: {phone}")
                            break

                    # Try href for tel: links
                    href = elem.get_attribute("href")
                    if href and href.startswith("tel:"):
                        phone = href.replace("tel:", "")
                        logging.info(f"Found phone via href: {phone}")
                        break

                    # Try text content
                    text = elem.text.strip()
                    phone_match = re.search(
                        r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text
                    )
                    if phone_match:
                        phone = phone_match.group()
                        logging.info(f"Found phone via text: {phone}")
                        break

                if phone:
                    break
            except Exception as e:
                logging.debug(f"Phone selector {selector} failed: {e}")

        # Extract website URL
        website_selectors = [
            'a[data-tooltip="Open website"]',
            'a[aria-label*="Open website"]',
            '[data-value="website"] a',
            'a[href^="http"]:not([href*="google.com"]):not([href*="googleusercontent.com"])',
        ]

        for selector in website_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    href = elem.get_attribute("href")
                    if (
                        href
                        and href.startswith("http")
                        and "google.com" not in href.lower()
                    ):
                        website = href
                        logging.info(f"Found website: {website}")
                        break
                if website:
                    break
            except Exception as e:
                logging.debug(f"Website selector {selector} failed: {e}")

        return phone, website, full_address

    except Exception as e:
        logging.error(f"Error scraping Google Maps data: {e}")
        return "", "", ""


# precompile all our detectors
_maps_re = re.compile(r"https?://(?:www\.)?google\.[^/]+/maps/place", re.I)
_url_re = re.compile(r"https?://\S+", re.I)
_phone_re = re.compile(r"\(?\d{3}\)?[ \-]?\d{3}[ \-]?\d{4}")
_email_re = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I)
_address_re = re.compile(
    r"\d+\s+\d*\s*(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Way|Pl|Place)\b",
    re.I,
)

# Google domains to exclude when looking for real websites
_google_domains = [
    "googleusercontent.com",
    "googleapis.com",
    "gstatic.com",
    "google.com",
]


def parse_row_enhanced(row):
    """
    Enhanced parser that handles both CSV formats:
    - clinics.csv: 14 columns, comma-delimited, no phone/website in CSV
    - clinics_bak.csv: 21 columns, tab-delimited, has phone[11] and website[12]

    Returns (maps_url, clinic_name, website, address, phone, email)
    """
    maps_url = clinic_name = website = address = phone = email = ""
    seen = set()

    # Detect format based on column count
    is_bak_format = len(row) > 16  # clinics_bak.csv has 21 columns

    if is_bak_format:
        # clinics_bak.csv format - extract from known positions
        if len(row) > 0 and row[0].strip():
            candidate = row[0].strip()
            if _maps_re.search(candidate):
                maps_url = candidate
                seen.add(candidate)

        if len(row) > 1 and row[1].strip():
            clinic_name = row[1].strip()
            seen.add(row[1].strip())

        if len(row) > 11 and row[11].strip():
            phone_candidate = row[11].strip()
            phone_match = _phone_re.search(phone_candidate)
            if phone_match:
                phone = phone_match.group()
                seen.add(phone_candidate)

        if len(row) > 12 and row[12].strip():
            website_candidate = row[12].strip()
            if website_candidate.startswith("http") and not any(
                domain in website_candidate.lower() for domain in _google_domains
            ):
                website = website_candidate
                seen.add(website_candidate)

        if len(row) > 8 and row[8].strip():
            address_candidate = row[8].strip()
            if _address_re.search(address_candidate):
                address = address_candidate
                seen.add(address_candidate)
    else:
        # clinics.csv format - use original logic but exclude Google image URLs
        for cell in row:
            text = cell.strip()
            if not text:
                continue

            if not maps_url and _maps_re.search(text):
                maps_url = text
                seen.add(text)
                continue

            # Enhanced website detection - exclude Google domains
            if (
                not website
                and _url_re.match(text)
                and "google.com/maps" not in text
                and not any(domain in text.lower() for domain in _google_domains)
            ):
                website = text
                seen.add(text)
                continue

            if not phone:
                m = _phone_re.search(text)
                if m:
                    phone = m.group()
                    seen.add(text)
                    continue

            if not email:
                m = _email_re.search(text)
                if m:
                    email = m.group()
                    seen.add(text)
                    continue

            if not address and _address_re.search(text):
                address = text
                seen.add(text)
                continue

    # If clinic_name not found yet, use first unseen cell that looks like a name
    if not clinic_name:
        for cell in row:
            text = cell.strip()
            if (
                text
                and text not in seen
                and not text.startswith("http")
                and len(text) < 200
                and not any(domain in text.lower() for domain in _google_domains)
                and not text.endswith("=w122-h92-k-no")
                and not text.endswith("=w163-h92-k-no")
                and "googleusercontent.com" not in text.lower()
                and not text.startswith("!")
            ):  # Skip Google Maps internal IDs
                clinic_name = text
                break

    # Final fallback - extract name from Maps URL if still no name found
    if not clinic_name and maps_url:
        # Extract church name from Google Maps URL
        import urllib.parse

        if "/place/" in maps_url:
            try:
                name_part = maps_url.split("/place/")[1].split("/")[0]
                name_part = urllib.parse.unquote_plus(name_part)
                if name_part and len(name_part) < 200:
                    clinic_name = name_part
            except Exception:
                pass

    return maps_url, clinic_name, website, address, phone, email


def process_csv(input_file, output_file, max_workers=3, force=False):
    """Read CSV of unknown shape, auto-detect fields, then scrape."""
    output_rows = []
    processed_websites = set()

    if not force and os.path.exists(output_file):
        try:
            with open(output_file, newline="", encoding="utf-8") as outf:
                existing = list(csv.DictReader(outf))
            output_rows = existing
            processed_websites = {r["WEBSITE"] for r in existing if r.get("WEBSITE")}
            logging.info(f"Loaded {len(existing)} existing row(s).")
        except Exception as e:
            logging.error(f"Error loading '{output_file}': {e}")
    elif force:
        logging.info("Force mode enabled.")

    tasks = []
    with open(input_file, newline="", encoding="utf-8-sig") as csvfile:
        # Enhanced delimiter detection for complex Google Maps URLs
        sample = csvfile.read(3000)
        csvfile.seek(0)

        # Count different potential delimiters in the sample
        tab_count = sample.count("\t")
        comma_count = sample.count(",")
        semicolon_count = sample.count(";")
        pipe_count = sample.count("|")

        # Look for the most common delimiter that makes sense
        delimiter_counts = [
            (tab_count, "\t"),
            (comma_count, ","),
            (semicolon_count, ";"),
            (pipe_count, "|"),
        ]

        # Sort by count, descending
        delimiter_counts.sort(key=lambda x: x[0], reverse=True)

        # Choose delimiter with highest count that's reasonably high
        delimiter = ","  # default fallback
        for count, delim in delimiter_counts:
            if count >= 10:  # Must have at least 10 occurrences
                delimiter = delim
                break

        # Override if we detect a clear pattern in the first few lines
        lines = sample.split("\n")[:5]  # Check first 5 lines
        for line in lines:
            if line.count("\t") >= 10 and line.count("\t") > line.count(","):
                delimiter = "\t"
                break
            elif line.count(",") >= 10 and line.count(",") > line.count("\t"):
                delimiter = ","
                break

        logging.info(
            f"Enhanced delimiter detection: {'TAB' if delimiter == chr(9) else repr(delimiter)} "
            f"(tabs: {tab_count}, commas: {comma_count})"
        )

        reader = csv.reader(csvfile, delimiter=delimiter)
        header = next(reader, None)  # skip header
        logging.info(f"CSV has {len(header)} columns")

        # Skip empty rows
        for row in reader:
            if not any(cell.strip() for cell in row):
                continue

            maps_url, clinic_name, website, address, phone, email = parse_row_enhanced(
                row
            )

            if not maps_url or not clinic_name:
                logging.warning(
                    f"Skipping row (missing maps or name): {clinic_name or 'UNKNOWN'}"
                )
                continue

            # Create unique identifier for deduplication
            identifier = website or maps_url
            if identifier in processed_websites:
                logging.debug(f"Already processed: {identifier}")
                continue

            tasks.append((clinic_name, maps_url, website, address, phone, email))
            processed_websites.add(identifier)

    logging.info(f"Queuing {len(tasks)} tasks with {max_workers} workers.")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_enhanced, t): t for t in tasks}
        for fut in concurrent.futures.as_completed(futures):
            try:
                output_rows.extend(fut.result())
            except Exception as e:
                logging.error(f"Task {futures[fut]} error: {e}")

    for drv in selenium_drivers:
        try:
            drv.quit()
        except:
            pass

    # Write output with proper headers
    with open(output_file, "w", newline="", encoding="utf-8") as outf:
        fieldnames = [
            "FIRSTNAME",
            "WEBSITE",
            "ADDRESS",
            "CITY",
            "PHONE_NUMBER",
            "EMAIL",
        ]
        writer = csv.DictWriter(outf, fieldnames=fieldnames)
        writer.writeheader()
        for r in output_rows:
            writer.writerow(r)

    logging.info(f"Saved {len(output_rows)} rows to '{output_file}'.")


def process_single_enhanced(task):
    """
    Enhanced processing function.
    task = (clinic_name, maps_url, website, address, phone, csv_email)
    returns a list of dicts to write out, including CITY.
    """
    clinic_name, maps_url, website, address, phone, csv_email = task
    driver = get_selenium_driver()

    # First, try to enhance data from Google Maps if needed
    enhanced_phone = phone
    enhanced_website = website
    enhanced_address = address

    # If we're missing critical data, scrape it from Google Maps
    if not phone or not website or not address:
        try:
            scraped_phone, scraped_website, scraped_address = scrape_google_maps_data(
                driver, maps_url
            )
            enhanced_phone = enhanced_phone or scraped_phone
            enhanced_website = enhanced_website or scraped_website
            enhanced_address = enhanced_address or scraped_address
        except Exception as e:
            logging.warning(
                f"Couldn't enhance data from Google Maps for {maps_url}: {e}"
            )

    logging.info(
        f"{clinic_name}: Phone={enhanced_phone}, Website={enhanced_website}, Address={enhanced_address}"
    )

    # Parse street, city, state from the address
    street, city, state = parse_address(enhanced_address)

    # Determine email strategy
    if csv_email:
        emails_set = {csv_email}
        logging.info(f"{clinic_name}: using parsed email {csv_email}")
    elif enhanced_website:
        logging.info(f"{clinic_name}: scraping emails from {enhanced_website}")
        start = time.time()
        emails_set = get_emails_for_website(enhanced_website)
        dur = time.time() - start
        logging.info(f"  → found {emails_set or 'none'} in {dur:.1f}s")
    else:
        logging.info(f"{clinic_name}: no website available, skipping email scraping")
        emails_set = set()

    rows_out = []
    if emails_set:
        for e in emails_set:
            rows_out.append(
                {
                    "FIRSTNAME": clinic_name,
                    "WEBSITE": enhanced_website,
                    "ADDRESS": enhanced_address,
                    "CITY": city,
                    "PHONE_NUMBER": enhanced_phone,
                    "EMAIL": e,
                }
            )
    else:
        rows_out.append(
            {
                "FIRSTNAME": clinic_name,
                "WEBSITE": enhanced_website,
                "ADDRESS": enhanced_address,
                "CITY": city,
                "PHONE_NUMBER": enhanced_phone,
                "EMAIL": "",
            }
        )

    return rows_out


def main():
    parser = argparse.ArgumentParser(
        description="Enhanced clinic CSV processor that handles both formats and scrapes emails from provider websites."
    )
    parser.add_argument(
        "input_csv", type=str, help="Input CSV file with clinic entries"
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output_csv",
        type=str,
        default="scraped_emails.csv",
        help="Output CSV file for scraped emails (default: scraped_emails.csv)",
    )
    parser.add_argument(
        "-f",
        "--force",
        dest="force",
        action="store_true",
        help="Force re-scraping all entries, ignoring existing output file",
    )
    parser.add_argument(
        "-w",
        "--workers",
        dest="workers",
        type=int,
        default=3,
        help="Number of parallel worker threads (default: 3)",
    )
    args = parser.parse_args()
    try:
        process_csv(
            args.input_csv, args.output_csv, max_workers=args.workers, force=args.force
        )
    except KeyboardInterrupt:
        # Handle Ctrl+C from user
        handle_exit(signal.SIGINT, None)


if __name__ == "__main__":
    main()
