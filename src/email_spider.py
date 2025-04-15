import argparse
import csv
import re
import time
from urllib.parse import urlparse, urljoin
import scrapy
from bs4 import BeautifulSoup

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
        print(f"URL validation error: {e}")
        return False

def get_provider_website(row):
    """
    Return the provider website URL from column 13 if available.
    It must be non-empty, start with 'http', not contain 'google.com', and pass validation.
    """
    if len(row) > 13:
        candidate = row[13].strip()
        if candidate:
            print(f"Found candidate URL in column 13: {candidate}")
        if candidate and candidate.startswith("http") and "google.com" not in candidate.lower() and is_valid_url(candidate):
            print(f"Using provider website: {candidate}")
            return candidate
    print("No valid provider website found in the expected column.")
    return ""

def setup_selenium():
    """Initialize and return a headless Selenium Chrome WebDriver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    # You can add additional options here (e.g., window size, disable images)
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
                    print(f"Clicking link: {candidate}")
                    driver.get(candidate)
                    time.sleep(3)
                    return driver.page_source
    except Exception as e:
        print(f"Error clicking contact/about link: {e}")
    # Fallback: try appending /contact to base URL
    try:
        base_url = driver.current_url.rstrip('/')
        fallback_url = base_url + "/contact"
        print(f"Trying fallback URL: {fallback_url}")
        driver.get(fallback_url)
        time.sleep(3)
        return driver.page_source
    except Exception as e:
        print(f"Fallback error: {e}")
    return ""


def scrape_emails_with_selenium(driver, url):
    """
    Load the base page with Selenium and attempt to extract emails.
    If no emails are found, try navigating to a contact page.
    """
    emails_found = set()
    try:
        print(f"\nLoading base page: {url}")
        driver.get(url)
        time.sleep(3)  # Wait for full page load
    except Exception as e:
        print(f"Error loading base page {url}: {e}")
        return emails_found

    base_html = driver.page_source
    print(f"Base page length: {len(base_html)}")
    emails_found.update(extract_emails_from_html(base_html))
    if emails_found:
        print(f"Emails found on base page: {emails_found}")
    else:
        print("No emails found on base page. Trying to click a Contact link...")
        contact_html = click_contact_page(driver)
        if contact_html:
            emails_found.update(extract_emails_from_html(contact_html))
            if emails_found:
                print(f"Emails found on contact page: {emails_found}")
            else:
                print("No emails found on contact page.")
        else:
            print("No contact page was detected.")
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
        print(f"Error with Selenium for {website}: {e}")
    finally:
        if driver is not None:
            driver.quit()
    return ", ".join(emails_found) if emails_found else ""

def process_csv(input_file, output_file):
    output_rows = []
    with open(input_file, newline='', encoding='utf-8') as csvfile:
        #reader = csv.reader(csvfile)
        reader = csv.reader(csvfile, delimiter='\t')
        headers = next(reader, None)
        if headers is None:
            print("Input CSV is empty.")
            return

    print("Starting CSV processing...")
    with open(input_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        #reader = csv.reader(csvfile)
        next(reader, None)  # Skip header
        for row in reader:
            if not any(row) or len(row) < 14:
                print(f"Skipping row due to insufficient data: {row}")
                continue

            website = get_provider_website(row)
            if not website:
                print(f"Skipping row because no valid non-Google website found: {row}")
                continue

            clinic_name = row[1].strip()
            address = row[8].strip()
            phone = row[12].strip()
            print(f"\nProcessing clinic: {clinic_name}")

            emails_scraped = scrape_emails_from_website(website)
            print(f"Emails scraped for {clinic_name}: {emails_scraped}")

            output_rows.append({
                "clinic_name": clinic_name,
                "website": website,
                "address": address,
                "phone": phone,
                "email_addresses": emails_scraped
            })

    with open(output_file, "w", newline='', encoding='utf-8') as outfile:
        fieldnames = ["clinic_name", "website", "address", "phone", "email_addresses"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in output_rows:
            writer.writerow(row)
    print(f"\nProcessed {len(output_rows)} valid row(s) and saved to '{output_file}'.")

def main():
    parser = argparse.ArgumentParser(
        description="Aggregate clinic CSV data and scrape emails from provider websites using an advanced Selenium approach."
    )
    parser.add_argument("input_csv", type=str, help="Input CSV file with clinic entries")
    args = parser.parse_args()
    process_csv(args.input_csv, "scraped_emails.csv")

if __name__ == '__main__':
    main()
