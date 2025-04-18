import logging
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup

def is_valid_url(url):
    """Validate a URL by checking its scheme and network location."""
    try:
        result = urlparse(url)
        return bool(result.scheme and result.netloc)
    except Exception as e:
        logging.error(f"URL validation error: {e}")
        return False

def extract_emails_from_html(html):
    """Extract emails from HTML while ignoring common image extensions."""
    emails = set()
    # Refined regex: ensure we match typical email patterns and exclude common image extensions.
    email_regex = r'(?i)\b(?!.*\.(?:png|jpg|jpeg|gif|bmp))([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})\b'
    for match in re.findall(email_regex, html):
        if not any(word in match.lower() for word in ['logo', 'icon', 'banner']):
            emails.add(match)
    # Check for mailto links
    try:
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a', href=re.compile(r'^mailto:', re.I)):
            email = a.get('href').split("mailto:")[-1].split('?')[0]
            if email and not any(word in email.lower() for word in ['logo', 'icon']):
                emails.add(email)
    except Exception as e:
        logging.error(f"Error parsing HTML for mailto links: {e}")
    return emails