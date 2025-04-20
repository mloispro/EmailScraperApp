import logging
import re
from urllib.parse import urlparse
# BeautifulSoup removed; we'll use regex-based extraction only

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
    # Also extract mailto: links
    for m in re.findall(r'(?i)mailto:([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})', html):
        if not any(word in m.lower() for word in ['logo', 'icon']):
            emails.add(m)
    return emails
   
def parse_address(full_address):
    """Split a full address string into street, city, and state components."""
    # Split on commas and trim whitespace
    parts = [p.strip() for p in full_address.split(',') if p.strip()]
    street = parts[0] if len(parts) > 0 else ''
    city = parts[1] if len(parts) > 1 else ''
    state = ''
    if len(parts) > 2:
        # parts[2] may contain state and zip; take first token as state
        state = parts[2].split()[0]
    elif len(parts) > 1:
        # If city and state are combined in parts[1]
        tokens = parts[1].split()
        if len(tokens) > 1:
            state = tokens[-1]
            city = ' '.join(tokens[:-1])
    return street, city, state