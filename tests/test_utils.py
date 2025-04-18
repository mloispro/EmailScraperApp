import unittest

from src.utils import is_valid_url, extract_emails_from_html

class TestUtils(unittest.TestCase):
    def test_is_valid_url(self):
        self.assertTrue(is_valid_url("http://example.com"))
        self.assertTrue(is_valid_url("https://sub.domain.com/path?query=1"))
        self.assertFalse(is_valid_url("not-a-url"))
        self.assertFalse(is_valid_url(""))

    def test_extract_emails_from_html(self):
        html = "<p>Contact: alice@example.com and bob@domain.org</p>"
        emails = extract_emails_from_html(html)
        self.assertEqual(emails, {"alice@example.com", "bob@domain.org"})

        # Exclude image filenames and suspect 'logo' pattern
        html2 = "Logo image: logo@images.png <a href=\"mailto:info@site.com?subject=hi\">Mail</a>"
        emails2 = extract_emails_from_html(html2)
        self.assertEqual(emails2, {"info@site.com"})

if __name__ == '__main__':
    unittest.main()