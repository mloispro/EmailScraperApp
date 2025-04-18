import unittest

from src.email_spider import get_provider_website

class TestEmailSpider(unittest.TestCase):
    def test_valid_provider_website(self):
        row = [None] * 14
        row[13] = "http://example.com"
        self.assertEqual(get_provider_website(row), "http://example.com")

    def test_empty_provider_column(self):
        row = [""] * 14
        self.assertEqual(get_provider_website(row), "")

    def test_google_domain_skipped(self):
        row = [""] * 14
        row[13] = "https://google.com"
        self.assertEqual(get_provider_website(row), "")

    def test_insufficient_columns(self):
        row = ["http://example.com"]
        self.assertEqual(get_provider_website(row), "")

if __name__ == '__main__':
    unittest.main()