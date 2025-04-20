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
    
    def test_parse_address(self):
        from src.utils import parse_address
        # Standard comma-separated with state and zip
        addr = "123 Main St, Springfield, IL 62704"
        street, city, state = parse_address(addr)
        self.assertEqual(street, "123 Main St")
        self.assertEqual(city, "Springfield")
        self.assertEqual(state, "IL")
        # Missing comma before state
        addr2 = "456 Elm Rd, Columbus OH 43085"
        street2, city2, state2 = parse_address(addr2)
        self.assertEqual(street2, "456 Elm Rd")
        self.assertEqual(city2, "Columbus")
        self.assertEqual(state2, "OH")
        # Only street and city
        addr3 = "789 Pine Ave, Seattle"
        s3, c3, st3 = parse_address(addr3)
        self.assertEqual(s3, "789 Pine Ave")
        self.assertEqual(c3, "Seattle")
        self.assertEqual(st3, "")
        # Empty string
        s4, c4, st4 = parse_address("")
        self.assertEqual((s4, c4, st4), ('', '', ''))

if __name__ == '__main__':
    unittest.main()