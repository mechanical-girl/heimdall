import unittest
from heimdall import Heimdall
import re

class TestUrls(unittest.TestCase):
    def setUp(self):

        self.heimdall = Heimdall('test_data', False, False, True)

        self.message_with_urls = (' '.join(['https://www.google.com',
                                            'http://insecure.com/',
                                            'http://sub.insecure.com',
                                            'www.insecure.com',
                                            'sub.insecure.com',
                                            'wiki.archlinux.org/index.php/arch_linux',
                                            'http://imgur.com/vKIVbMz']),
                                            ['https://www.google.com',
                                            'http://insecure.com',
                                            'http://sub.insecure.com',
                                            'www.insecure.com',
                                            'sub.insecure.com',
                                            'wiki.archlinux.org/index.php/arch_linux'])
        self.message_without_urls = 'This messages discusses the http and https:// standards, the merits of the www. prefix, the .com and .biz TLDs and websites with .php and.html files.'

    def test_url_stripping(self):
        assert self.heimdall.get_urls(self.message_with_urls[0]) == self.message_with_urls[1]
        assert self.heimdall.get_urls(self.message_without_urls) == []

    def test_url_title_check(self):
        assert self.heimdall.get_page_titles(self.message_with_urls[0].split()) == "Title: Google\nTitle: Imgur: The magic of the Internet\n" 

