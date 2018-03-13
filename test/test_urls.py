from heimdall import Heimdall
import calendar
import re
import time

class TestBasics:
    def setUp(self):
        from datetime import date, timedelta

        self.heimdall = Heimdall('test', False, False)

        self.tomorrow = int(calendar.timegm(date.fromtimestamp(time.time()).timetuple())+(60*60*24))
        self.message_with_urls = (' '.join(['https://www.google.com',
                                            'http://insecure.com/',
                                            'https://mail.google.com',
                                            'http://sub.insecure.com',
                                            'www.insecure.com',
                                            'sub.insecure.com',
                                            'google.com/test.html',
                                            'wiki.archlinux.org/index.php/arch_linux',
                                            'http://imgur.com/vKIVbMz']),
                                            ['http://insecure.com/',
                                            'http://sub.insecure.com',
                                            'www.insecure.com', 
                                            'sub.insecure.com', 
                                            'wiki.archlinux.org/index.php/arch_linux'])
        self.message_without_urls = 'This messages discusses the http and https:// standards, the merits of the www. prefix, the .com and .biz TLDs, and sub.domains of websites with .php and.html files.'

    def tearDown(self):
        pass

    def test_url_stripping(self):
        assert self.heimdall.get_urls(self.message_with_urls[0]) == self.message_with_urls[1]
        assert self.heimdall.get_urls(self.message_without_urls) == []

    def test_url_title_check(self):
        assert self.heimdall.get_page_titles(self.message_with_urls[0].split()) == "Title: Google\nTitle: Gmail\nTitle: Imgur: The magic of the Internet\n" 

