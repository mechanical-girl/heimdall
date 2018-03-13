from heimdall import Heimdall

class TestBasics:
    def setUp(self):
        import calendar
        import re
        import time

        from datetime import date, timedelta

        tomorrow = int(calendar.timegm(date.fromtimestamp(time.time()).timetuple())+(60*60*24))
        message_with_urls = (' '.join(['https://www.google.com',
                                      'http://insecure.com',
                                      'https://mail.google.com',
                                      'http://sub.insecure.com',
                                      'www.insecure.com',
                                      'sub.insecure.com',
                                      'google.com/test.html',
                                      'imgur.com/vKIVbMz',
                                      'https://imgur.com/vKIVbMz']),
                             '')
        message_without_urls = 'This messages discusses the http and https:// standards, the merits of the www. prefix, the .com and .biz TLDs, and sub.domains of websites with .php and.html files.'

    def tearDown(self):
        pass

    def test_url_stripping(self):
        message_with_urls = (' '.join(['https://www.google.com',
                                      'http://insecure.com',
                                      'https://mail.google.com',
                                      'http://sub.insecure.com',
                                      'www.insecure.com',
                                      'sub.insecure.com',
                                      'google.com/test.html',
                                      'imgur.com/vKIVbMz',
                                      'https://imgur.com/vKIVbMz']),
                             '')
        message_without_urls = 'This messages discusses the http and https:// standards, the merits of the www. prefix, the .com and .biz TLDs, and sub.domains of websites with .php and.html files.'
        print(Heimdall.get_urls(message_with_urls[0]))
        assert get_urls(message_with_urls[0]) == message_with_urls[1]
        assert get_urls(message_without_urls) == None
