import unittest
from heimdall import Heimdall
import calendar
from datetime import date, timedelta
import time

class TestBasics(unittest.TestCase):
    def setUp(self):
        self.heimdall = Heimdall('test_data', verbose=False)
        self.tomorrow = int(calendar.timegm(date.fromtimestamp(time.time()).timetuple())+(60*60*24))        

    def test_tomorrow(self):
        assert self.heimdall.next_day(time.time()) == self.tomorrow

    def test_get_position(self):
        self.heimdall.connect_to_database()
        assert self.heimdall.get_position('Pouncy Silverkitten') == 1
        assert self.heimdall.get_position('ThisUserDoesnaeExist') == "unknown"

    def test_date_from_timestamp(self):
        assert self.heimdall.date_from_timestamp(946688461) == "2000-01-01"
        assert self.heimdall.date_from_timestamp(978310861) == "2001-01-01"
        assert self.heimdall.date_from_timestamp(1521314568) == "2018-03-17"
        assert self.heimdall.date_from_timestamp(1552850568) == "2019-03-17"

