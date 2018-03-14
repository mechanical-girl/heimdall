import unittest
from heimdall import Heimdall
import calendar
from datetime import date, timedelta
import time

class TestBasics(unittest.TestCase):
    def setUp(self):
        self.heimdall = Heimdall('test', False, False, True)
        self.tomorrow = int(calendar.timegm(date.fromtimestamp(time.time()).timetuple())+(60*60*24))        

    def test_tomorrow(self):
        assert self.heimdall.next_day(time.time()) == self.tomorrow

    def test_get_position(self):
        self.heimdall.connect_to_database()
        assert self.heimdall.get_position('Pouncy Silverkitten') == 1
        assert self.heimdall.get_position('ThisUserDoesnaeExist') == "unknown"

def main():
    unittest.main()

if __name__ == "__main__":
    main()
