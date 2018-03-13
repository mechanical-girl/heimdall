from heimdall import Heimdall
import calendar
from datetime import date, timetuple
import time

class TestBasics:
    def setUp(self):
        self.heimdall = Heimdall('test', False, False)
        self.tomorrow = int(calendar.timegm(date.fromtimestamp(time.time()).timetuple())+(60*60*24))        

    def test_tomorrow(self):
        assert self.heimdall.next_day(time.time()) == self.tomorrow

    def get_position(self):
        assert self.heimdall.get_position('Pouncy Silverkitten') == 1
        assert self.heimdall.get_position('ThisUserDoesnaeExist') == "unknown"


