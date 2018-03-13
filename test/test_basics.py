from heimdall import Heimdall

class TestBasics:
    def setUp(self):
        self.heimdall = Heimdall('test', False, False)

def test_tomorrow(self):
    assert self.heimdall.next_day(time.time()) == self.tomorrow

def get_position(self):
    assert self.heimdall.get_position('Pouncy Silverkitten') == 1
    assert self.heimdall.get_position('ThisUserDoesnaeExist') == "unknown"

