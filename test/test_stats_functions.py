import unittest

import heimdall


class TestAllFunctionsRun(unittest.TestCase):
    def setUp(self):
        self.heimdall = heimdall.Heimdall('test')
        self.heimdall.use_logs = 'xkcd'
        self.heimdall.database = 'test_data.db'
        self.heimdall.connect_to_database()

    def tearDown(self):
        pass

    def test_get_rank_of_user_who_exists(self):
        assert self.heimdall.get_rank_of_user('dogbarrier') == 'Position 4'

    def test_get_rank_of_user_who_does_not_exist(self):
        assert self.heimdall.get_rank_of_user('PouncySilverkitten') == 'User @PouncySilverkitten not found.'

    def test_get_user_at_known_rank(self):
        assert self.heimdall.get_user_at_position(4) == "The user at position 4 is @dogbarrier."

    def test_get_user_at_unknown_rank(self):
        assert self.heimdall.get_user_at_position(1000) == "Position not found; there have been 145 posters posters in &xkcd."
