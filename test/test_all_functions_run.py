import unittest

import heimdall


class TestAllFunctionsRun(unittest.TestCase):
    def setUp(self):
        self.heimdall = heimdall.Heimdall('test')
        self.heimdall.database = 'test_data.db'
        self.heimdall.connect_to_database()

    def tearDown(self):
        pass

    def test_get_user_stats_runs_real_user_no_aliases(self):
        self.heimdall.get_user_stats('Pouncy Silverkitten', ['messages', 'engagement', 'text'])

    def test_get_user_stats_runs_real_user_aliases(self):
        self.heimdall.get_user_stats('Pouncy Silverkitten', ['--aliases', 'messages', 'engagement', 'text'])

    def test_get_user_stats_runs_false_user(self):
        self.heimdall.get_user_stats('alkdjbl\iugbzjkvblviu\.,seiuvb\le;siubs,lvkablifguawebl\ksdbflaeiuflefuilefiu\dl', ['messages', 'engagement', 'text'])

    def test_get_room_stats_runs(self):
        # Not used since matplotlib is currently throwing an error -> self.heimdall.get_room_stats()
        pass
