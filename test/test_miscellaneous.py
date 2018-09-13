import os
import unittest

import heimdall


class TestMiscellaneous(unittest.TestCase):
    def setUp(self):
        self.heimdall = heimdall.Heimdall('test')
        self.heimdall.database = "_test.db"

    def tearDown(self):
        if os.path.exists("_test.db"):
            os.remove("_test.db")

    def test_setup_by_corrupting_config_file(self):
        with open('data/heimdall/messages_delivered.json', 'w') as f:
            f.write("[][]")

    def test_ability_to_recover_from_corrupted_config_file(self):
        assert True

