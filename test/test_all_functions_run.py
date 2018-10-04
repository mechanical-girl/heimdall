import json
import unittest

import karelia

import heimdall


class TestAllFunctionsRun(unittest.TestCase):
    def setUp(self):
        self.heimdall = heimdall.Heimdall('test')
        self.heimdall.use_logs = 'xkcd'
        self.heimdall.database = 'test_data.db'
        self.heimdall.connect_to_database()

        class Packet:
            def __init__(self):
                self.content = ""
                self.name = ""

        self.decoy_packet = Packet()
        self.decoy_packet.data = Packet()
        self.decoy_packet.data.sender = Packet()

    def tearDown(self):
        pass

    def test_get_user_stats_runs_real_user_no_aliases(self):
        def fake_reply(string):
            pass

        self.decoy_packet.data.content = "!stats -met"
        self.decoy_packet.data.sender.name = "dog barrier"

        self.heimdall.heimdall.reply = fake_reply
        self.heimdall.heimdall.packet = self.decoy_packet
        self.heimdall.get_user_stats()

    def test_get_user_stats_runs_real_user_aliases(self):
        def fake_reply(string):
            pass

        self.decoy_packet.data.content = "!stats -meta"
        self.decoy_packet.data.sender.name = "Xyzzy"

        self.heimdall.heimdall.reply = fake_reply
        self.heimdall.heimdall.packet = self.decoy_packet
        self.heimdall.get_user_stats()

    def test_get_user_stats_runs_false_user(self):
        def fake_reply(string):
            pass

        self.decoy_packet.data.content = "!stats -meta"
        self.decoy_packet.data.sender.name = "lsdfkbgzlfgkjbzflgkjzbfgkzfbg"

        self.heimdall.heimdall.reply = fake_reply
        self.heimdall.heimdall.packet = self.decoy_packet
        self.heimdall.get_user_stats()

    def test_get_room_stats_runs(self):
        def fake_reply(string):
            pass

        self.decoy_packet.data.content = "!roomstats"
        self.decoy_packet.data.sender.name = "dog barrier"

        self.heimdall.heimdall.reply = fake_reply
        self.heimdall.heimdall.packet = self.decoy_packet
        self.heimdall.get_room_stats()
