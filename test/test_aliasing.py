import unittest

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

        self.decoy_packet = Packet()
        self.decoy_packet.data = Packet()

    def tearDown(self):
        pass

    def test_get_rank_of_user_who_exists(self):
        def fake_reply(string):
            assert string == "Position 4"

        self.decoy_packet.data.content = "!rank @dogbarrier"

        self.heimdall.heimdall.reply = fake_reply
        self.heimdall.heimdall.packet = self.decoy_packet
        self.heimdall.get_rank()

    def test_get_rank_of_user_who_does_not_exist(self):
        def fake_reply(string):
            assert string == "User @PouncySilverkitten not found."

        self.decoy_packet.data.content = "!rank @PouncySilverkitten"

        self.heimdall.heimdall.reply = fake_reply
        self.heimdall.heimdall.packet = self.decoy_packet
        self.heimdall.get_rank()

    def test_get_user_at_known_rank(self):
        assert self.heimdall.get_user_at_position(4, 'xkcd') == "The user at position 4 is @dogbarrier."

    def test_get_user_at_unknown_rank(self):
        assert self.heimdall.get_user_at_position(1000, 'xkcd') == "Position not found; there have been 145 posters in &xkcd."
