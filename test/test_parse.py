import unittest
from heimdall import Heimdall

class TestParse(unittest.TestCase):
    def generate_packet(self, content):
        return({    'type': 'send-event',
                    'data': {   'content': content,
                                'id': 'uniqueid1',
                                'parent': 'parent',
                                'time': "123456789",
                                'sender': { 'id': 'uniqueid',
                                            'name': 'Heimdall'}}})

    def send(self, message, parent):
        pass

    def insert_message(self, message):
        pass

    def get_user_stats(self, user):
        self.user_stats_results += 1

    def get_room_stats(self):
        self.room_stats_results += 1

    def get_rank_of_user(self, user):
        self.rank_of_user += 1

    def get_user_at_position(self, position):
        self.user_at_position += 1

    def setUp(self):
        self.heimdall = Heimdall('test_data', verbose=False)
        self.heimdall.heimdall.send = self.send
        self.heimdall.insert_message = self.insert_message
        self.heimdall.get_user_stats = self.get_user_stats
        self.user_stats_results = 0
        self.heimdall.get_room_stats = self.get_room_stats
        self.room_stats_results = 0
        self.heimdall.get_rank_of_user = self.get_rank_of_user
        self.rank_of_user = 0
        self.heimdall.get_user_at_position = self.get_user_at_position
        self.user_at_position = 0
        self.messages = [[  "!stats",       "!stats blah", "    !stats @Heimdall",          "!stats @ThisUserAintReal"],
                         [  "!roomstats",   "!roomstats  ",     "!roomstats &xkcd"],
                         [  "!rank",        "!rank @Heimdall",  "!rank @ThisUserAintReal",  "!rank 42"],
                         [  "!rank 11",     "!rank a_nice_fish_sandwich",  "!rank 999",     "!rank 0"]]
        self.total_messages_all_time = 2
    def test_user_stats(self):
        for message in self.messages[0]:
            self.heimdall.parse(self.generate_packet(message))
        assert self.user_stats_results == 3

    def test_room_stats(self):
        for message in self.messages[1]:
            self.heimdall.parse(self.generate_packet(message))
        assert self.room_stats_results == 2 

    def test_rank_user(self):
        for message in self.messages[2]:
            self.heimdall.parse(self.generate_packet(message))
        assert self.rank_of_user == 3

    def test_position_number(self):
        for message in self.messages[3]:
            self.heimdall.parse(self.generate_packet(message))
        assert self.user_at_position == 3
