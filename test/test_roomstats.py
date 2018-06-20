from calendar import timegm
from datetime import datetime
import unittest
from heimdall import Heimdall

class TestBasics(unittest.TestCase):
    def setUp(self):
        self.heimdall = Heimdall('test_data', verbose=False)
        self.heimdall.connect_to_database()

    def tearDown(self):
        self.heimdall.c.execute('''DELETE FROM messages WHERE id IS "id_of_message_to_be_deleted"''')
        self.heimdall.conn.commit()
        self.heimdall.conn.close()

    def test_roomstats(self):
        recvd = [line.replace('\t','') for line in self.heimdall.get_room_stats().split('\n') if not line.isspace()]
        self.expcd = [  'There have been 1503 posts in &test_data (0 today), averaging 107 posts per day over the last 28 days (the busiest was 2018-03-12 with 1319 messages sent).',
                        '',
                        'The top ten posters are:', 
                        ' 1) 117    Pouncy Silverkitten',
                        ' 2) 93     User 9',
                        ' 3) 90     User 6',
                        ' 4) 89     User 5',
                        ' 5) 89     User 7',
                        ' 6) 87     User 4',
                        ' 7) 86     User 8',
                        ' 8) 85     User 2',
                        ' 9) 83     User 0',
                        '10) 83     User 1',
                        '',
                        'all_time_url last_28_url']

        assert self.expcd[0] == recvd[0]
        assert self.expcd[1] == recvd[1]
        assert self.expcd[2] == recvd[2]
        assert self.expcd[3] == recvd[3]
        assert self.expcd[4] == recvd[4]
        assert self.expcd[5] == recvd[5]
        assert self.expcd[6] == recvd[6]
        assert self.expcd[7] == recvd[7]
        assert self.expcd[8] == recvd[8]
        assert self.expcd[9] == recvd[9]
        assert self.expcd[10] == recvd[10]
        assert self.expcd[11] == recvd[11]
        assert self.expcd[12] == recvd[12]
        assert self.expcd[13] == recvd[13]
        assert self.expcd[14] == recvd[14]

    def test_add_message(self):
        packet = {  'type': 'send-event',
                    'data': {   'content': "Just adding a test message to check that the today count updates itself properly.",
                                'id': "id_of_message_to_be_deleted",
                                'parent': '',
                                'sender': { 'id':   'agent:12345',
                                            'name': 'DeleteMeWhenTestsAreDone'},
                                'time': timegm(datetime.utcnow().utctimetuple())}}

        self.heimdall.parse(packet)

        recvd = [line.replace('\t','') for line in self.heimdall.get_room_stats().split('\n') if not line.isspace()]
        self.expcd = [  'There have been 1504 posts in &test_data (1 today), averaging 107 posts per day over the last 28 days (the busiest was 2018-03-12 with 1319 messages sent).',
                        '',
                        'The top ten posters are:', 
                        ' 1) 117    Pouncy Silverkitten',
                        ' 2) 93     User 9',
                        ' 3) 90     User 6',
                        ' 4) 89     User 5',
                        ' 5) 89     User 7',
                        ' 6) 87     User 4',
                        ' 7) 86     User 8',
                        ' 8) 85     User 2',
                        ' 9) 83     User 0',
                        '10) 83     User 1',
                        '',
                        'all_time_url last_28_url']

        assert self.expcd[0] == recvd[0]
        assert self.expcd[1] == recvd[1]
        assert self.expcd[2] == recvd[2]
        assert self.expcd[3] == recvd[3]
        assert self.expcd[4] == recvd[4]
        assert self.expcd[5] == recvd[5]
        assert self.expcd[6] == recvd[6]
        assert self.expcd[7] == recvd[7]
        assert self.expcd[8] == recvd[8]
        assert self.expcd[9] == recvd[9]
        assert self.expcd[10] == recvd[10]
        assert self.expcd[11] == recvd[11]
        assert self.expcd[12] == recvd[12]
        assert self.expcd[13] == recvd[13]
        assert self.expcd[14] == recvd[14]

    def test_agreement_user_room(self):
        assert self.heimdall.get_room_stats().split('Pouncy Silverkitten')[0].split(')')[-1].strip() == self.heimdall.get_user_stats('Pouncy Silverkitten').split(':')[2].split('\n')[0].strip()
