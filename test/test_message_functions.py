import time
import unittest
from heimdall import Heimdall

class TestBasics(unittest.TestCase):
    def setUp(self):
        self.heimdall = Heimdall('test_data', verbose = False)
        self.message_with_data_parent = {   'type': 'send-event',
                                            'data': {   'content': 'test message',
                                                        'id': 'uniqueid1',
                                                        'parent': 'parent',
                                                        'time': str(time.time()),
                                                        'sender': { 'id': 'uniqueid',
                                                                    'name': 'Testing Heimdall'}}}


        self.message_with_data = {  'type': 'send-event',
                                    'data': {   'content': 'test message',
                                                'id': 'uniqueid2',
                                                'time': str(time.time()),
                                                'sender': { 'id': 'uniqueid',
                                                            'name': 'Testing Heimdall'}}}


        self.message_with_parent = {'type': 'send-event',
                                    'content': 'test message',
                                    'id': 'uniqueid3',
                                    'parent': 'parent',
                                    'time': str(time.time()),
                                    'sender': { 'id': 'uniqueid',
                                                'name': 'Testing Heimdall'}}

        self.message_with_none = {  'type': 'send-event',
                                    'content': 'test message',
                                    'id': 'uniqueid4',
                                    'time': str(time.time()),
                                    'sender': { 'id': 'uniqueid',
                                                'name': 'Testing Heimdall'}}
        self.heimdall.connect_to_database()

    def test_data_parent(self):
        assert self.heimdall.insert_message(self.message_with_data_parent) == None
    
    def test_data(self):
        assert self.heimdall.insert_message(self.message_with_data) == None

    def test_parent(self):
        assert self.heimdall.insert_message(self.message_with_parent) == None

    def test_none(self):
        assert self.heimdall.insert_message(self.message_with_none) == None

    def tearDown(self):
        self.heimdall.c.execute('''DELETE FROM test_data WHERE normname = ?''',('testingheimdall',))
        self.heimdall.conn.commit()
