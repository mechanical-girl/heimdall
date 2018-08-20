import os
import sqlite3
import unittest

import heimdall


class TestDatabaseFunctions(unittest.TestCase):
    def setUp(self):
        self.heimdall = heimdall.Heimdall('test')
        self.heimdall.database = "_test.db"

    def tearDown(self):
        if os.path.exists("_test.db"):
            os.remove("_test.db")

    def test_func_connect_to_database(self):
        conn = sqlite3.connect('_test.db')
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        assert c.fetchall() == []
        self.heimdall.connect_to_database()
        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        assert c.fetchall() == [('messages',), ('aliases',)]
        c.execute('select * from messages')
        assert list(map(lambda x: x[0], c.description)) == ['content', 'id', 'parent', 'senderid', 'sendername', 'normname', 'time', 'room', 'globalid']
        c.execute('select * from aliases')
        assert list(map(lambda x: x[0], c.description)) == ['master', 'alias']

    def test_func_write_to_database_unspecified_mode_no_queue(self):
        self.heimdall.connect_to_database()
        self.heimdall.write_to_database('''INSERT INTO messages VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', values=("This is content", "randomid", "parentid", "senderid", "sendername", "normname", "0123456789", "test", "testrandomid",))

        conn = sqlite3.connect('_test.db')
        c = conn.cursor()
        c.execute('''SELECT COUNT(*) FROM messages''')
        assert c.fetchall()[0][0] == 1

    def test_func_write_to_database_execute_no_queue(self):
        self.heimdall.connect_to_database()
        self.heimdall.write_to_database('''INSERT INTO messages VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', values=("This is content", "randomid", "parentid", "senderid", "sendername", "normname", "0123456789", "test", "testrandomid",), mode="execute")

        conn = sqlite3.connect('_test.db')
        c = conn.cursor()
        c.execute('''SELECT COUNT(*) FROM messages''')
        assert c.fetchall()[0][0] == 1

    def test_func_write_to_database_execute_all_no_queue(self):
        self.heimdall.connect_to_database()
        values = (("This is content", "randomid", "parentid", "senderid", "sendername", "normname", "0123456789", "test", "testrandomid",), ("This is content", "randomidtwo", "parentid", "senderid", "sendername", "normname", "0123456789", "test", "testrandomidtwo",))
        self.heimdall.write_to_database('''INSERT INTO messages VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', values=values, mode='executemany')

        conn = sqlite3.connect('_test.db')
        c = conn.cursor()
        c.execute('''SELECT COUNT(*) FROM messages''')
        assert c.fetchall()[0][0] == 2

    def test_func_insert_message_no_parent(self):
        packet = {'data': {'content': 'This is content', 'id': 'randomid', 'sender': {'id': 'senderid', 'name': 'sendername'}, 'time': '0123456789'}}
        self.heimdall.connect_to_database()
        self.heimdall.insert_message(packet)

        conn = sqlite3.connect('_test.db')
        c = conn.cursor()
        c.execute('''SELECT COUNT(*) FROM messages''')
        assert c.fetchall()[0][0] == 1
