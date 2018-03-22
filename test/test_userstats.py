import unittest
from heimdall import Heimdall

class TestUserStats(unittest.TestCase):
    def setUp(self):
        self.heimdall = Heimdall('test_data', verbose=False)
        self.heimdall.connect_to_database()

    def test_userstats(self):
        recvd = [line.replace("\t","") for line in self.heimdall.get_user_stats('Pouncy Silverkitten').split('\n') if not line.startswith("First Message Date:")][1:]
        self.expcd = [  "User:Pouncy Silverkitten",
                        "Messages:117",
                        "Messages Sent Today:0",
                        "First Message:'https://www.google.com','http://insecure.com', 'https://mail.google.com','http://sub.insecure.com','www.insecure.com','sub.insecure.com','google.com/test.html','imgur.com/vKIVbMz','https://imgur.com/vKIVbMz'",
                        "Most Recent Message:2018-03-18",
                        "Average Messages/Day:19",
                        "Busiest Day:2018-03-13, with 90 messages",
                        "Ranking:1 of 28.",
                        "url_goes_here url_goes_here"]

        for i in range(len(recvd)):
            assert recvd[i] == self.expcd[i]

    def test_addusername(self):
        assert self.heimdall.get_user_stats('Non-existent User') == "User @Non-existentUser not found."
