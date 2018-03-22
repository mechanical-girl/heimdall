import unittest

from heimdall import Heimdall

class TestRanks(unittest.TestCase):
    def setUp(self):
        self.heimdall = Heimdall('test_data', verbose=False)
        
        self.rank_pairs = [ ['1', 'The user at position 1 is Pouncy Silverkitten'],
                            ['99', 'You requested a position which doesn\'t exist. There have been 28 uniquely-named posters in &test_data.'],
                            ['28', 'The user at position 28 is THSayBot'],
                            ['nine', 'The position you specified was invalid.'],
                            ['0', 'The position you specified was invalid.']]
        self.position_pairs = [ ['PouncySilverkitten', 1],
                                ['Heimdall',12],
                                ['User10', 13],
                                ['Flibbertigibbet', 'unknown'],
                                ['', 'unknown']]

    def test_rank(self):
        self.heimdall.connect_to_database()
        for pair in self.rank_pairs:
            assert self.heimdall.get_user_at_position(pair[0]) == pair[1]

    def test_position(self):
        self.heimdall.connect_to_database()
        for pair in self.position_pairs:
            assert self.heimdall.get_position(pair[0]) == pair[1]
