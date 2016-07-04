import unittest
from decimal import Decimal

from Athena.apis.database_api import RedisAPI
from Athena.settings import AthenaConfig

__author__ = 'zed'


class TestRedisAPI(unittest.TestCase):

    """
    Test Redis API.
    """
    def setUp(self):
        """
        set up the redis api.
        :return:
        """
        self.redis_api = RedisAPI(db=0)

    def test_zipped_dict(self):
        """
        Test zipping one tick record with SQL table header to
        obtain a dictionary record that can be inserted to Redis.
        :return:
        """
        tick_record = ('au1606', '2015-08-06', '09:00:00.0000000',
                       '2015-08-06 09:00:00.000', 'QUOTE', 'ASK',
                       Decimal('221.30'), 5, Decimal('0.00'))
        header = AthenaConfig.ATHENA_SQL_TABLE_HEADERS_TICK
        # make dictionary.
        d1 = dict(zip(header, tick_record))
        self.assertEqual(d1['VOLUME'], 5)
        self.assertEqual(d1['DATE'], '2015-08-06')
        self.assertEqual(d1['TIME'], '09:00:00.0000000')
        self.assertEqual(d1['PRICE'], Decimal('221.30'))
        self.assertEqual(d1['LOCAL_TIME'], '2015-08-06 09:00:00.000')
        self.assertEqual(d1['VALUE'], Decimal('0.00'))
        self.assertEqual(d1['MD_TYPE'], 'QUOTE')
        self.assertEqual(d1['MD_SUBTYPE'], 'ASK')
        self.assertEqual(d1['SECURITY'], 'au1606')

    def test_redis_set_data(self):
        """
        test setting one hash set in redis with a dict.
        :return:
        """
        str_dict = {'VALUE': '0.00',
                    'MD_SUBTYPE': 'ASK',
                    'MD_TYPE': 'QUOTE',
                    'LOCAL_TIME': '2015-08-06 09:00:00.000',
                    'PRICE': '221.30',
                    'DATE': '2015-08-06',
                    'SECURITY': 'au1606',
                    'VOLUME': '5',
                    'TIME': '09:00:00.0000000'}

        tick_record = ('au1606', '2015-08-06', '09:00:00.0000000',
                       '2015-08-06 09:00:00.000', 'QUOTE', 'ASK',
                       Decimal('221.30'), 5, Decimal('0.00'))

        header = AthenaConfig.ATHENA_SQL_TABLE_HEADERS_TICK
        # make dictionary.
        d1 = dict(zip(header, tick_record))
        # the key
        k1 = 'au1606:0'
        # insert dictionary
        self.redis_api.set_dict(k1, d1)

        # get the same dictionary
        d2 = self.redis_api.get_dict(k1)
        self.assertEqual(d2, str_dict)

        # clean-up.
        self.redis_api.flush_all()

    def test_redis_get_batch(self):
        """
        get keys of a batch of data
        :return:
        """
        # insert some data
        self.redis_api.set_dict('md_backtest:0', {'price': 1})
        self.redis_api.set_dict('md_backtest:1', {'price': 3})
        self.redis_api.set_dict('md_backtest:2', {'price': 4})
        self.redis_api.set_dict('md_backtest:3', {'price': 5})
        self.redis_api.set_dict('md_other:0', {'price': 2})

        # get keys
        keys = self.redis_api.get_keys('md_backtest:*')
        keys = sorted(keys)
        self.assertEqual(self.redis_api.get_dict(keys[1]), {'price': '3'})
        self.assertEqual(self.redis_api.get_dict(keys[0]), {'price': '1'})
        self.assertEqual(self.redis_api.get_dict(keys[2]), {'price': '4'})

if __name__ == '__main__':
    unittest.main()